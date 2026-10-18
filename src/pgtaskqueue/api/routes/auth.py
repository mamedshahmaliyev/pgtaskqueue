"""Tokens and accounts.

`POST /v1/auth/token` is an OAuth 2.0 token endpoint (RFC 6749) with three grants:

    password             a person:  username + password            → access + refresh
    client_credentials   a service: client_id + client_secret      → access
    refresh_token        either:    a refresh token                → access + new refresh

`scope` (space-separated permission names) narrows a token below what the account's role
allows — a way to hand a script a token that can only read.
"""

from __future__ import annotations

import base64
import binascii
from typing import Annotated, Any

from fastapi import APIRouter, Form, Request, Response, status
from fastapi.responses import JSONResponse

from pgtaskqueue.api.deps import AppState, State, Who
from pgtaskqueue.api.ratelimit import client_address
from pgtaskqueue.api.schemas import (
    AccountLimitsOut,
    Me,
    PasswordChange,
    RoleOut,
    TokenResponse,
    UserCreate,
    UserOut,
    UserPatch,
)
from pgtaskqueue.auth.tokens import REFRESH, TokenError, narrow_scope
from pgtaskqueue.models import User

router = APIRouter(tags=["auth"])


class OAuthError(Exception):
    """An RFC 6749 §5.2 error response."""

    def __init__(self, error: str, description: str, status_code: int = 400) -> None:
        self.error, self.description, self.status_code = error, description, status_code


def oauth_error_response(exc: OAuthError) -> JSONResponse:
    headers = {"Cache-Control": "no-store"}
    if exc.status_code == 401:
        headers["WWW-Authenticate"] = 'Basic realm="pgtaskqueue"'
    return JSONResponse(
        {"error": exc.error, "error_description": exc.description},
        status_code=exc.status_code,
        headers=headers,
    )


def _basic_credentials(request: Request) -> tuple[str, str] | None:
    header = request.headers.get("authorization", "")
    if not header.lower().startswith("basic "):
        return None
    try:
        username, _, password = base64.b64decode(header[6:]).decode().partition(":")
    except (binascii.Error, UnicodeDecodeError):
        return None
    return username, password


async def refreshed_account(state: AppState, refresh_token: str) -> tuple[User, dict[str, Any]]:
    """The account a refresh token belongs to, and the token's claims, if it still stands."""
    try:
        claims = state.tokens.decode(refresh_token, REFRESH)
    except TokenError as exc:
        raise OAuthError("invalid_grant", str(exc)) from exc
    user = await state.users.get(claims["sub"])
    # The one place a token is checked against the database: a disabled account, a
    # changed password or a "log out everywhere" all show up as a version mismatch.
    if user is None or not user.is_active or user.token_version != claims.get("ver"):
        raise OAuthError("invalid_grant", "the refresh token has been revoked")
    return user, claims


async def sign_in(
    request: Request, state: AppState, name: str, secret: str, *, kind: str
) -> User | None:
    """Check credentials, within the sign-in rate limits: the limits are asked before the
    password is hashed, so a refused attempt costs no Argon2. Raises RateLimited."""
    limiter = state.limiter
    if limiter is not None:
        await limiter.check_login(client_address(request.scope), name)
    user = await state.users.authenticate(name, secret, kind=kind)
    if user is None and limiter is not None:
        await limiter.login_failed(name)
    return user


@router.post("/auth/token", response_model=TokenResponse, response_model_exclude_none=True)
async def token(
    request: Request,
    response: Response,
    state: State,
    grant_type: Annotated[str, Form()],
    username: Annotated[str | None, Form()] = None,
    password: Annotated[str | None, Form()] = None,
    refresh_token: Annotated[str | None, Form()] = None,
    client_id: Annotated[str | None, Form()] = None,
    client_secret: Annotated[str | None, Form()] = None,
    scope: Annotated[str | None, Form()] = None,
) -> TokenResponse:
    if not state.tokens.can_issue:
        raise OAuthError(
            "invalid_request",
            "this server verifies tokens from an external issuer and does not issue its own",
        )
    # RFC 6749 §5.1: a token response must not be cached.
    response.headers["Cache-Control"] = "no-store"
    user: User | None

    if grant_type == "password":
        if not username or not password:
            raise OAuthError("invalid_request", "username and password are required")
        user = await sign_in(request, state, username, password, kind="user")
        if user is None:
            raise OAuthError("invalid_grant", "wrong username or password")
        granted = narrow_scope(user.role, scope)
        access = state.tokens.issue_access(user, granted)
        refresh = state.tokens.issue_refresh(user, granted)
        return TokenResponse(
            access_token=access.token,
            expires_in=access.expires_in,
            refresh_token=refresh.token,
            scope=granted,
        )

    if grant_type == "client_credentials":
        # Credentials in the Authorization header (preferred by the RFC) or in the form.
        credentials = _basic_credentials(request) or (client_id or "", client_secret or "")
        if not all(credentials):
            raise OAuthError("invalid_client", "client credentials are required", 401)
        user = await sign_in(request, state, *credentials, kind="service")
        if user is None:
            raise OAuthError("invalid_client", "unknown client or wrong secret", 401)
        granted = narrow_scope(user.role, scope)
        access = state.tokens.issue_access(user, granted)
        # No refresh token: a service can simply ask again (RFC 6749 §4.4.3).
        return TokenResponse(access_token=access.token, expires_in=access.expires_in, scope=granted)

    if grant_type == "refresh_token":
        if not refresh_token:
            raise OAuthError("invalid_request", "refresh_token is required")
        user, claims = await refreshed_account(state, refresh_token)
        granted = claims.get("scope")
        if scope is not None:
            # May narrow further on refresh; never widen past what was first granted.
            requested = narrow_scope(user.role, scope) or ""
            granted = (
                requested
                if granted is None
                else " ".join(p for p in requested.split() if p in granted.split())
            )
        access = state.tokens.issue_access(user, granted)
        refresh = state.tokens.issue_refresh(user, granted)
        return TokenResponse(
            access_token=access.token,
            expires_in=access.expires_in,
            refresh_token=refresh.token,
            scope=granted,
        )

    raise OAuthError("unsupported_grant_type", f"unsupported grant_type {grant_type!r}")


@router.get("/auth/me", response_model=Me)
async def me(who: Who) -> Me:
    """What the presented token allows."""
    return Me(
        subject=who.subject,
        role=who.role,
        permissions=sorted(who.permissions),
        queues=list(who.queues),
        task_names=list(who.task_names),
    )


@router.post("/auth/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(who: Who, state: State) -> None:
    """Revoke every refresh token of the calling account. Access tokens already issued
    remain valid until they expire, which is at most TQ_ACCESS_TOKEN_MINUTES away."""
    await state.users.revoke_tokens(who.subject)


# --------------------------------------------------------------------------- accounts
#
# Who may manage which account is decided in `Service`; docs/auth.md has the rule.


@router.get("/auth/limits", response_model=AccountLimitsOut)
async def my_limits(who: Who, state: State) -> dict[str, Any]:
    """The calling account's limits, and how much of each it is using."""
    return await state.service.account_limits(who)


@router.get("/roles", response_model=list[RoleOut], tags=["users"])
async def list_roles(who: Who, state: State) -> list[dict[str, Any]]:
    """Every role, its permissions, and whether the caller may give it to an account."""
    return state.service.roles(who)


@router.get("/users", response_model=list[UserOut], tags=["users"])
async def list_users(who: Who, state: State) -> list[UserOut]:
    return [
        UserOut.model_validate(user).model_copy(update={"manageable": manageable})
        for user, manageable in await state.service.accounts(who)
    ]


@router.post("/users", response_model=UserOut, status_code=status.HTTP_201_CREATED, tags=["users"])
async def create_user(body: UserCreate, who: Who, state: State) -> User:
    return await state.service.create_account(who, body)


@router.patch("/users/{username}", response_model=UserOut, tags=["users"])
async def update_user(username: str, body: UserPatch, who: Who, state: State) -> User:
    """Change an account. Only the fields sent change; a limit sent as null goes back to
    the default."""
    return await state.service.update_account(who, username, body)


@router.get("/users/{username}/limits", response_model=AccountLimitsOut, tags=["users"])
async def user_limits(username: str, who: Who, state: State) -> dict[str, Any]:
    return await state.service.account_limits(who, username)


@router.post("/users/{username}/password", status_code=status.HTTP_204_NO_CONTENT, tags=["users"])
async def set_password(username: str, body: PasswordChange, who: Who, state: State) -> None:
    """Anyone may set their own; an account manager, that of any account it manages."""
    await state.service.set_account_password(who, username, body.password)


@router.delete("/users/{username}", status_code=status.HTTP_204_NO_CONTENT, tags=["users"])
async def delete_user(username: str, who: Who, state: State) -> None:
    await state.service.delete_account(who, username)
