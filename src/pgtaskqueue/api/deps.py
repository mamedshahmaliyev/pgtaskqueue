from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer

from pgtaskqueue.api.service import Service
from pgtaskqueue.auth.principal import Principal
from pgtaskqueue.auth.tokens import TokenError, TokenService
from pgtaskqueue.auth.users import UserService
from pgtaskqueue.db import Database
from pgtaskqueue.settings import Settings

if TYPE_CHECKING:
    from pgtaskqueue.api.ratelimit import RateLimiter


@dataclass(slots=True)
class AppState:
    """Everything the routes share, built once in the app's lifespan."""

    settings: Settings
    db: Database
    service: Service
    tokens: TokenService
    users: UserService
    # None when TQ_REDIS_URL is unset: no request rate limits.
    limiter: RateLimiter | None = None


def get_state(request: Request) -> AppState:
    return request.app.state.tq


# Declared as the password flow so the "Authorize" button in /docs can sign in; any bearer
# token is accepted, however it was obtained.
_bearer = OAuth2PasswordBearer(tokenUrl="/v1/auth/token", auto_error=False)


# The web UI's session: the same access token a client would send as a bearer, kept where
# the page's JavaScript cannot read it.
ACCESS_COOKIE = "tq_access"
# A request authenticated by the cookie must carry this header to change anything. A page
# on another origin cannot add a header to a request without a CORS preflight, and the
# preflight is refused: the header is not in the allowed list.
UI_HEADER = "x-tq-ui"
_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


def unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status.HTTP_401_UNAUTHORIZED,
        detail,
        headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
    )


def get_principal(
    request: Request,
    state: Annotated[AppState, Depends(get_state)],
    token: Annotated[str | None, Depends(_bearer)],
) -> Principal:
    if not token and state.settings.ui_enabled:
        # A bearer token wins when both are sent, so a script run from a browser session
        # acts as the account it names.
        token = request.cookies.get(ACCESS_COOKIE)
        if token and request.method not in _SAFE_METHODS and UI_HEADER not in request.headers:
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                "a request authenticated by the session cookie must send the X-TQ-UI header",
            )
    if not token:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        return state.tokens.principal(token)
    except TokenError as exc:
        raise unauthorized(str(exc)) from exc


State = Annotated[AppState, Depends(get_state)]
Who = Annotated[Principal, Depends(get_principal)]
