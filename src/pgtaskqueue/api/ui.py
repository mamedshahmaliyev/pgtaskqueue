"""The web UI: a few server-rendered pages at `/ui`, and the browser session behind them.

    /ui                 dashboard
    /ui/tasks           tasks: filter, enqueue, cancel, kill, restart
    /ui/tasks/{id}      one task and its log
    /ui/stats           statistics
    /ui/users           accounts: roles, ACLs, limits (for `users:manage`)
    /ui/login           sign in (POST), /ui/logout, /ui/session/refresh

The pages are shells. Everything they show is fetched by their JavaScript from the same
`/v1` routes any other client uses, so the UI can do exactly what its account's role and ACL
allow and nothing here decides a permission.

A session is the pair of tokens `POST /v1/auth/token` would have returned, kept in HttpOnly
cookies instead of being handed to the page. docs/ui.md has the reasoning.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import parse_qs, quote

from fastapi import APIRouter, Form, Request, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.types import Scope

from pgtaskqueue._version import __version__
from pgtaskqueue.api import assets
from pgtaskqueue.api.deps import ACCESS_COOKIE, UI_HEADER, AppState, State
from pgtaskqueue.api.ratelimit import RateLimited
from pgtaskqueue.api.routes.auth import OAuthError, refreshed_account, sign_in
from pgtaskqueue.auth.principal import Principal
from pgtaskqueue.auth.tokens import TokenError
from pgtaskqueue.models import User

HERE = Path(__file__).parent
REFRESH_COOKIE = "tq_refresh"

router = APIRouter(prefix="/ui", include_in_schema=False)
templates = Jinja2Templates(directory=HERE / "templates")

# Scripts and styles only from this origin, and none inline: text that reaches a page
# through a task's payload or log cannot become code. `frame-ancestors` stops another site
# drawing the UI inside a frame to have its buttons clicked.
_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; "
        "form-action 'self'; frame-ancestors 'none'"
    ),
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "same-origin",
    "Cache-Control": "no-store",
}


class Static(StaticFiles):
    """`static/`. A file asked for with the current `?v=` never changes, so a browser may
    keep it a year without asking. Any other version, or none, is served too, but must be
    checked again each time: during a rolling deploy a page from an older API can ask a
    newer one for its files.
    """

    def __init__(self) -> None:
        super().__init__(directory=assets.STATIC_DIR)

    async def get_response(self, path: str, scope: Scope) -> Response:
        response = await super().get_response(path, scope)
        current = parse_qs(scope["query_string"].decode()).get("v") == [assets.version()]
        response.headers["Cache-Control"] = (
            "public, max-age=31536000, immutable" if current else "no-cache"
        )
        return response


@dataclass(slots=True)
class _Session:
    who: Principal
    # Set when the access token had expired and the refresh token bought a new pair, which
    # the response must then carry.
    renewed: User | None = None
    scope: str | None = None


def _root(request: Request) -> str:
    """The prefix the application is served under, when a proxy strips one."""
    return request.scope.get("root_path", "").rstrip("/")


def _set_cookies(
    request: Request, response: Response, state: AppState, user: User, scope: str | None
) -> None:
    access = state.tokens.issue_access(user, scope)
    refresh = state.tokens.issue_refresh(user, scope)
    root = _root(request)
    common: dict[str, Any] = {
        "httponly": True,
        # Lax: not sent with another site's POST or fetch, but sent when a link to the UI
        # is followed, so a bookmark does not land on the login page.
        "samesite": "lax",
        "secure": request.url.scheme == "https",
    }
    response.set_cookie(
        ACCESS_COOKIE, access.token, max_age=access.expires_in, path=root or "/", **common
    )
    # Only the UI's own routes ever see the refresh token.
    response.set_cookie(
        REFRESH_COOKIE, refresh.token, max_age=refresh.expires_in, path=f"{root}/ui", **common
    )


def _clear_cookies(request: Request, response: Response) -> None:
    root = _root(request)
    response.delete_cookie(ACCESS_COOKIE, path=root or "/")
    response.delete_cookie(REFRESH_COOKIE, path=f"{root}/ui")


async def _session(request: Request, state: AppState) -> _Session | None:
    """Who the browser is signed in as, renewing the access token if it has run out."""
    if token := request.cookies.get(ACCESS_COOKIE):
        try:
            return _Session(state.tokens.principal(token))
        except TokenError:
            pass
    refresh = request.cookies.get(REFRESH_COOKIE)
    if not refresh or not state.tokens.can_issue:
        return None
    try:
        user, claims = await refreshed_account(state, refresh)
    except OAuthError:
        return None
    scope = claims.get("scope")
    who = state.tokens.principal(state.tokens.issue_access(user, scope).token)
    return _Session(who, renewed=user, scope=scope)


def _next(request: Request, target: str | None) -> str:
    """Where to go after signing in: a page of the UI, never an address someone put in a
    link to send the user elsewhere."""
    home = f"{_root(request)}/ui"
    if target and (target == home or target.startswith(f"{home}/")) and "\\" not in target:
        return target
    return home


def _render(
    request: Request,
    template: str,
    status_code: int = 200,
    headers: dict[str, str] | None = None,
    **context: Any,
) -> Response:
    return templates.TemplateResponse(
        request,
        template,
        {
            "root": _root(request),
            "static_version": assets.version(),
            "version": __version__,
            **context,
        },
        status_code=status_code,
        headers={**_HEADERS, **(headers or {})},
    )


async def _page(request: Request, state: AppState, template: str, **context: Any) -> Response:
    session = await _session(request, state)
    if session is None:
        here = request.url.path + (f"?{request.url.query}" if request.url.query else "")
        return RedirectResponse(
            f"{_root(request)}/ui/login?next={quote(_root(request) + here, safe='')}",
            status.HTTP_303_SEE_OTHER,
        )
    who = session.who
    me = {"subject": who.subject, "role": who.role, "permissions": sorted(who.permissions)}
    response = _render(request, template, me=me, **context)
    if session.renewed is not None:
        _set_cookies(request, response, state, session.renewed, session.scope)
    return response


# --------------------------------------------------------------------------- session


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, state: State, next: str | None = None) -> Response:
    if await _session(request, state) is not None:
        return RedirectResponse(_next(request, next), status.HTTP_303_SEE_OTHER)
    return _render(
        request, "login.html", next=_next(request, next), can_issue=state.tokens.can_issue
    )


@router.post("/login", response_class=HTMLResponse)
async def login(
    request: Request,
    state: State,
    username: Annotated[str, Form()] = "",
    password: Annotated[str, Form()] = "",
    next: Annotated[str | None, Form()] = None,
) -> Response:
    user = None
    if state.tokens.can_issue and username and password:
        try:
            user = await sign_in(request, state, username, password, kind="user")
        except RateLimited as exc:
            return _render(
                request,
                "login.html",
                status.HTTP_429_TOO_MANY_REQUESTS,
                {"Retry-After": str(exc.retry_after)},
                next=_next(request, next),
                can_issue=state.tokens.can_issue,
                username=username,
                error=f"Too many sign-in attempts. Try again in {exc.retry_after} s.",
            )
    if user is None:
        return _render(
            request,
            "login.html",
            status.HTTP_401_UNAUTHORIZED,
            next=_next(request, next),
            can_issue=state.tokens.can_issue,
            username=username,
            error="Wrong username or password.",
        )
    response = RedirectResponse(_next(request, next), status.HTTP_303_SEE_OTHER)
    _set_cookies(request, response, state, user, None)
    return response


@router.post("/session/refresh")
async def refresh(request: Request, state: State) -> Response:
    """A new pair of cookies for a page whose access token ran out while it was open."""
    token = request.cookies.get(REFRESH_COOKIE)
    if UI_HEADER not in request.headers or not token or not state.tokens.can_issue:
        return Response(status_code=status.HTTP_401_UNAUTHORIZED)
    try:
        user, claims = await refreshed_account(state, token)
    except OAuthError:
        response = Response(status_code=status.HTTP_401_UNAUTHORIZED)
        _clear_cookies(request, response)
        return response
    response = Response(status_code=status.HTTP_204_NO_CONTENT)
    _set_cookies(request, response, state, user, claims.get("scope"))
    return response


@router.post("/logout")
async def logout(request: Request, state: State) -> Response:
    """End the session. Like `POST /v1/auth/logout`, this revokes every refresh token the
    account holds, so a copy of the cookie is worth nothing afterwards."""
    session = await _session(request, state)
    if session is not None:
        await state.users.revoke_tokens(session.who.subject)
    response = RedirectResponse(f"{_root(request)}/ui/login", status.HTTP_303_SEE_OTHER)
    _clear_cookies(request, response)
    return response


# --------------------------------------------------------------------------- pages


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, state: State) -> Response:
    return await _page(request, state, "dashboard.html", page="dashboard")


@router.get("/tasks", response_class=HTMLResponse)
async def tasks(request: Request, state: State) -> Response:
    return await _page(request, state, "tasks.html", page="tasks")


@router.get("/tasks/{task_id}", response_class=HTMLResponse)
async def task(task_id: uuid.UUID, request: Request, state: State) -> Response:
    return await _page(request, state, "task.html", page="tasks", task_id=str(task_id))


@router.get("/stats", response_class=HTMLResponse)
async def stats(request: Request, state: State) -> Response:
    return await _page(request, state, "stats.html", page="stats")


@router.get("/users", response_class=HTMLResponse)
async def users(request: Request, state: State) -> Response:
    return await _page(request, state, "users.html", page="users")
