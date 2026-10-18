"""The HTTP API: `create_app()` builds it, `tq api` serves it.

    /v1/auth/*       tokens and accounts
    /v1/tasks/*      enqueue, read, cancel, requeue, edit
    /v1/schedules/*  periodic tasks
    /v1/queues, /v1/workers, /v1/stats, /v1/task-types
    /v1/worker/*     the remote-worker protocol
    /mcp             the same operations as MCP tools
    /ui              the web UI: dashboard, tasks, statistics
    /health/*        liveness and readiness (no token)
    /docs            OpenAPI

The API runs no tasks and imports no executors. It needs the database and nothing else, so
it can be scaled, restarted and deployed separately from the workers. Redis, if
TQ_REDIS_URL is set, holds request rate limits and nothing more (ratelimit.py).
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse

from pgtaskqueue._version import __version__
from pgtaskqueue.api.deps import AppState
from pgtaskqueue.api.ratelimit import (
    LimiterUnavailable,
    RateLimited,
    RateLimiter,
    RateLimitMiddleware,
)
from pgtaskqueue.api.routes import auth, schedules, tasks, worker
from pgtaskqueue.api.routes.auth import OAuthError, oauth_error_response
from pgtaskqueue.api.service import ApiError, Service
from pgtaskqueue.auth.tokens import TokenService
from pgtaskqueue.auth.users import UserService
from pgtaskqueue.db import Database
from pgtaskqueue.settings import Settings, get_settings

logger = logging.getLogger("pgtaskqueue.api")

DESCRIPTION = """\
A PostgreSQL-backed task queue. Get a token from `POST /v1/auth/token`, then send it as
`Authorization: Bearer <token>`. What a token may do depends on its account's role and ACL
— see `GET /v1/auth/me`.
"""


def create_app(settings: Settings | None = None, db: Database | None = None) -> FastAPI:
    settings = settings or get_settings()
    # Built here rather than in the lifespan so a signing key that cannot work stops the
    # process at start-up, with the reason, instead of failing the first login.
    tokens = TokenService(settings)
    owns_db = db is None

    mcp = None
    if settings.mcp_enabled:
        from pgtaskqueue.api.mcp import MCP_PATH, BearerGate, build_mcp, transport_security

        mcp = build_mcp(lambda: app.state.tq)
        mcp_app = mcp.streamable_http_app(
            streamable_http_path=MCP_PATH,
            # One JSON answer per request and no session kept between them: the endpoint
            # behaves like any other stateless route behind a load balancer.
            json_response=True,
            stateless_http=True,
            transport_security=transport_security(settings),
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        database = db or Database(settings=settings, application_name="tq-api")
        limiter = RateLimiter.from_settings(settings)
        app.state.tq = AppState(
            settings=settings,
            db=database,
            service=Service(database, settings),
            tokens=tokens,
            users=UserService(database),
            limiter=limiter,
        )
        async with AsyncExitStack() as stack:
            if mcp is not None:
                # A mounted app's own lifespan is not run by the app it is mounted in.
                await stack.enter_async_context(mcp.session_manager.run())
            try:
                yield
            finally:
                if limiter is not None:
                    await limiter.close()
                if owns_db:
                    await database.dispose()

    app = FastAPI(
        title="Task Queue",
        version=__version__,
        description=DESCRIPTION,
        lifespan=lifespan,
        swagger_ui_parameters={"persistAuthorization": True},
    )

    # Added before CORS, so it runs inside it: a 429 still carries the CORS headers a
    # browser needs to read it.
    app.add_middleware(RateLimitMiddleware, get_state=lambda: app.state.tq)

    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_methods=["*"],
            allow_headers=[
                "Authorization",
                "Content-Type",
                "Mcp-Session-Id",
                "Mcp-Protocol-Version",
            ],
        )

    @app.exception_handler(ApiError)
    async def _api_error(_: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse({"detail": exc.detail}, status_code=exc.status, headers=exc.headers)

    @app.exception_handler(RateLimited)
    async def _rate_limited(_: Request, exc: RateLimited) -> JSONResponse:
        return JSONResponse(
            {"detail": str(exc)}, status_code=429, headers={"Retry-After": str(exc.retry_after)}
        )

    @app.exception_handler(LimiterUnavailable)
    async def _limiter_unavailable(_: Request, exc: LimiterUnavailable) -> JSONResponse:
        return JSONResponse(
            {"detail": "the rate limiter is unavailable. Try again shortly."},
            status_code=503,
            headers={"Retry-After": "5"},
        )

    @app.exception_handler(OAuthError)
    async def _oauth_error(_: Request, exc: OAuthError) -> JSONResponse:
        return oauth_error_response(exc)

    @app.get("/health/live", tags=["health"], include_in_schema=False)
    async def live() -> dict[str, str]:
        """The process is up. Says nothing about the database."""
        return {"status": "ok"}

    @app.get("/health/ready", tags=["health"])
    async def ready(request: Request) -> JSONResponse:
        """The database answers. What a load balancer should check."""
        try:
            await request.app.state.tq.db.ping()
        except Exception as exc:
            logger.warning("readiness check failed: %s", exc)
            return JSONResponse({"status": "unavailable"}, status_code=503)
        return JSONResponse({"status": "ok", "version": __version__})

    for router in (auth.router, tasks.router, schedules.router, worker.router):
        app.include_router(router, prefix="/v1")

    if settings.ui_enabled:
        # Imported here, so an API with the UI off never loads the template engine.
        from pgtaskqueue.api import ui

        app.include_router(ui.router)
        app.mount("/ui/static", ui.Static(), name="ui-static")

        @app.get("/", include_in_schema=False)
        async def _home(request: Request) -> RedirectResponse:
            return RedirectResponse(f"{request.scope.get('root_path', '').rstrip('/')}/ui")

    if mcp is not None:
        # Mounted at the root with its route at /mcp, so the endpoint is exactly `/mcp`:
        # mounting at "/mcp" would make it "/mcp/" and answer the bare path with a redirect
        # that not every client follows for a POST. Registered last, so it only sees what
        # no route above matched.
        app.mount("/", BearerGate(mcp_app, lambda: app.state.tq))

    return app
