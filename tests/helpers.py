"""What the tests share that is not a fixture."""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
import uuid
from collections.abc import AsyncIterator, Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import make_url

from pgtaskqueue import Database, Registry, Settings, Task, TaskQueue, Worker
from pgtaskqueue.enums import TERMINAL_STATUSES
from pgtaskqueue.settings import get_settings


def _test_database_url() -> str:
    if url := os.environ.get("TQ_TEST_DATABASE_URL"):
        return url
    base = make_url(get_settings().database_url)
    return base.set(database=f"{base.database}_test").render_as_string(hide_password=False)


TEST_DATABASE_URL = _test_database_url()


def _test_archive_database_url() -> str:
    if url := os.environ.get("TQ_TEST_ARCHIVE_DATABASE_URL"):
        return url
    base = make_url(TEST_DATABASE_URL)
    return base.set(database=f"{base.database}_archive").render_as_string(hide_password=False)


# A database of its own, as the archive usually is, so the tests see the two kept apart.
TEST_ARCHIVE_DATABASE_URL = _test_archive_database_url()
JWT_SECRET = "test-secret-" + "x" * 40
# The compose Redis, database 15. test_ratelimit.py empties it before each test, so it must
# not be a Redis anything else uses.
TEST_REDIS_URL = os.environ.get("TQ_TEST_REDIS_URL", "redis://localhost:63790/15")


def make_settings(**overrides: Any) -> Settings:
    """Settings with every interval shrunk, so a test of "the heartbeat notices within one
    interval" takes a fraction of a second rather than several."""
    values: dict[str, Any] = {
        "database_url": TEST_DATABASE_URL,
        "db_pool_size": 20,
        "db_max_overflow": 20,
        "poll_interval_seconds": 0.2,
        "heartbeat_interval_seconds": 0.2,
        "stale_after_seconds": 1.0,
        "promote_interval_seconds": 0.1,
        "schedule_interval_seconds": 0.1,
        "maintenance_interval_seconds": 0.3,
        "log_flush_interval_seconds": 0.05,
        "retry_backoff_base_seconds": 0.05,
        "retry_backoff_jitter": 0.0,
        "worker_shutdown_grace_seconds": 3.0,
        "webhook_allow_private": True,
        "webhook_timeout_seconds": 3.0,
        "jwt_secret": JWT_SECRET,
        "long_poll_max_seconds": 5.0,
        # No rate limits unless a test asks for them, whatever the environment says.
        "redis_url": None,
    }
    values.update(overrides)
    # _env_file=None: a developer's .env must not leak into what the tests assert.
    return Settings(_env_file=None, **values)


async def wait_for(
    queue: TaskQueue,
    task_id: uuid.UUID,
    statuses: Sequence[str] = tuple(TERMINAL_STATUSES),
    timeout: float = 10.0,
) -> Task:
    """Poll until the task is in one of `statuses`. Fails the test with what it saw."""
    deadline = time.monotonic() + timeout
    task = await queue.get(task_id)
    while task is None or task.status not in statuses:
        if time.monotonic() > deadline:
            raise AssertionError(f"task still {task and task.status!r} after {timeout}s: {task}")
        await asyncio.sleep(0.02)
        task = await queue.get(task_id)
    return task


async def until(
    condition: Callable[[], Any], timeout: float = 10.0, what: str = "the condition"
) -> None:
    """Poll until `condition()` (plain or awaitable) is truthy."""
    deadline = time.monotonic() + timeout
    while True:
        result = condition()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for {what}")
        await asyncio.sleep(0.02)


@contextlib.asynccontextmanager
async def running_worker(
    db: Database, registry: Registry, settings: Settings, **kwargs: Any
) -> AsyncIterator[Worker]:
    """A worker running in the background for the length of the block. Leaving the block
    stops it gracefully, which also flushes the logs of what it ran."""
    worker = Worker(db, registry=registry, settings=settings, handle_signals=False, **kwargs)
    runner = asyncio.create_task(worker.run())
    try:
        await until(lambda: worker.pools or runner.done(), what="the worker to start")
        if runner.done():
            runner.result()
        yield worker
    finally:
        worker.stop()
        await asyncio.wait_for(runner, timeout=20)


class Gauge:
    """How many executors are inside `held()` right now, and the most there ever were."""

    def __init__(self) -> None:
        self.now = 0
        self.peak = 0

    @contextlib.contextmanager
    def held(self) -> Iterator[None]:
        self.now += 1
        self.peak = max(self.peak, self.now)
        try:
            yield
        finally:
            self.now -= 1


@dataclass(slots=True)
class Received:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes


class Receiver:
    """A minimal HTTP server that records what it is sent — the far end of a webhook.

    Answers with `statuses` in turn, then 200.
    """

    def __init__(self, statuses: Iterable[int] = (), headers: dict[str, str] | None = None):
        self.requests: list[Received] = []
        self._statuses = list(statuses)
        self._headers = headers or {}
        self._server: asyncio.Server | None = None
        self.port = 0

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/hook"

    async def __aenter__(self) -> Receiver:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc: object) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1")
            request_line, *lines = head.split("\r\n")
            method, path, _ = request_line.split(" ", 2)
            headers = {
                key.strip().lower(): value.strip()
                for key, _, value in (line.partition(":") for line in lines if line)
            }
            body = await reader.readexactly(int(headers.get("content-length", 0)))
            self.requests.append(Received(method, path, headers, body))
            status = self._statuses.pop(0) if self._statuses else 200
            extra = "".join(f"{key}: {value}\r\n" for key, value in self._headers.items())
            head = f"HTTP/1.1 {status} X\r\nContent-Length: 0\r\nConnection: close\r\n"
            writer.write(f"{head}{extra}\r\n".encode())
            await writer.drain()
        finally:
            writer.close()


def bearer(
    settings: Settings,
    username: str,
    role: str = "submitter",
    *,
    queues: Sequence[str] = ("*",),
    task_names: Sequence[str] = ("*",),
    scope: str | None = None,
) -> dict[str, str]:
    """An Authorization header for an account with this role and ACL.

    Minted directly, without a login: access tokens are self-contained, so the API cannot
    tell the difference — and a test of permissions does not pay for a password hash.
    """
    from pgtaskqueue.auth.tokens import TokenService
    from pgtaskqueue.models import User

    user = User(
        username=username,
        kind="user",
        role=role,
        queues=list(queues),
        task_names=list(task_names),
        password_hash="",
        token_version=0,
    )
    return {"Authorization": f"Bearer {TokenService(settings).issue_access(user, scope).token}"}


@contextlib.asynccontextmanager
async def serving(db: Database, settings: Settings) -> AsyncIterator[Any]:
    """The API with its lifespan running, and an HTTP client wired straight to it.

    The lifespan is entered and left inside one task of its own: the MCP session manager
    it starts holds an anyio task group, which must be closed by the task that opened it.
    """
    import httpx

    from pgtaskqueue.api.app import create_app

    app = create_app(settings, db)
    up, down = asyncio.Event(), asyncio.Event()

    async def lifespan() -> None:
        async with app.router.lifespan_context(app):
            up.set()
            await down.wait()

    runner = asyncio.create_task(lifespan())
    await until(lambda: up.is_set() or runner.done(), what="the API to start")
    if runner.done():
        runner.result()
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://api", timeout=30
        ) as client:
            client.app = app  # type: ignore[attr-defined]
            yield client
    finally:
        down.set()
        await runner
