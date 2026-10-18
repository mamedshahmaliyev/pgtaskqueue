"""The async engine and its sessions.

A `Database` is cheap to describe and expensive to open, so it is created once per process
and passed to whatever needs it (`TaskQueue`, `Monitor`, the API's dependencies).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from pgtaskqueue.settings import Settings, get_settings


def connect_args(application_name: str, *, pgbouncer: bool = False) -> dict[str, Any]:
    """Driver arguments for one connection. `pgbouncer` is for a transaction-mode pooler."""
    args: dict[str, Any] = {"server_settings": {"application_name": application_name}}
    if pgbouncer:
        # A prepared statement belongs to one server connection, and behind a transaction-mode
        # pooler the next transaction may be on another. So: cache none, in the driver or in
        # SQLAlchemy, and name each one uniquely, because the names asyncpg would generate
        # (a counter per client connection) collide when two clients share a server.
        args["statement_cache_size"] = 0
        args["prepared_statement_cache_size"] = 0
        args["prepared_statement_name_func"] = lambda: f"__asyncpg_{uuid4()}__"
    return args


class Database:
    def __init__(
        self,
        url: str | None = None,
        *,
        pool_size: int | None = None,
        max_overflow: int | None = None,
        echo: bool | None = None,
        application_name: str = "pgtaskqueue",
        pgbouncer: bool | None = None,
        listen_url: str | None = None,
        settings: Settings | None = None,
    ) -> None:
        settings = settings or get_settings()
        self.url = url or settings.database_url
        self.pgbouncer = settings.db_pgbouncer if pgbouncer is None else pgbouncer
        self.listen_url = listen_url or settings.listen_database_url
        self.engine: AsyncEngine = create_async_engine(
            self.url,
            pool_size=pool_size if pool_size is not None else settings.db_pool_size,
            max_overflow=max_overflow if max_overflow is not None else settings.db_max_overflow,
            echo=settings.db_echo if echo is None else echo,
            pool_pre_ping=settings.db_pool_pre_ping,
            connect_args=connect_args(application_name, pgbouncer=self.pgbouncer),
        )
        # expire_on_commit=False: queue methods hand back rows after their transaction has
        # ended, and those must stay readable without a session behind them.
        self.sessionmaker = async_sessionmaker(self.engine, expire_on_commit=False)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """A session inside one transaction: committed on exit, rolled back on error."""
        async with self.sessionmaker() as session, session.begin():
            yield session

    @property
    def can_listen(self) -> bool:
        """Whether `dedicated_connection` gives a session that stays on one server
        connection: always, except through a transaction-mode pooler with no direct URL."""
        return not self.pgbouncer or self.listen_url is not None

    @asynccontextmanager
    async def dedicated_connection(self, application_name: str = "pgtaskqueue-listen"):
        """An asyncpg connection of its own, outside the pool, closed on exit.

        For the two things a pooled connection cannot do: LISTEN, which needs a connection
        that stays put, and COPY. It is opened through the same driver as everything else,
        so whatever makes the pool connect (TLS, a socket path) makes this connect too. It
        goes to `listen_url` when there is one, which is how it gets past a pooler.
        """
        engine = create_async_engine(
            self.listen_url or self.url,
            poolclass=NullPool,
            connect_args=connect_args(
                application_name, pgbouncer=self.pgbouncer and self.listen_url is None
            ),
        )
        try:
            async with engine.connect() as conn:
                raw = await conn.get_raw_connection()
                yield raw.driver_connection
        finally:
            await engine.dispose()

    async def ping(self) -> None:
        async with self.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    async def dispose(self) -> None:
        await self.engine.dispose()
