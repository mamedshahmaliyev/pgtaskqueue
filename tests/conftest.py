"""Fixtures. The tests run against a real PostgreSQL — the queue *is* its SQL, so a fake
database would test nothing.

The database used is `TQ_TEST_DATABASE_URL`, or `TQ_DATABASE_URL` with `_test` appended to
the database name. It is created if missing, migrated once per run, and emptied before each
test.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import asyncpg
import pytest
from sqlalchemy import make_url, text

from pgtaskqueue import Archive, Database, Registry, Settings, TaskQueue, migrate
from pgtaskqueue.archive import open_archive_database
from tests.helpers import (
    TEST_ARCHIVE_DATABASE_URL,
    TEST_DATABASE_URL,
    bearer,
    make_settings,
    running_worker,
    serving,
)


async def _ensure_database(database_url: str = TEST_DATABASE_URL) -> None:
    url = make_url(database_url)
    conn = await asyncpg.connect(
        host=url.host,
        port=url.port,
        user=url.username,
        password=url.password,
        database="postgres",
        statement_cache_size=0,  # so this also works through a transaction-mode pooler
    )
    try:
        exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", url.database)
        if not exists:
            await conn.execute(f'CREATE DATABASE "{url.database}"')
    finally:
        await conn.close()


@pytest.fixture(scope="session")
async def _schema() -> None:
    await _ensure_database()
    await asyncio.to_thread(migrate.upgrade, TEST_DATABASE_URL)


@pytest.fixture(scope="session")
def settings() -> Settings:
    return make_settings()


@pytest.fixture(scope="session")
async def db(_schema: None, settings: Settings) -> AsyncIterator[Database]:
    database = Database(settings=settings, application_name="tq-tests")
    yield database
    await database.dispose()


@pytest.fixture(autouse=True)
async def _clean(db: Database) -> None:
    async with db.transaction() as tx:
        await tx.execute(
            text(
                "TRUNCATE tq_tasks, tq_task_logs, tq_task_types, tq_queues, tq_workers, "
                "tq_users, tq_schedules"
            )
        )


@pytest.fixture
def queue(db: Database, settings: Settings) -> TaskQueue:
    return TaskQueue(db, settings)


@pytest.fixture(scope="session")
async def archive_db(settings: Settings) -> AsyncIterator[Database]:
    """The archive's database, created and migrated on first use. Only the tests that ask
    for it need it to exist."""
    await _ensure_database(TEST_ARCHIVE_DATABASE_URL)
    await asyncio.to_thread(migrate.upgrade_archive, TEST_ARCHIVE_DATABASE_URL)
    database = open_archive_database(
        settings.model_copy(update={"archive_database_url": TEST_ARCHIVE_DATABASE_URL}),
        "tq-tests-archive",
    )
    yield database
    await database.dispose()


@pytest.fixture
async def archive(db: Database, archive_db: Database, settings: Settings) -> Archive:
    """An `Archive` between the test database and an emptied archive database."""
    async with archive_db.transaction() as tx:
        await tx.execute(text("TRUNCATE tq_archive_tasks, tq_archive_task_logs"))
    return Archive(db, archive_db, settings)


@pytest.fixture
def registry() -> Registry:
    """A registry of its own per test, so an executor defined in one test is never claimed
    by a worker started in another."""
    return Registry()


@pytest.fixture
def worker_factory(db: Database, registry: Registry, settings: Settings):
    """`async with worker_factory(slots=3) as worker:` — a worker over the test's registry."""

    def factory(**kwargs: Any):
        kwargs.setdefault("settings", settings)
        return running_worker(db, registry, **kwargs)

    return factory


@pytest.fixture
async def api(db: Database, settings: Settings) -> AsyncIterator[Any]:
    """An `httpx.AsyncClient` talking to the API in-process."""
    async with serving(db, settings) as client:
        yield client


@pytest.fixture
def auth(settings: Settings):
    """`auth("alice", "submitter", task_names=["mail.*"])` → headers for that account."""

    def headers(username: str, role: str = "submitter", **acl: Any) -> dict[str, str]:
        return bearer(settings, username, role, **acl)

    return headers
