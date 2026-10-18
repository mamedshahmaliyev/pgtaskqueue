"""The migrations are what users run; the models are what the code reads. They must agree."""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from asyncio.subprocess import PIPE, STDOUT

import asyncpg
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from pgtaskqueue import migrate
from pgtaskqueue.migrate import ARCHIVE, ARCHIVE_VERSION_TABLE, MAIN, VERSION_TABLE, Track
from pgtaskqueue.models import Base, archive_metadata
from tests.helpers import TEST_DATABASE_URL


def _differences(connection, track: Track) -> list:
    def ours(obj, name, type_, reflected, compare_to) -> bool:
        if type_ == "table":
            return track.owns(name) and name != track.version_table
        return True

    context = MigrationContext.configure(
        connection, opts={"include_object": ours, "compare_type": True}
    )
    return compare_metadata(context, track.metadata())


async def test_models_and_migrations_describe_the_same_schema(db):
    async with db.engine.connect() as connection:
        found = await connection.run_sync(_differences, MAIN)
    assert found == [], "write a migration (or fix the model) for: " + repr(found)


async def test_archive_models_and_migrations_describe_the_same_schema(archive_db):
    """Also what fails when a column is added to tq_tasks without an archive migration:
    the archive's model copies the column, its migrations do not have it."""
    async with archive_db.engine.connect() as connection:
        found = await connection.run_sync(_differences, ARCHIVE)
    assert found == [], "write an archive migration (or fix the model) for: " + repr(found)
    assert set(Base.metadata.tables["tq_tasks"].c.keys()) < set(
        archive_metadata.tables["tq_archive_tasks"].c.keys()
    )


async def test_upgrade_downgrade_upgrade():
    """In a database of its own, next to a table that is not ours."""
    url = make_url(TEST_DATABASE_URL)
    name = f"tq_migrate_{uuid.uuid4().hex[:8]}"
    scratch = url.set(database=name).render_as_string(hide_password=False)

    async def connect(database: str) -> asyncpg.Connection:
        return await asyncpg.connect(
            host=url.host, port=url.port, user=url.username, password=url.password,
            database=database, statement_cache_size=0,
        )  # fmt: skip

    async def tables(conn: asyncpg.Connection) -> set[str]:
        rows = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        return {row["tablename"] for row in rows}

    admin = await connect("postgres")
    await admin.execute(f'CREATE DATABASE "{name}"')
    try:
        conn = await connect(name)
        try:
            # The host application's own table, and its own Alembic history.
            await conn.execute("CREATE TABLE orders (id int primary key)")
            await conn.execute("CREATE TABLE alembic_version (version_num text primary key)")
            await conn.execute("INSERT INTO alembic_version VALUES ('host-app-revision')")

            await asyncio.to_thread(migrate.upgrade, scratch)
            ours = {
                "tq_tasks", "tq_task_logs", "tq_task_types", "tq_queues", "tq_workers",
                "tq_users", "tq_schedules", VERSION_TABLE,
            }  # fmt: skip
            assert await tables(conn) == ours | {"orders", "alembic_version"}

            await asyncio.to_thread(migrate.downgrade, scratch)
            assert await tables(conn) - {VERSION_TABLE} == {"orders", "alembic_version"}
            assert await conn.fetchval("SELECT version_num FROM alembic_version") == (
                "host-app-revision"
            )

            await asyncio.to_thread(migrate.upgrade, scratch)
            assert await tables(conn) == ours | {"orders", "alembic_version"}
            # Running it again is a no-op, which is what makes it safe on every deploy.
            await asyncio.to_thread(migrate.upgrade, scratch)

            # And so is running it from several places at once: they take turns. Separate
            # processes, as replicas deploying together would be (and because Alembic's
            # `context` is a module-level global, which threads in one process would share).
            await asyncio.to_thread(migrate.downgrade, scratch)
            command = [sys.executable, "-m", "pgtaskqueue", "migrate", "--no-bootstrap"]
            environ = {**os.environ, "TQ_DATABASE_URL": scratch}
            runs = [
                await asyncio.create_subprocess_exec(
                    *command, env=environ, stdout=PIPE, stderr=STDOUT
                )
                for _ in range(4)
            ]
            results = await asyncio.wait_for(
                asyncio.gather(*(run.communicate() for run in runs)), timeout=60
            )
            assert [run.returncode for run in runs] == [0] * 4, [out.decode() for out, _ in results]
            assert await tables(conn) == ours | {"orders", "alembic_version"}
        finally:
            await conn.close()
    finally:
        await admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
        await admin.close()


async def test_the_archive_can_share_the_queues_database():
    """Its own tables and version table, so each history leaves the other alone."""
    url = make_url(TEST_DATABASE_URL)
    name = f"tq_migrate_{uuid.uuid4().hex[:8]}"
    scratch = url.set(database=name).render_as_string(hide_password=False)
    admin = await asyncpg.connect(
        host=url.host, port=url.port, user=url.username, password=url.password,
        database="postgres", statement_cache_size=0,
    )  # fmt: skip
    await admin.execute(f'CREATE DATABASE "{name}"')
    try:
        await asyncio.to_thread(migrate.upgrade, scratch)
        await asyncio.to_thread(migrate.upgrade_archive, scratch)
        engine = create_async_engine(scratch, poolclass=NullPool)
        try:
            async with engine.connect() as connection:
                # Neither track's autogenerate would propose dropping the other's tables.
                assert await connection.run_sync(_differences, MAIN) == []
                assert await connection.run_sync(_differences, ARCHIVE) == []
                versions = await connection.scalar(
                    text(
                        "SELECT (SELECT version_num FROM tq_alembic_version) || ' ' || "
                        "(SELECT version_num FROM tq_archive_alembic_version)"
                    )
                )
                assert versions == "0003 0001"
            await asyncio.to_thread(migrate.downgrade_archive, scratch)
            async with engine.connect() as connection:
                tables = set(
                    await connection.scalars(
                        text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
                    )
                )
            assert not {t for t in tables if t.startswith("tq_archive_")} - {ARCHIVE_VERSION_TABLE}
            assert "tq_tasks" in tables
            await asyncio.to_thread(migrate.upgrade_archive, scratch)
        finally:
            await engine.dispose()
    finally:
        await admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
        await admin.close()
