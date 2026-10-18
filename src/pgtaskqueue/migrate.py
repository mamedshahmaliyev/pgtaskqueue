"""Running the packaged Alembic migrations without an `alembic.ini`.

There are two histories, each with its own directory and version table:

* `migrations/` is the queue's schema, in `TQ_DATABASE_URL`, tracked in
  `tq_alembic_version` rather than Alembic's default `alembic_version`, so the queue's
  tables can be installed into a database that has migrations of its own without the two
  histories ever meeting.
* `migrations_archive/` is the archive's (`tq_archive_*`), in `TQ_ARCHIVE_DATABASE_URL`,
  tracked in `tq_archive_alembic_version`. A separate history because it is usually a
  separate database, and because it may also be the same one: the two tracks then share a
  database without sharing a version table.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import MetaData

from pgtaskqueue.settings import get_settings

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
ARCHIVE_MIGRATIONS_DIR = Path(__file__).parent / "migrations_archive"
VERSION_TABLE = "tq_alembic_version"
ARCHIVE_VERSION_TABLE = "tq_archive_alembic_version"
ARCHIVE_PREFIX = "tq_archive_"
# Advisory lock held for the length of a migration run, so two `tq migrate` started at the
# same moment (two replicas deploying, say) take turns rather than both creating the tables.
LOCK_MIGRATE = 0x7471_0003


@dataclass(frozen=True)
class Track:
    """One migration history: where it lives, what it records itself in, what it owns."""

    name: str
    directory: Path
    version_table: str
    # The tables autogenerate may compare. Anything else in the database is someone else's.
    owns: Callable[[str], bool]
    metadata: Callable[[], MetaData]
    default_url: Callable[[], str | None]
    # Whether the driver's prepared-statement caches must be off (a transaction-mode pooler).
    pgbouncer: Callable[[], bool]


def _main_metadata() -> MetaData:
    from pgtaskqueue.models import Base

    return Base.metadata


def _archive_metadata() -> MetaData:
    from pgtaskqueue.models import archive_metadata

    return archive_metadata


MAIN = Track(
    name="main",
    directory=MIGRATIONS_DIR,
    version_table=VERSION_TABLE,
    owns=lambda name: name.startswith("tq_") and not name.startswith(ARCHIVE_PREFIX),
    metadata=_main_metadata,
    default_url=lambda: get_settings().database_url,
    pgbouncer=lambda: get_settings().db_pgbouncer,
)
ARCHIVE = Track(
    name="archive",
    directory=ARCHIVE_MIGRATIONS_DIR,
    version_table=ARCHIVE_VERSION_TABLE,
    owns=lambda name: name.startswith(ARCHIVE_PREFIX),
    metadata=_archive_metadata,
    default_url=lambda: get_settings().archive_database_url,
    # As the archiver does (archive.open_archive_database): nothing says whether the archive
    # URL is a pooler, and statements without a cache work either way.
    pgbouncer=lambda: True,
)


def alembic_config(database_url: str | None = None, track: Track = MAIN) -> Config:
    config = Config()
    config.set_main_option("script_location", str(track.directory))
    url = database_url or track.default_url()
    if not url:
        raise ValueError(f"no database URL for the {track.name} migrations")
    # ConfigParser treats % as interpolation; a URL-encoded password contains them.
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return config


def upgrade(database_url: str | None = None, revision: str = "head") -> None:
    """Bring the schema up to `revision`. Synchronous: it starts its own event loop, so
    call it from a thread (`asyncio.to_thread`) when one is already running."""
    command.upgrade(alembic_config(database_url), revision)


def downgrade(database_url: str | None = None, revision: str = "base") -> None:
    command.downgrade(alembic_config(database_url), revision)


def upgrade_archive(database_url: str | None = None, revision: str = "head") -> None:
    """The archive's schema, in `database_url` or `TQ_ARCHIVE_DATABASE_URL`."""
    command.upgrade(alembic_config(database_url, ARCHIVE), revision)


def downgrade_archive(database_url: str | None = None, revision: str = "base") -> None:
    command.downgrade(alembic_config(database_url, ARCHIVE), revision)


# --------------------------------------------------------------------------- env.py


def run_environment(track: Track) -> None:
    """The body of each track's `env.py`: run that track's migrations, offline or online."""
    from alembic import context

    config = context.config
    # `tq migrate` passes the URL in; the bare `alembic` command leaves it to the environment.
    url = config.get_main_option("sqlalchemy.url") or track.default_url()
    if not url:
        raise ValueError(f"set the database URL for the {track.name} migrations")

    def include_object(obj, name, type_, reflected, compare_to) -> bool:
        # Autogenerate against a shared database must not propose dropping the host
        # application's tables, or the other track's: only what this track owns is compared.
        if type_ == "table":
            return track.owns(name)
        return True

    options = {
        "target_metadata": track.metadata(),
        "version_table": track.version_table,
        "include_object": include_object,
    }
    if context.is_offline_mode():
        context.configure(url=url, literal_binds=True, **options)
        with context.begin_transaction():
            context.run_migrations()
        return

    def run(connection) -> None:
        context.configure(connection=connection, **options)
        with context.begin_transaction():
            context.run_migrations()

    asyncio.run(_run_online(url, track, run))


async def _take_turn(holder) -> None:
    """Wait until no other `tq migrate` is running, then hold the lock that says so in an
    open transaction on `holder` until this run is over.

    The lock is on a connection of its own because a migration that builds an index
    `CONCURRENTLY` commits the migration's transaction part-way through; a lock held there
    would let the next run in while the index is still being built. And it is taken by
    asking again in short transactions rather than by waiting in one: a run blocked inside
    a statement holds a snapshot, which `CREATE INDEX CONCURRENTLY` in the run ahead of it
    would wait for, while it waits for that run. Idle between two questions, it holds
    nothing the index build waits for.
    """
    from sqlalchemy import text

    while True:
        if await holder.scalar(
            text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": LOCK_MIGRATE}
        ):
            # A statement sent with the extended protocol, as SQLAlchemy sends every one,
            # leaves the backend's xmin set until the next command, and the index build
            # would wait for it. A simple-protocol statement clears it.
            raw = await holder.get_raw_connection()
            await raw.driver_connection.execute("SELECT 1")
            return
        await holder.rollback()
        await asyncio.sleep(0.2)


async def _run_online(url: str, track: Track, run: Callable) -> None:
    from sqlalchemy import pool
    from sqlalchemy.ext.asyncio import create_async_engine

    from pgtaskqueue.db import connect_args

    engine = create_async_engine(
        url,
        poolclass=pool.NullPool,
        connect_args=connect_args(f"tq-migrate-{track.name}", pgbouncer=track.pgbouncer()),
    )
    async with engine.connect() as holder:
        # The second of two concurrent runs waits here, then finds the schema already at
        # the revision it wanted and does nothing.
        await _take_turn(holder)
        async with engine.connect() as connection:
            await connection.run_sync(run)
        await holder.rollback()
    await engine.dispose()
