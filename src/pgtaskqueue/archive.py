"""Moving finished tasks, with their logs, out of the queue's database into an archive.

The queue does not need old tasks: a claim reads a partial index over pending rows and
costs the same with ten million finished rows behind it. The disk, the backups and every
report over `tq_tasks` do pay for them, so they can be moved to `TQ_ARCHIVE_DATABASE_URL`
once they are `TQ_ARCHIVE_AFTER_DAYS` old, and read there.

Two databases cannot commit together, so a batch is moved in an order that can repeat but
cannot lose anything:

1. In the queue's database, lock the batch: finished tasks past the threshold, `FOR UPDATE
   SKIP LOCKED`. The transaction stays open, so no one can requeue them meanwhile; a
   requeue waits, then finds the task gone.
2. In the archive, write them and their logs, and commit.
3. Delete them from the queue (their logs go by cascade), and commit.

A failure before 2 commits changes nothing. A failure between 2 and 3 leaves the tasks in
both databases; the next batch archives them again, and the archive's copy is replaced by
the newer one. A task is deleted from the queue only after the archive has it.
"""

from __future__ import annotations

import builtins
import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import delete, func, select, text
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.dialects.postgresql import insert

from pgtaskqueue.db import Database
from pgtaskqueue.enums import TERMINAL_STATUSES
from pgtaskqueue.models import ArchivedTask, ArchivedTaskLog, Task, TaskLog
from pgtaskqueue.settings import Settings, get_settings

_t = Task.__table__
_logs = TaskLog.__table__
_at = ArchivedTask
_alogs = ArchivedTaskLog

_TERMINAL = [s.value for s in TERMINAL_STATUSES]
_ATTR_BY_COLUMN = {
    column.name: attr.key for attr in sa_inspect(Task).column_attrs for column in attr.columns
}

# One archiver at a time. Not needed for correctness (SKIP LOCKED gives two archivers
# disjoint batches) but every worker tries, and one is enough load on the archive.
LOCK_ARCHIVE = 0x7471_0006


def open_archive_database(settings: Settings, application_name: str) -> Database:
    """The archive's `Database`: a small pool, for one batch at a time."""
    if not settings.archive_database_url:
        raise ValueError("set TQ_ARCHIVE_DATABASE_URL to the database to archive tasks to")
    return Database(
        settings.archive_database_url,
        pool_size=2,
        max_overflow=0,
        application_name=application_name,
        # No setting says whether the archive is behind a pooler, and statements without
        # a prepared-statement cache work either way. There are a handful per batch.
        pgbouncer=True,
        settings=settings,
    )


def _record(row: Any) -> Task:
    """A `Task` holding one archived row, so it prints and serializes like any other."""
    return Task(**{_ATTR_BY_COLUMN[k]: v for k, v in row._mapping.items() if k in _ATTR_BY_COLUMN})


class Archive:
    """Moves finished tasks to the archive database, and reads them back."""

    def __init__(
        self, db: Database, archive_db: Database, settings: Settings | None = None
    ) -> None:
        self.db = db
        self.archive_db = archive_db
        self.settings = settings or get_settings()

    async def move(
        self,
        *,
        older_than_days: float | None = None,
        batch: int | None = None,
        max_batches: int | None = None,
    ) -> int:
        """Archive finished tasks that finished more than `older_than_days` ago (default
        `TQ_ARCHIVE_AFTER_DAYS`), batch by batch, until none are left or `max_batches`.
        Returns how many. Returns early, with what it moved, if another process is
        archiving."""
        days = self.settings.archive_after_days if older_than_days is None else older_than_days
        size = batch or self.settings.archive_batch_size
        total = 0
        done = 0
        while max_batches is None or done < max_batches:
            moved = await self._move_batch(days, size)
            done += 1
            if moved is None:
                return total
            total += moved
            if moved < size:
                return total
        return total

    async def _move_batch(self, days: float, size: int) -> int | None:
        """One batch, in the order the module docstring gives. None: the lock was taken."""
        async with self.db.transaction() as tx:
            # Per batch, not per run, so no transaction in the queue's database lasts longer
            # than one batch: a long one would hold back vacuum on a table that needs it.
            if not await tx.scalar(
                text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": LOCK_ARCHIVE}
            ):
                return None
            # The status test is next to the lock, as in the claim: a task requeued after
            # this statement's snapshot is re-checked when its row lock is reached, and left.
            tasks = (
                await tx.execute(
                    select(*_t.c)
                    .where(
                        _t.c.status.in_(_TERMINAL),
                        _t.c.completed_at < func.now() - timedelta(days=days),
                    )
                    .order_by(_t.c.completed_at)
                    .limit(size)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            if not tasks:
                return 0
            ids = [row.id for row in tasks]
            logs = (await tx.execute(select(*_logs.c).where(_logs.c.task_id.in_(ids)))).all()

            async with self.archive_db.transaction() as atx:
                # A task archived before, whose delete here did not commit, is written again
                # whole: its logs are the full set, its row the newer one.
                await atx.execute(delete(_alogs).where(_alogs.c.task_id.in_(ids)))
                upsert = insert(_at)
                await atx.execute(
                    upsert.on_conflict_do_update(
                        index_elements=[_at.c.id],
                        set_={
                            **{
                                column.name: upsert.excluded[column.name]
                                for column in _t.columns
                                if column.name != "id"
                            },
                            "archived_at": func.now(),
                        },
                    ),
                    [dict(row._mapping) for row in tasks],
                )
                if logs:
                    await atx.execute(_alogs.insert(), [dict(row._mapping) for row in logs])

            # Only now that the archive has committed. The rows are still locked by this
            # transaction, so they are what was written.
            await tx.execute(delete(_t).where(_t.c.id.in_(ids)))
        return len(tasks)

    # ------------------------------------------------------------------ reads

    async def get(self, task_id: uuid.UUID) -> Task | None:
        async with self.archive_db.sessionmaker() as session:
            row = (await session.execute(select(*_at.c).where(_at.c.id == task_id))).first()
            return _record(row) if row is not None else None

    async def get_logs(self, task_id: uuid.UUID, *, limit: int = 500) -> builtins.list[TaskLog]:
        """An archived task's log lines, oldest first."""
        async with self.archive_db.sessionmaker() as session:
            result = await session.execute(
                select(*_alogs.c)
                .where(_alogs.c.task_id == task_id)
                .order_by(_alogs.c.id)
                .limit(limit)
            )
            return [TaskLog(**row._mapping) for row in result]

    async def list(
        self,
        *,
        queue: str | None = None,
        name: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> builtins.list[Task]:
        """Archived tasks, newest first."""
        stmt = select(*_at.c).order_by(_at.c.id.desc()).limit(limit)
        if queue is not None:
            stmt = stmt.where(_at.c.queue_name == queue)
        if name is not None:
            stmt = stmt.where(_at.c.name == name)
        if status is not None:
            stmt = stmt.where(_at.c.status == status)
        async with self.archive_db.sessionmaker() as session:
            return [_record(row) for row in await session.execute(stmt)]
