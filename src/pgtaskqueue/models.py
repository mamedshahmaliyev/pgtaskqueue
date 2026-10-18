"""The schema. Seven tables, all prefixed `tq_` so they can live in an application's own
database without colliding with it.

The indexes are the design, so they are written out here rather than left to a migration:

* `tq_tasks_claim_idx` is **partial on `status = 'pending'`** and ordered exactly as the claim
  query orders, so a claim is one short index scan whose cost depends on how many tasks are
  ready, never on how many the table has ever held.
* `tq_tasks_running_idx` indexes `worker_id`, not `heartbeat_at`, so a heartbeat changes no
  indexed column and Postgres can apply it as a HOT update (no index writes at all).
* `tq_tasks_dedupe_idx` is unique only among *active* tasks: a key may be reused as soon as
  the task that held it has finished.

The archive's two tables, at the end, have a metadata and a migration history of their
own: they live in `TQ_ARCHIVE_DATABASE_URL`, usually another database.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    DDL,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Double,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    SmallInteger,
    Table,
    Text,
    Uuid,
    event,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from pgtaskqueue.ids import uuid7

_EMPTY_JSON = text("'{}'::jsonb")
# For nullable JSON columns: Python None is stored as SQL NULL rather than the JSON literal
# `null`, so `IS NULL` means what it says.
_NULLABLE_JSON = JSONB(none_as_null=True)
_NOW = func.now()


_NAMING_CONVENTION = {
    "ix": "%(table_name)s_%(column_0_N_name)s_idx",
    "uq": "%(table_name)s_%(column_0_N_name)s_key",
    "ck": "%(table_name)s_%(constraint_name)s_check",
    "fk": "%(table_name)s_%(column_0_name)s_fkey",
    "pk": "%(table_name)s_pkey",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=_NAMING_CONVENTION)


class Task(Base):
    __tablename__ = "tq_tasks"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid7)
    queue_name: Mapped[str] = mapped_column(Text, server_default="default")
    # Which executor runs it. Dotted by convention (`image.resize`); `tq.` is reserved.
    name: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, server_default="pending")
    # Lower runs first. Ties are broken by `run_after`, then by id — that is, FIFO.
    priority: Mapped[int] = mapped_column(SmallInteger, server_default="100")

    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=_EMPTY_JSON)
    # Caller-supplied labels (a tenant, a trace id). Never interpreted by the queue; here to
    # be filtered and grouped on. The attribute is `meta` because SQLAlchemy reserves
    # `metadata` on declarative classes; the column and the API field are `metadata`.
    meta: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, server_default=_EMPTY_JSON)
    # Written by the executor while it runs (`ctx.set_progress`, `ctx.step`). Survives a
    # retry, which is what lets a second attempt resume instead of starting over.
    progress: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=_EMPTY_JSON)
    output: Mapped[Any | None] = mapped_column(_NULLABLE_JSON)
    # The last failure: {type, message, traceback, attempt, worker}.
    error: Mapped[dict[str, Any] | None] = mapped_column(_NULLABLE_JSON)
    step: Mapped[str | None] = mapped_column(Text)

    # Incremented by the claim, so it doubles as a fencing token: a worker settles a task
    # with `WHERE attempt = <the attempt it claimed>` and loses to any later claim.
    attempt: Mapped[int] = mapped_column(Integer, server_default="0")
    max_attempts: Mapped[int] = mapped_column(Integer, server_default="3")
    timeout_seconds: Mapped[float | None] = mapped_column(Double)
    run_after: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_NOW)
    dedupe_key: Mapped[str | None] = mapped_column(Text)
    # {url, headers?, secret?, events?} — see docs/api.md#webhooks.
    callback: Mapped[dict[str, Any] | None] = mapped_column(_NULLABLE_JSON)

    # The token subject that enqueued it. Text, not a foreign key: tokens may come from an
    # external identity provider whose subjects are not rows here.
    created_by: Mapped[str | None] = mapped_column(Text)
    # Set at enqueue when `created_by` is an account with a running limit. Such a task is
    # held in `scheduled` and only `TaskQueue.release_capped` moves it to `pending`, so the
    # account never has more than its limit pending or running. The promoter skips it.
    capped: Mapped[bool] = mapped_column(Boolean, server_default=text("false"))
    # The worker that last claimed it. Kept after the task finishes, for analytics.
    worker_id: Mapped[str | None] = mapped_column(Text)
    # Execution time summed over every attempt.
    spent_seconds: Mapped[float] = mapped_column(Double, server_default="0")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_NOW)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint(
            "status IN ('scheduled', 'pending', 'running', 'succeeded', 'failed', 'canceled')",
            name="status",
        ),
        Index(
            "tq_tasks_claim_idx",
            "queue_name",
            "name",
            "priority",
            "run_after",
            "id",
            postgresql_where=text("status = 'pending'"),
        ),
        # What the promoter walks. Held tasks are left out, or one account with a long
        # backlog behind its running limit would be re-read on every promote.
        Index(
            "tq_tasks_due_idx",
            "run_after",
            postgresql_where=text("status = 'scheduled' AND NOT capped"),
        ),
        # Held tasks, by account and then oldest first: what `release_capped` walks.
        Index(
            "tq_tasks_held_idx",
            "created_by",
            "run_after",
            "id",
            postgresql_where=text("status = 'scheduled' AND capped"),
        ),
        # An account's unfinished tasks, for its limits: waiting is (scheduled, pending),
        # what counts against its running limit is (pending, running).
        Index(
            "tq_tasks_active_by_idx",
            "created_by",
            "status",
            postgresql_where=text(
                "created_by IS NOT NULL AND status IN ('scheduled', 'pending', 'running')"
            ),
        ),
        Index(
            "tq_tasks_running_idx",
            "worker_id",
            postgresql_where=text("status = 'running'"),
        ),
        Index(
            "tq_tasks_dedupe_idx",
            "queue_name",
            "name",
            "dedupe_key",
            unique=True,
            postgresql_where=text(
                "dedupe_key IS NOT NULL AND status IN ('scheduled', 'pending', 'running')"
            ),
        ),
        Index("tq_tasks_status_idx", "status", "id"),
        Index("tq_tasks_name_idx", "name", "id"),
        Index(
            "tq_tasks_created_by_idx",
            "created_by",
            "id",
            postgresql_where=text("created_by IS NOT NULL"),
        ),
        Index(
            "tq_tasks_completed_idx",
            "completed_at",
            postgresql_where=text("completed_at IS NOT NULL"),
        ),
    )

    # Not a column. False on the row `enqueue` returns when a `dedupe_key` matched a task
    # that was already active, so the caller can tell "queued" from "already queued".
    created: bool = True

    def __repr__(self) -> str:
        return f"<Task {self.id} {self.name} {self.status} attempt={self.attempt}>"


# A queue table is rewritten constantly: every task is updated at least twice and then never
# again. Leave room on each page for HOT updates (heartbeats, progress), and have autovacuum
# visit long before the default 20% of the table is dead, so the partial indexes stay small.
TASKS_STORAGE_SQL = (
    "ALTER TABLE tq_tasks SET ("
    "fillfactor = 85, "
    "autovacuum_vacuum_scale_factor = 0.02, "
    "autovacuum_analyze_scale_factor = 0.02, "
    "autovacuum_vacuum_cost_limit = 2000)"
)
event.listen(Task.__table__, "after_create", DDL(TASKS_STORAGE_SQL))


class TaskLog(Base):
    __tablename__ = "tq_task_logs"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    task_id: Mapped[uuid.UUID] = mapped_column(Uuid, ForeignKey("tq_tasks.id", ondelete="CASCADE"))
    attempt: Mapped[int] = mapped_column(Integer, server_default="0")
    level: Mapped[str] = mapped_column(Text, server_default="info")
    step: Mapped[str | None] = mapped_column(Text)
    message: Mapped[str] = mapped_column(Text)
    data: Mapped[dict[str, Any] | None] = mapped_column(_NULLABLE_JSON)
    # When the line was written, by the writer's clock — lines are buffered and inserted in
    # batches, so the insert time would be up to a flush interval late.
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_NOW)

    __table_args__ = (Index("tq_task_logs_task_idx", "task_id", "id"),)


class TaskType(Base):
    """What workers say they can run. Written by a worker when it starts.

    The API cannot import executor code — a worker's dependencies are its own — so this is
    how it learns a task's payload schema and defaults without them.
    """

    __tablename__ = "tq_task_types"

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    description: Mapped[str | None] = mapped_column(Text)
    payload_schema: Mapped[dict[str, Any] | None] = mapped_column(_NULLABLE_JSON)
    max_attempts: Mapped[int | None] = mapped_column(Integer)
    timeout_seconds: Mapped[float | None] = mapped_column(Double)
    registered_by: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_NOW)


class Queue(Base):
    """Per-queue switches. A queue needs no row to exist; a row is only what pauses it."""

    __tablename__ = "tq_queues"

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    paused: Mapped[bool] = mapped_column(Boolean, server_default=text("false"))
    note: Mapped[str | None] = mapped_column(Text)
    updated_by: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_NOW)


class ScheduleRow(Base):
    """A periodic task: what to enqueue, and when next. See docs/schedules.md.

    No index beyond the primary key, on purpose: the table holds one row per schedule, so
    the scan for due rows reads a page or two, and `next_run_at` changes at every fire.
    """

    __tablename__ = "tq_schedules"

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    task_name: Mapped[str] = mapped_column(Text)
    queue_name: Mapped[str] = mapped_column(Text, server_default="default")
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=_EMPTY_JSON)
    # None leaves each to what an enqueue without it would get.
    priority: Mapped[int | None] = mapped_column(SmallInteger)
    max_attempts: Mapped[int | None] = mapped_column(Integer)
    timeout_seconds: Mapped[float | None] = mapped_column(Double)

    # Exactly one of the two. `timezone` is where a cron expression's wall clock is read.
    cron: Mapped[str | None] = mapped_column(Text)
    every_seconds: Mapped[float | None] = mapped_column(Double)
    timezone: Mapped[str] = mapped_column(Text, server_default="UTC")

    # The operator's switch. Kept when a worker publishes the schedule again.
    paused: Mapped[bool] = mapped_column(Boolean, server_default=text("false"))
    # 'code': declared next to an executor and written by workers when they start.
    # 'api': created through the API, the MCP server or `tq schedule`.
    source: Mapped[str] = mapped_column(Text)
    # Becomes `created_by` of the tasks it enqueues. Null for a schedule declared in code.
    created_by: Mapped[str | None] = mapped_column(Text)

    # Advanced in the transaction that enqueues the task, so a fire time is used once.
    next_run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    # The last fire that enqueued a task. Not a foreign key: the task may be pruned.
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_task_id: Mapped[uuid.UUID | None] = mapped_column(Uuid)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_NOW)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_NOW)

    __table_args__ = (
        CheckConstraint("(cron IS NULL) <> (every_seconds IS NULL)", name="timing"),
        CheckConstraint("source IN ('code', 'api')", name="source"),
    )

    # Not a column. True on the row `Schedules.put` returns when the name was new.
    created: bool = False


class WorkerInfo(Base):
    """One row per worker process, refreshed by its maintenance loop."""

    __tablename__ = "tq_workers"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    queue_name: Mapped[str] = mapped_column(Text)
    hostname: Mapped[str] = mapped_column(Text)
    pid: Mapped[int] = mapped_column(Integer)
    # [{"names": [...], "slots": n}, ...]
    pools: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, server_default=text("'[]'::jsonb"))
    slots: Mapped[int] = mapped_column(Integer, server_default="0")
    in_flight: Mapped[int] = mapped_column(Integer, server_default="0")
    # Counters since the process started: {succeeded, failed, retried, lost}.
    counters: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=_EMPTY_JSON)
    version: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_NOW)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_NOW)
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (Index("tq_workers_heartbeat_idx", "heartbeat_at"),)


class User(Base):
    """A person or a service account that may call the API."""

    __tablename__ = "tq_users"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid7)
    username: Mapped[str] = mapped_column(Text, unique=True)
    # 'user' signs in with a password; 'service' with the client-credentials grant.
    kind: Mapped[str] = mapped_column(Text, server_default="user")
    password_hash: Mapped[str] = mapped_column(Text)
    role: Mapped[str] = mapped_column(Text)
    # The ACL: glob patterns for the queues and task names this account may touch.
    queues: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default=text("'{*}'::text[]"))
    task_names: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default=text("'{*}'::text[]"))
    is_active: Mapped[bool] = mapped_column(Boolean, server_default=text("true"))
    # Bumped to invalidate every refresh token this account holds.
    token_version: Mapped[int] = mapped_column(Integer, server_default="0")
    # Limits on what the account may enqueue. NULL takes the TQ_ACCOUNT_* default; 0 means
    # no limit. docs/auth.md#limits.
    # Tasks scheduled or pending at once; more is refused.
    max_waiting: Mapped[int | None] = mapped_column(Integer)
    # Tasks pending or running at once; more are held back until one finishes.
    max_running: Mapped[int | None] = mapped_column(Integer)
    # Tasks created in any 60 seconds; more is refused.
    enqueue_per_minute: Mapped[int | None] = mapped_column(Integer)
    # The most urgent priority the account may ask for (lower runs first).
    min_priority: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=_NOW)
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint("kind IN ('user', 'service')", name="kind"),
        CheckConstraint(
            "role IN ('admin', 'manager', 'operator', 'submitter', 'viewer', 'worker')",
            name="role",
        ),
        CheckConstraint(
            "max_waiting >= 0 AND max_running >= 0 AND enqueue_per_minute >= 0 "
            "AND min_priority BETWEEN 0 AND 32767",
            name="limits",
        ),
    )


# --------------------------------------------------------------------------- the archive

# The archive's tables, in `TQ_ARCHIVE_DATABASE_URL`, with migrations of their own in
# `migrations_archive/`. Their columns are copied from tq_tasks and tq_task_logs rather than
# written out again, so a column added to a task is a column of the archive too, and the
# archive's model-against-migration test fails until an archive migration adds it.
archive_metadata = MetaData(naming_convention=_NAMING_CONVENTION)

ArchivedTask = Table(
    "tq_archive_tasks",
    archive_metadata,
    *(column._copy() for column in Task.__table__.columns),
    # When it last arrived. A task can be archived twice (see `Archive.move`); the second
    # copy replaces the first.
    Column("archived_at", DateTime(timezone=True), server_default=_NOW, nullable=False),
    CheckConstraint("status IN ('succeeded', 'failed', 'canceled')", name="status"),
    # Few indexes, on purpose: the table is written in bulk and read now and then. Time
    # ranges for reports, and the two lookups the queue's own listing offers most.
    Index("tq_archive_tasks_completed_idx", "completed_at"),
    Index("tq_archive_tasks_name_idx", "name", "id"),
    Index(
        "tq_archive_tasks_created_by_idx",
        "created_by",
        "id",
        postgresql_where=text("created_by IS NOT NULL"),
    ),
)

ArchivedTaskLog = Table(
    "tq_archive_task_logs",
    archive_metadata,
    # The id the line had in tq_task_logs, kept so the order of lines is the order they were
    # written in. Not an identity: the archive never makes one up.
    Column("id", BigInteger, primary_key=True, autoincrement=False),
    Column(
        "task_id",
        Uuid,
        ForeignKey("tq_archive_tasks.id", ondelete="CASCADE"),
        nullable=False,
    ),
    *(
        column._copy()
        for column in TaskLog.__table__.columns
        if column.name not in ("id", "task_id")
    ),
    Index("tq_archive_task_logs_task_idx", "task_id", "id"),
)
