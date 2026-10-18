"""Every operation on a task: enqueue, claim, heartbeat, settle, cancel, requeue, read.

Three rules hold throughout, and most of this file is those rules written as SQL:

1. **A state change is one conditional statement.** The status a transition expects is in
   its `WHERE`, so two writers racing for the same row are settled by the row lock and the
   loser changes nothing. Nothing is read first and written second.
2. **A worker owns a task only while `status = 'running' AND worker_id = <it> AND
   attempt = <the attempt it claimed>`.** Every write a worker makes carries all three, so
   a worker that was presumed dead and replaced cannot overwrite its successor's result.
3. **Time is the database's.** `now()` decides what is due and what is stale, so hosts with
   drifting clocks cannot disagree about it.
"""

from __future__ import annotations

import builtins
import random
import uuid
from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import asynccontextmanager
from dataclasses import KW_ONLY, dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import (
    DateTime,
    Text,
    Uuid,
    any_,
    bindparam,
    case,
    delete,
    func,
    literal,
    select,
    text,
    update,
)
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from pgtaskqueue.db import Database
from pgtaskqueue.enums import (
    ACTIVE_STATUSES,
    CHANNEL_CANCEL,
    CHANNEL_ENQUEUE,
    TERMINAL_STATUSES,
    WEBHOOK_TASK,
    TaskEvent,
    TaskStatus,
)
from pgtaskqueue.errors import DuplicateTaskError
from pgtaskqueue.ids import uuid7
from pgtaskqueue.jsonutil import to_jsonable
from pgtaskqueue.models import Task, TaskLog
from pgtaskqueue.settings import Settings, get_settings

_t = Task.__table__
_logs = TaskLog.__table__

_ACTIVE = [s.value for s in ACTIVE_STATUSES]
_TERMINAL = [s.value for s in TERMINAL_STATUSES]
_ATTR_BY_COLUMN = {
    column.name: attr.key for attr in sa_inspect(Task).column_attrs for column in attr.columns
}
_EVENT_BY_STATUS = {
    TaskStatus.SUCCEEDED.value: TaskEvent.SUCCEEDED,
    TaskStatus.FAILED.value: TaskEvent.FAILED,
    TaskStatus.CANCELED.value: TaskEvent.CANCELED,
}
# Seconds this attempt has been running, for adding to `spent_seconds` when it ends.
_ELAPSED = func.coalesce(func.extract("epoch", func.now() - _t.c.started_at), 0)

# Advisory-lock keys for the housekeeping that only one worker needs to do at a time.
LOCK_PROMOTE = 0x7471_0001
LOCK_MAINTENANCE = 0x7471_0002
LOCK_RELEASE = 0x7471_0004


def _task(row: Any) -> Task:
    """A `Task` holding one returned row. Not attached to a session: it is a record."""
    return Task(**{_ATTR_BY_COLUMN[key]: value for key, value in row._mapping.items()})


def glob_to_like(pattern: str) -> str:
    """A shell-style pattern (`media-*`) as a LIKE pattern, with LIKE's own wildcards escaped."""
    escaped = pattern.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return escaped.replace("*", "%").replace("?", "_")


@dataclass(slots=True)
class NewTask:
    """One task to enqueue. `TaskQueue.enqueue` takes the same fields as keyword arguments."""

    name: str
    payload: dict[str, Any] | None = None
    _: KW_ONLY
    queue: str = "default"
    # Lower runs first. None uses TQ_DEFAULT_PRIORITY.
    priority: int | None = None
    # Not before this moment. `delay` is the same thing measured from now, by the database's
    # clock; give one or neither.
    run_after: datetime | None = None
    delay: float | timedelta | None = None
    # None uses the task type's registered default, then TQ_DEFAULT_MAX_ATTEMPTS.
    max_attempts: int | None = None
    timeout: float | None = None
    # At most one *active* task per (queue, name, dedupe_key).
    dedupe_key: str | None = None
    metadata: dict[str, Any] | None = None
    callback: dict[str, Any] | None = None
    created_by: str | None = None

    def row(self, settings: Settings) -> dict[str, Any]:
        """This task as one element of the JSON array the enqueue statement reads."""
        if self.run_after is not None and self.delay is not None:
            raise ValueError("give run_after or delay, not both")
        if self.run_after is not None and self.run_after.tzinfo is None:
            raise ValueError("run_after must be timezone-aware")
        delay = self.delay or 0
        if isinstance(delay, timedelta):
            delay = delay.total_seconds()
        return {
            "id": str(uuid7()),
            "queue_name": self.queue,
            "name": self.name,
            "priority": settings.default_priority if self.priority is None else self.priority,
            "payload": to_jsonable(self.payload or {}),
            "metadata": to_jsonable(self.metadata or {}),
            "run_after": self.run_after.isoformat() if self.run_after else None,
            "delay_seconds": float(delay),
            "max_attempts": self.max_attempts,
            "timeout_seconds": self.timeout,
            "dedupe_key": self.dedupe_key,
            "callback": to_jsonable(self.callback) if self.callback else None,
            "created_by": self.created_by,
        }


# The enqueue statement: one shape for a single task and for a batch of thousands. The tasks
# arrive as one JSON array, so a batch is one round trip and one parameter however long it is.
#
# Two decisions are made here, in SQL, rather than by the caller:
#
# * `status` — `scheduled` if `run_after` is in the future by the database's clock, else
#   `pending`. A caller on a host whose clock runs ahead cannot enqueue "now" into the past.
# * `max_attempts` / `timeout_seconds` — what the caller said, else what the task type
#   registered, else the global default. The API has no executor code to ask, so the defaults
#   a worker published in `tq_task_types` are read by the insert itself.
# * `capped` — whether `created_by` is an account with a running limit. Such a task is held
#   in `scheduled` whatever its `run_after`, and `release_capped` lets it out. Deciding it
#   here covers every way in: the API, `TaskQueue` called directly, a schedule firing.
#   Built-in tasks (`tq.`) are never held: a webhook must not wait behind its own account.
#
# `ON CONFLICT ... DO NOTHING` names the partial unique index over active dedupe keys, so a
# task whose key is already held by an active task is skipped rather than raising. Ids are
# minted in the order the tasks were given, so `ORDER BY r.id` makes the first of two tasks
# with the same key in one batch the one that is kept.
_INSERT_SQL = text(
    """
    INSERT INTO tq_tasks (
        id, queue_name, name, status, priority, payload, metadata, run_after,
        max_attempts, timeout_seconds, dedupe_key, callback, created_by, capped
    )
    SELECT r.id, r.queue_name, r.name,
           CASE WHEN lim.capped OR due.run_after > now() THEN 'scheduled' ELSE 'pending' END,
           r.priority, r.payload, r.metadata, due.run_after,
           COALESCE(r.max_attempts, tt.max_attempts, :default_max_attempts),
           COALESCE(r.timeout_seconds, tt.timeout_seconds),
           r.dedupe_key, r.callback, r.created_by, lim.capped
    FROM jsonb_to_recordset(:rows) AS r(
        id uuid, queue_name text, name text, priority integer, payload jsonb,
        metadata jsonb, run_after timestamptz, delay_seconds double precision,
        max_attempts integer, timeout_seconds double precision, dedupe_key text,
        callback jsonb, created_by text
    )
    CROSS JOIN LATERAL (
        SELECT COALESCE(r.run_after, now() + make_interval(secs => r.delay_seconds))
               AS run_after
    ) AS due
    LEFT JOIN tq_task_types tt ON tt.name = r.name
    LEFT JOIN tq_users u ON u.username = r.created_by
    CROSS JOIN LATERAL (
        SELECT r.created_by IS NOT NULL
               AND left(r.name, 3) <> 'tq.'
               AND COALESCE(u.max_running, :default_max_running) > 0
               AS capped
    ) AS lim
    ORDER BY r.id
    ON CONFLICT (queue_name, name, dedupe_key)
        WHERE dedupe_key IS NOT NULL AND status IN ('scheduled', 'pending', 'running')
        DO NOTHING
    RETURNING *
    """
).bindparams(bindparam("rows", type_=JSONB))

# Rows per enqueue statement. Bounds the size of one JSON parameter, not correctness.
ENQUEUE_CHUNK = 5000


# The claim. Read it with docs/architecture.md#the-claim open.
#
# * One LATERAL subquery per task name, each a `LIMIT n` walk of `tq_tasks_claim_idx` — which
#   is partial on `status = 'pending'` and ordered `(queue_name, name, priority, run_after,
#   id)`, so the walk stops after n ready rows however many rows the table holds. A single
#   `name = ANY(...)` scan could not use that ordering across names.
# * `FOR UPDATE SKIP LOCKED` is on that innermost SELECT, next to the `status = 'pending'`
#   test, and both halves matter. SKIP LOCKED steps over a row another claimer holds *right
#   now*; the status test is what Postgres re-checks (under READ COMMITTED) when a row was
#   pending in this statement's snapshot but claimed and committed before the lock was
#   reached. With the lock taken on an outer `id = (subquery)` instead, that re-check has
#   nothing to fail on and the same task is handed to two workers.
# * A paused queue yields no names, so nothing is scanned or locked.
# * `attempt + 1` in the same statement is the fencing token the settle statements carry.
_CLAIM_SQL = text(
    """
    WITH picked AS (
        SELECT c.id
        FROM (
            SELECT u.name
            FROM unnest(CAST(:names AS text[])) AS u(name)
            WHERE NOT EXISTS (
                SELECT 1 FROM tq_queues q WHERE q.name = :queue AND q.paused
            )
        ) AS n
        CROSS JOIN LATERAL (
            SELECT t.id, t.priority, t.run_after
            FROM tq_tasks t
            WHERE t.queue_name = :queue
              AND t.name = n.name
              AND t.status = 'pending'
            ORDER BY t.priority, t.run_after, t.id
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
        ) AS c
        ORDER BY c.priority, c.run_after, c.id
        LIMIT :limit
    )
    UPDATE tq_tasks t
    SET status = 'running',
        attempt = t.attempt + 1,
        worker_id = :worker_id,
        started_at = now(),
        heartbeat_at = now()
    FROM picked
    WHERE t.id = picked.id
    RETURNING t.*
    """
).bindparams(bindparam("names", type_=ARRAY(Text)))

_PROMOTE_SQL = text(
    """
    WITH due AS (
        SELECT id FROM tq_tasks
        WHERE status = 'scheduled' AND NOT capped AND run_after <= now()
        ORDER BY run_after
        LIMIT :batch
        FOR UPDATE SKIP LOCKED
    )
    UPDATE tq_tasks t SET status = 'pending'
    FROM due WHERE t.id = due.id
    RETURNING t.queue_name
    """
)

# Held tasks out to `pending`, as many per account as its running limit has room for.
#
# * `owners` is a loose index scan of `tq_tasks_held_idx`: one probe per account that has
#   held tasks, however many it holds.
# * Room is the limit minus what the account has pending or running, counted no further
#   than the limit. A limit since removed (or a task whose account was deleted) has room
#   for a batch: the task was held when it had one, and is let out now that it has none.
# * Only this statement turns a held task `pending`, and `release_capped` runs it under a
#   lock, so two releases never fill the same room. Everything else that touches an
#   account's tasks meanwhile only frees room (a task finishes, or is retried later) or
#   leaves it as it was (a claim, a worker handing a task back), so the count read here
#   can only be too high, never too low. (Tasks the account enqueued before it had a limit
#   are not held, and are promoted as usual until they are gone.)
# * Oldest first within an account. Priority orders what has been let out, as everywhere.
_RELEASE_SQL = text(
    """
    WITH RECURSIVE owners(created_by) AS (
        (SELECT created_by FROM tq_tasks
         WHERE status = 'scheduled' AND capped
         ORDER BY created_by LIMIT 1)
        UNION ALL
        SELECT (SELECT t.created_by FROM tq_tasks t
                WHERE t.status = 'scheduled' AND t.capped AND t.created_by > o.created_by
                ORDER BY t.created_by LIMIT 1)
        FROM owners o
        WHERE o.created_by IS NOT NULL
    ),
    room AS (
        SELECT o.created_by,
               CASE WHEN l.cap > 0 THEN l.cap - (
                   SELECT count(*) FROM (
                       SELECT 1 FROM tq_tasks a
                       WHERE a.created_by = o.created_by
                         AND a.status IN ('pending', 'running')
                         AND left(a.name, 3) <> 'tq.'
                       LIMIT l.cap
                   ) AS busy
               ) ELSE :batch END AS n
        FROM owners o
        LEFT JOIN tq_users u ON u.username = o.created_by
        CROSS JOIN LATERAL (
            SELECT COALESCE(u.max_running, :default_max_running) AS cap
        ) AS l
        WHERE o.created_by IS NOT NULL
    ),
    picked AS (
        SELECT h.id
        FROM room r
        CROSS JOIN LATERAL (
            SELECT t.id FROM tq_tasks t
            WHERE t.created_by = r.created_by
              AND t.status = 'scheduled' AND t.capped
              AND t.run_after <= now()
            ORDER BY t.run_after, t.id
            LIMIT r.n
            FOR UPDATE SKIP LOCKED
        ) AS h
        WHERE r.n > 0
    )
    UPDATE tq_tasks t SET status = 'pending'
    FROM picked WHERE t.id = picked.id
    RETURNING t.queue_name
    """
)

# A worker that stopped heartbeating is presumed dead. Its task goes back to `pending` if it
# has attempts left and to `failed` if not — the bound is what stops a task that kills its
# worker (out of memory, a segfault in a native library) from doing so forever.
_RECOVER_SQL = text(
    """
    UPDATE tq_tasks
    SET status = CASE WHEN attempt < max_attempts THEN 'pending' ELSE 'failed' END,
        completed_at = CASE WHEN attempt < max_attempts THEN NULL ELSE now() END,
        spent_seconds = spent_seconds
            + GREATEST(COALESCE(EXTRACT(EPOCH FROM heartbeat_at - started_at), 0), 0),
        error = jsonb_build_object(
            'type', 'WorkerLost',
            'message', 'no heartbeat from worker ' || COALESCE(worker_id, '?')
                       || ' for more than ' || CAST(:stale AS double precision) || 's',
            'attempt', attempt,
            'worker', worker_id
        )
    WHERE status = 'running'
      AND heartbeat_at < now() - make_interval(secs => CAST(:stale AS double precision))
    RETURNING *
    """
)


class TaskQueue:
    def __init__(self, db: Database, settings: Settings | None = None) -> None:
        self.db = db
        self.settings = settings or get_settings()

    @asynccontextmanager
    async def _tx(self, session: AsyncSession | None = None) -> AsyncIterator[AsyncSession]:
        """The caller's session if one was given (their transaction, their commit), else a
        transaction of our own."""
        if session is not None:
            yield session
        else:
            async with self.db.transaction() as own:
                yield own

    async def _insert(
        self, tx: AsyncSession, rows: builtins.list[dict[str, Any]]
    ) -> builtins.list[Task]:
        result = await tx.execute(
            _INSERT_SQL,
            {
                "rows": rows,
                "default_max_attempts": self.settings.default_max_attempts,
                "default_max_running": self.settings.account_max_running,
            },
        )
        return [_task(row) for row in result]

    async def _notify(self, session: AsyncSession, channel: str, payload: str) -> None:
        if self.settings.notify:
            await session.execute(
                text("SELECT pg_notify(:channel, :payload)"),
                {"channel": channel, "payload": payload},
            )

    # ------------------------------------------------------------------ enqueue

    async def enqueue(
        self,
        name: str,
        payload: dict[str, Any] | None = None,
        *,
        queue: str = "default",
        priority: int | None = None,
        run_after: datetime | None = None,
        delay: float | timedelta | None = None,
        max_attempts: int | None = None,
        timeout: float | None = None,
        dedupe_key: str | None = None,
        metadata: dict[str, Any] | None = None,
        callback: dict[str, Any] | None = None,
        created_by: str | None = None,
        session: AsyncSession | None = None,
    ) -> Task:
        """Add one task and return it.

        Pass `session` to enqueue inside your own transaction: the task becomes visible to
        workers when — and only if — that transaction commits, so "write the order and
        queue the receipt e-mail" is atomic.

        With a `dedupe_key`, an active task under the same (queue, name, key) is returned
        instead of a second one being created; its `created` attribute is False.
        """
        new = NewTask(
            name,
            payload,
            queue=queue,
            priority=priority,
            run_after=run_after,
            delay=delay,
            max_attempts=max_attempts,
            timeout=timeout,
            dedupe_key=dedupe_key,
            metadata=metadata,
            callback=callback,
            created_by=created_by,
        )
        row = new.row(self.settings)
        async with self._tx(session) as tx:
            # The conflicting task can finish between the insert that lost to it and the
            # read that looks for it, in which case the key is free again: insert once more.
            for _ in range(3):
                if created := await self._insert(tx, [row]):
                    task = created[0]
                    if task.status == TaskStatus.PENDING:
                        await self._notify(tx, CHANNEL_ENQUEUE, task.queue_name)
                    return task
                existing = (
                    await tx.execute(
                        select(*_t.c).where(
                            _t.c.queue_name == queue,
                            _t.c.name == name,
                            _t.c.dedupe_key == dedupe_key,
                            _t.c.status.in_(_ACTIVE),
                        )
                    )
                ).first()
                if existing is not None:
                    task = _task(existing)
                    task.created = False
                    return task
        raise DuplicateTaskError(f"could not enqueue {name!r} with dedupe_key {dedupe_key!r}")

    async def enqueue_many(
        self, tasks: Iterable[NewTask], *, session: AsyncSession | None = None
    ) -> builtins.list[Task]:
        """Add many tasks, one statement per `ENQUEUE_CHUNK`. Returns the ones created, in
        the order given.

        A task whose `dedupe_key` matched an active task (or an earlier task of the same
        batch) is skipped and is not in the result.
        """
        rows = [task.row(self.settings) for task in tasks]
        created: builtins.list[Task] = []
        async with self._tx(session) as tx:
            for start in range(0, len(rows), ENQUEUE_CHUNK):
                created += await self._insert(tx, rows[start : start + ENQUEUE_CHUNK])
            for queue in {t.queue_name for t in created if t.status == TaskStatus.PENDING}:
                await self._notify(tx, CHANNEL_ENQUEUE, queue)
        # Ids are minted in order, so sorting by id restores the order the tasks were given in.
        created.sort(key=lambda t: t.id)
        return created

    # ------------------------------------------------------------------ worker side

    async def claim(
        self,
        names: Sequence[str],
        *,
        worker_id: str,
        queue: str = "default",
        limit: int = 1,
    ) -> builtins.list[Task]:
        """Take up to `limit` ready tasks for `worker_id`, best priority first.

        Returns them already `running`. Safe to call from any number of workers at once:
        no task is returned twice.
        """
        if not names or limit < 1:
            return []
        async with self.db.transaction() as tx:
            result = await tx.execute(
                _CLAIM_SQL,
                {"names": list(names), "queue": queue, "limit": limit, "worker_id": worker_id},
            )
            tasks = [_task(row) for row in result]
        tasks.sort(key=lambda t: (t.priority, t.run_after, t.id))
        return tasks

    async def heartbeat(self, worker_id: str, task_ids: Sequence[uuid.UUID]) -> set[uuid.UUID]:
        """Say these tasks are alive. Returns the ones `worker_id` still owns.

        An id that is not returned was cancelled, or recovered as stale and possibly claimed
        by someone else: the worker should stop running it.
        """
        if not task_ids:
            return set()
        async with self.db.transaction() as tx:
            result = await tx.execute(
                update(_t)
                .where(
                    _t.c.id == any_(bindparam("ids", type_=ARRAY(Uuid))),
                    _t.c.status == TaskStatus.RUNNING,
                    _t.c.worker_id == worker_id,
                )
                .values(heartbeat_at=func.now())
                .returning(_t.c.id),
                {"ids": list(task_ids)},
            )
            return {row.id for row in result}

    def _owned(self, task_id: uuid.UUID, worker_id: str, attempt: int):
        return (
            _t.c.id == task_id,
            _t.c.status == TaskStatus.RUNNING,
            _t.c.worker_id == worker_id,
            _t.c.attempt == attempt,
        )

    async def succeed(
        self, task_id: uuid.UUID, *, worker_id: str, attempt: int, output: Any = None
    ) -> Task | None:
        """Finish a task. None when `worker_id` no longer owns it (nothing was written)."""
        async with self.db.transaction() as tx:
            row = (
                await tx.execute(
                    update(_t)
                    .where(*self._owned(task_id, worker_id, attempt))
                    .values(
                        status=TaskStatus.SUCCEEDED,
                        output=to_jsonable(output),
                        error=None,
                        completed_at=func.now(),
                        spent_seconds=_t.c.spent_seconds + _ELAPSED,
                    )
                    .returning(*_t.c)
                )
            ).first()
            return await self._settled(tx, row)

    async def fail(
        self,
        task_id: uuid.UUID,
        *,
        worker_id: str,
        attempt: int,
        error: dict[str, Any],
        retry_in: float | None = None,
    ) -> Task | None:
        """Record a failed attempt.

        With `retry_in` the task is scheduled to run again that many seconds from now;
        without it the task is `failed` for good. Whether an attempt is left is the caller's
        decision — see `Worker` — because only the caller knows what was raised.
        """
        if retry_in is None:
            values: dict[str, Any] = {"status": TaskStatus.FAILED, "completed_at": func.now()}
        else:
            values = self._reschedule_values(retry_in)
        async with self.db.transaction() as tx:
            row = (
                await tx.execute(
                    update(_t)
                    .where(*self._owned(task_id, worker_id, attempt))
                    .values(
                        error=to_jsonable(error),
                        spent_seconds=_t.c.spent_seconds + _ELAPSED,
                        **values,
                    )
                    .returning(*_t.c)
                )
            ).first()
            return await self._settled(tx, row)

    async def snooze(
        self, task_id: uuid.UUID, *, worker_id: str, attempt: int, delay: float
    ) -> Task | None:
        """Put a running task back to wait `delay` seconds, handing its attempt back."""
        async with self.db.transaction() as tx:
            row = (
                await tx.execute(
                    update(_t)
                    .where(*self._owned(task_id, worker_id, attempt))
                    .values(
                        attempt=func.greatest(_t.c.attempt - 1, 0),
                        spent_seconds=_t.c.spent_seconds + _ELAPSED,
                        **self._reschedule_values(delay),
                    )
                    .returning(*_t.c)
                )
            ).first()
            return await self._settled(tx, row)

    @staticmethod
    def _reschedule_values(delay: float) -> dict[str, Any]:
        if delay <= 0:
            return {"status": TaskStatus.PENDING, "run_after": func.now()}
        return {
            "status": TaskStatus.SCHEDULED,
            "run_after": func.now() + timedelta(seconds=delay),
        }

    async def release(self, worker_id: str, task_ids: Sequence[uuid.UUID] | None = None) -> int:
        """Hand running tasks back to the queue untouched: `pending`, attempt not counted.

        What a worker does with whatever is still running when it has to stop. Without
        `task_ids`, everything `worker_id` holds.
        """
        where = [_t.c.status == TaskStatus.RUNNING, _t.c.worker_id == worker_id]
        params: dict[str, Any] = {}
        if task_ids is not None:
            where.append(_t.c.id == any_(bindparam("ids", type_=ARRAY(Uuid))))
            params["ids"] = list(task_ids)
        async with self.db.transaction() as tx:
            result = await tx.execute(
                update(_t)
                .where(*where)
                .values(
                    status=TaskStatus.PENDING,
                    attempt=func.greatest(_t.c.attempt - 1, 0),
                    spent_seconds=_t.c.spent_seconds + _ELAPSED,
                )
                .returning(_t.c.queue_name),
                params,
            )
            queues = {row.queue_name for row in result}
            for queue in queues:
                await self._notify(tx, CHANNEL_ENQUEUE, queue)
            return result.rowcount

    async def set_progress(
        self,
        task_id: uuid.UUID,
        patch: dict[str, Any],
        *,
        worker_id: str,
        attempt: int,
        step: str | None = None,
    ) -> bool:
        """Merge `patch` into the task's progress (top-level keys). False if not owned."""
        values: dict[str, Any] = {
            "progress": _t.c.progress.op("||")(bindparam("patch", type_=JSONB))
        }
        if step is not None:
            values["step"] = step
        async with self.db.transaction() as tx:
            result = await tx.execute(
                update(_t).where(*self._owned(task_id, worker_id, attempt)).values(**values),
                {"patch": to_jsonable(patch)},
            )
            return result.rowcount == 1

    async def save_step(
        self, task_id: uuid.UUID, step: str, result: Any, *, worker_id: str, attempt: int
    ) -> bool:
        """Record a finished step's result under `progress.steps.<step>`."""
        steps = func.coalesce(_t.c.progress["steps"], text("'{}'::jsonb")).op("||")(
            bindparam("entry", type_=JSONB)
        )
        async with self.db.transaction() as tx:
            done = await tx.execute(
                update(_t)
                .where(*self._owned(task_id, worker_id, attempt))
                .values(progress=_t.c.progress.op("||")(func.jsonb_build_object("steps", steps))),
                {"entry": {step: to_jsonable(result)}},
            )
            return done.rowcount == 1

    # ------------------------------------------------------------------ control

    async def cancel(self, task_id: uuid.UUID) -> Task | None:
        """Cancel a task that has not finished. None if it already had (or does not exist).

        A running task is not interrupted by this statement: its worker finds out at its
        next heartbeat — or at once, if it is listening — and cancels the coroutine.
        """
        async with self.db.transaction() as tx:
            row = (
                await tx.execute(
                    update(_t)
                    .where(_t.c.id == task_id, _t.c.status.in_(_ACTIVE))
                    .values(
                        status=TaskStatus.CANCELED,
                        completed_at=func.now(),
                        spent_seconds=_t.c.spent_seconds
                        + case((_t.c.status == TaskStatus.RUNNING, _ELAPSED), else_=0),
                    )
                    .returning(*_t.c)
                )
            ).first()
            if row is not None:
                await self._notify(tx, CHANNEL_CANCEL, str(task_id))
            return await self._settled(tx, row)

    async def requeue(self, task_id: uuid.UUID, *, reset_progress: bool = False) -> Task | None:
        """Run a finished task again, in place. None unless it is in a terminal status.

        Attempts start over. `progress` is kept unless `reset_progress`, so an executor
        written with `ctx.step` resumes after the last step that completed.
        """
        values: dict[str, Any] = {
            # A held task goes back to being held: requeueing is not a way past the limit.
            "status": case((_t.c.capped, TaskStatus.SCHEDULED.value), else_=TaskStatus.PENDING),
            "attempt": 0,
            "run_after": func.now(),
            "error": None,
            "output": None,
            "step": None,
            "started_at": None,
            "heartbeat_at": None,
            "completed_at": None,
        }
        if reset_progress:
            values["progress"] = {}
        try:
            async with self.db.transaction() as tx:
                row = (
                    await tx.execute(
                        update(_t)
                        .where(_t.c.id == task_id, _t.c.status.in_(_TERMINAL))
                        .values(**values)
                        .returning(*_t.c)
                    )
                ).first()
                if row is None:
                    return None
                if row.status == TaskStatus.PENDING:
                    await self._notify(tx, CHANNEL_ENQUEUE, row.queue_name)
                return _task(row)
        except IntegrityError as exc:
            raise DuplicateTaskError(
                "another active task already holds this task's dedupe_key"
            ) from exc

    async def update_waiting(self, task_id: uuid.UUID, **fields: Any) -> Task | None:
        """Change a task that nothing has claimed yet. None if it is no longer waiting.

        Accepts `priority`, `run_after`, `payload`, `metadata`, `max_attempts` and
        `timeout_seconds`. The status is in the WHERE, so a worker claiming the row at the
        same moment either loses the race or wins it and leaves this returning None — an
        executor reads its payload once, and editing it under a running task would change
        nothing it acts on.
        """
        allowed = {
            "priority",
            "run_after",
            "payload",
            "metadata",
            "max_attempts",
            "timeout_seconds",
        }
        if unknown := set(fields) - allowed:
            raise ValueError(f"cannot update {sorted(unknown)}")
        values: dict[str, Any] = dict(fields)
        for key in ("payload", "metadata"):
            if key in values:
                values[key] = to_jsonable(values[key] or {})
        params: dict[str, Any] = {}
        if "run_after" in values:
            # Decided by the database's clock, like the same choice at enqueue. A held task
            # stays held, and one already let out goes back: a new time is a new wait.
            values["status"] = case(
                (_t.c.capped, "scheduled"),
                (bindparam("ra", type_=DateTime(timezone=True)) > func.now(), "scheduled"),
                else_="pending",
            )
            params["ra"] = values["run_after"]
        if not values:
            return await self.get(task_id)
        async with self.db.transaction() as tx:
            row = (
                await tx.execute(
                    update(_t)
                    .where(
                        _t.c.id == task_id,
                        _t.c.status.in_([TaskStatus.SCHEDULED, TaskStatus.PENDING]),
                    )
                    .values(**values)
                    .returning(*_t.c),
                    params,
                )
            ).first()
            if row is None:
                return None
            if row.status == TaskStatus.PENDING:
                await self._notify(tx, CHANNEL_ENQUEUE, row.queue_name)
            return _task(row)

    # ------------------------------------------------------------------ reads

    async def get(self, task_id: uuid.UUID) -> Task | None:
        async with self.db.sessionmaker() as session:
            row = (await session.execute(select(*_t.c).where(_t.c.id == task_id))).first()
            return _task(row) if row is not None else None

    async def list(
        self,
        *,
        queue: str | None = None,
        name: str | None = None,
        status: str | Sequence[str] | None = None,
        created_by: str | None = None,
        worker_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        queue_patterns: Sequence[str] | None = None,
        name_patterns: Sequence[str] | None = None,
        before: uuid.UUID | None = None,
        limit: int = 50,
    ) -> builtins.list[Task]:
        """Tasks newest first. Page with `before=<last id of the previous page>`.

        `queue_patterns` / `name_patterns` are glob lists (an account's ACL); `metadata`
        matches tasks whose metadata contains every given key and value.
        """
        stmt = select(*_t.c).order_by(_t.c.id.desc()).limit(limit)
        if queue is not None:
            stmt = stmt.where(_t.c.queue_name == queue)
        if name is not None:
            stmt = stmt.where(_t.c.name == name)
        if status is not None:
            statuses = [status] if isinstance(status, str) else list(status)
            stmt = stmt.where(_t.c.status.in_(statuses))
        if created_by is not None:
            stmt = stmt.where(_t.c.created_by == created_by)
        if worker_id is not None:
            stmt = stmt.where(_t.c.worker_id == worker_id)
        if metadata:
            stmt = stmt.where(_t.c.metadata.contains(to_jsonable(metadata)))
        for column, patterns in ((_t.c.queue_name, queue_patterns), (_t.c.name, name_patterns)):
            if patterns is not None and "*" not in patterns:
                likes = [glob_to_like(p) for p in patterns]
                stmt = stmt.where(column.like(any_(literal(likes, ARRAY(Text)))))
        if before is not None:
            stmt = stmt.where(_t.c.id < before)
        async with self.db.sessionmaker() as session:
            return [_task(row) for row in await session.execute(stmt)]

    async def find_active(
        self, name: str, dedupe_key: str, *, queue: str = "default"
    ) -> Task | None:
        """The active task holding a dedupe key, if there is one."""
        async with self.db.sessionmaker() as session:
            row = (
                await session.execute(
                    select(*_t.c).where(
                        _t.c.queue_name == queue,
                        _t.c.name == name,
                        _t.c.dedupe_key == dedupe_key,
                        _t.c.status.in_(_ACTIVE),
                    )
                )
            ).first()
            return _task(row) if row is not None else None

    # ------------------------------------------------------------------ logs

    async def append_logs(self, entries: Sequence[dict[str, Any]]) -> None:
        """Insert log lines. Each entry: task_id, attempt, level, message, and optionally
        step, data, created_at."""
        if not entries:
            return
        rows = [
            {
                "task_id": e["task_id"],
                "attempt": e.get("attempt", 0),
                "level": e.get("level", "info"),
                "step": e.get("step"),
                "message": e["message"],
                "data": e.get("data"),
                "created_at": e.get("created_at") or datetime.now().astimezone(),
            }
            for e in entries
        ]
        async with self.db.transaction() as tx:
            await tx.execute(_logs.insert(), rows)

    async def get_logs(
        self, task_id: uuid.UUID, *, after_id: int = 0, limit: int = 500
    ) -> builtins.list[TaskLog]:
        """A task's log lines, oldest first. Follow with `after_id=<last id seen>`."""
        async with self.db.sessionmaker() as session:
            result = await session.scalars(
                select(TaskLog)
                .where(TaskLog.task_id == task_id, TaskLog.id > after_id)
                .order_by(TaskLog.id)
                .limit(limit)
            )
            return list(result)

    # ------------------------------------------------------------------ housekeeping

    async def promote_due(self, batch: int = 1000) -> int:
        """Move `scheduled` tasks whose time has come to `pending`. Returns how many.

        Keeping not-yet-due tasks out of `pending` is what lets the claim query carry no
        time test: its index holds only tasks that can run *now*, so a million tasks
        scheduled for next week cost a claim nothing.
        """
        total = 0
        while True:
            async with self.db.transaction() as tx:
                locked = await tx.scalar(
                    text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": LOCK_PROMOTE}
                )
                if not locked:
                    return total
                result = await tx.execute(_PROMOTE_SQL, {"batch": batch})
                queues = [row.queue_name for row in result]
                for queue in set(queues):
                    await self._notify(tx, CHANNEL_ENQUEUE, queue)
            total += len(queues)
            if len(queues) < batch:
                return total

    async def release_capped(self, batch: int = 1000) -> int:
        """Move held tasks to `pending` as their accounts' running limits allow. Returns
        how many. One worker at a time: the others find the lock taken and return 0."""
        async with self.db.transaction() as tx:
            # Held with the transaction, so the count and the update are one decision.
            locked = await tx.scalar(
                text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": LOCK_RELEASE}
            )
            if not locked:
                return 0
            result = await tx.execute(
                _RELEASE_SQL,
                {"batch": batch, "default_max_running": self.settings.account_max_running},
            )
            queues = [row.queue_name for row in result]
            for queue in set(queues):
                await self._notify(tx, CHANNEL_ENQUEUE, queue)
        return len(queues)

    async def recover_stale(self, stale_after: float | None = None) -> builtins.list[Task]:
        """Re-queue (or fail) running tasks whose worker stopped heartbeating."""
        stale = self.settings.stale_after_seconds if stale_after is None else stale_after
        async with self.db.transaction() as tx:
            result = await tx.execute(_RECOVER_SQL, {"stale": float(stale)})
            tasks = [_task(row) for row in result]
            if not tasks:
                return []
            await tx.execute(
                _logs.insert(),
                [
                    {
                        "task_id": t.id,
                        "attempt": t.attempt,
                        "level": "error",
                        "message": (
                            f"worker {t.worker_id} stopped heartbeating; "
                            + ("re-queued" if t.status == TaskStatus.PENDING else "out of attempts")
                        ),
                    }
                    for t in tasks
                ],
            )
            for task in tasks:
                if task.status == TaskStatus.FAILED:
                    await self._enqueue_webhook(tx, task)
            for queue in {t.queue_name for t in tasks if t.status == TaskStatus.PENDING}:
                await self._notify(tx, CHANNEL_ENQUEUE, queue)
        return tasks

    async def prune(self, retention_days: int | None = None, batch: int = 5000) -> int:
        """Delete terminal tasks (and, by cascade, their logs) older than the retention."""
        days = self.settings.retention_days if retention_days is None else retention_days
        if days <= 0:
            return 0
        doomed = (
            select(_t.c.id)
            .where(
                _t.c.completed_at < func.now() - timedelta(days=days),
                _t.c.status.in_(_TERMINAL),
            )
            .limit(batch)
        )
        total = 0
        while True:
            async with self.db.transaction() as tx:
                result = await tx.execute(delete(_t).where(_t.c.id.in_(doomed)))
            total += result.rowcount
            if result.rowcount < batch:
                return total

    @asynccontextmanager
    async def try_lock(self, key: int) -> AsyncIterator[bool]:
        """Hold an advisory lock for the duration of the block, if nobody else has it.

        Yields whether it was acquired. For housekeeping that every worker is willing to do
        but only one needs to: the others see False and skip their turn.
        """
        async with self.db.transaction() as tx:
            yield bool(
                await tx.scalar(text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": key})
            )

    # ------------------------------------------------------------------ internals

    async def _settled(self, tx: AsyncSession, row: Any) -> Task | None:
        """After a status-changing UPDATE: the task, with whatever its new status triggers."""
        if row is None:
            return None
        task = _task(row)
        if task.status in _TERMINAL:
            await self._enqueue_webhook(tx, task)
        elif task.status == TaskStatus.PENDING:
            await self._notify(tx, CHANNEL_ENQUEUE, task.queue_name)
        return task

    async def _enqueue_webhook(self, tx: AsyncSession, task: Task) -> None:
        """Queue the delivery of `task.callback`, in the transaction that settled the task.

        A notification is itself a task (`tq.webhook`), which is the whole design: it is
        durable the instant the status is, it is retried with the same backoff as anything
        else, and every delivery attempt is a row and some log lines you can query.
        """
        callback = task.callback
        if not callback or not callback.get("url") or task.name == WEBHOOK_TASK:
            return
        event = _EVENT_BY_STATUS[task.status]
        if (events := callback.get("events")) and event.value not in events:
            return
        new = NewTask(
            WEBHOOK_TASK,
            {"task_id": str(task.id), "event": event.value},
            queue=task.queue_name,
            max_attempts=self.settings.webhook_max_attempts,
            timeout=self.settings.webhook_timeout_seconds + 5,
            metadata={"for_task": str(task.id), "for_name": task.name},
            created_by=task.created_by,
        )
        await self._insert(tx, [new.row(self.settings)])
        await self._notify(tx, CHANNEL_ENQUEUE, task.queue_name)

    def backoff(self, attempt: int) -> float:
        """Seconds to wait before the attempt after `attempt` (1-based)."""
        s = self.settings
        # The exponent is capped so a task on its 2000th attempt does not overflow a float.
        delay = s.retry_backoff_base_seconds * 2 ** min(max(attempt - 1, 0), 32)
        delay = min(delay, s.retry_backoff_max_seconds)
        # Jitter spreads out tasks that failed together (a dependency went down) so they do
        # not all come back in the same instant. Not a security use of randomness.
        return delay * (1 + random.random() * s.retry_backoff_jitter)
