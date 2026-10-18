"""Periodic tasks: schedules that enqueue a task on a cron expression or a fixed interval.

    @task("report.daily", schedule=Schedule("0 3 * * *", timezone="Europe/Berlin"))
    async def daily_report(ctx, payload): ...

A schedule is a row in `tq_schedules`. Every worker looks for due rows once a second, and
firing one is a single transaction that enqueues the task **and** moves `next_run_at` on, so
a fire time is used once however many workers are looking. What is enqueued is an ordinary
task: it is claimed, retried, logged and delivered at least once like any other.

Three decisions shape the rest:

* **Time is the database's.** A row is due when `next_run_at <= now()`, and the next fire
  time is computed from the `now()` that query returned, never from the host's clock.
* **Missed fire times collapse into one.** After an outage, or a pause, one task is
  enqueued and the schedule continues from now. Nothing is backfilled.
* **One active task per schedule.** The task carries a `dedupe_key`, so if the previous run
  is still waiting or running when the next fire time comes, that fire is skipped. A slow
  consumer gets a late run, not a backlog.

docs/schedules.md is the guide.
"""

from __future__ import annotations

import builtins
import logging
import math
import re
from collections.abc import Sequence
from dataclasses import KW_ONLY, dataclass
from datetime import UTC, datetime, timedelta, tzinfo
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import bindparam, case, delete, func, literal_column, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from pgtaskqueue.cron import Cron
from pgtaskqueue.db import Database
from pgtaskqueue.errors import ScheduleConflict
from pgtaskqueue.jsonutil import to_jsonable
from pgtaskqueue.models import ScheduleRow, Task
from pgtaskqueue.monitor import matches
from pgtaskqueue.queue import NewTask, TaskQueue
from pgtaskqueue.settings import Settings, get_settings

logger = logging.getLogger("pgtaskqueue.schedules")

_s = ScheduleRow.__table__

SOURCE_CODE = "code"
SOURCE_API = "api"
# The same shape as a task name.
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:\-]{0,127}$")
# Shorter than the loop that fires schedules could keep up with.
MIN_EVERY_SECONDS = 1.0
# The dedupe key of a schedule's tasks: what keeps it to one active task at a time.
DEDUPE_PREFIX = "tq.schedule:"


@dataclass(frozen=True, slots=True)
class Schedule:
    """When a task enqueues itself. Give a cron expression or `every`, not both.

    Schedule("0 3 * * *", timezone="Europe/Berlin")
    Schedule(every=300, payload={"full": False})
    """

    # Five fields, or an alias such as `@hourly`. See pgtaskqueue/cron.py for what is supported.
    cron: str | None = None
    _: KW_ONLY
    # Seconds, or a timedelta. The first run is one interval after the schedule is created.
    every: float | timedelta | None = None
    # The IANA zone a cron expression's wall clock is read in. Ignored for `every`.
    timezone: str = "UTC"
    # Unique among schedules. Defaults to the task's name; a task with two schedules has to
    # name at least one of them.
    name: str | None = None
    payload: dict[str, Any] | None = None
    # None: the queue of the worker that publishes it.
    queue: str | None = None
    priority: int | None = None
    max_attempts: int | None = None
    timeout: float | None = None

    @property
    def every_seconds(self) -> float | None:
        if isinstance(self.every, timedelta):
            return self.every.total_seconds()
        return None if self.every is None else float(self.every)

    def validate(self) -> None:
        """Raise ValueError, in words meant for whoever wrote the schedule, if it cannot run."""
        if (self.cron is None) == (self.every is None):
            raise ValueError("give a cron expression or every, not both and not neither")
        if self.name is not None:
            check_name(self.name)
        if self.cron is not None:
            Cron.parse(self.cron)
            zone(self.timezone)
        elif not math.isfinite(every := self.every_seconds or 0) or every < MIN_EVERY_SECONDS:
            raise ValueError(f"every must be at least {MIN_EVERY_SECONDS:g} second")


@dataclass(frozen=True, slots=True)
class Fired:
    """One schedule that was due, and what came of it."""

    schedule: str
    # None when the fire was skipped because the previous run is still active.
    task: Task | None
    next_run_at: datetime


def check_name(name: str) -> None:
    if not _NAME.fullmatch(name):
        raise ValueError(
            f"{name!r} is not a schedule name: use letters, digits and . _ : - (128 at most)"
        )


def zone(name: str) -> tzinfo:
    # Without a lookup, so the default works on a host that has no timezone database.
    if name == "UTC":
        return UTC
    try:
        return ZoneInfo(name)
    except (KeyError, ValueError, OSError) as exc:
        raise ValueError(
            f"unknown timezone {name!r}: use an IANA name such as Europe/Berlin "
            "(the host needs the system timezone database or the tzdata package)"
        ) from exc


def next_run(
    *,
    cron: str | None,
    every_seconds: float | None,
    timezone: str = "UTC",
    after: datetime,
    previous: datetime | None = None,
) -> datetime:
    """The first fire time after `after`.

    For an interval, `previous` is the fire time it is counted from, so the schedule keeps
    its phase: one that fires at :00:07 goes on firing at :07 past, however late each
    fire was noticed, and fire times that were missed are stepped over.
    """
    if cron is not None:
        return Cron.parse(cron).after(after, zone(timezone))
    step = timedelta(seconds=every_seconds or 0)
    if previous is None or previous > after:
        return after + step
    return previous + step * ((after - previous) // step + 1)


# Due schedules, locked. `SKIP LOCKED` steps over a row another worker is firing right now;
# the `next_run_at <= now()` test is what Postgres re-checks (under READ COMMITTED) for a row
# that was due in this statement's snapshot but fired and committed before the lock was
# reached. Both halves are needed, as in the claim. `now()` is returned so that the next
# fire time is computed from the same instant that made the row due.
_DUE_SQL = text(
    """
    SELECT s.name, s.task_name, s.queue_name, s.payload, s.priority, s.max_attempts,
           s.timeout_seconds, s.cron, s.every_seconds, s.timezone, s.created_by,
           s.next_run_at, now() AS now
    FROM tq_schedules s
    WHERE NOT s.paused AND s.next_run_at <= now()
    ORDER BY s.next_run_at
    LIMIT :batch
    FOR UPDATE SKIP LOCKED
    """
)


class Schedules:
    def __init__(self, db: Database, settings: Settings | None = None) -> None:
        self.db = db
        self.settings = settings or get_settings()
        self.queue = TaskQueue(db, self.settings)

    # ------------------------------------------------------------------ write

    async def _upsert(
        self,
        tx: Any,
        now: datetime,
        name: str,
        task_name: str,
        schedule: Schedule,
        *,
        queue: str,
        source: str,
        created_by: str | None,
    ) -> ScheduleRow | None:
        """Create the schedule, or replace one of the same source. None when the name is
        held by a schedule of the other source, which is left as it was."""
        check_name(name)
        schedule.validate()
        values = {
            "name": name,
            "task_name": task_name,
            "queue_name": queue,
            "payload": to_jsonable(schedule.payload or {}),
            "priority": schedule.priority,
            "max_attempts": schedule.max_attempts,
            "timeout_seconds": schedule.timeout,
            "cron": schedule.cron,
            "every_seconds": schedule.every_seconds,
            "timezone": schedule.timezone if schedule.cron is not None else "UTC",
            "source": source,
            "created_by": created_by,
            "next_run_at": next_run(
                cron=schedule.cron,
                every_seconds=schedule.every_seconds,
                timezone=schedule.timezone,
                after=now,
            ),
        }
        stmt = pg_insert(ScheduleRow).values(values)
        new = stmt.excluded
        retimed = tuple_(_s.c.cron, _s.c.every_seconds, _s.c.timezone).is_distinct_from(
            tuple_(new.cron, new.every_seconds, new.timezone)
        )
        replaced = {key: new[key] for key in values if key not in ("name", "source", "next_run_at")}
        stmt = stmt.on_conflict_do_update(
            index_elements=[_s.c.name],
            set_={
                **replaced,
                # Only a change of timing starts the clock again. Otherwise a worker that
                # restarts more often than an interval would push its next run away for ever.
                "next_run_at": case((retimed, new.next_run_at), else_=_s.c.next_run_at),
                "updated_at": func.now(),
            },
            where=_s.c.source == new.source,
            # `xmax` is zero on a row this statement inserted and set on one it updated.
        ).returning(ScheduleRow, literal_column("(tq_schedules.xmax = 0)").label("inserted"))
        row = (await tx.execute(stmt)).first()
        if row is None:
            return None
        saved: ScheduleRow = row[0]
        saved.created = bool(row.inserted)
        return saved

    async def put(
        self,
        name: str,
        task_name: str,
        schedule: Schedule,
        *,
        queue: str | None = None,
        source: str = SOURCE_API,
        created_by: str | None = None,
    ) -> ScheduleRow:
        """Create a schedule or replace it whole; `created` on the result says which.

        `paused`, and `next_run_at` unless the timing changed, are kept on a replace.
        Raises ValueError for a schedule that cannot run and ScheduleConflict when the name
        belongs to a schedule declared in code.
        """
        async with self.db.transaction() as tx:
            now = await tx.scalar(select(func.now()))
            saved = await self._upsert(
                tx,
                now,
                name,
                task_name,
                schedule,
                queue=queue or schedule.queue or "default",
                source=source,
                created_by=created_by,
            )
        if saved is None:
            raise ScheduleConflict(
                f"schedule {name!r} is declared in code; change it there, or pause it"
                if source == SOURCE_API
                else f"schedule {name!r} was created through the API; delete it first"
            )
        return saved

    async def publish(
        self,
        declared: Sequence[tuple[str, Schedule]],
        *,
        queue: str,
        task_names: Sequence[str],
    ) -> builtins.list[str]:
        """Write the schedules a worker's executors declare: `(task name, schedule)` pairs.

        Last writer wins, as for task types, which is what a rolling deploy wants. A
        schedule declared in code earlier for one of `task_names` (the names this worker
        runs) and not declared now is deleted: removing it from the code removes it.

        Returns the names that were not written because a schedule created through the API
        holds them.
        """
        refused: builtins.list[str] = []
        # Sorted, so two workers starting together lock the rows in the same order.
        named = sorted(((s.name or task, task, s) for task, s in declared), key=lambda d: d[0])
        async with self.db.transaction() as tx:
            now = await tx.scalar(select(func.now()))
            for name, task_name, schedule in named:
                saved = await self._upsert(
                    tx,
                    now,
                    name,
                    task_name,
                    schedule,
                    queue=schedule.queue or queue,
                    source=SOURCE_CODE,
                    created_by=None,
                )
                if saved is None:
                    refused.append(name)
            await tx.execute(
                delete(_s).where(
                    _s.c.source == SOURCE_CODE,
                    _s.c.task_name.in_(list(task_names)),
                    _s.c.name.not_in([name for name, _, _ in named]),
                )
            )
        return refused

    async def set_paused(self, name: str, paused: bool) -> ScheduleRow | None:
        """Pause or resume. None if there is no such schedule.

        A paused schedule fires nothing. If a fire time passed while it was paused, it
        fires once when resumed, as it does after any other gap.
        """
        async with self.db.transaction() as tx:
            return await tx.scalar(
                update(ScheduleRow)
                .where(_s.c.name == name)
                .values(paused=paused, updated_at=func.now())
                .returning(ScheduleRow)
            )

    async def delete(self, name: str) -> bool:
        """Delete a schedule. The tasks it enqueued are untouched.

        One declared in code comes back when a worker that declares it next starts.
        """
        async with self.db.transaction() as tx:
            result = await tx.execute(delete(_s).where(_s.c.name == name))
            return result.rowcount == 1

    # ------------------------------------------------------------------ read

    async def get(self, name: str) -> ScheduleRow | None:
        async with self.db.sessionmaker() as session:
            return await session.get(ScheduleRow, name)

    async def list(
        self,
        *,
        queue_patterns: Sequence[str] | None = None,
        name_patterns: Sequence[str] | None = None,
    ) -> builtins.list[ScheduleRow]:
        """Every schedule, by name. The patterns are an account's ACL: globs for the queue
        and for the *task* name."""
        async with self.db.sessionmaker() as session:
            rows = await session.scalars(select(ScheduleRow).order_by(ScheduleRow.name))
            return [
                row
                for row in rows
                if matches(row.queue_name, queue_patterns) and matches(row.task_name, name_patterns)
            ]

    # ------------------------------------------------------------------ fire

    async def fire_due(self, batch: int = 100) -> builtins.list[Fired]:
        """Enqueue a task for every schedule whose time has come, and move each one on.

        Safe to call from any number of workers at once: a row is locked while it is fired,
        and the task, its NOTIFY and the new `next_run_at` commit together or not at all.
        """
        fired: builtins.list[Fired] = []
        while True:
            async with self.db.transaction() as tx:
                due = (await tx.execute(_DUE_SQL, {"batch": batch})).all()
                moves: dict[str, datetime] = {}
                tasks: builtins.list[NewTask] = []
                for row in due:
                    try:
                        moves[row.name] = next_run(
                            cron=row.cron,
                            every_seconds=row.every_seconds,
                            timezone=row.timezone,
                            after=row.now,
                            previous=row.next_run_at,
                        )
                    except ValueError as exc:
                        # Written by hand, or with a timezone this host does not have. Left
                        # as it is, for a worker that can read it or a person who can fix it.
                        logger.warning("schedule %s cannot be fired: %s", row.name, exc)
                        continue
                    tasks.append(
                        NewTask(
                            row.task_name,
                            row.payload,
                            queue=row.queue_name,
                            priority=row.priority,
                            max_attempts=row.max_attempts,
                            timeout=row.timeout_seconds,
                            dedupe_key=DEDUPE_PREFIX + row.name,
                            metadata={
                                "schedule": row.name,
                                "scheduled_for": row.next_run_at.isoformat(),
                            },
                            created_by=row.created_by,
                        )
                    )
                # In this transaction: the tasks become visible if and only if the
                # schedules move on. A task whose key is still held is not in the result.
                created = {
                    task.dedupe_key: task
                    for task in await self.queue.enqueue_many(tasks, session=tx)
                }
                if moves:
                    task_id = bindparam("task_id", type_=_s.c.last_task_id.type)
                    await tx.execute(
                        update(_s)
                        .where(_s.c.name == bindparam("schedule"))
                        .values(
                            next_run_at=bindparam("next"),
                            # A skipped fire moves the schedule on and records nothing.
                            last_run_at=case(
                                (task_id.is_not(None), func.now()), else_=_s.c.last_run_at
                            ),
                            last_task_id=func.coalesce(task_id, _s.c.last_task_id),
                        ),
                        [
                            {
                                "schedule": name,
                                "next": moved,
                                "task_id": getattr(created.get(DEDUPE_PREFIX + name), "id", None),
                            }
                            for name, moved in moves.items()
                        ],
                    )
            fired += [
                Fired(name, created.get(DEDUPE_PREFIX + name), moved)
                for name, moved in moves.items()
            ]
            # Counted in schedules moved, not rows read: a batch of rows that cannot be fired
            # would otherwise be read again for ever.
            if len(moves) < batch:
                return fired
