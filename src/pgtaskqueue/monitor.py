"""What is around the tasks: who is working, what they can run, which queues are paused —
and the numbers. Everything here is a plain query over the same tables the queue writes;
docs/analytics.md has more of them to run by hand.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Iterable, Sequence
from datetime import timedelta
from typing import Any

from sqlalchemy import delete, func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from pgtaskqueue.db import Database
from pgtaskqueue.enums import CHANNEL_ENQUEUE
from pgtaskqueue.models import Queue, TaskType, WorkerInfo
from pgtaskqueue.settings import Settings, get_settings

# A worker row is kept for a while after the worker is gone: "which worker ran this, and
# when did it die" is a question asked days later.
WORKER_ROW_RETENTION = timedelta(days=7)

# One branch per status, each written so the planner can answer it from that status's
# partial index instead of walking every row the table has ever held.
_ACTIVE_SQL = text(
    """
    SELECT queue_name, name, 'pending' AS status, count(*) AS n,
           EXTRACT(EPOCH FROM now() - min(run_after)) AS oldest_seconds
    FROM tq_tasks WHERE status = 'pending' GROUP BY queue_name, name
    UNION ALL
    SELECT queue_name, name, 'scheduled', count(*),
           EXTRACT(EPOCH FROM min(run_after) - now())
    FROM tq_tasks WHERE status = 'scheduled' AND NOT capped GROUP BY queue_name, name
    UNION ALL
    SELECT queue_name, name, 'held', count(*), NULL
    FROM tq_tasks WHERE status = 'scheduled' AND capped GROUP BY queue_name, name
    UNION ALL
    SELECT queue_name, name, 'running', count(*),
           EXTRACT(EPOCH FROM now() - min(started_at))
    FROM tq_tasks WHERE status = 'running' GROUP BY queue_name, name
    """
)

_WINDOW_SQL = text(
    """
    SELECT queue_name, name, status, count(*) AS n,
           avg(spent_seconds) AS avg_seconds,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY spent_seconds) AS p50_seconds,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY spent_seconds) AS p95_seconds,
           avg(GREATEST(EXTRACT(EPOCH FROM started_at - run_after), 0))
               FILTER (WHERE started_at IS NOT NULL) AS avg_wait_seconds,
           avg(attempt) AS avg_attempts
    FROM tq_tasks
    WHERE completed_at >= now() - make_interval(mins => :minutes)
    GROUP BY queue_name, name, status
    """
)

# Finished tasks counted into buckets of `step` seconds, numbered back from now: bucket 0 is
# the most recent `step` seconds. Measured from the database's `now()`, so the buckets do
# not depend on the clock of whoever asks. Reads `tq_tasks_completed_idx`, like the window.
_TIMELINE_SQL = text(
    """
    SELECT floor(EXTRACT(EPOCH FROM now() - completed_at) / :step)::int AS age,
           queue_name, name, status, count(*) AS n, now() AS now
    FROM tq_tasks
    WHERE completed_at >= now() - make_interval(secs => :step * :buckets)
    GROUP BY age, queue_name, name, status
    """
)


def matches(value: str, patterns: Sequence[str] | None) -> bool:
    """Whether `value` is allowed by a list of globs. None allows everything."""
    return patterns is None or any(fnmatch.fnmatchcase(value, p) for p in patterns)


class Monitor:
    def __init__(self, db: Database, settings: Settings | None = None) -> None:
        self.db = db
        self.settings = settings or get_settings()

    # ------------------------------------------------------------------ task types

    async def register_task_types(
        self, definitions: Iterable[dict[str, Any]], *, registered_by: str
    ) -> None:
        """Publish what a worker can run: description, payload schema, defaults.

        Last writer wins, which is what a rolling deploy wants: the newest worker's
        definition replaces the old one's.
        """
        # Sorted, so two workers starting together lock the rows in the same order.
        rows = sorted(
            ({**d, "registered_by": registered_by} for d in definitions), key=lambda d: d["name"]
        )
        if not rows:
            return
        stmt = pg_insert(TaskType).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=[TaskType.name],
            set_={
                "description": stmt.excluded.description,
                "payload_schema": stmt.excluded.payload_schema,
                "max_attempts": stmt.excluded.max_attempts,
                "timeout_seconds": stmt.excluded.timeout_seconds,
                "registered_by": stmt.excluded.registered_by,
                "updated_at": func.now(),
            },
        )
        async with self.db.transaction() as tx:
            await tx.execute(stmt)

    async def list_task_types(self, name_patterns: Sequence[str] | None = None) -> list[TaskType]:
        async with self.db.sessionmaker() as session:
            rows = await session.scalars(select(TaskType).order_by(TaskType.name))
            return [row for row in rows if matches(row.name, name_patterns)]

    async def get_task_type(self, name: str) -> TaskType | None:
        async with self.db.sessionmaker() as session:
            return await session.get(TaskType, name)

    # ------------------------------------------------------------------ queues

    async def set_paused(
        self, name: str, paused: bool, *, by: str | None = None, note: str | None = None
    ) -> Queue:
        """Pause or resume a queue. A paused queue keeps accepting tasks and finishes what
        is running; workers simply claim nothing from it until it is resumed."""
        stmt = pg_insert(Queue).values(name=name, paused=paused, note=note, updated_by=by)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Queue.name],
            set_={"paused": paused, "note": note, "updated_by": by, "updated_at": func.now()},
        ).returning(Queue)
        async with self.db.transaction() as tx:
            queue = (await tx.scalars(stmt)).one()
            if not paused and self.settings.notify:
                await tx.execute(
                    text("SELECT pg_notify(:channel, :payload)"),
                    {"channel": CHANNEL_ENQUEUE, "payload": name},
                )
            return queue

    async def list_queues(
        self, queue_patterns: Sequence[str] | None = None
    ) -> list[dict[str, Any]]:
        """Every queue that has a task waiting or running, a worker, or a pause switch."""
        queues: dict[str, dict[str, Any]] = {}

        def entry(name: str) -> dict[str, Any]:
            return queues.setdefault(
                name,
                {
                    "name": name,
                    "paused": False,
                    "note": None,
                    "scheduled": 0,
                    "held": 0,
                    "pending": 0,
                    "running": 0,
                    "workers": 0,
                    "slots": 0,
                },
            )

        async with self.db.sessionmaker() as session:
            for row in await session.execute(_ACTIVE_SQL):
                item = entry(row.queue_name)
                item[row.status] += row.n
                if row.status == "held":
                    item["scheduled"] += row.n
            for queue in await session.scalars(select(Queue)):
                entry(queue.name).update(paused=queue.paused, note=queue.note)
            for worker in await session.scalars(self._alive_workers()):
                item = entry(worker.queue_name)
                item["workers"] += 1
                item["slots"] += worker.slots
        return [queues[name] for name in sorted(queues) if matches(name, queue_patterns)]

    # ------------------------------------------------------------------ workers

    async def worker_started(self, worker_id: str, **info: Any) -> None:
        stmt = pg_insert(WorkerInfo).values(id=worker_id, **info)
        stmt = stmt.on_conflict_do_update(
            index_elements=[WorkerInfo.id],
            set_={
                **info,
                "started_at": func.now(),
                "heartbeat_at": func.now(),
                "stopped_at": None,
                "in_flight": 0,
                "counters": {},
            },
        )
        async with self.db.transaction() as tx:
            await tx.execute(stmt)

    async def worker_heartbeat(
        self, worker_id: str, *, in_flight: int, counters: dict[str, int]
    ) -> None:
        async with self.db.transaction() as tx:
            await tx.execute(
                update(WorkerInfo)
                .where(WorkerInfo.id == worker_id)
                .values(in_flight=in_flight, counters=dict(counters), heartbeat_at=func.now())
            )

    async def worker_stopped(
        self, worker_id: str, *, in_flight: int, counters: dict[str, int]
    ) -> None:
        async with self.db.transaction() as tx:
            await tx.execute(
                update(WorkerInfo)
                .where(WorkerInfo.id == worker_id)
                .values(
                    in_flight=in_flight,
                    counters=dict(counters),
                    heartbeat_at=func.now(),
                    stopped_at=func.now(),
                )
            )

    def _alive_after(self) -> Any:
        # A worker refreshes its row every maintenance interval; three misses is dead.
        return func.now() - timedelta(seconds=self.settings.maintenance_interval_seconds * 3)

    def _alive_workers(self) -> Any:
        return select(WorkerInfo).where(
            WorkerInfo.stopped_at.is_(None), WorkerInfo.heartbeat_at >= self._alive_after()
        )

    async def list_workers(
        self, *, alive_only: bool = True, queue_patterns: Sequence[str] | None = None
    ) -> list[dict[str, Any]]:
        alive = WorkerInfo.stopped_at.is_(None) & (WorkerInfo.heartbeat_at >= self._alive_after())
        stmt = select(WorkerInfo, alive.label("alive")).order_by(WorkerInfo.started_at.desc())
        if alive_only:
            stmt = stmt.where(alive)
        async with self.db.sessionmaker() as session:
            rows = (await session.execute(stmt)).all()
        return [
            {
                "id": worker.id,
                "queue": worker.queue_name,
                "hostname": worker.hostname,
                "pid": worker.pid,
                "pools": worker.pools,
                "slots": worker.slots,
                "in_flight": worker.in_flight,
                "counters": worker.counters,
                "version": worker.version,
                "alive": is_alive,
                "started_at": worker.started_at,
                "heartbeat_at": worker.heartbeat_at,
                "stopped_at": worker.stopped_at,
            }
            for worker, is_alive in rows
            if matches(worker.queue_name, queue_patterns)
        ]

    async def reap_workers(self) -> None:
        """Mark workers that vanished as stopped, and forget the long gone."""
        async with self.db.transaction() as tx:
            await tx.execute(
                update(WorkerInfo)
                .where(
                    WorkerInfo.stopped_at.is_(None), WorkerInfo.heartbeat_at < self._alive_after()
                )
                .values(stopped_at=WorkerInfo.heartbeat_at)
            )
            await tx.execute(
                delete(WorkerInfo).where(WorkerInfo.stopped_at < func.now() - WORKER_ROW_RETENTION)
            )

    # ------------------------------------------------------------------ numbers

    async def stats(
        self,
        *,
        window_minutes: int = 60,
        queue_patterns: Sequence[str] | None = None,
        name_patterns: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Where things stand now, and how the last `window_minutes` went, per queue and name.

        `oldest_pending_seconds` is the number to alert on: how long the task at the head
        of the line has been ready and not started.
        """
        groups: dict[tuple[str, str], dict[str, Any]] = {}

        def entry(queue: str, name: str) -> dict[str, Any]:
            return groups.setdefault(
                (queue, name),
                {
                    "queue": queue,
                    "name": name,
                    "scheduled": 0,
                    # Of `scheduled`, the tasks held back by their account's running limit.
                    "held": 0,
                    "pending": 0,
                    "running": 0,
                    "oldest_pending_seconds": None,
                    "longest_running_seconds": None,
                    "next_scheduled_in_seconds": None,
                    "window": {"succeeded": 0, "failed": 0, "canceled": 0},
                },
            )

        timing_keys = {
            "pending": "oldest_pending_seconds",
            "running": "longest_running_seconds",
            "scheduled": "next_scheduled_in_seconds",
        }
        async with self.db.sessionmaker() as session:
            active = (await session.execute(_ACTIVE_SQL)).all()
            window = (await session.execute(_WINDOW_SQL, {"minutes": window_minutes})).all()
        for row in active:
            if matches(row.queue_name, queue_patterns) and matches(row.name, name_patterns):
                item = entry(row.queue_name, row.name)
                if row.status == "held":
                    item["held"] = row.n
                    item["scheduled"] += row.n
                    continue
                item[row.status] += row.n
                item[timing_keys[row.status]] = _round(row.oldest_seconds)
        for row in window:
            if matches(row.queue_name, queue_patterns) and matches(row.name, name_patterns):
                item = entry(row.queue_name, row.name)["window"]
                item[row.status] = row.n
                if row.status == "succeeded":
                    item.update(
                        avg_seconds=_round(row.avg_seconds),
                        p50_seconds=_round(row.p50_seconds),
                        p95_seconds=_round(row.p95_seconds),
                        avg_wait_seconds=_round(row.avg_wait_seconds),
                        avg_attempts=_round(row.avg_attempts),
                    )
        tasks = [groups[key] for key in sorted(groups)]
        totals = {
            key: sum(item[key] for item in tasks)
            for key in ("scheduled", "held", "pending", "running")
        }
        for key in ("succeeded", "failed", "canceled"):
            totals[key] = sum(item["window"][key] for item in tasks)
        finished = totals["succeeded"] + totals["failed"] + totals["canceled"]
        totals["throughput_per_minute"] = round(finished / max(window_minutes, 1), 2)
        pending_ages = [
            item["oldest_pending_seconds"]
            for item in tasks
            if item["oldest_pending_seconds"] is not None
        ]
        totals["oldest_pending_seconds"] = max(pending_ages) if pending_ages else None
        return {"window_minutes": window_minutes, "totals": totals, "tasks": tasks}

    async def timeline(
        self,
        *,
        window_minutes: int = 60,
        buckets: int = 30,
        queue_patterns: Sequence[str] | None = None,
        name_patterns: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """How many tasks finished, and how, in each of `buckets` equal slices of the last
        `window_minutes`. Oldest slice first; the last one ends now."""
        step = window_minutes * 60 / buckets
        async with self.db.sessionmaker() as session:
            now = await session.scalar(select(func.now()))
            rows = (await session.execute(_TIMELINE_SQL, {"step": step, "buckets": buckets})).all()
        series = [
            {
                "start": now - timedelta(seconds=step * (age + 1)),
                "succeeded": 0,
                "failed": 0,
                "canceled": 0,
            }
            for age in reversed(range(buckets))
        ]
        for row in rows:
            # A task that finished between the two statements has a negative age.
            index = buckets - 1 - max(row.age, 0)
            if (
                index >= 0
                and matches(row.queue_name, queue_patterns)
                and matches(row.name, name_patterns)
            ):
                series[index][row.status] += row.n
        return {"window_minutes": window_minutes, "bucket_seconds": step, "buckets": series}


def _round(value: Any) -> float | None:
    return None if value is None else round(float(value), 3)
