"""Demonstration data: `tq seed` fills a queue so the web UI has something to show.

It writes three things:

* **accounts**, one per role a person signs in with, all with the same published password;
* **history**: finished tasks spread over the last hours, with logs, so the charts and the
  statistics have a past;
* **work in hand**: tasks waiting, scheduled and running, and one paused queue.

The task names are those of `examples/executors`, with payloads those executors accept, so
a worker started with them picks the waiting tasks up and the numbers start to move.

This is for a database you are trying the queue in. The accounts are a way in for anyone
who has read this file.
"""

from __future__ import annotations

import random
import secrets
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import bindparam, func, select, text
from sqlalchemy.dialects.postgresql import JSONB

from pgtaskqueue.db import Database
from pgtaskqueue.models import Task
from pgtaskqueue.monitor import Monitor
from pgtaskqueue.queue import NewTask, TaskQueue
from pgtaskqueue.settings import Settings, get_settings

DEMO_PASSWORD = "demo-password"  # noqa: S105 - published on purpose; see the module docstring
# (username, role, task-name ACL). The submitter's ACL shows what a narrowed account sees.
DEMO_USERS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("demo-admin", "admin", ("*",)),
    ("demo-manager", "manager", ("*",)),
    ("demo-operator", "operator", ("*",)),
    ("demo-submitter", "submitter", ("demo.*",)),
    ("demo-viewer", "viewer", ("*",)),
)
# The worker id the seeded "running" tasks are held by. No such worker exists, so they are
# recovered as lost, and run for real, once a worker's maintenance loop sees them.
SEED_WORKER = "seed-demo"
PAUSED_QUEUE = "bulk"


@dataclass(frozen=True, slots=True)
class _Kind:
    """One task name: how often it appears, how long it takes, how often it fails."""

    name: str
    weight: int
    seconds: float
    fail_rate: float
    error: tuple[str, str]

    def payload(self, rng: random.Random) -> dict[str, Any]:
        match self.name:
            case "demo.echo":
                return {"message": rng.choice(["hello", "ping", "good morning", "42"])}
            case "demo.sleep":
                return {"seconds": rng.choice([5, 10, 20, 30, 45])}
            case "demo.flaky":
                return {"fail_times": rng.choice([0, 1, 1, 2])}
            case "demo.fetch":
                return {"url": f"https://example.com/{rng.choice(['', 'a', 'b', 'status'])}"}
            case _:
                return {"rows": rng.choice([100, 500, 2000, 10000])}


KINDS = (
    _Kind("demo.echo", 30, 0.02, 0.0, ("", "")),
    _Kind("demo.sleep", 20, 12.0, 0.02, ("TaskTimeout", "the task ran past its timeout")),
    _Kind("demo.flaky", 15, 0.05, 0.25, ("RuntimeError", "attempt 3 failed on purpose")),
    _Kind("demo.fetch", 20, 0.6, 0.12, ("ConnectError", "All connection attempts failed")),
    _Kind("report.build", 15, 4.0, 0.06, ("RuntimeError", "load: the warehouse refused")),
)

# Finished rows are written as finished: there was no worker to run them an hour ago. Ages
# are seconds before the database's `now()`, so the history is placed by the same clock the
# statistics are read by.
_HISTORY_SQL = text(
    """
    INSERT INTO tq_tasks (
        id, queue_name, name, status, payload, metadata, output, error, step, attempt,
        max_attempts, run_after, created_by, worker_id, spent_seconds,
        created_at, started_at, completed_at
    )
    SELECT r.id, 'default', r.name, r.status, r.payload, r.metadata, r.output, r.error,
           r.step, r.attempt, GREATEST(r.attempt, 3), due.at, r.created_by, r.worker_id,
           r.spent, due.at,
           CASE WHEN r.attempt > 0 THEN now() - make_interval(secs => r.age + r.spent) END,
           now() - make_interval(secs => r.age)
    FROM jsonb_to_recordset(:rows) AS r(
        id uuid, name text, status text, payload jsonb, metadata jsonb, output jsonb,
        error jsonb, step text, attempt integer, created_by text, worker_id text,
        spent double precision, age double precision, wait double precision
    )
    CROSS JOIN LATERAL (
        SELECT now() - make_interval(secs => r.age + r.spent + r.wait) AS at
    ) AS due
    """
).bindparams(bindparam("rows", type_=JSONB))


def _uuid7_at(unix_ms: int, rng: random.Random) -> str:
    """A UUIDv7 for a moment in the past, so a seeded task sorts where its age puts it."""
    value = (
        (unix_ms & 0xFFFF_FFFF_FFFF) << 80
        | 0x7 << 76
        | rng.getrandbits(12) << 64
        | 0b10 << 62
        | secrets.randbits(62)
    )
    return str(uuid.UUID(int=value))


def _history(count: int, hours: float, rng: random.Random) -> tuple[list[dict], list[dict]]:
    """`count` finished tasks over the last `hours`, and their log lines."""
    now_ms = time.time_ns() // 1_000_000
    now = datetime.now().astimezone()
    tasks: list[dict[str, Any]] = []
    logs: list[dict[str, Any]] = []
    for _ in range(count):
        kind = rng.choices(KINDS, [k.weight for k in KINDS])[0]
        # Squaring pulls ages toward now: the recent past is busier than yesterday.
        age = hours * 3600 * rng.random() ** 2
        roll = rng.random()
        status = (
            "failed"
            if roll < kind.fail_rate
            else "canceled"
            if roll < kind.fail_rate + 0.03
            else "succeeded"
        )
        ran = status != "canceled" or rng.random() < 0.5
        spent = kind.seconds * rng.lognormvariate(0, 0.5) if ran else 0.0
        wait = rng.expovariate(1 / 1.5)
        attempt = 0 if not ran else (3 if status == "failed" else rng.choice([1, 1, 1, 1, 2]))
        # The submitter's ACL covers only `demo.*`, so it can have created only those.
        owners = ["demo-operator", "demo-admin", "cli"]
        if kind.name.startswith("demo."):
            owners += ["demo-submitter"] * 3
        task_id = _uuid7_at(now_ms - int((age + spent + wait) * 1000), rng)
        error = None
        if status == "failed":
            error = {"type": kind.error[0], "message": kind.error[1], "attempt": attempt}
        payload = kind.payload(rng)
        tasks.append(
            {
                "id": task_id,
                "name": kind.name,
                "status": status,
                "payload": payload,
                "metadata": {"seeded": True, "tenant": rng.choice(["acme", "globex", "initech"])},
                "output": {"ok": True, **payload} if status == "succeeded" else None,
                "error": error,
                "step": "load" if error and kind.name == "report.build" else None,
                "attempt": attempt,
                "created_by": rng.choice(owners),
                "worker_id": f"seed-worker-{rng.randint(1, 3)}" if ran else None,
                "spent": spent,
                "age": age,
                "wait": wait,
            }
        )
        if not ran:
            continue
        finished = now - timedelta(seconds=age)
        lines = [("info", "started", spent), ("info", f"payload accepted: {payload}", spent * 0.9)]
        if status == "succeeded":
            lines.append(("info", "done", 0.0))
        elif status == "failed":
            lines.append(("error", f"{kind.error[0]}: {kind.error[1]}", 0.0))
        else:
            lines.append(("warning", "canceled while running; stopped", 0.0))
        logs.extend(
            {
                "task_id": task_id,
                "attempt": attempt,
                "level": level,
                "message": message,
                "created_at": finished - timedelta(seconds=before),
            }
            for level, message, before in lines
        )
    return tasks, logs


async def _users(db: Database, password: str) -> list[str]:
    from pgtaskqueue.auth.users import UserError, UserService

    created = []
    service = UserService(db)
    for username, role, task_names in DEMO_USERS:
        try:
            await service.create(username, password, role=role, task_names=task_names)
        except UserError as exc:
            if "is taken" not in str(exc):
                raise
        else:
            created.append(username)
    return created


async def _live(queue: TaskQueue, monitor: Monitor, rng: random.Random) -> dict[str, int]:
    """Tasks that are waiting, scheduled and running, made through the queue's own calls."""
    labels = {"seeded": True}
    by_name = {kind.name: kind for kind in KINDS}

    def new(name: str, owner: str, **kwargs: Any) -> NewTask:
        return NewTask(
            name, by_name[name].payload(rng), metadata=labels, created_by=owner, **kwargs
        )

    pending = [
        new(rng.choice(["demo.echo", "demo.fetch", "demo.flaky"]), "demo-submitter")
        for _ in range(8)
    ]
    pending += [new("report.build", "demo-operator") for _ in range(4)]
    scheduled = [
        new(kind.name, "demo-operator", delay=rng.uniform(300, 4 * 3600)) for kind in KINDS
    ]
    parked = [new("report.build", "demo-operator", queue=PAUSED_QUEUE) for _ in range(6)]
    # Priority 0, so the claim below takes these three and not the tasks around them.
    to_run = [
        NewTask("demo.sleep", {"seconds": 60}, metadata=labels, created_by=owner, priority=0)
        for owner in ("demo-submitter", "demo-submitter", "demo-operator")
    ]

    await monitor.set_paused(PAUSED_QUEUE, True, by="tq seed", note="paused by tq seed")
    await queue.enqueue_many([*pending, *scheduled, *parked, *to_run])
    running = await queue.claim(["demo.sleep"], worker_id=SEED_WORKER, limit=len(to_run))
    for task in running:
        percent = rng.choice([10, 40, 70])
        await queue.set_progress(
            task.id, {"percent": percent}, worker_id=SEED_WORKER, attempt=task.attempt
        )
        await queue.append_logs(
            [
                {"task_id": task.id, "attempt": task.attempt, "message": f"{percent}% done"},
                {
                    "task_id": task.id,
                    "attempt": task.attempt,
                    "level": "warning",
                    "message": "seeded as running: no worker holds this task; it is re-queued "
                    "as lost once a worker's maintenance loop runs",
                },
            ]
        )
    return {
        "pending": len(pending) + len(to_run) - len(running),
        "scheduled": len(scheduled),
        "running": len(running),
        "parked": len(parked),
    }


async def seed(
    db: Database,
    settings: Settings | None = None,
    *,
    tasks: int = 400,
    hours: float = 24,
    users: bool = True,
    password: str = DEMO_PASSWORD,
    force: bool = False,
    rng: random.Random | None = None,
) -> dict[str, Any]:
    """Fill the database with demonstration data. Returns what was written.

    Refuses a database that already holds tasks unless `force`: seeding is for an empty
    queue, and demonstration rows mixed into real ones are hard to tell apart later.
    """
    settings = settings or get_settings()
    rng = rng or random.Random()
    queue, monitor = TaskQueue(db, settings), Monitor(db, settings)
    async with db.sessionmaker() as session:
        existing = await session.scalar(select(func.count()).select_from(Task))
    if existing and not force:
        raise RuntimeError(
            f"the database already holds {existing} tasks. Seed an empty one, or pass --force."
        )
    created = await _users(db, password) if users else []
    history, logs = _history(tasks, hours, rng)
    if history:
        async with db.transaction() as tx:
            await tx.execute(_HISTORY_SQL, {"rows": history})
        await queue.append_logs(logs)
    live = await _live(queue, monitor, rng)
    return {"users": created, "history": len(history), "logs": len(logs), **live}
