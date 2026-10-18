"""Periodic tasks: the `tq_schedules` table, firing, and a worker that does both."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from pydantic import BaseModel
from sqlalchemy import text

from pgtaskqueue import (
    Database,
    Executor,
    QueueConfigError,
    Registry,
    Schedule,
    ScheduleConflict,
    Schedules,
    TaskQueue,
)
from tests.helpers import running_worker, until, wait_for

HOURLY = Schedule("0 * * * *")


@pytest.fixture
def schedules(db: Database, settings) -> Schedules:
    return Schedules(db, settings)


async def make_due(db: Database, *names: str, ago: float = 1.0) -> None:
    """Move schedules' next run into the past, which is what waiting would have done."""
    async with db.transaction() as tx:
        await tx.execute(
            text(
                "UPDATE tq_schedules SET next_run_at = now() - make_interval(secs => :ago) "
                "WHERE name = ANY(:names) OR cardinality(:names) = 0"
            ),
            {"ago": ago, "names": list(names)},
        )


# --------------------------------------------------------------------------- the table


async def test_put_creates_then_replaces(schedules: Schedules):
    first = await schedules.put(
        "nightly", "report.build", Schedule("0 3 * * *", timezone="Europe/Berlin"), created_by="al"
    )
    assert (first.created, first.source, first.queue_name, first.created_by) == (
        True,
        "api",
        "default",
        "al",
    )
    assert (first.cron, first.timezone, first.every_seconds) == ("0 3 * * *", "Europe/Berlin", None)
    assert first.next_run_at > first.created_at and first.last_run_at is None

    await schedules.set_paused("nightly", True)
    again = await schedules.put(
        "nightly",
        "report.build",
        Schedule("0 3 * * *", timezone="Europe/Berlin", payload={"full": True}, priority=5),
        queue="reports",
    )
    assert (again.created, again.payload, again.priority, again.queue_name) == (
        False,
        {"full": True},
        5,
        "reports",
    )
    # Replacing what it does leaves when it runs, and the operator's switch, alone.
    assert again.next_run_at == first.next_run_at and again.paused is True

    retimed = await schedules.put("nightly", "report.build", Schedule(every=timedelta(minutes=5)))
    assert (retimed.cron, retimed.every_seconds, retimed.timezone) == (None, 300.0, "UTC")
    assert retimed.next_run_at != first.next_run_at
    assert [s.name for s in await schedules.list()] == ["nightly"]


@pytest.mark.parametrize(
    ("name", "schedule", "complaint"),
    [
        ("ok", Schedule(), "not both and not neither"),
        ("ok", Schedule("0 * * * *", every=60), "not both and not neither"),
        ("ok", Schedule("61 * * * *"), "minute"),
        ("ok", Schedule("0 * * * *", timezone="Mars/Olympus"), "unknown timezone"),
        ("ok", Schedule(every=0.5), "at least 1 second"),
        ("ok", Schedule(every=float("inf")), "at least 1 second"),
        ("has spaces", HOURLY, "not a schedule name"),
    ],
)
async def test_a_schedule_that_cannot_run_is_refused(
    schedules: Schedules, name, schedule, complaint
):
    with pytest.raises(ValueError, match=complaint):
        await schedules.put(name, "t", schedule)
    assert await schedules.list() == []


async def test_code_and_api_schedules_do_not_overwrite_each_other(schedules: Schedules):
    await schedules.put("from-api", "t", HOURLY, created_by="al")
    refused = await schedules.publish(
        [("t", Schedule(every=60, name="from-api")), ("t", Schedule(every=60, name="from-code"))],
        queue="default",
        task_names=["t"],
    )
    assert refused == ["from-api"]
    kept = await schedules.get("from-api")
    assert (kept.source, kept.cron, kept.created_by) == ("api", "0 * * * *", "al")

    with pytest.raises(ScheduleConflict, match="declared in code"):
        await schedules.put("from-code", "t", HOURLY)
    assert (await schedules.get("from-code")).every_seconds == 60


async def test_publishing_again_keeps_the_clock_and_the_pause(schedules: Schedules, db):
    declared = [("sweep", Schedule(every=300))]
    await schedules.publish(declared, queue="default", task_names=["sweep"])
    first = await schedules.get("sweep")
    await schedules.set_paused("sweep", True)

    # What a restarting worker does. Were the clock to start again here, a worker
    # restarted every four minutes would never run a five-minute schedule.
    await schedules.publish(declared, queue="default", task_names=["sweep"])
    again = await schedules.get("sweep")
    assert again.next_run_at == first.next_run_at and again.paused is True

    await schedules.publish(
        [("sweep", Schedule(every=600, payload={"deep": True}))],
        queue="default",
        task_names=["sweep"],
    )
    changed = await schedules.get("sweep")
    assert changed.next_run_at > first.next_run_at and changed.payload == {"deep": True}


async def test_a_schedule_removed_from_the_code_is_removed(schedules: Schedules):
    await schedules.put("by-hand", "sweep", HOURLY)
    await schedules.publish(
        [("sweep", Schedule(every=60)), ("sweep", Schedule(every=90, name="sweep-deep"))],
        queue="default",
        task_names=["sweep"],
    )
    # Another worker, running other tasks: it removes nothing of the first one's.
    await schedules.publish([("mail", HOURLY)], queue="default", task_names=["mail"])
    assert [s.name for s in await schedules.list()] == ["by-hand", "mail", "sweep", "sweep-deep"]

    await schedules.publish([("sweep", Schedule(every=60))], queue="default", task_names=["sweep"])
    assert [s.name for s in await schedules.list()] == ["by-hand", "mail", "sweep"]


# --------------------------------------------------------------------------- firing


async def test_a_due_schedule_enqueues_its_task_and_moves_on(schedules, queue: TaskQueue, db):
    await schedules.put(
        "nightly",
        "report.build",
        Schedule(every=3600, payload={"full": True}, priority=7, max_attempts=2, timeout=30),
        queue="reports",
        created_by="al",
    )
    assert await schedules.fire_due() == []

    await make_due(db, "nightly")
    due = (await schedules.get("nightly")).next_run_at
    (fired,) = await schedules.fire_due()
    task = await queue.get(fired.task.id)
    assert (task.name, task.queue_name, task.status, task.payload) == (
        "report.build",
        "reports",
        "pending",
        {"full": True},
    )
    assert (task.priority, task.max_attempts, task.timeout_seconds, task.created_by) == (
        7,
        2,
        30.0,
        "al",
    )
    assert task.meta == {"schedule": "nightly", "scheduled_for": due.isoformat()}

    after = await schedules.get("nightly")
    assert after.next_run_at == fired.next_run_at == due + timedelta(hours=1)
    assert (after.last_task_id, after.last_run_at is not None) == (task.id, True)
    assert await schedules.fire_due() == []


async def test_missed_fire_times_become_one_task(schedules, queue: TaskQueue, db):
    await schedules.put("sweep", "sweep", Schedule(every=60))
    # No worker was up for a day.
    await make_due(db, "sweep", ago=86400)
    (fired,) = await schedules.fire_due()
    assert fired.task is not None
    assert await schedules.fire_due() == []
    assert len(await queue.list(name="sweep")) == 1
    async with db.transaction() as tx:
        ahead = await tx.scalar(
            text(
                "SELECT EXTRACT(EPOCH FROM next_run_at - now()) FROM tq_schedules "
                "WHERE name = 'sweep'"
            )
        )
    assert 0 < ahead <= 60


async def test_a_paused_schedule_fires_once_when_resumed(schedules, queue: TaskQueue, db):
    await schedules.put("sweep", "sweep", Schedule(every=60))
    await schedules.set_paused("sweep", True)
    await make_due(db, "sweep", ago=600)
    assert await schedules.fire_due() == []

    await schedules.set_paused("sweep", False)
    assert [f.schedule for f in await schedules.fire_due()] == ["sweep"]
    assert len(await queue.list(name="sweep")) == 1


async def test_a_fire_is_skipped_while_the_previous_task_is_active(schedules, queue, db):
    await schedules.put("sweep", "sweep", Schedule(every=60))
    await make_due(db, "sweep")
    (first,) = await schedules.fire_due()
    (running,) = await queue.claim(["sweep"], worker_id="w")

    await make_due(db, "sweep")
    (skipped,) = await schedules.fire_due()
    assert skipped.task is None
    row = await schedules.get("sweep")
    # It moved on all the same, and still points at the task that did run.
    assert row.next_run_at == skipped.next_run_at and row.last_task_id == first.task.id
    assert len(await queue.list(name="sweep")) == 1

    await queue.succeed(running.id, worker_id="w", attempt=1)
    await make_due(db, "sweep")
    (third,) = await schedules.fire_due()
    assert third.task is not None and third.task.id != first.task.id


async def test_concurrent_fires_use_each_fire_time_once(schedules: Schedules, db):
    """Every worker runs the loop, so this is the race that has to be lost safely."""
    total = 60
    for i in range(total):
        await schedules.put(f"s{i:02}", f"task.{i:02}", Schedule(every=3600))
    await make_due(db)

    results = await asyncio.gather(*(schedules.fire_due(batch=7) for _ in range(16)))
    fired = [fire for result in results for fire in result]
    # Counted in schedules moved on, not in tasks: the dedupe key would hide a second fire.
    assert len(fired) == total
    assert len({fire.schedule for fire in fired}) == total
    assert all(fire.task is not None for fire in fired)


async def test_a_row_that_cannot_be_read_does_not_stop_the_others(schedules, db, caplog):
    await schedules.put("good", "t", Schedule(every=60))
    await schedules.put("odd", "t2", HOURLY)
    async with db.transaction() as tx:
        await tx.execute(
            text("UPDATE tq_schedules SET timezone = 'Mars/Olympus' WHERE name = 'odd'")
        )
    await make_due(db)
    before = (await schedules.get("odd")).next_run_at

    assert [f.schedule for f in await schedules.fire_due()] == ["good"]
    assert (await schedules.get("odd")).next_run_at == before
    assert "schedule odd cannot be fired" in caplog.text


# --------------------------------------------------------------------------- declared in code


class ReportPayload(BaseModel):
    full: bool = False


def test_schedules_are_checked_when_the_executor_is_registered(registry: Registry):
    with pytest.raises(QueueConfigError, match="schedule of task 'a': minute"):
        registry.task("a", schedule=Schedule("99 * * * *"))(lambda ctx, payload: None)

    async def report(ctx, payload: ReportPayload): ...

    with pytest.raises(QueueConfigError, match="invalid payload"):
        registry.task("b", schedule=Schedule(every=60, payload={"full": "maybe"}))(report)

    class Twice(Executor):
        name = "c"
        schedules = (Schedule(every=60), Schedule(every=90))

    registry.register(Twice)
    with pytest.raises(QueueConfigError, match="two schedules are named 'c'"):
        registry.schedules()
    assert len(registry) == 1


async def test_a_worker_publishes_and_runs_what_its_executors_declare(
    queue: TaskQueue, registry: Registry, worker_factory, schedules: Schedules, db
):
    seen = []

    @registry.task(
        "report.build",
        schedule=[
            Schedule("0 3 * * *", timezone="Europe/Berlin", payload={"full": True}),
            Schedule(every=3600, name="report.hourly"),
        ],
    )
    async def build(ctx, payload: ReportPayload):
        seen.append((ctx.metadata["schedule"], payload.full))

    async with worker_factory(queue_name="reports"):
        await until(schedules.list, what="the worker to publish its schedules")
        rows = {s.name: s for s in await schedules.list()}
        assert set(rows) == {"report.build", "report.hourly"}
        nightly = rows["report.build"]
        # The worker's own queue, unless the schedule names one.
        assert (nightly.source, nightly.queue_name, nightly.task_name) == (
            "code",
            "reports",
            "report.build",
        )

        await make_due(db, "report.build")
        await until(lambda: seen, what="the scheduled task to run")
        assert seen == [("report.build", True)]
        row = await schedules.get("report.build")
        done = await wait_for(queue, row.last_task_id)
        assert (done.status, done.created_by) == ("succeeded", None)


async def test_any_worker_fires_any_schedule(queue, registry, db, settings, schedules):
    """Firing inserts a row and needs no executor, so a schedule does not wait for a worker
    of its own task, or of its own queue, to be the one that looks."""

    @registry.task("something.else")
    async def other(ctx, payload): ...

    await schedules.put("remote", "gpu.render", Schedule(every=3600), queue="gpu")
    await make_due(db, "remote")
    async with running_worker(db, registry, settings):
        await until(lambda: queue.list(name="gpu.render", queue="gpu"), what="the schedule to fire")
    (task,) = await queue.list(name="gpu.render")
    assert task.status == "pending"
