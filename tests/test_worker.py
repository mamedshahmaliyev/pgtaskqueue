"""A real worker against a real database, one behaviour per test."""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel
from sqlalchemy import text

from pgtaskqueue import (
    Database,
    Executor,
    Monitor,
    NewTask,
    NonRetryable,
    QueueConfigError,
    Registry,
    Retry,
    Snooze,
    TaskContext,
    TaskQueue,
    TaskStopped,
    Worker,
    threads,
)
from pgtaskqueue.logs import LogBuffer
from pgtaskqueue.threads import run_in_thread
from pgtaskqueue.worker import Pool, PoolSpec, resolve_pools
from tests.helpers import Gauge, make_settings, running_worker, until, wait_for

app_log = logging.getLogger("tests.app")
app_log.setLevel(logging.INFO)


class AddPayload(BaseModel):
    a: int
    b: int = 0


async def logs_of(queue: TaskQueue, task_id) -> list[str]:
    return [line.message for line in await queue.get_logs(task_id)]


# --------------------------------------------------------------------------- pools


def test_pool_spec_parsing():
    assert PoolSpec.parse("image.resize:4") == PoolSpec("image.resize", 4)
    assert PoolSpec.parse(" image.* : 2 ") == PoolSpec("image.*", 2)
    assert PoolSpec.parse("image.resize") == PoolSpec("image.resize", None)
    # Task names may contain colons; only a trailing number is a slot count.
    assert PoolSpec.parse("ns:task") == PoolSpec("ns:task", None)
    assert PoolSpec.parse("ns:task:3") == PoolSpec("ns:task", 3)
    with pytest.raises(QueueConfigError, match="negative"):
        PoolSpec.parse("a:-1")


def test_resolve_pools():
    names = ["image.resize", "image.thumb", "mail.send", "report.build"]
    assert resolve_pools([], names, 10) == [Pool("ALL", tuple(names), 10)]
    assert resolve_pools([], [], 10) == []

    # Exact names first, then globs, then ALL takes the rest — whatever order they come in.
    assert resolve_pools(["ALL:20", "image.*:4", "image.resize:1"], names, 10) == [
        Pool("image.resize", ("image.resize",), 1),
        Pool("image.*", ("image.thumb",), 4),
        Pool("ALL", ("mail.send", "report.build"), 20),
    ]
    # Without ALL, only what is listed is consumed; a bare name gets the default slots.
    assert resolve_pools(["mail.send"], names, 7) == [Pool("mail.send", ("mail.send",), 7)]
    # `:0` excludes.
    assert resolve_pools(["image.*:0", "ALL:5"], names, 10) == [
        Pool("ALL", ("mail.send", "report.build"), 5)
    ]


@pytest.mark.parametrize(
    ("specs", "message"),
    [
        (["nope:1"], "no executor is registered"),
        (["video.*:1"], "matches no registered"),
        (["a:1", "a:2"], "listed twice"),
        (["ALL:1", "ALL:2"], "may be given once"),
    ],
)
def test_resolve_pools_rejects_mistakes(specs, message):
    with pytest.raises(QueueConfigError, match=message):
        resolve_pools(specs, ["a", "b"], 10)


async def test_worker_refuses_to_start_misconfigured(db: Database, settings):
    reserved = Registry()

    @reserved.task("tq.mine")
    async def mine(ctx, payload): ...

    with pytest.raises(QueueConfigError, match="reserved"):
        await Worker(db, registry=reserved, settings=settings, handle_signals=False).run()

    nothing = make_settings(webhook_slots=0)
    with pytest.raises(QueueConfigError, match="nothing to run"):
        await Worker(db, registry=Registry(), settings=nothing, handle_signals=False).run()


# --------------------------------------------------------------------------- running


async def test_runs_a_task_and_records_the_result(queue, registry, worker_factory):
    @registry.task("math.add")
    async def add(ctx: TaskContext, payload: AddPayload) -> dict:
        ctx.log("adding", data={"a": payload.a})
        return {"sum": payload.a + payload.b}

    async with worker_factory() as worker:
        task = await queue.enqueue("math.add", {"a": 2, "b": 3})
        done = await wait_for(queue, task.id)

    assert (done.status, done.output, done.attempt) == ("succeeded", {"sum": 5}, 1)
    assert done.worker_id == worker.id
    assert done.started_at <= done.completed_at
    assert worker.counters["claimed"] == worker.counters["succeeded"] == 1
    lines = await queue.get_logs(task.id)
    assert [(line.message, line.level, line.attempt, line.data) for line in lines] == [
        ("adding", "info", 1, {"a": 2})
    ]


async def test_invalid_payload_fails_without_retrying(queue, registry, worker_factory):
    @registry.task("math.add", max_attempts=5)
    async def add(ctx: TaskContext, payload: AddPayload) -> int:
        return payload.a

    async with worker_factory():
        task = await queue.enqueue("math.add", {"a": "not a number"})
        done = await wait_for(queue, task.id)
    assert (done.status, done.attempt) == ("failed", 1)
    assert done.error["type"] == "NonRetryable"
    assert "invalid payload for math.add" in done.error["message"]


async def test_output_that_is_not_json_fails_the_task(queue, registry, worker_factory):
    @registry.task("t.bad_output")
    async def bad(ctx, payload):
        return object()

    async with worker_factory():
        task = await queue.enqueue("t.bad_output")
        done = await wait_for(queue, task.id)
    assert done.status == "failed" and "not JSON" in done.error["message"]


async def test_sync_executor_runs_in_a_thread(queue, registry, worker_factory):
    @registry.register
    class Blocking(Executor):
        name = "t.sync"

        def run(self, ctx: TaskContext, payload: dict) -> dict:
            time.sleep(0.05)  # would stall every other task if it ran on the loop
            ctx.log("from a thread")
            ctx.from_thread(ctx.set_progress(percent=50))
            return {"on_main_thread": threading.current_thread() is threading.main_thread()}

    async with worker_factory():
        task = await queue.enqueue("t.sync")
        done = await wait_for(queue, task.id)
    assert done.output == {"on_main_thread": False}
    assert done.progress == {"percent": 50}
    assert await logs_of(queue, task.id) == ["from a thread"]


async def test_sync_function_task_runs_in_a_thread(queue, registry, worker_factory):
    @registry.task("sync.add")
    def add(ctx: TaskContext, payload: AddPayload) -> dict:
        on_main = threading.current_thread() is threading.main_thread()
        return {"sum": payload.a + payload.b, "on_main_thread": on_main}

    async with worker_factory():
        task = await queue.enqueue("sync.add", {"a": 2, "b": 3})
        done = await wait_for(queue, task.id)
    assert done.output == {"sum": 5, "on_main_thread": False}


async def test_scheduled_task_runs_when_due(queue, registry, worker_factory):
    @registry.task("t.later")
    async def later(ctx, payload):
        return "ran"

    async with worker_factory():
        task = await queue.enqueue("t.later", delay=0.4)
        assert task.status == "scheduled"
        done = await wait_for(queue, task.id)
    assert done.status == "succeeded"
    assert done.started_at >= task.run_after


async def test_child_task_knows_its_parent(queue, registry, worker_factory):
    @registry.task("t.parent")
    async def parent(ctx: TaskContext, payload: dict) -> str:
        child = await ctx.enqueue("t.child", {"n": 1}, metadata={"kind": "follow-up"})
        return str(child.id)

    @registry.task("t.child")
    async def child(ctx: TaskContext, payload: dict) -> dict:
        return ctx.metadata

    async with worker_factory():
        task = await queue.enqueue("t.parent", queue="default", created_by="alice")
        done = await wait_for(queue, task.id)
        (spawned,) = await queue.list(name="t.child")
        spawned = await wait_for(queue, spawned.id)
    assert str(spawned.id) == done.output
    assert spawned.output == {"parent_task_id": str(task.id), "kind": "follow-up"}
    assert spawned.created_by == "alice"


# --------------------------------------------------------------------------- concurrency


async def test_never_runs_more_than_its_slots(queue, registry, worker_factory):
    gauge = Gauge()

    @registry.task("t.sleep")
    async def sleep(ctx, payload):
        with gauge.held():
            await asyncio.sleep(0.15)

    tasks = await queue.enqueue_many(NewTask("t.sleep") for _ in range(12))
    async with worker_factory(slots=3):
        for task in tasks:
            assert (await wait_for(queue, task.id)).status == "succeeded"
    assert gauge.peak == 3


async def test_pools_limit_names_independently(queue, registry, worker_factory):
    heavy, light = Gauge(), Gauge()

    @registry.task("render.heavy")
    async def run_heavy(ctx, payload):
        with heavy.held():
            await asyncio.sleep(0.1)

    @registry.task("mail.light")
    async def run_light(ctx, payload):
        with light.held():
            await asyncio.sleep(0.1)

    tasks = await queue.enqueue_many(
        [*(NewTask("render.heavy") for _ in range(4)), *(NewTask("mail.light") for _ in range(8))]
    )
    async with worker_factory(pools=["render.*:1", "ALL:4"]) as worker:
        assert [(p.label, p.slots) for p in worker.pools][:2] == [("render.*", 1), ("ALL", 4)]
        for task in tasks:
            await wait_for(queue, task.id)
    assert (heavy.peak, light.peak) == (1, 4)


async def test_worker_only_claims_what_it_was_given(queue, registry, worker_factory):
    @registry.task("a.run")
    async def a(ctx, payload): ...

    @registry.task("b.run")
    async def b(ctx, payload): ...

    async with worker_factory(pools=["a.run:2"]):
        mine = await queue.enqueue("a.run")
        other = await queue.enqueue("b.run")
        elsewhere = await queue.enqueue("a.run", queue="other-queue")
        await wait_for(queue, mine.id)
        await asyncio.sleep(0.5)
    assert (await queue.get(other.id)).status == "pending"
    assert (await queue.get(elsewhere.id)).status == "pending"


async def test_one_failing_task_does_not_disturb_the_others(queue, registry, worker_factory):
    @registry.task("t.mixed", max_attempts=1)
    async def mixed(ctx, payload):
        await asyncio.sleep(0.05)
        if payload["i"] % 3 == 0:
            raise RuntimeError("this one fails")
        return payload["i"]

    tasks = await queue.enqueue_many(NewTask("t.mixed", {"i": i}) for i in range(30))
    async with worker_factory(slots=10) as worker:
        done = [await wait_for(queue, task.id) for task in tasks]
    assert [t.status for t in done] == ["failed" if i % 3 == 0 else "succeeded" for i in range(30)]
    assert (worker.counters["failed"], worker.counters["succeeded"]) == (10, 20)


async def test_behind_a_pooler_the_driver_caches_no_prepared_statements(settings):
    """Through a transaction-mode pooler the next statement may reach another server
    connection, where a statement prepared earlier does not exist."""
    pooled = Database(settings=settings, pgbouncer=True)
    try:
        async with pooled.engine.connect() as conn:
            for _ in range(3):
                assert await conn.scalar(text("SELECT 41 + :n"), {"n": 1}) == 42
            raw = await conn.get_raw_connection()
            assert raw.driver_connection._stmt_cache.get_max_size() == 0
    finally:
        await pooled.dispose()


async def test_behind_a_pooler_a_worker_polls_and_opens_no_listen_connection(
    queue, registry, settings
):
    """LISTEN through a transaction-mode pooler hears nothing, and says nothing about it;
    the worker must not rely on it."""

    @registry.task("t.quick")
    async def quick(ctx, payload):
        return "ok"

    pooled = Database(
        settings=settings.model_copy(update={"listen_database_url": None}), pgbouncer=True
    )
    assert not pooled.can_listen
    assert Database(settings=settings, pgbouncer=True, listen_url=settings.database_url).can_listen
    try:
        async with running_worker(pooled, registry, settings) as worker:
            task = await queue.enqueue("t.quick", {})
            done = await wait_for(queue, task.id)
            assert done.status == "succeeded"
            async with pooled.transaction() as tx:
                listening = await tx.scalar(
                    text("SELECT count(*) FROM pg_stat_activity WHERE application_name = :name"),
                    {"name": f"tq-listen {worker.id}"},
                )
            assert listening == 0
    finally:
        await pooled.dispose()


async def test_notify_wakes_an_idle_worker_long_before_its_poll(queue, registry, db):
    """With polling effectively off, only LISTEN/NOTIFY can explain a prompt start."""
    if not db.can_listen:
        pytest.skip("needs LISTEN: set TQ_LISTEN_DATABASE_URL when testing through a pooler")

    @registry.task("t.quick")
    async def quick(ctx, payload): ...

    slow_poll = make_settings(poll_interval_seconds=60)
    async with running_worker(db, registry, slow_poll):
        await asyncio.sleep(0.6)  # the first claim has come back empty; the pool is asleep
        started = time.monotonic()
        task = await queue.enqueue("t.quick")
        await wait_for(queue, task.id, timeout=5)
        assert time.monotonic() - started < 2


async def test_polling_alone_is_enough(queue, registry, db):
    @registry.task("t.quick")
    async def quick(ctx, payload): ...

    async with running_worker(db, registry, make_settings(notify=False)):
        await asyncio.sleep(0.3)
        task = await queue.enqueue("t.quick")
        assert (await wait_for(queue, task.id, timeout=5)).status == "succeeded"


async def test_paused_queue_is_left_alone(queue, registry, worker_factory, db):
    @registry.task("t.quick")
    async def quick(ctx, payload): ...

    monitor = Monitor(db)
    await monitor.set_paused("default", True)
    async with worker_factory():
        task = await queue.enqueue("t.quick")
        await asyncio.sleep(0.6)
        assert (await queue.get(task.id)).status == "pending"
        await monitor.set_paused("default", False)
        assert (await wait_for(queue, task.id)).status == "succeeded"


# --------------------------------------------------------------------------- failure


async def test_failure_is_retried_until_it_succeeds(queue, registry, worker_factory):
    @registry.task("t.flaky", max_attempts=4)
    async def flaky(ctx: TaskContext, payload: dict) -> int:
        if ctx.attempt < 3:
            raise ConnectionError(f"attempt {ctx.attempt} could not connect")
        return ctx.attempt

    async with worker_factory() as worker:
        # After the worker starts: the task type's max_attempts is applied at enqueue.
        task = await queue.enqueue("t.flaky")
        assert task.max_attempts == 4
        done = await wait_for(queue, task.id)

    assert (done.status, done.attempt, done.output) == ("succeeded", 3, 3)
    assert (worker.counters["retried"], worker.counters["succeeded"]) == (2, 1)
    lines = await queue.get_logs(task.id)
    assert [(line.attempt, line.level) for line in lines] == [(1, "warning"), (2, "warning")]
    assert "attempt 1/4 failed: ConnectionError: attempt 1 could not connect" in lines[0].message


async def test_attempts_run_out(queue, registry, worker_factory):
    @registry.task("t.broken", max_attempts=2)
    async def broken(ctx, payload):
        raise ValueError("always")

    async with worker_factory() as worker:
        task = await queue.enqueue("t.broken")
        done = await wait_for(queue, task.id)

    assert (done.status, done.attempt) == ("failed", 2)
    assert done.error["type"] == "ValueError" and done.error["message"] == "always"
    assert "ValueError: always" in done.error["traceback"]
    assert done.error["attempt"] == 2 and done.error["worker"] == worker.id
    assert (worker.counters["retried"], worker.counters["failed"]) == (1, 1)


async def test_non_retryable_fails_at_once(queue, registry, worker_factory):
    @registry.task("t.fatal", max_attempts=5)
    async def fatal(ctx, payload):
        raise NonRetryable("the input can never work")

    async with worker_factory():
        task = await queue.enqueue("t.fatal")
        done = await wait_for(queue, task.id)
    assert (done.status, done.attempt) == ("failed", 1)


async def test_retry_delay_is_chosen_by_the_exception_then_the_executor(
    queue, registry, worker_factory
):
    @registry.task("t.asks", max_attempts=3)
    async def asks(ctx, payload):
        raise Retry("rate limited", delay=120)

    @registry.register
    class Decides(Executor):
        name = "t.decides"
        max_attempts = 3

        async def run(self, ctx, payload):
            raise RuntimeError("down")

        def retry_delay(self, attempt: int, error: BaseException) -> float:
            return 300.0 * attempt

    async with worker_factory():
        first = await queue.enqueue("t.asks")
        second = await queue.enqueue("t.decides")
        first = await wait_for(queue, first.id, ["scheduled"])
        second = await wait_for(queue, second.id, ["scheduled"])
    now = datetime.now(UTC)
    assert timedelta(seconds=100) < first.run_after - now < timedelta(seconds=130)
    assert timedelta(seconds=280) < second.run_after - now < timedelta(seconds=310)
    assert first.error["type"] == "Retry"


async def test_timeout_fails_the_attempt(queue, registry, worker_factory):
    cancelled = asyncio.Event()

    @registry.task("t.slow", timeout=0.2, max_attempts=1)
    async def slow(ctx, payload):
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    @registry.task("t.upstream", max_attempts=1)
    async def upstream(ctx, payload):
        raise TimeoutError("the upstream call timed out")

    async with worker_factory():
        task = await queue.enqueue("t.slow")
        assert task.timeout_seconds == 0.2
        done = await wait_for(queue, task.id)
        other = await wait_for(queue, (await queue.enqueue("t.upstream")).id)

    assert cancelled.is_set()
    assert done.status == "failed"
    assert done.error["type"] == "TaskTimeout" and "0.2s" in done.error["message"]
    # A timeout of the executor's own making keeps its own name.
    assert other.error["type"] == "TimeoutError"


async def test_snooze_waits_without_using_an_attempt(queue, registry, worker_factory):
    seen: list[int] = []

    @registry.task("t.poll", max_attempts=1)
    async def poll(ctx: TaskContext, payload: dict) -> str:
        seen.append(ctx.attempt)
        if len(seen) < 3:
            raise Snooze(0.05, "the export is not ready")
        return "ready"

    async with worker_factory() as worker:
        task = await queue.enqueue("t.poll")
        done = await wait_for(queue, task.id)
    # Three runs, every one of them "attempt 1" of a task that allows only one.
    assert (done.status, done.attempt, seen) == ("succeeded", 1, [1, 1, 1])
    assert worker.counters["snoozed"] == 2


async def test_steps_resume_where_the_last_attempt_stopped(queue, registry, worker_factory):
    calls: list[str] = []

    @registry.task("t.pipeline", max_attempts=3)
    async def pipeline(ctx: TaskContext, payload: dict) -> int:
        async def download() -> dict:
            calls.append("download")
            return {"rows": 10}

        async def transform() -> int:
            calls.append("transform")
            if ctx.attempt == 1:
                raise RuntimeError("transform crashed")
            return 2

        data = await ctx.step("download", download)
        factor = await ctx.step("transform", transform)
        return data["rows"] * factor

    async with worker_factory():
        task = await queue.enqueue("t.pipeline")
        done = await wait_for(queue, task.id)

    assert (done.status, done.attempt, done.output) == ("succeeded", 2, 20)
    assert calls == ["download", "transform", "transform"]
    assert done.progress == {"steps": {"download": {"rows": 10}, "transform": 2}}
    assert done.step == "transform"
    lines = await queue.get_logs(task.id)
    assert "step 'download': reusing the result of an earlier attempt" in [
        line.message for line in lines if line.attempt == 2
    ]
    assert {line.step for line in lines} >= {"download", "transform"}


# --------------------------------------------------------------------------- logs


async def test_logging_inside_a_task_lands_in_that_tasks_log(queue, registry, worker_factory):
    @registry.task("t.logs")
    async def logs(ctx: TaskContext, payload: dict) -> None:
        app_log.info("%s starts", payload["who"])
        await asyncio.sleep(0.05)  # both tasks are mid-run and interleaved here
        app_log.warning("%s was slow", payload["who"])
        await asyncio.to_thread(app_log.info, "%s from a thread", payload["who"])
        app_log.debug("below the capture level")
        try:
            raise KeyError("missing")
        except KeyError:
            app_log.exception("%s caught one", payload["who"])

    async with worker_factory():
        a = await queue.enqueue("t.logs", {"who": "a"})
        b = await queue.enqueue("t.logs", {"who": "b"})
        await wait_for(queue, a.id)
        await wait_for(queue, b.id)

    for task, who in ((a, "a"), (b, "b")):
        lines = await queue.get_logs(task.id)
        assert [line.message.splitlines()[0] for line in lines] == [
            f"{who} starts",
            f"{who} was slow",
            f"{who} from a thread",
            f"{who} caught one",
        ]
        assert [line.level for line in lines] == ["info", "warning", "info", "error"]
        assert lines[0].data == {"logger": "tests.app"}
        assert "KeyError: 'missing'" in lines[3].message


async def test_long_log_lines_are_clipped(queue, registry, db):
    @registry.task("t.chatty")
    async def chatty(ctx: TaskContext, payload: dict) -> None:
        ctx.log("x" * 500)

    async with running_worker(db, registry, make_settings(log_message_max_chars=100)):
        task = await queue.enqueue("t.chatty")
        await wait_for(queue, task.id)
    (line,) = await logs_of(queue, task.id)
    assert len(line) == 100 and line.endswith("…")


@pytest.mark.parametrize("cancelled", ["before the insert", "after the commit"])
async def test_a_cancelled_flush_writes_its_lines_exactly_once(queue, db, settings, cancelled):
    """The flush loop is cancelled when its worker stops, possibly halfway through a write.
    It cannot tell whether that write was committed, so it must neither drop the batch
    (lost lines) nor put it back for the final flush (the same lines twice)."""
    writing, release = asyncio.Event(), asyncio.Event()

    class Slow(TaskQueue):
        async def append_logs(self, lines):
            if cancelled == "after the commit":
                await super().append_logs(lines)
            writing.set()
            await release.wait()
            if cancelled == "before the insert":
                await super().append_logs(lines)

    task = await queue.enqueue("t.any")
    buffer = LogBuffer(Slow(db, settings))
    buffer.add(task.id, 1, "one")
    loop_flush = asyncio.create_task(buffer.flush())
    await writing.wait()
    loop_flush.cancel()
    with pytest.raises(asyncio.CancelledError):
        await loop_flush

    buffer.add(task.id, 1, "two")
    final_flush = asyncio.create_task(buffer.flush())
    await asyncio.sleep(0.05)
    assert not final_flush.done()  # it waits for the write that is still under way
    release.set()
    await final_flush
    assert await logs_of(queue, task.id) == ["one", "two"]


# --------------------------------------------------------------------------- losing a task


async def test_cancel_stops_a_running_task(queue, registry, worker_factory):
    started, cancelled = asyncio.Event(), asyncio.Event()

    @registry.task("t.long")
    async def long(ctx, payload):
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async with worker_factory() as worker:
        task = await queue.enqueue("t.long")
        await asyncio.wait_for(started.wait(), 5)
        assert (await queue.cancel(task.id)).status == "canceled"
        await asyncio.wait_for(cancelled.wait(), 5)
        await until(lambda: worker.counters["canceled"] == 1)
        assert worker.in_flight == 0

    assert (await queue.get(task.id)).status == "canceled"
    assert "canceled while running; stopped" in await logs_of(queue, task.id)


async def test_cancel_does_not_wait_for_the_next_heartbeat(queue, registry, db):
    """The cancel NOTIFY triggers a heartbeat at once; without it this would take 20s."""
    if not db.can_listen:
        pytest.skip("needs LISTEN: set TQ_LISTEN_DATABASE_URL when testing through a pooler")
    started, cancelled = asyncio.Event(), asyncio.Event()

    @registry.task("t.long")
    async def long(ctx, payload):
        started.set()
        try:
            await asyncio.sleep(60)
        finally:
            cancelled.set()

    rare = make_settings(heartbeat_interval_seconds=20, stale_after_seconds=60)
    async with running_worker(db, registry, rare):
        task = await queue.enqueue("t.long")
        await asyncio.wait_for(started.wait(), 5)
        await asyncio.sleep(0.3)
        await queue.cancel(task.id)
        await asyncio.wait_for(cancelled.wait(), 3)


async def test_a_task_given_to_another_worker_is_dropped(queue, registry, worker_factory, db):
    started, cancelled = asyncio.Event(), asyncio.Event()

    @registry.task("t.long")
    async def long(ctx, payload):
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async with worker_factory(pools=["t.long:1"]) as worker:
        task = await queue.enqueue("t.long")
        await asyncio.wait_for(started.wait(), 5)
        # What stale recovery plus another worker's claim would leave behind.
        async with db.transaction() as tx:
            await tx.execute(
                text("UPDATE tq_tasks SET worker_id = 'someone-else', attempt = 2 WHERE id = :id"),
                {"id": task.id},
            )
        await asyncio.wait_for(cancelled.wait(), 5)
        await until(lambda: worker.counters["lost"] == 1)

    # The row belongs to the other worker now, and this one wrote nothing to it.
    current = await queue.get(task.id)
    assert (current.status, current.worker_id, current.attempt) == ("running", "someone-else", 2)


async def test_result_of_a_canceled_task_is_discarded(queue, registry, worker_factory):
    """An executor that swallows its cancellation and returns anyway."""
    started = asyncio.Event()

    @registry.task("t.stubborn")
    async def stubborn(ctx, payload):
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            return "finished anyway"

    async with worker_factory() as worker:
        task = await queue.enqueue("t.stubborn")
        await asyncio.wait_for(started.wait(), 5)
        await queue.cancel(task.id)
        await until(lambda: worker.counters["lost"] + worker.counters["canceled"] == 1)
    current = await queue.get(task.id)
    assert (current.status, current.output) == ("canceled", None)


async def test_tasks_of_a_dead_worker_are_recovered(queue, registry, worker_factory, db):
    @registry.task("t.work", max_attempts=3)
    async def work(ctx: TaskContext, payload: dict) -> int:
        return ctx.attempt

    monitor = Monitor(db)
    await monitor.register_task_types(registry.definitions(), registered_by="ghost")
    task = await queue.enqueue("t.work")
    (claimed,) = await queue.claim(["t.work"], worker_id="ghost")
    assert claimed.attempt == 1
    # "ghost" never heartbeats: after stale_after_seconds a live worker takes the task over.
    async with worker_factory() as worker:
        done = await wait_for(queue, task.id)
    assert (done.status, done.output, done.worker_id) == ("succeeded", 2, worker.id)
    assert any("stopped heartbeating" in line for line in await logs_of(queue, task.id))


# --------------------------------------------------------------------------- stopping


async def test_stop_waits_for_running_tasks(queue, registry, worker_factory):
    started = asyncio.Event()

    @registry.task("t.brief")
    async def brief(ctx, payload):
        started.set()
        await asyncio.sleep(0.4)
        return "finished"

    async with worker_factory():
        task = await queue.enqueue("t.brief")
        await asyncio.wait_for(started.wait(), 5)
    # Leaving the block stopped the worker while the task was mid-run.
    done = await queue.get(task.id)
    assert (done.status, done.output) == ("succeeded", "finished")


async def test_stop_releases_what_outlasts_the_grace_period(queue, registry, db):
    started = asyncio.Event()

    @registry.task("t.long")
    async def long(ctx, payload):
        started.set()
        await asyncio.sleep(30)

    impatient = make_settings(worker_shutdown_grace_seconds=0.2)
    async with running_worker(db, registry, impatient) as worker:
        task = await queue.enqueue("t.long")
        waiting = await queue.enqueue("t.long")
        await asyncio.wait_for(started.wait(), 5)
        stopping_at = time.monotonic()
    assert time.monotonic() - stopping_at < 3

    # Handed back with the attempt it was using, for the next worker to start over.
    released = await queue.get(task.id)
    assert (released.status, released.attempt) == ("pending", 0)
    assert worker.counters["released"] >= 1
    assert (await queue.get(waiting.id)).status == "pending"
    assert "worker is stopping; released back to the queue" in await logs_of(queue, task.id)


async def test_kill_does_not_wait(queue, registry, worker_factory):
    started = asyncio.Event()

    @registry.task("t.long")
    async def long(ctx, payload):
        started.set()
        await asyncio.sleep(30)

    async with worker_factory() as worker:
        task = await queue.enqueue("t.long")
        await asyncio.wait_for(started.wait(), 5)
        killed_at = time.monotonic()
        worker.kill()
        await until(lambda: worker.in_flight == 0, timeout=2)
    assert time.monotonic() - killed_at < 2
    assert (await queue.get(task.id)).status == "pending"


async def test_idle_exit_only_after_doing_some_work(queue, registry, db):
    @registry.task("t.quick")
    async def quick(ctx, payload): ...

    idle = make_settings(worker_idle_exit_seconds=0.4)
    worker = Worker(db, registry=registry, settings=idle, handle_signals=False)
    runner = asyncio.create_task(worker.run())
    try:
        # A worker that has run nothing stays up: restarting it would give nothing back.
        await asyncio.sleep(1.5)
        assert not runner.done()
        task = await queue.enqueue("t.quick")
        assert await asyncio.wait_for(runner, 10) == "idle"
    finally:
        worker.stop()
        await runner
    assert (await queue.get(task.id)).status == "succeeded"


async def test_max_tasks_is_exact(queue, registry, db):
    @registry.task("t.quick")
    async def quick(ctx, payload): ...

    @registry.task("u.quick")
    async def other(ctx, payload): ...

    await queue.enqueue_many(NewTask(name) for name in ["t.quick", "u.quick"] * 10)
    limited = make_settings(worker_max_tasks=7)
    # Two pools, so two claim loops compete for the one budget.
    worker = Worker(
        db, registry=registry, settings=limited, pools=["t.*:4", "u.*:4"], handle_signals=False
    )
    assert await asyncio.wait_for(worker.run(), 15) == "max_tasks"
    assert worker.counters["claimed"] == 7
    assert len(await queue.list(status="succeeded")) == 7
    assert len(await queue.list(status="pending")) == 13


# --------------------------------------------------------------------------- registry


async def test_worker_publishes_itself_and_its_task_types(queue, registry, worker_factory, db):
    @registry.task("math.add", description="Adds two numbers.", max_attempts=4, timeout=30)
    async def add(ctx: TaskContext, payload: AddPayload) -> int:
        return payload.a + payload.b

    monitor = Monitor(db)
    async with worker_factory(slots=6) as worker:
        # The factory returns once the pools exist, which is a moment before the row does.
        await until(monitor.list_workers, what="the worker to publish itself")
        (row,) = await monitor.list_workers()
        assert (row["id"], row["queue"], row["alive"]) == (worker.id, "default", True)
        assert row["slots"] == 6 + 2  # its pool, plus the two webhook slots every worker has
        assert row["pools"][0] == {"label": "ALL", "names": ["math.add"], "slots": 6}

        kind = await monitor.get_task_type("math.add")
        assert (kind.description, kind.max_attempts, kind.timeout_seconds) == (
            "Adds two numbers.",
            4,
            30.0,
        )
        assert kind.payload_schema["required"] == ["a"]
        assert kind.registered_by == worker.id

    assert await monitor.list_workers() == []
    (stopped,) = await monitor.list_workers(alive_only=False)
    assert stopped["alive"] is False and stopped["stopped_at"] is not None


def test_description_is_the_executors_own_docstring():
    class Documented(Executor):
        """Says what it does.

        And more.
        """

        name = "doc.yes"

    class Undocumented(Executor):
        name = "doc.no"

    class Explicit(Executor):
        """Ignored in favour of the attribute."""

        name = "doc.explicit"
        description = "Set by hand."

    assert Documented.definition()["description"] == "Says what it does.\n\nAnd more."
    # Not the docstring of `Executor`, which is what attribute lookup would find.
    assert Undocumented.definition()["description"] is None
    assert Explicit.definition()["description"] == "Set by hand."


def test_registry_rejects_two_executors_for_one_name():
    registry = Registry()

    @registry.register
    class First(Executor):
        name = "same.name"

    with pytest.raises(QueueConfigError, match="registered by both"):

        @registry.register
        class Second(Executor):
            name = "same.name"

    with pytest.raises(QueueConfigError, match="must set a task `name`"):

        @registry.register
        class Nameless(Executor):
            pass

    assert registry.names() == ["same.name"] and registry.match("same.*") == ["same.name"]


# --------------------------------------------------------------------------- stopping threads


def _spin(started: threading.Event, stopped: threading.Event) -> None:
    """Blocking work that never ends by itself, and says whether it was stopped."""
    started.set()
    try:
        while True:
            time.sleep(0.01)
    except TaskStopped:
        stopped.set()
        raise


async def test_cancel_stops_a_sync_executor_in_its_thread(queue, registry, worker_factory):
    """Without the exception raised in the thread, the task is canceled and the function
    goes on running for ever."""
    started, stopped = threading.Event(), threading.Event()

    @registry.task("t.spin")
    def spin(ctx, payload):
        _spin(started, stopped)

    async with worker_factory() as worker:
        task = await queue.enqueue("t.spin")
        await until(started.is_set)
        await queue.cancel(task.id)
        await until(stopped.is_set, what="the thread to be stopped")
        await until(lambda: worker.counters["canceled"] == 1)
    assert (await queue.get(task.id)).status == "canceled"


async def test_timeout_stops_a_sync_executor_in_its_thread(queue, registry, worker_factory):
    started, stopped = threading.Event(), threading.Event()

    @registry.task("t.spin", timeout=0.2, max_attempts=1)
    def spin(ctx, payload):
        _spin(started, stopped)

    async with worker_factory():
        task = await queue.enqueue("t.spin")
        done = await wait_for(queue, task.id)
        await until(stopped.is_set, what="the thread to be stopped")
    assert (done.status, done.error["type"]) == ("failed", "TaskTimeout")


async def test_cancel_stops_a_thread_an_async_executor_started(queue, registry, worker_factory):
    started, stopped = threading.Event(), threading.Event()
    seen: list[TaskContext] = []

    @registry.task("t.spin")
    async def spin(ctx, payload):
        seen.append(ctx)
        await ctx.to_thread(_spin, started, stopped)

    async with worker_factory():
        task = await queue.enqueue("t.spin")
        await until(started.is_set)
        assert not seen[0].stopped.is_set()
        await queue.cancel(task.id)
        await until(stopped.is_set, what="the thread to be stopped")
    assert seen[0].stopped.is_set()


async def test_a_stopping_worker_stops_its_threads(queue, registry, worker_factory):
    started, stopped = threading.Event(), threading.Event()

    @registry.task("t.spin")
    def spin(ctx, payload):
        _spin(started, stopped)

    async with worker_factory() as worker:
        task = await queue.enqueue("t.spin")
        await until(started.is_set)
        worker.kill()
        await until(stopped.is_set, what="the thread to be stopped")
    # Released with its attempt handed back, like any task a stopping worker gives up.
    assert (await queue.get(task.id)).status == "pending"


async def test_run_in_thread_returns_and_raises_what_the_function_does():
    assert await run_in_thread(lambda a, b: a + b, 2, 3) == 5
    with pytest.raises(ZeroDivisionError):
        await run_in_thread(lambda: 1 / 0)

    def gives_up():
        raise TaskStopped

    # A BaseException from task code would otherwise end the whole worker.
    with pytest.raises(RuntimeError, match="TaskStopped"):
        await run_in_thread(gives_up)


async def test_a_thread_that_has_finished_is_not_sent_an_exception(monkeypatch):
    """Stopping late must do nothing: the thread's id may be another thread's by then."""
    sent = []
    monkeypatch.setattr(threads, "_set_async_exc", lambda *args: sent.append(args))
    call = threads._Call(asyncio.get_running_loop(), lambda: 1, ())
    call.thread.start()
    assert await call.future == 1
    call.stop()
    assert sent == []
