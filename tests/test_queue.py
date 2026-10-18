"""The queue's SQL, one guarantee per test."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from pgtaskqueue import Database, DuplicateTaskError, NewTask, TaskQueue
from pgtaskqueue.monitor import Monitor
from pgtaskqueue.queue import glob_to_like

W = "worker-a"


async def claim_one(queue: TaskQueue, name: str = "t", worker_id: str = W):
    tasks = await queue.claim([name], worker_id=worker_id)
    assert len(tasks) == 1
    return tasks[0]


# --------------------------------------------------------------------------- enqueue


async def test_enqueue_defaults(queue: TaskQueue):
    task = await queue.enqueue("t", {"a": 1})
    assert task.status == "pending"
    assert task.payload == {"a": 1}
    assert (task.priority, task.attempt, task.max_attempts) == (100, 0, 3)
    assert task.created is True
    assert task.id.version == 7


async def test_delay_is_scheduled_then_promoted(queue: TaskQueue):
    task = await queue.enqueue("t", delay=0.3)
    assert task.status == "scheduled"
    # Not claimable while it waits, and not promoted before it is due.
    assert await queue.claim(["t"], worker_id=W) == []
    assert await queue.promote_due() == 0
    await asyncio.sleep(0.35)
    assert await queue.promote_due() == 1
    assert (await claim_one(queue)).id == task.id


async def test_run_after_in_the_past_is_pending(queue: TaskQueue):
    task = await queue.enqueue("t", run_after=datetime.now(UTC) - timedelta(hours=1))
    assert task.status == "pending"


async def test_run_after_must_be_aware_and_exclusive_with_delay(queue: TaskQueue):
    with pytest.raises(ValueError, match="timezone-aware"):
        await queue.enqueue("t", run_after=datetime(2030, 1, 1))
    with pytest.raises(ValueError, match="not both"):
        await queue.enqueue("t", run_after=datetime.now(UTC), delay=5)


async def test_task_type_defaults_apply_at_enqueue(queue: TaskQueue, db: Database):
    await Monitor(db).register_task_types(
        [{"name": "t", "description": None, "payload_schema": None, "max_attempts": 7,
          "timeout_seconds": 42.0}],
        registered_by="w",
    )  # fmt: skip
    assert (await queue.enqueue("t")).max_attempts == 7
    assert (await queue.enqueue("t")).timeout_seconds == 42.0
    # What the caller says wins over what the type registered.
    assert (await queue.enqueue("t", max_attempts=1, timeout=5)).max_attempts == 1


async def test_dedupe_returns_the_active_task(queue: TaskQueue):
    first = await queue.enqueue("t", dedupe_key="k")
    again = await queue.enqueue("t", dedupe_key="k")
    assert again.id == first.id
    assert again.created is False
    # The key is per (queue, name).
    assert (await queue.enqueue("other", dedupe_key="k")).id != first.id
    assert (await queue.enqueue("t", dedupe_key="k", queue="q2")).id != first.id


async def test_dedupe_key_is_free_once_the_task_finishes(queue: TaskQueue):
    first = await queue.enqueue("t", dedupe_key="k")
    claimed = await claim_one(queue)
    await queue.succeed(claimed.id, worker_id=W, attempt=claimed.attempt)
    second = await queue.enqueue("t", dedupe_key="k")
    assert second.id != first.id and second.created


async def test_enqueue_many_keeps_order_and_skips_duplicates(queue: TaskQueue):
    tasks = await queue.enqueue_many(
        NewTask("t", {"i": i}, dedupe_key=f"k{i % 3}") for i in range(7)
    )
    # The first of each key is the one kept.
    assert [t.payload["i"] for t in tasks] == [0, 1, 2]
    assert await queue.enqueue_many([]) == []


async def test_enqueue_in_callers_transaction(queue: TaskQueue, db: Database):
    """Enqueued inside a transaction that rolls back: the task never existed."""
    with pytest.raises(RuntimeError):
        async with db.transaction() as tx:
            task = await queue.enqueue("t", session=tx)
            raise RuntimeError("the business write failed")
    assert await queue.get(task.id) is None

    async with db.transaction() as tx:
        task = await queue.enqueue("t", session=tx)
        # Invisible to a claim until the commit.
        assert await queue.claim(["t"], worker_id=W) == []
    assert (await claim_one(queue)).id == task.id


async def test_nul_characters_are_stripped(queue: TaskQueue):
    task = await queue.enqueue("t", {"text": "a\x00b"}, metadata={"k": "\x00"})
    assert task.payload == {"text": "ab"} and task.meta == {"k": ""}


# --------------------------------------------------------------------------- claim


async def test_claim_order_is_priority_then_fifo(queue: TaskQueue):
    low = await queue.enqueue("t", priority=200)
    first = await queue.enqueue("t")
    second = await queue.enqueue("t")
    urgent = await queue.enqueue("t", priority=1)
    claimed = await queue.claim(["t"], worker_id=W, limit=10)
    assert [t.id for t in claimed] == [urgent.id, first.id, second.id, low.id]
    assert all(t.status == "running" and t.attempt == 1 and t.worker_id == W for t in claimed)


async def test_claim_orders_across_names(queue: TaskQueue):
    a = await queue.enqueue("a", priority=50)
    b = await queue.enqueue("b", priority=10)
    c = await queue.enqueue("c", priority=30)
    claimed = await queue.claim(["a", "b", "c"], worker_id=W, limit=2)
    assert [t.id for t in claimed] == [b.id, c.id]
    assert (await claim_one(queue, "a")).id == a.id


async def test_claim_respects_limit_names_and_queue(queue: TaskQueue):
    for _ in range(5):
        await queue.enqueue("t")
    await queue.enqueue("other")
    await queue.enqueue("t", queue="q2")
    assert len(await queue.claim(["t"], worker_id=W, limit=3)) == 3
    assert len(await queue.claim(["t"], worker_id=W, limit=10)) == 2
    assert await queue.claim(["missing"], worker_id=W) == []
    assert await queue.claim([], worker_id=W) == []
    assert len(await queue.claim(["t"], worker_id=W, queue="q2")) == 1


async def test_paused_queue_gives_nothing(queue: TaskQueue, db: Database):
    monitor = Monitor(db)
    await queue.enqueue("t")
    await monitor.set_paused("default", True, by="test", note="why")
    assert await queue.claim(["t"], worker_id=W) == []
    await monitor.set_paused("default", False)
    assert len(await queue.claim(["t"], worker_id=W)) == 1


async def test_concurrent_claims_never_deliver_twice(queue: TaskQueue):
    """The property everything else rests on."""
    total = 600
    await queue.enqueue_many(NewTask("t") for _ in range(total))

    async def claimer(i: int) -> list[uuid.UUID]:
        mine: list[uuid.UUID] = []
        while batch := await queue.claim(["t"], worker_id=f"w{i}", limit=7):
            mine += [t.id for t in batch]
        return mine

    results = await asyncio.gather(*(claimer(i) for i in range(16)))
    claimed = [task_id for mine in results for task_id in mine]
    assert len(claimed) == total
    assert len(set(claimed)) == total


# --------------------------------------------------------------------------- settle


async def test_succeed_records_output_and_time(queue: TaskQueue):
    await queue.enqueue("t")
    task = await claim_one(queue)
    done = await queue.succeed(task.id, worker_id=W, attempt=1, output={"n": 1})
    assert done.status == "succeeded" and done.output == {"n": 1}
    assert done.completed_at is not None and done.spent_seconds >= 0


async def test_fencing_rejects_a_stale_writer(queue: TaskQueue):
    """A worker presumed dead comes back after its task was given to another."""
    await queue.enqueue("t", max_attempts=5)
    old = await claim_one(queue, worker_id="old")
    assert [t.status for t in await queue.recover_stale(stale_after=0)] == ["pending"]
    new = await claim_one(queue, worker_id="new")
    assert new.attempt == 2

    # Every write the old worker can make is refused...
    assert await queue.succeed(old.id, worker_id="old", attempt=old.attempt) is None
    assert await queue.fail(old.id, worker_id="old", attempt=old.attempt, error={}) is None
    assert not await queue.set_progress(old.id, {"x": 1}, worker_id="old", attempt=old.attempt)
    assert await queue.heartbeat("old", [old.id]) == set()
    # ...including with the right worker id but the attempt it no longer holds.
    assert await queue.succeed(old.id, worker_id="new", attempt=1) is None
    # The new owner is untouched.
    assert (await queue.succeed(new.id, worker_id="new", attempt=2, output=1)).status == "succeeded"


async def test_fail_with_retry_schedules_the_next_attempt(queue: TaskQueue):
    await queue.enqueue("t")
    task = await claim_one(queue)
    failed = await queue.fail(
        task.id, worker_id=W, attempt=1, error={"type": "E", "message": "x\x00y"}, retry_in=30
    )
    assert failed.status == "scheduled"
    assert failed.error == {"type": "E", "message": "xy"}
    assert failed.run_after > datetime.now(UTC) + timedelta(seconds=25)
    assert failed.completed_at is None


async def test_fail_without_retry_is_terminal(queue: TaskQueue):
    await queue.enqueue("t")
    task = await claim_one(queue)
    failed = await queue.fail(task.id, worker_id=W, attempt=1, error={"type": "E"})
    assert failed.status == "failed" and failed.completed_at is not None


async def test_snooze_and_release_hand_the_attempt_back(queue: TaskQueue):
    await queue.enqueue("t")
    task = await claim_one(queue)
    snoozed = await queue.snooze(task.id, worker_id=W, attempt=1, delay=0)
    assert (snoozed.status, snoozed.attempt) == ("pending", 0)

    task = await claim_one(queue)
    assert task.attempt == 1
    assert await queue.release(W) == 1
    released = await queue.get(task.id)
    assert (released.status, released.attempt) == ("pending", 0)


async def test_progress_merges_and_steps_accumulate(queue: TaskQueue):
    await queue.enqueue("t")
    task = await claim_one(queue)
    own = {"worker_id": W, "attempt": 1}
    assert await queue.set_progress(task.id, {"percent": 10, "note": "a"}, step="one", **own)
    assert await queue.set_progress(task.id, {"percent": 20}, **own)
    assert await queue.save_step(task.id, "one", {"path": "/x"}, **own)
    assert await queue.save_step(task.id, "two", [1, 2], **own)
    current = await queue.get(task.id)
    assert current.step == "one"
    assert current.progress == {
        "percent": 20,
        "note": "a",
        "steps": {"one": {"path": "/x"}, "two": [1, 2]},
    }


# --------------------------------------------------------------------------- control


async def test_cancel(queue: TaskQueue):
    waiting = await queue.enqueue("t", delay=60)
    assert (await queue.cancel(waiting.id)).status == "canceled"
    assert await queue.cancel(waiting.id) is None  # already finished
    assert await queue.cancel(uuid.uuid4()) is None

    await queue.enqueue("t")
    running = await claim_one(queue)
    assert (await queue.cancel(running.id)).status == "canceled"
    # The worker learns from its heartbeat, and its result is refused.
    assert await queue.heartbeat(W, [running.id]) == set()
    assert await queue.succeed(running.id, worker_id=W, attempt=1) is None


async def test_requeue_keeps_progress_unless_reset(queue: TaskQueue):
    await queue.enqueue("t", max_attempts=1)
    task = await claim_one(queue)
    await queue.save_step(task.id, "one", 1, worker_id=W, attempt=1)
    assert await queue.requeue(task.id) is None  # still running
    await queue.fail(task.id, worker_id=W, attempt=1, error={"type": "E"})

    again = await queue.requeue(task.id)
    assert (again.status, again.attempt, again.error) == ("pending", 0, None)
    assert again.progress == {"steps": {"one": 1}}

    task = await claim_one(queue)
    await queue.succeed(task.id, worker_id=W, attempt=1)
    assert (await queue.requeue(task.id, reset_progress=True)).progress == {}


async def test_requeue_refuses_to_break_dedupe(queue: TaskQueue):
    first = await queue.enqueue("t", dedupe_key="k")
    await queue.cancel(first.id)
    await queue.enqueue("t", dedupe_key="k")
    with pytest.raises(DuplicateTaskError):
        await queue.requeue(first.id)


async def test_update_waiting(queue: TaskQueue):
    task = await queue.enqueue("t", {"a": 1})
    updated = await queue.update_waiting(task.id, priority=5, payload={"a": 2}, metadata={"x": 1})
    assert (updated.priority, updated.payload, updated.meta) == (5, {"a": 2}, {"x": 1})

    later = datetime.now(UTC) + timedelta(hours=1)
    assert (await queue.update_waiting(task.id, run_after=later)).status == "scheduled"
    # "Due" is judged by the database's clock, so use a time that is past on either clock.
    past = datetime.now(UTC) - timedelta(minutes=1)
    assert (await queue.update_waiting(task.id, run_after=past)).status == "pending"

    with pytest.raises(ValueError, match="cannot update"):
        await queue.update_waiting(task.id, status="succeeded")
    await claim_one(queue)
    assert await queue.update_waiting(task.id, priority=1) is None


# --------------------------------------------------------------------------- housekeeping


async def test_recover_stale_requeues_or_fails(queue: TaskQueue, db: Database):
    await queue.enqueue("t", max_attempts=2)
    await queue.enqueue("t", max_attempts=1, callback={"url": "http://127.0.0.1:9/hook"})
    await queue.enqueue("t")
    stale = await queue.claim(["t"], worker_id="dead", limit=2)
    alive = await claim_one(queue, worker_id="alive")
    async with db.transaction() as tx:
        await tx.execute(
            text("UPDATE tq_tasks SET heartbeat_at = now() - interval '5 minutes' "
                 "WHERE worker_id = 'dead'")
        )  # fmt: skip

    recovered = {t.id: t for t in await queue.recover_stale()}
    assert set(recovered) == {t.id for t in stale}
    assert sorted(t.status for t in recovered.values()) == ["failed", "pending"]
    failed = next(t for t in recovered.values() if t.status == "failed")
    assert failed.error["type"] == "WorkerLost"
    # The live task was left alone, and the one that ran out of attempts told its callback.
    assert (await queue.get(alive.id)).status == "running"
    hooks = await queue.list(name="tq.webhook")
    assert [h.payload["task_id"] for h in hooks] == [str(failed.id)]
    assert "stopped heartbeating" in (await queue.get_logs(failed.id))[0].message


async def test_prune_removes_old_finished_tasks_and_their_logs(queue: TaskQueue, db: Database):
    old = await queue.enqueue("t")
    await queue.cancel(old.id)
    await queue.append_logs([{"task_id": old.id, "message": "line"}])
    recent = await queue.enqueue("t")
    await queue.cancel(recent.id)
    waiting = await queue.enqueue("t")
    async with db.transaction() as tx:
        await tx.execute(
            text("UPDATE tq_tasks SET completed_at = now() - interval '40 days' WHERE id = :id"),
            {"id": old.id},
        )
    assert await queue.prune() == 0  # retention is off by default
    assert await queue.prune(retention_days=30) == 1
    assert await queue.get(old.id) is None
    assert await queue.get_logs(old.id) == []
    assert await queue.get(recent.id) and await queue.get(waiting.id)


# --------------------------------------------------------------------------- reads


async def test_list_filters_and_pages(queue: TaskQueue):
    for i in range(5):
        await queue.enqueue("email.send", metadata={"tenant": "acme", "i": i}, created_by="alice")
    await queue.enqueue("image.resize", queue="media", metadata={"tenant": "other"})
    canceled = await queue.enqueue("email.send", created_by="bob")
    await queue.cancel(canceled.id)

    assert len(await queue.list()) == 7
    assert len(await queue.list(name="email.send", status="pending")) == 5
    assert len(await queue.list(status=["canceled", "failed"])) == 1
    assert len(await queue.list(created_by="alice")) == 5
    assert len(await queue.list(metadata={"tenant": "acme"})) == 5
    assert len(await queue.list(metadata={"tenant": "acme", "i": 3})) == 1
    assert len(await queue.list(queue_patterns=["med*"])) == 1
    assert len(await queue.list(name_patterns=["email.*"], queue_patterns=["default"])) == 6
    assert await queue.list(name_patterns=[]) == []

    first = await queue.list(limit=3)
    second = await queue.list(limit=3, before=first[-1].id)
    third = await queue.list(limit=3, before=second[-1].id)
    ids = [t.id for t in (*first, *second, *third)]
    assert len(ids) == 7 and ids == sorted(ids, reverse=True)


def test_glob_to_like_escapes_like_wildcards():
    assert glob_to_like("media-*") == "media-%"
    assert glob_to_like("a_b%c?") == "a\\_b\\%c_"


async def test_logs_are_ordered_and_followable(queue: TaskQueue):
    task = await queue.enqueue("t")
    await queue.append_logs(
        [{"task_id": task.id, "message": f"line {i}", "level": "info"} for i in range(5)]
    )
    lines = await queue.get_logs(task.id)
    assert [line.message for line in lines] == [f"line {i}" for i in range(5)]
    assert [line.message for line in await queue.get_logs(task.id, after_id=lines[2].id)] == [
        "line 3",
        "line 4",
    ]


def test_backoff_doubles_and_is_capped(queue: TaskQueue):
    # base 0.05s, no jitter, in the test settings.
    assert [queue.backoff(n) for n in (1, 2, 3)] == [0.05, 0.1, 0.2]
    assert queue.backoff(500) == queue.settings.retry_backoff_max_seconds
