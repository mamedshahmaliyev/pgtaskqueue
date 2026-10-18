"""Moving finished tasks to the archive database: nothing lost, nothing moved early."""

from __future__ import annotations

import asyncio
import json
import random
import uuid

import pytest
from pydantic import ValidationError
from sqlalchemy import text

from pgtaskqueue import Archive, Database, Task, TaskQueue
from pgtaskqueue.archive import open_archive_database
from tests.helpers import TEST_ARCHIVE_DATABASE_URL, make_settings, until
from tests.test_cli import ENV, start_err, tq


async def finished(queue: TaskQueue, db: Database, *, days: float = 40, logs: int = 0) -> Task:
    """A canceled task that finished `days` ago, with `logs` log lines."""
    task = await queue.enqueue("t", {"n": 1}, metadata={"tenant": "acme"})
    await queue.cancel(task.id)
    if logs:
        await queue.append_logs(
            [{"task_id": task.id, "message": f"line {i}", "data": {"i": i}} for i in range(logs)]
        )
    async with db.transaction() as tx:
        # Test setup only: there is no other way to make a task old.
        await tx.execute(
            text(
                "UPDATE tq_tasks SET completed_at = now() - make_interval(days => :d) "
                "WHERE id = :id"
            ),
            {"id": task.id, "d": days},
        )
    return await queue.get(task.id)


async def test_move_takes_old_finished_tasks_and_their_logs(
    queue: TaskQueue, db: Database, archive: Archive
):
    old = await finished(queue, db, logs=3)
    recent = await finished(queue, db, days=1)
    waiting = await queue.enqueue("t")

    assert await archive.move() == 1  # TQ_ARCHIVE_AFTER_DAYS is 30

    assert await queue.get(old.id) is None
    assert await queue.get_logs(old.id) == []
    archived = await archive.get(old.id)
    assert (archived.status, archived.payload, archived.meta, archived.completed_at) == (
        "canceled",
        {"n": 1},
        {"tenant": "acme"},
        old.completed_at,
    )
    lines = await archive.get_logs(old.id)
    assert [(line.message, line.data) for line in lines] == [
        (f"line {i}", {"i": i}) for i in range(3)
    ]
    # Too recent, and not finished: both stay where they are.
    assert await queue.get(recent.id) and await queue.get(waiting.id)
    assert await archive.get(recent.id) is None and await archive.get(waiting.id) is None
    assert [t.id for t in await archive.list(status="canceled")] == [old.id]
    assert await archive.move() == 0


async def test_move_goes_batch_by_batch_until_none_are_left(
    queue: TaskQueue, db: Database, archive: Archive
):
    tasks = [await finished(queue, db) for _ in range(7)]
    assert await archive.move(batch=3, max_batches=2) == 6
    assert await archive.move(batch=3) == 1
    assert {t.id for t in await archive.list(limit=100)} == {t.id for t in tasks}
    assert await queue.list(limit=100) == []


async def test_nothing_is_deleted_unless_the_archive_has_it(
    queue: TaskQueue, db: Database, settings
):
    task = await finished(queue, db, logs=1)
    unreachable = settings.model_copy(
        update={"archive_database_url": TEST_ARCHIVE_DATABASE_URL + "_does_not_exist"}
    )
    archive_db = open_archive_database(unreachable, "tq-tests")
    try:
        with pytest.raises(Exception, match="does not exist"):
            await Archive(db, archive_db, settings).move()
    finally:
        await archive_db.dispose()
    assert (await queue.get(task.id)).status == "canceled"
    assert len(await queue.get_logs(task.id)) == 1


async def test_a_task_archived_twice_is_replaced_whole(
    queue: TaskQueue, db: Database, archive: Archive
):
    """The archive committed, the delete in the queue did not, and the task was requeued
    and finished again before the next batch: the newer copy wins, logs not doubled."""
    task = await finished(queue, db, logs=2)
    async with archive.archive_db.transaction() as tx:
        await tx.execute(
            text(
                "INSERT INTO tq_archive_tasks (id, name, status, archived_at) "
                "VALUES (:id, 't', 'failed', now() - interval '1 day')"
            ),
            {"id": task.id},
        )
        await tx.execute(
            text("INSERT INTO tq_archive_task_logs (id, task_id, message) VALUES (-1, :id, 'old')"),
            {"id": task.id},
        )
    assert await archive.move() == 1
    archived = await archive.get(task.id)
    assert (archived.status, archived.payload) == ("canceled", {"n": 1})
    assert [line.message for line in await archive.get_logs(task.id)] == ["line 0", "line 1"]


async def test_a_requeue_racing_the_archive_either_wins_or_finds_the_task_gone(
    queue: TaskQueue, db: Database, archive: Archive
):
    """Without the row locks, a task requeued between the archive's read and its delete
    is deleted while it is pending: gone from the queue, and the archive has the old copy."""
    tasks = [await finished(queue, db) for _ in range(120)]
    order = [t.id for t in tasks]
    random.shuffle(order)
    requeued: dict[uuid.UUID, Task | None] = {}

    async def requeue_some(ids: list[uuid.UUID]) -> None:
        for task_id in ids:
            requeued[task_id] = await queue.requeue(task_id)

    async with asyncio.TaskGroup() as tg:
        for i in range(6):
            tg.create_task(requeue_some(order[i::6]))
        moving = tg.create_task(archive.move(batch=4))

    for task_id, result in requeued.items():
        in_queue, in_archive = await queue.get(task_id), await archive.get(task_id)
        if result is not None:
            assert in_queue is not None and in_queue.status == "pending", task_id
            assert in_archive is None, task_id
        else:
            assert in_queue is None and in_archive is not None, task_id
    assert moving.result() == sum(1 for r in requeued.values() if r is None)


async def test_concurrent_archivers_move_each_task_once(
    queue: TaskQueue, db: Database, archive: Archive
):
    tasks = [await finished(queue, db, logs=1) for _ in range(40)]
    async with asyncio.TaskGroup() as tg:
        runs = [tg.create_task(archive.move(batch=3)) for _ in range(4)]
    # Whoever found the lock taken stopped early; the rest is moved by the next pass.
    moved = sum(run.result() for run in runs) + await archive.move()
    assert moved == len(tasks)
    assert len(await archive.list(limit=100)) == len(tasks)
    async with archive.archive_db.transaction() as tx:
        assert await tx.scalar(text("SELECT count(*) FROM tq_archive_task_logs")) == len(tasks)


async def test_a_worker_archives_in_the_background(
    queue: TaskQueue, db: Database, archive: Archive, registry, worker_factory
):
    @registry.task("t")
    async def t(ctx, payload):
        return None

    task = await finished(queue, db)
    settings = make_settings(
        archive_database_url=TEST_ARCHIVE_DATABASE_URL, archive_interval_seconds=0.1
    )
    async with worker_factory(settings=settings) as worker:
        assert worker.archive is not None
        await until(lambda: archive.get(task.id), what="the task to be archived")
    assert await queue.get(task.id) is None


async def test_archive_command(queue: TaskQueue, db: Database, archive: Archive):
    task = await finished(queue, db, logs=1)
    ENV["TQ_ARCHIVE_DATABASE_URL"] = TEST_ARCHIVE_DATABASE_URL
    try:
        assert "archived 0 finished task(s)" in await tq("archive", "run", "--older-than", "50")
        assert "archived 1 finished task(s)" in await tq("archive", "run")
        shown = await tq("archive", "show", str(task.id), "--logs")
        assert '"status": "canceled"' in shown and "line 0" in shown
        listed = json.loads(await tq("archive", "tasks", "--json"))
        assert [item["id"] for item in listed] == [str(task.id)]
        await tq("archive", "show", str(uuid.uuid4()), expect=1)
    finally:
        del ENV["TQ_ARCHIVE_DATABASE_URL"]
    assert "TQ_ARCHIVE_DATABASE_URL" in await start_err("archive", "run")


def test_retention_must_not_delete_what_the_archive_has_not_taken():
    with pytest.raises(ValidationError, match="TQ_RETENTION_DAYS"):
        make_settings(archive_database_url="postgresql+asyncpg://x/y", retention_days=30)
    make_settings(archive_database_url="postgresql+asyncpg://x/y", retention_days=400)
    make_settings(archive_database_url="", retention_days=30)
