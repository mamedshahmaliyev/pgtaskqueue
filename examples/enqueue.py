"""Enqueueing from your own code, straight to the database — no API in between.

With a worker running in another terminal:

    tq worker --executors examples.executors.basic
    python -m examples.enqueue
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from pgtaskqueue import Database, NewTask, TaskQueue
from pgtaskqueue.enums import TERMINAL_STATUSES


async def main() -> None:
    db = Database(application_name="example-enqueue")
    queue = TaskQueue(db)
    try:
        # One task. `metadata` is yours: labels to find it by later.
        task = await queue.enqueue(
            "demo.echo", {"message": "hello"}, metadata={"tenant": "acme", "order": 1001}
        )
        print("enqueued", task.id, task.status)

        # Lower priority numbers run first; `delay` and `run_after` hold a task back.
        await queue.enqueue("demo.echo", {"message": "urgent"}, priority=10)
        await queue.enqueue("demo.echo", {"message": "in a minute"}, delay=60)
        tomorrow = datetime.now(UTC) + timedelta(days=1)
        await queue.enqueue("demo.echo", {"message": "tomorrow"}, run_after=tomorrow)

        # `dedupe_key`: while a task with this name and key is waiting or running, enqueueing
        # it again returns that task instead of adding another. Safe to call twice.
        first = await queue.enqueue("demo.sleep", {"seconds": 2}, dedupe_key="nightly-sync")
        again = await queue.enqueue("demo.sleep", {"seconds": 2}, dedupe_key="nightly-sync")
        print("deduplicated:", again.id == first.id, "created:", again.created)

        # Thousands at once: one INSERT, one round trip.
        batch = await queue.enqueue_many(
            NewTask("demo.sleep", {"seconds": 0.2}, metadata={"batch": "example"})
            for _ in range(200)
        )
        print("batch of", len(batch))

        # The reason to keep a queue in your application's database: the task is written in
        # the *same transaction* as the data it is about. Both commit or neither does — no
        # order without its confirmation email, no email for an order that rolled back.
        async with db.transaction() as tx:
            await tx.execute(text("CREATE TEMP TABLE orders (id int, total int) ON COMMIT DROP"))
            await tx.execute(text("INSERT INTO orders VALUES (1001, 5990)"))
            await queue.enqueue("demo.echo", {"message": "order 1001 confirmed"}, session=tx)

        # Follow one to the end. (Over HTTP, `GET /v1/tasks/{id}?wait=30` does this for you.)
        while task.status not in TERMINAL_STATUSES:
            await asyncio.sleep(0.2)
            task = await queue.get(task.id)
        print(task.status, task.output)
        for line in await queue.get_logs(task.id):
            print(f"  [{line.level}] {line.message}")

        # Everything is a row, so "what happened to acme's tasks" is a query.
        for found in await queue.list(metadata={"tenant": "acme"}, limit=5):
            print(found.id, found.name, found.status)
    finally:
        await db.dispose()


if __name__ == "__main__":
    asyncio.run(main())
