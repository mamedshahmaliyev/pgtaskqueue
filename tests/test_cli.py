"""The `tq` command, run as a user runs it: a separate process against the database."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import uuid
from pathlib import Path

from tests.helpers import JWT_SECRET, TEST_DATABASE_URL, until, wait_for

ROOT = Path(__file__).resolve().parent.parent
ENV = {
    **os.environ,
    "TQ_DATABASE_URL": TEST_DATABASE_URL,
    "TQ_JWT_SECRET": JWT_SECRET,
    "TQ_POLL_INTERVAL_SECONDS": "0.2",
    "TQ_WORKER_SHUTDOWN_GRACE_SECONDS": "1",
    "PYTHONUNBUFFERED": "1",
}


async def start(*args: str) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        sys.executable, "-m", "pgtaskqueue", *args,
        cwd=ROOT, env=ENV, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )  # fmt: skip


async def tq(*args: str, expect: int = 0) -> str:
    process = await start(*args)
    out, err = await asyncio.wait_for(process.communicate(), 60)
    assert process.returncode == expect, f"tq {' '.join(args)}\n{out.decode()}\n{err.decode()}"
    return out.decode()


async def start_err(*args: str) -> str:
    """Run a command that is expected to refuse; return what it said on stderr."""
    process = await start(*args)
    _, err = await asyncio.wait_for(process.communicate(), 60)
    assert process.returncode == 1, err.decode()
    return err.decode()


async def test_enqueue_inspect_and_control(queue):
    out = await tq("enqueue", "mail.send", '{"to": "a@example.com"}', "--priority", "5")
    task_id, status = out.split()
    assert status == "pending"
    task = await queue.get(uuid.UUID(task_id))
    assert (task.payload, task.priority, task.created_by) == ({"to": "a@example.com"}, 5, "cli")

    again = await tq("enqueue", "mail.send", "--dedupe-key", "k")
    assert "(already queued)" in await tq("enqueue", "mail.send", "--dedupe-key", "k")
    assert again.split()[0] in await tq("tasks", "--status", "pending")
    listed = json.loads(await tq("tasks", "--json", "--name", "mail.send"))
    assert len(listed) == 2

    shown = json.loads(await tq("show", task_id))
    assert (shown["id"], shown["status"]) == (task_id, "pending")
    assert "no such task" not in await tq("show", task_id)
    await tq("show", str(uuid.uuid4()), expect=1)
    await tq("enqueue", "mail.send", "[1, 2]", expect=2)

    assert "canceled" in await tq("cancel", task_id)
    await tq("cancel", task_id, expect=1)
    assert "pending" in await tq("requeue", task_id)
    await tq("requeue", task_id, expect=1)

    stats = json.loads(await tq("stats", "--json"))
    assert stats["totals"]["pending"] == 2
    assert "default/mail.send" in await tq("stats")

    assert "is paused" in await tq("pause", "default")
    assert await queue.claim(["mail.send"], worker_id="w") == []
    assert "is running" in await tq("resume", "default")
    assert json.loads(await tq("workers", "--json")) == []


async def test_schedule_commands(db, queue):
    from pgtaskqueue import Schedules

    out = await tq(
        "schedule", "set", "nightly", "report.build", '{"full": true}',
        "--cron", "0 3 * * *", "--timezone", "Europe/Berlin", "--queue", "reports",
    )  # fmt: skip
    assert out.startswith("created") and "0 3 * * * (Europe/Berlin)" in out
    row = await Schedules(db).get("nightly")
    assert (row.task_name, row.queue_name, row.payload, row.source, row.created_by) == (
        "report.build",
        "reports",
        {"full": True},
        "api",
        "cli",
    )
    out = await tq("schedule", "set", "nightly", "report.build", "--every", "300")
    assert out.startswith("replaced") and "every 300s" in out

    assert "(paused)" in await tq("schedule", "pause", "nightly")
    assert "nightly" in await tq("schedule", "list")
    (listed,) = json.loads(await tq("schedule", "list", "--json"))
    assert (listed["name"], listed["task"], listed["paused"]) == ("nightly", "report.build", True)
    assert "(paused)" not in await tq("schedule", "resume", "nightly")

    for bad in (
        ("--cron", "99 * * * *"),
        ("--cron", "0 3 * * *", "--timezone", "Mars/Olympus"),
        ("--every", "0"),
    ):
        process = await start("schedule", "set", "broken", "t", *bad)
        _, err = await asyncio.wait_for(process.communicate(), 60)
        assert process.returncode == 1 and err.decode().strip(), bad
    await tq("schedule", "set", "broken", "t", "[1]", "--every", "60", expect=2)
    await tq("schedule", "set", "broken", "t", expect=2)  # neither --cron nor --every
    assert [s.name for s in await Schedules(db).list()] == ["nightly"]

    assert "deleted" in await tq("schedule", "delete", "nightly")
    await tq("schedule", "delete", "nightly", expect=1)
    await tq("schedule", "pause", "nightly", expect=1)


async def test_user_commands(db):
    from pgtaskqueue.auth import UserService

    out = await tq("user", "create", "alice", "--role", "operator", "--generate",
                   "--queues", "media,default", "--task-names", "image.*")  # fmt: skip
    password = out.strip().splitlines()[-1].split()[-1]
    users = UserService(db)
    alice = await users.authenticate("alice", password, kind="user")
    assert (alice.role, alice.queues, alice.task_names) == (
        "operator",
        ["media", "default"],
        ["image.*"],
    )
    await tq("user", "create", "svc", "--service", "--password", "svc-secret-value")
    assert await users.authenticate("svc", "svc-secret-value", kind="service")
    # What is generated is called what its account will present it as.
    assert "password: " in out
    out = await tq("user", "set-password", "svc", "--generate")
    label, secret = out.strip().splitlines()[-1].split(": ")
    assert label == "client secret"
    assert await users.authenticate("svc", secret, kind="service")

    await tq("user", "set-password", "alice", "--password", "a-brand-new-password")
    assert await users.authenticate("alice", "a-brand-new-password", kind="user")
    listing = await tq("user", "list")
    assert "alice" in listing and "svc" in listing
    await tq("user", "create", "alice", "--password", "another-password", expect=1)
    await tq("user", "create", "bob", "--password", "short", expect=1)

    out = await tq("user", "set-limits", "alice", "--max-waiting", "100", "--max-running", "4")
    assert "max_waiting=100 max_running=4 enqueue_per_minute=None" in out
    await tq("user", "set-limits", "alice", "--max-waiting", "default", "--min-priority", "50")
    alice = await users.get("alice")
    assert (alice.max_waiting, alice.max_running, alice.min_priority) == (None, 4, 50)
    assert "max_running=4 min_priority=50" in await tq("user", "list")
    await tq("user", "set-limits", "alice", expect=1)
    await tq("user", "set-limits", "nobody", "--max-waiting", "1", expect=1)
    await tq("user", "set-limits", "alice", "--max-waiting", "-1", expect=2)
    await tq("user", "create", "mia", "--role", "manager", "--password", "mia-password")


async def test_seed_fills_an_empty_queue_for_the_ui(db, queue, settings):
    from pgtaskqueue import Monitor
    from pgtaskqueue.auth import UserService
    from pgtaskqueue.seed import DEMO_PASSWORD, DEMO_USERS, PAUSED_QUEUE, SEED_WORKER

    await UserService(db).create("demo-viewer", "their-own-password", role="viewer")
    out = await tq("seed", "--tasks", "60", "--hours", "2")
    assert "seeded 60 finished tasks" in out and "demo-viewer" in out
    assert "already there" in out.split("demo-viewer")[1]

    users = UserService(db)
    for username, role, _ in DEMO_USERS[:3]:
        assert (await users.authenticate(username, DEMO_PASSWORD, kind="user")).role == role
    # An account that was there before keeps its password.
    assert await users.authenticate("demo-viewer", DEMO_PASSWORD, kind="user") is None

    stats = await Monitor(db, settings).stats(window_minutes=120)
    totals = stats["totals"]
    assert totals["succeeded"] + totals["failed"] + totals["canceled"] == 60
    assert (totals["scheduled"], totals["running"]) == (5, 3)
    # Finished tasks sort where their age puts them: newest first, by id as by time.
    finished = await queue.list(status=["succeeded", "failed"], limit=60)
    assert [t.completed_at for t in finished] != sorted(t.completed_at for t in finished)
    assert all(t.created_at <= t.completed_at for t in finished)
    assert await queue.get_logs(next(t for t in finished if t.attempt).id)
    queues = {q["name"]: q for q in await Monitor(db, settings).list_queues()}
    assert queues[PAUSED_QUEUE]["paused"] and queues[PAUSED_QUEUE]["pending"] == 6

    # The tasks seeded as running belong to no worker, so they come back like any lost one.
    recovered = await queue.recover_stale(stale_after=0)
    assert {t.worker_id for t in recovered} == {SEED_WORKER} and len(recovered) == 3

    # Not twice by accident: demonstration rows do not pile up, or land among real ones.
    assert "already holds" in (await start_err("seed"))
    assert "seeded 5 finished" in await tq("seed", "--tasks", "5", "--no-users", "--force")


async def test_token_command_mints_a_scoped_access_token(db, settings):
    from pgtaskqueue.auth import TokenService, UserService

    await UserService(db).create("carol", "a-long-password", role="operator")
    out = await tq("token", "carol", "--days", "30", "--scope", "tasks:read stats:read")
    claims = TokenService(settings).decode(out.strip())
    assert (claims["sub"], claims["role"]) == ("carol", "operator")
    assert claims["scope"] == "tasks:read stats:read"
    assert claims["exp"] - claims["iat"] == 30 * 86400
    await tq("token", "nobody", expect=1)


async def test_worker_process_runs_tasks_and_stops_on_sigterm(queue):
    worker = await start("worker", "--executors", "examples.executors.basic", "--tasks", "demo.*:4")
    try:
        echo = await queue.enqueue("demo.echo", {"message": "from the test"})
        done = await wait_for(queue, echo.id, timeout=30)
        assert (done.status, done.output["message"]) == ("succeeded", "from the test")

        long = await queue.enqueue("demo.sleep", {"seconds": 60})
        await wait_for(queue, long.id, ["running"], timeout=10)
        worker.send_signal(signal.SIGTERM)
        # Grace is 1s here: the sleep is cut short and handed back, and the exit is clean.
        assert await asyncio.wait_for(worker.wait(), 20) == 0
    finally:
        if worker.returncode is None:
            worker.kill()
            await worker.wait()
    released = await queue.get(long.id)
    assert (released.status, released.attempt) == ("pending", 0)
    assert "exit: signal" in (await worker.stderr.read()).decode()


async def test_idle_worker_exits_so_its_container_restarts_it(queue):
    """What `restart: unless-stopped` plus `--idle-exit` is built on: exit code 0 after a
    quiet period, having done some work."""
    worker = await start("worker", "--executors", "examples.executors.basic", "--idle-exit", "1")
    try:
        task = await queue.enqueue("demo.echo", {"message": "one job"})
        await wait_for(queue, task.id, timeout=30)
        assert await asyncio.wait_for(worker.wait(), 20) == 0
    finally:
        if worker.returncode is None:
            worker.kill()
            await worker.wait()
    assert "exit: idle" in (await worker.stderr.read()).decode()


async def test_max_tasks_exit(queue):
    for i in range(5):
        await queue.enqueue("demo.echo", {"message": str(i)})
    worker = await start(
        "worker", "--executors", "examples.executors.basic", "--max-tasks", "3", "--slots", "1"
    )
    try:
        assert await asyncio.wait_for(worker.wait(), 30) == 0
    finally:
        if worker.returncode is None:
            worker.kill()
            await worker.wait()
    assert "exit: max_tasks" in (await worker.stderr.read()).decode()
    await until(lambda: _count(queue, "succeeded", 3), what="three tasks done")
    assert len(await queue.list(status="pending")) == 2


async def _count(queue, status: str, expected: int) -> bool:
    return len(await queue.list(status=status)) == expected
