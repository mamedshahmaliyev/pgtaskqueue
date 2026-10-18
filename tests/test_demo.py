"""The public demo's reset and repair (examples/demo)."""

from __future__ import annotations

from examples.demo import reset as reset_cli
from examples.demo import seed as demo
from sqlalchemy import text

from pgtaskqueue import NewTask, Schedule, Schedules, TaskQueue
from pgtaskqueue import seed as base
from pgtaskqueue.auth import UserService
from pgtaskqueue.enums import TERMINAL_STATUSES
from pgtaskqueue.monitor import Monitor
from tests.helpers import make_settings


async def test_reset_keeps_admins_and_workers_and_seeds_the_demo(db, queue: TaskQueue):
    users = UserService(db)
    await users.create("root", "root-password", role="admin")
    await users.create("remote", "remote-password", role="worker")
    await users.create("eve", "eve-password", role="operator")
    await queue.enqueue_many([NewTask("demo.echo", {"left": "over"}, created_by="eve")])

    done = await demo.reset(db)

    names = {u.username for u in await users.list()}
    assert names == {"root", "remote", *(a.username for a in demo.ACCOUNTS)}
    tasks = await queue.list(limit=10_000)
    assert len(tasks) == done["tasks"] > 100
    assert all(demo.allowed(t.name) for t in tasks), {t.name for t in tasks}
    assert not any(t.created_by == "eve" for t in tasks)
    operator = await users.get("demo-operator")
    assert operator.task_names == list(demo.TASK_NAMES)
    assert operator.max_running == demo.STAFF_LIMITS["max_running"]


async def test_the_paused_queue_uses_up_no_demo_account_running_limit(db, queue: TaskQueue):
    await demo.reset(db)
    parked = [
        t
        for t in await queue.list(limit=10_000)
        if t.queue_name == base.PAUSED_QUEUE and t.status not in TERMINAL_STATUSES
    ]
    assert parked
    assert {t.created_by for t in parked} == {demo.PARKED_OWNER}


async def test_repair_resumes_the_default_queue(db):
    monitor = Monitor(db, make_settings())
    await monitor.set_paused("default", True, by="demo-operator")
    assert (await demo.repair(db))["default_resumed"] is True
    queues = {q["name"]: q["paused"] for q in await monitor.list_queues()}
    assert queues["default"] is False
    assert (await demo.repair(db))["default_resumed"] is False


async def test_the_demo_accounts_leave_out_the_executors_a_stranger_could_misuse():
    for name in ("demo.fetch", "report.fanout"):
        assert not demo.allowed(name)
    for name in ("demo.echo", "demo.sleep", "report.build", "image.render"):
        assert demo.allowed(name)


async def test_a_manager_may_manage_every_demo_account_but_itself_loosen_nothing():
    """The demo's limits are set so the product's rule lets demo-manager manage the rest."""
    staff = demo.STAFF_LIMITS
    for account in demo.ACCOUNTS:
        for key, value in account.limits.items():
            assert value <= staff[key] if key != "min_priority" else value >= staff[key]
    running = sum(a.limits["max_running"] for a in demo.ACCOUNTS if a.role != "viewer")
    assert running < 20  # the general worker's slots in compose.yml


async def test_repair_puts_accounts_back_without_ending_other_sessions(db):
    await demo.repair(db)
    users = UserService(db)
    untouched = (await users.get("demo-submitter")).token_version

    await users.set_password("demo-operator", "changed-by-a-visitor")
    await users.update("demo-operator", role="viewer", limits={"max_running": 0})
    await users.update("demo-manager", is_active=False)
    await users.delete("demo-viewer")

    done = await demo.repair(db)

    assert set(done["fixed"]) == {"demo-operator", "demo-manager", "demo-viewer"}
    for account in demo.ACCOUNTS:
        user = await users.authenticate(account.username, demo.PASSWORD, kind="user")
        assert user is not None, account.username
        assert (user.role, user.is_active) == (account.role, True)
        assert user.max_running == account.limits["max_running"]
    assert (await users.get("demo-submitter")).token_version == untouched
    assert (await demo.repair(db))["fixed"] == []


async def test_repair_caps_what_visitors_add_and_keeps_the_oldest(db):
    await demo.repair(db)
    users = UserService(db)
    extra = [f"visitor-{i:02}" for i in range(demo.MAX_EXTRA_ACCOUNTS + 3)]
    for name in extra:
        await users.create(name, f"{name}-password", role="submitter")
    schedules = Schedules(db)
    for i in range(demo.MAX_SCHEDULES + 2):
        await schedules.put(f"s{i:02}", "demo.echo", Schedule(every=60), created_by="demo-operator")
    await schedules.put("from-code", "demo.echo", Schedule(every=60), source="code")

    done = await demo.repair(db)

    assert (done["accounts_removed"], done["schedules_removed"]) == (3, 2)
    left = {u.username for u in await users.list()} - {a.username for a in demo.ACCOUNTS}
    assert left == set(extra[: demo.MAX_EXTRA_ACCOUNTS])
    async with db.transaction() as tx:
        code = await tx.scalar(text("SELECT count(*) FROM tq_schedules WHERE source = 'code'"))
    assert code == 1


async def test_reset_refuses_a_database_it_was_not_pointed_at(monkeypatch, capsys):
    monkeypatch.setenv("DEMO_RESET_DATABASE", "some_other_database")
    assert await reset_cli.main(["--once"]) == 2
    assert "DEMO_RESET_DATABASE=" in capsys.readouterr().err
