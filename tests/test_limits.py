"""Account limits: waiting, running, rate and priority. And who may manage accounts."""

from __future__ import annotations

import asyncio

from sqlalchemy import text

from pgtaskqueue import NewTask, Schedules, TaskQueue
from pgtaskqueue.auth import UserService
from tests.helpers import Gauge, make_settings, running_worker, serving, wait_for

W = "worker-1"


async def account(db, username="alice", role="submitter", queues=("*",), **limits):
    return await UserService(db).create(
        username, f"{username}-password", role=role, queues=queues, limits=limits
    )


async def statuses(queue: TaskQueue, created_by: str = "alice") -> dict[str, int]:
    found: dict[str, int] = {}
    for task in await queue.list(created_by=created_by, limit=1000):
        found[task.status] = found.get(task.status, 0) + 1
    return found


# --------------------------------------------------------------------------- running limit


async def test_tasks_over_the_running_limit_are_held(queue: TaskQueue, db):
    await account(db, max_running=2)
    tasks = await queue.enqueue_many(NewTask("t", created_by="alice") for _ in range(5))
    # Held, however due: the promoter leaves them alone and only a release lets them out.
    assert {t.status for t in tasks} == {"scheduled"} and all(t.capped for t in tasks)
    assert await queue.promote_due() == 0

    assert await queue.release_capped() == 2
    assert await queue.release_capped() == 0
    claimed = await queue.claim(["t"], worker_id=W, limit=5)
    assert [t.id for t in claimed] == [t.id for t in tasks[:2]]  # oldest first
    assert await queue.release_capped() == 0  # running counts as much as pending

    await queue.succeed(claimed[0].id, worker_id=W, attempt=1)
    assert await queue.release_capped() == 1
    await queue.fail(claimed[1].id, worker_id=W, attempt=1, error={}, retry_in=60)
    # A retry waits held, like any of the account's tasks, and frees its place meanwhile.
    assert (await queue.get(claimed[1].id)).status == "scheduled"
    assert await queue.release_capped() == 1
    assert await statuses(queue) == {"succeeded": 1, "pending": 2, "scheduled": 2}


async def test_only_an_account_with_a_running_limit_is_held(queue: TaskQueue, db):
    await account(db, "alice", max_running=1)
    await account(db, "bob", max_running=0)
    for created_by in ("alice", "bob", None):
        task = await queue.enqueue("t", created_by=created_by)
        assert task.capped == (created_by == "alice"), created_by
    # Built-in tasks never wait behind their account.
    assert not (await queue.enqueue("tq.webhook", created_by="alice")).capped


async def test_the_default_running_limit_applies_to_accounts_without_their_own(db):
    queue = TaskQueue(db, make_settings(account_max_running=1))
    await account(db, "alice")
    await account(db, "bob", max_running=0)  # 0: explicitly none
    assert (await queue.enqueue("t", created_by="alice")).capped
    assert not (await queue.enqueue("t", created_by="bob")).capped
    # A subject with no row (an external identity provider's) takes the default too.
    assert (await queue.enqueue("t", created_by="external-sub")).capped


async def test_requeue_and_edits_do_not_get_past_the_limit(queue: TaskQueue, db):
    await account(db, max_running=1)
    task = await queue.enqueue("t", created_by="alice")
    await queue.release_capped()
    edited = await queue.update_waiting(task.id, run_after=(await queue.get(task.id)).run_after)
    assert edited.status == "scheduled"  # a new time is a new wait

    await queue.release_capped()
    [claimed] = await queue.claim(["t"], worker_id=W)
    await queue.succeed(claimed.id, worker_id=W, attempt=1)
    again = await queue.requeue(task.id)
    assert (again.status, again.capped) == ("scheduled", True)


async def test_a_removed_limit_lets_held_tasks_go(queue: TaskQueue, db):
    await account(db, max_running=1)
    await queue.enqueue_many(NewTask("t", created_by="alice") for _ in range(4))
    await UserService(db).update("alice", limits={"max_running": 0})
    assert await queue.release_capped() == 4


async def test_concurrent_releases_never_exceed_the_limit(queue: TaskQueue, db, monkeypatch):
    await account(db, max_running=3)
    await queue.enqueue_many(NewTask("t", created_by="alice") for _ in range(60))
    notify = queue._notify

    async def slow_notify(tx, channel, payload):
        # Each release holds its transaction open a while after its UPDATE, so they overlap:
        # without the lock, every one would count none of the others' uncommitted work.
        await asyncio.sleep(0.2)
        await notify(tx, channel, payload)

    monkeypatch.setattr(queue, "_notify", slow_notify)
    await asyncio.gather(*(queue.release_capped() for _ in range(8)))
    assert (await statuses(queue))["pending"] == 3


async def test_a_worker_runs_no_more_of_an_account_than_its_limit(db, registry):
    settings = make_settings()
    queue = TaskQueue(db, settings)
    await account(db, "alice", max_running=2)
    gauges = {"alice": Gauge(), "bob": Gauge()}

    @registry.task("slow")
    async def slow(ctx, payload):
        with gauges[payload["who"]].held():
            await asyncio.sleep(0.15)

    tasks = await queue.enqueue_many(
        NewTask("slow", {"who": who}, created_by=who) for who in ["alice"] * 6 + ["bob"] * 4
    )
    async with running_worker(db, registry, settings, slots=10):
        for task in tasks:
            assert (await wait_for(queue, task.id)).status == "succeeded"
    # Ten slots: bob, who has no limit, filled them; alice never had more than two.
    assert (gauges["alice"].peak, gauges["bob"].peak) == (2, 4)


# --------------------------------------------------------------------------- API: enqueue


async def test_the_waiting_limit_refuses_with_429(api, auth, db, queue):
    await account(db, max_waiting=3)
    alice = auth("alice")
    for _ in range(3):
        assert (await api.post("/v1/tasks", json={"name": "t"}, headers=alice)).status_code == 201
    refused = await api.post("/v1/tasks", json={"name": "t"}, headers=alice)
    assert refused.status_code == 429
    assert "3 tasks waiting" in refused.json()["detail"]
    # Another account has its own count.
    assert (await api.post("/v1/tasks", json={"name": "t"}, headers=auth("bob"))).status_code == 201

    [first, *_] = await queue.list(created_by="alice")
    await queue.cancel(first.id)
    assert (await api.post("/v1/tasks", json={"name": "t"}, headers=alice)).status_code == 201

    batch = {"tasks": [{"name": "t"}] * 4}
    response = await api.post("/v1/tasks/batch", json=batch, headers=alice)
    assert response.status_code == 413
    assert len(await queue.list(created_by="alice", status="pending")) == 3


async def test_concurrent_enqueues_never_pass_the_waiting_limit(db, auth):
    await account(db, max_waiting=5)
    alice = auth("alice")
    async with serving(db, make_settings()) as api:
        responses = await asyncio.gather(
            *(api.post("/v1/tasks", json={"name": "t"}, headers=alice) for _ in range(20))
        )
    assert sorted(r.status_code for r in responses) == [201] * 5 + [429] * 15


async def test_the_rate_limit_says_when_to_try_again(api, auth, db):
    await account(db, enqueue_per_minute=3)
    alice = auth("alice")
    batch = {"tasks": [{"name": "t"}] * 2}
    assert (await api.post("/v1/tasks/batch", json=batch, headers=alice)).status_code == 201
    assert (await api.post("/v1/tasks", json={"name": "t"}, headers=alice)).status_code == 201
    refused = await api.post("/v1/tasks", json={"name": "t"}, headers=alice)
    assert refused.status_code == 429
    assert 50 <= int(refused.headers["retry-after"]) <= 60


async def test_the_rate_limit_counts_only_the_last_minute(api, auth, db, queue):
    await account(db, enqueue_per_minute=2)
    alice = auth("alice")
    for _ in range(2):
        assert (await api.post("/v1/tasks", json={"name": "t"}, headers=alice)).status_code == 201
    async with db.transaction() as tx:
        await tx.execute(text("UPDATE tq_tasks SET created_at = now() - interval '61 seconds'"))
    assert (await api.post("/v1/tasks", json={"name": "t"}, headers=alice)).status_code == 201


async def test_the_priority_limit(api, auth, db, queue):
    await account(db, min_priority=150)  # above the default of 100
    alice = auth("alice", "operator")

    refused = await api.post("/v1/tasks", json={"name": "t", "priority": 10}, headers=alice)
    assert refused.status_code == 403 and "below 150" in refused.json()["detail"]
    task = (await api.post("/v1/tasks", json={"name": "t"}, headers=alice)).json()
    assert task["priority"] == 150
    edit = await api.patch(f"/v1/tasks/{task['id']}", json={"priority": 0}, headers=alice)
    assert edit.status_code == 403

    schedule = {"task": "t", "every_seconds": 60}
    put = await api.put("/v1/schedules/s", json=schedule, headers=alice)
    assert put.status_code == 201, put.text
    assert (await Schedules(db).get("s")).priority == 150
    bad = await api.put("/v1/schedules/s", json={**schedule, "priority": 1}, headers=alice)
    assert bad.status_code == 403


async def test_defaults_apply_without_an_account_row(db, auth):
    settings = make_settings(account_max_waiting=1, account_min_priority=5)
    async with serving(db, settings) as api:
        headers = auth("external-sub")
        assert (await api.post("/v1/tasks", json={"name": "t"}, headers=headers)).status_code == 201
        second = await api.post("/v1/tasks", json={"name": "t"}, headers=headers)
        assert second.status_code == 429
        mine = (await api.get("/v1/auth/limits", headers=headers)).json()
    assert mine["effective"]["max_waiting"] == 1
    assert mine["account"]["max_waiting"] is None
    assert mine["usage"] == {"waiting": 1, "held": 0, "running": 0, "last_minute": 1}


# --------------------------------------------------------------------------- API: accounts


async def test_admin_sets_limits_and_null_restores_the_default(api, auth, db):
    admin = auth("root", "admin")
    body = {"username": "alice", "password": "alice-password", "max_waiting": 10}
    created = await api.post("/v1/users", json=body, headers=admin)
    assert created.status_code == 201 and created.json()["max_waiting"] == 10

    patched = await api.patch(
        "/v1/users/alice", json={"max_running": 2, "min_priority": 50}, headers=admin
    )
    assert (patched.json()["max_waiting"], patched.json()["max_running"]) == (10, 2)
    patched = await api.patch("/v1/users/alice", json={"max_waiting": None}, headers=admin)
    assert patched.json()["max_waiting"] is None and patched.json()["max_running"] == 2

    limits = (await api.get("/v1/users/alice/limits", headers=admin)).json()
    assert limits["effective"] == {
        "max_waiting": 0,
        "max_running": 2,
        "enqueue_per_minute": 0,
        "min_priority": 50,
    }
    negative = await api.patch("/v1/users/alice", json={"max_running": -1}, headers=admin)
    assert negative.status_code == 422


async def test_a_manager_manages_accounts_below_it(api, auth, db):
    await account(db, "root", role="admin")
    await account(db, "remote", role="worker")
    await account(db, "otto", role="operator")
    await account(db, "mia", role="manager")
    mia = auth("mia", "manager")

    roles = {r["name"]: r["assignable"] for r in (await api.get("/v1/roles", headers=mia)).json()}
    assert roles == {
        "admin": False,
        "manager": True,
        "operator": True,
        "submitter": True,
        "viewer": True,
        "worker": False,
    }
    listed = {
        u["username"]: u["manageable"] for u in (await api.get("/v1/users", headers=mia)).json()
    }
    assert listed == {"mia": True, "otto": True, "remote": False, "root": False}

    new = {"username": "sam", "password": "sam-password", "role": "submitter", "max_waiting": 5}
    assert (await api.post("/v1/users", json=new, headers=mia)).status_code == 201
    for role in ("admin", "worker"):
        body = {**new, "username": f"x-{role}", "role": role}
        assert (await api.post("/v1/users", json=body, headers=mia)).status_code == 403
    # Not by promotion either.
    assert (
        await api.patch("/v1/users/sam", json={"role": "admin"}, headers=mia)
    ).status_code == 403

    # An admin, or a worker, is out of reach whole: no edits, no password, no removal.
    for target in ("root", "remote"):
        assert (
            await api.patch(f"/v1/users/{target}", json={"max_waiting": 1}, headers=mia)
        ).status_code == 403
        password = {"password": "taken-over-now"}
        assert (
            await api.post(f"/v1/users/{target}/password", json=password, headers=mia)
        ).status_code == 403
        assert (await api.delete(f"/v1/users/{target}", headers=mia)).status_code == 403
        assert (await api.get(f"/v1/users/{target}/limits", headers=mia)).status_code == 403

    assert (
        await api.patch("/v1/users/otto", json={"max_running": 3}, headers=mia)
    ).status_code == 200
    assert (await api.delete("/v1/users/sam", headers=mia)).status_code == 204


async def test_no_account_loosens_its_own_limits(api, auth, db):
    await account(db, "mia", role="manager", max_waiting=5)
    mia = auth("mia", "manager")
    for change in ({"max_waiting": 0}, {"max_waiting": None}, {"queues": ["*"]}):
        response = await api.patch("/v1/users/mia", json=change, headers=mia)
        assert response.status_code == 403, change
    # An admin holds every permission, so there is no one above it to ask.
    await account(db, "root", role="admin")
    own = await api.patch("/v1/users/root", json={"max_waiting": 0}, headers=auth("root", "admin"))
    assert own.status_code == 200


async def test_a_manager_gives_only_acl_patterns_it_holds(api, auth, db):
    await account(db, "carl", queues=["media"])
    mia = auth("mia", "manager", queues=["media", "mail"])
    body = {"username": "sam", "password": "sam-password"}
    for queues in (["*"], ["media-*"], ["other"]):
        response = await api.post("/v1/users", json={**body, "queues": queues}, headers=mia)
        assert response.status_code == 403, queues
    ok = await api.post("/v1/users", json={**body, "queues": ["mail"]}, headers=mia)
    assert ok.status_code == 201
    assert (
        await api.patch("/v1/users/carl", json={"max_waiting": 2}, headers=mia)
    ).status_code == 200


async def test_account_management_needs_the_permission(api, auth):
    otto = auth("otto", "operator")
    for method, path in (("GET", "/v1/users"), ("GET", "/v1/roles"), ("GET", "/v1/users/x/limits")):
        assert (await api.request(method, path, headers=otto)).status_code == 403, path
    assert (await api.get("/v1/auth/limits", headers=otto)).status_code == 200


async def test_a_manager_role_can_be_stored(db):
    """The role's CHECK constraint, in the model and in the migration."""
    user = await account(db, "mia", role="manager", max_running=1, min_priority=10)
    assert (user.role, user.max_running, user.min_priority) == ("manager", 1, 10)


async def test_held_tasks_appear_in_the_statistics(api, auth, db, queue):
    await account(db, max_running=1)
    await queue.enqueue_many(NewTask("t", created_by="alice") for _ in range(3))
    await queue.release_capped()
    stats = (await api.get("/v1/stats", headers=auth("otto", "operator"))).json()
    assert (stats["totals"]["scheduled"], stats["totals"]["held"]) == (2, 2)
    assert stats["totals"]["pending"] == 1
    [queue_row] = (await api.get("/v1/queues", headers=auth("otto", "operator"))).json()
    assert (queue_row["scheduled"], queue_row["held"], queue_row["pending"]) == (2, 2, 1)
