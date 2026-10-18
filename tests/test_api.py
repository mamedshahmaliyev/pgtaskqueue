"""The REST API: what each role and ACL may do to which tasks."""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest
from sqlalchemy import text

from pgtaskqueue import Monitor, Schedule, Schedules
from tests.helpers import make_settings, serving

ADD_TYPE = {
    "name": "math.add",
    "description": "Adds two numbers.",
    "payload_schema": {
        "type": "object",
        "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
        "required": ["a"],
        "additionalProperties": False,
    },
    "max_attempts": 5,
    "timeout_seconds": 30.0,
}


@pytest.fixture
async def add_type(db):
    await Monitor(db).register_task_types([ADD_TYPE], registered_by="worker-1")


async def enqueue(api, headers, **body) -> dict:
    response = await api.post("/v1/tasks", json={"name": "mail.send", **body}, headers=headers)
    assert response.status_code == 201, response.text
    return response.json()


# --------------------------------------------------------------------------- plumbing


async def test_health_and_docs_need_no_token(api):
    assert (await api.get("/health/live")).json() == {"status": "ok"}
    ready = await api.get("/health/ready")
    assert ready.status_code == 200 and ready.json()["status"] == "ok"
    schema = (await api.get("/openapi.json")).json()
    assert "/v1/tasks" in schema["paths"] and "/v1/worker/claim" in schema["paths"]
    assert (await api.get("/docs")).status_code == 200
    # The MCP mount at the root does not swallow unknown paths.
    assert (await api.get("/nothing-here")).status_code == 404


# --------------------------------------------------------------------------- enqueue


async def test_enqueue_and_read_back(api, auth):
    alice = auth("alice")
    created = await enqueue(
        api,
        alice,
        payload={"to": "a@example.com"},
        queue="mail",
        priority=5,
        metadata={"tenant": "acme"},
        max_attempts=2,
        timeout_seconds=60,
    )
    assert created["created"] is True
    assert (created["status"], created["queue"], created["priority"]) == ("pending", "mail", 5)
    assert (created["created_by"], created["max_attempts"], created["timeout_seconds"]) == (
        "alice",
        2,
        60.0,
    )
    fetched = (await api.get(f"/v1/tasks/{created['id']}", headers=alice)).json()
    assert fetched["payload"] == {"to": "a@example.com"}
    assert fetched["metadata"] == {"tenant": "acme"}


async def test_scheduling(api, auth):
    alice = auth("alice")
    delayed = await enqueue(api, alice, delay_seconds=120)
    assert delayed["status"] == "scheduled"
    timed = await enqueue(api, alice, run_after="2099-01-01T00:00:00Z")
    assert timed["run_after"].startswith("2099-01-01T00:00:00")

    for bad in (
        {"run_after": "2099-01-01T00:00:00"},  # no timezone
        {"run_after": "2099-01-01T00:00:00Z", "delay_seconds": 5},
        {"delay_seconds": -1},
    ):
        response = await api.post("/v1/tasks", json={"name": "mail.send", **bad}, headers=alice)
        assert response.status_code == 422, bad


async def test_dedupe_answers_200_with_the_existing_task(api, auth):
    alice = auth("alice")
    first = await enqueue(api, alice, dedupe_key="invoice-1")
    again = await api.post(
        "/v1/tasks", json={"name": "mail.send", "dedupe_key": "invoice-1"}, headers=alice
    )
    assert again.status_code == 200
    assert (again.json()["id"], again.json()["created"]) == (first["id"], False)


@pytest.mark.parametrize(
    "body",
    [
        {"name": "mail.send", "unknown_field": 1},
        {"name": "has spaces"},
        {"name": "tq.webhook"},
        {"name": "mail.send", "queue": "bad queue"},
        {"name": "mail.send", "priority": 40000},
        {"name": "mail.send", "max_attempts": 0},
        {"name": "mail.send", "payload": [1, 2]},
        {"name": "mail.send", "callback": {"url": "not a url"}},
        {"name": "mail.send", "callback": {"url": "https://example.com", "secret": "short"}},
        {"name": "mail.send", "callback": {"url": "https://example.com", "events": ["nope"]}},
        {},
    ],
)
async def test_malformed_requests_are_refused(api, auth, body):
    response = await api.post("/v1/tasks", json=body, headers=auth("alice"))
    assert response.status_code == 422, response.text


async def test_payload_is_checked_against_the_registered_schema(api, auth, add_type):
    alice = auth("alice")
    ok = await api.post(
        "/v1/tasks", json={"name": "math.add", "payload": {"a": 1, "b": 2}}, headers=alice
    )
    assert ok.status_code == 201
    # The type's defaults were applied.
    assert (ok.json()["max_attempts"], ok.json()["timeout_seconds"]) == (5, 30.0)

    for payload, complaint in (
        ({"a": "one"}, "a: 'one' is not of type 'integer'"),
        ({}, "'a' is a required property"),
        ({"a": 1, "c": 3}, "Additional properties are not allowed"),
    ):
        response = await api.post(
            "/v1/tasks", json={"name": "math.add", "payload": payload}, headers=alice
        )
        assert response.status_code == 422
        assert "does not match the schema of 'math.add'" in response.json()["detail"]
        assert complaint in response.json()["detail"]

    # A name nobody has registered is accepted as it comes, by default...
    assert (
        await api.post("/v1/tasks", json={"name": "new.task"}, headers=alice)
    ).status_code == 201


async def test_unregistered_names_can_be_refused(db, auth, add_type):
    strict = make_settings(api_require_registered_tasks=True)
    async with serving(db, strict) as api:
        response = await api.post("/v1/tasks", json={"name": "new.task"}, headers=auth("alice"))
        assert response.status_code == 422
        assert "no worker has registered" in response.json()["detail"]
        ok = await api.post(
            "/v1/tasks", json={"name": "math.add", "payload": {"a": 1}}, headers=auth("alice")
        )
        assert ok.status_code == 201


async def test_payload_size_limit(db, auth):
    async with serving(db, make_settings(max_payload_bytes=200)) as api:
        response = await api.post(
            "/v1/tasks",
            json={"name": "mail.send", "payload": {"body": "x" * 500}},
            headers=auth("alice"),
        )
        assert response.status_code == 413
        assert "the limit is 200" in response.json()["detail"]


async def test_batch_is_all_or_nothing(api, auth, queue):
    alice = auth("alice", task_names=["mail.*"])
    body = {
        "tasks": [
            {"name": "mail.send", "payload": {"i": i}, "dedupe_key": f"k{i % 3}"} for i in range(5)
        ]
    }
    response = await api.post("/v1/tasks/batch", json=body, headers=alice)
    assert response.status_code == 201
    assert (len(response.json()["created"]), response.json()["skipped"]) == (3, 2)

    # One task outside the ACL refuses the whole batch.
    body["tasks"] = [{"name": "mail.send"}, {"name": "billing.charge"}]
    response = await api.post("/v1/tasks/batch", json=body, headers=alice)
    assert response.status_code == 403
    assert len(await queue.list()) == 3

    assert (await api.post("/v1/tasks/batch", json={"tasks": []}, headers=alice)).status_code == 422


# --------------------------------------------------------------------------- permissions


async def test_roles_decide_what_can_be_done(api, auth):
    task = await enqueue(api, auth("alice"))
    url = f"/v1/tasks/{task['id']}"

    viewer, worker = auth("vic", "viewer"), auth("w1", "worker")
    assert (await api.post("/v1/tasks", json={"name": "mail.send"}, headers=viewer)).json() == {
        "detail": "this token lacks the 'tasks:enqueue' permission"
    }
    assert (await api.get(url, headers=viewer)).status_code == 200
    assert (await api.post(f"{url}/cancel", headers=viewer)).status_code == 403
    assert (await api.patch(url, json={"priority": 1}, headers=viewer)).status_code == 403
    assert (await api.get("/v1/stats", headers=viewer)).status_code == 200
    assert (await api.post("/v1/queues/default/pause", headers=viewer)).status_code == 403

    # A worker account claims and settles; it does not browse.
    assert (await api.get("/v1/tasks", headers=worker)).status_code == 403
    assert (await api.get(url, headers=worker)).status_code == 404
    assert (await api.get("/v1/stats", headers=worker)).status_code == 403
    assert (await api.get("/v1/task-types", headers=worker)).status_code == 403

    # A submitter follows its own work but not the system's.
    submitter = auth("alice")
    for path in ("/v1/stats", "/v1/queues", "/v1/workers", "/v1/users"):
        assert (await api.get(path, headers=submitter)).status_code == 403, path
    assert (await api.get("/v1/task-types", headers=submitter)).status_code == 200

    operator = auth("olga", "operator")
    assert (await api.patch(url, json={"priority": 1}, headers=operator)).status_code == 200
    assert (await api.post(f"{url}/cancel", headers=operator)).status_code == 200
    assert (await api.get("/v1/users", headers=operator)).status_code == 403


async def test_a_submitter_sees_and_manages_only_its_own_tasks(api, auth):
    alice, bob = auth("alice"), auth("bob")
    mine = await enqueue(api, alice)
    theirs = await enqueue(api, bob)

    # Someone else's task looks exactly like one that does not exist.
    for response in (
        await api.get(f"/v1/tasks/{theirs['id']}", headers=alice),
        await api.get(f"/v1/tasks/{theirs['id']}/logs", headers=alice),
        await api.post(f"/v1/tasks/{theirs['id']}/cancel", headers=alice),
        await api.patch(f"/v1/tasks/{theirs['id']}", json={"priority": 1}, headers=alice),
        await api.get(f"/v1/tasks/{uuid.uuid4()}", headers=alice),
    ):
        assert response.status_code == 404
        assert response.json() == {"detail": "no such task"}

    listed = (await api.get("/v1/tasks", headers=alice)).json()["items"]
    assert [task["id"] for task in listed] == [mine["id"]]
    # Asking for someone else's by name does not get around it.
    assert (await api.get("/v1/tasks?created_by=bob", headers=alice)).json()["items"] == []

    everything = (await api.get("/v1/tasks", headers=auth("vic", "viewer"))).json()["items"]
    assert {task["id"] for task in everything} == {mine["id"], theirs["id"]}
    by_bob = (await api.get("/v1/tasks?created_by=bob", headers=auth("vic", "viewer"))).json()
    assert [task["id"] for task in by_bob["items"]] == [theirs["id"]]

    assert (await api.post(f"/v1/tasks/{mine['id']}/cancel", headers=alice)).status_code == 200


async def test_acl_limits_queues_and_names(api, auth):
    admin = auth("root", "admin")
    media = await enqueue(api, admin, name="image.resize", queue="media")
    mail = await enqueue(api, admin, name="mail.send", queue="default")
    report = await enqueue(api, admin, name="report.build", queue="media")

    scoped = auth("olga", "operator", queues=["media*"], task_names=["image.*", "video.*"])
    refused = await api.post(
        "/v1/tasks", json={"name": "image.resize", "queue": "default"}, headers=scoped
    )
    assert refused.status_code == 403
    assert refused.json()["detail"] == "queue 'default' is outside this account's ACL"
    refused = await api.post(
        "/v1/tasks", json={"name": "report.build", "queue": "media"}, headers=scoped
    )
    assert refused.json()["detail"] == "task name 'report.build' is outside this account's ACL"
    assert (
        await api.post(
            "/v1/tasks", json={"name": "video.encode", "queue": "media-hd"}, headers=scoped
        )
    ).status_code == 201

    # Even with "read all", only what the ACL covers.
    listed = (await api.get("/v1/tasks", headers=scoped)).json()["items"]
    assert {task["name"] for task in listed} == {"image.resize", "video.encode"}
    assert (await api.get(f"/v1/tasks/{media['id']}", headers=scoped)).status_code == 200
    for hidden in (mail, report):
        assert (await api.get(f"/v1/tasks/{hidden['id']}", headers=scoped)).status_code == 404
        assert (
            await api.post(f"/v1/tasks/{hidden['id']}/cancel", headers=scoped)
        ).status_code == 404

    queues = (await api.get("/v1/queues", headers=scoped)).json()
    assert {q["name"] for q in queues} == {"media", "media-hd"}
    stats = (await api.get("/v1/stats", headers=scoped)).json()
    assert {(t["queue"], t["name"]) for t in stats["tasks"]} == {
        ("media", "image.resize"),
        ("media-hd", "video.encode"),
    }
    assert (await api.post("/v1/queues/default/pause", headers=scoped)).status_code == 403
    assert (await api.post("/v1/queues/media/pause", headers=scoped)).status_code == 200


async def test_tracebacks_are_for_operators(api, auth, queue):
    alice = auth("alice")
    task = await enqueue(api, alice, max_attempts=1)
    (claimed,) = await queue.claim(["mail.send"], worker_id="w")
    error = {"type": "ValueError", "message": "bad", "traceback": 'File "/srv/app/secret.py"'}
    await queue.fail(claimed.id, worker_id="w", attempt=1, error=error)

    own = (await api.get(f"/v1/tasks/{task['id']}", headers=alice)).json()
    assert own["status"] == "failed"
    assert own["error"] == {"type": "ValueError", "message": "bad"}
    full = (await api.get(f"/v1/tasks/{task['id']}", headers=auth("olga", "operator"))).json()
    assert "secret.py" in full["error"]["traceback"]


async def test_callback_secret_is_never_returned(api, auth, queue):
    alice = auth("alice")
    created = await enqueue(
        api,
        alice,
        delay_seconds=60,
        callback={
            "url": "https://example.com/hooks/tq",
            "secret": "whsec-super-secret",
            "headers": {"X-Api-Key": "key-123"},
            "events": ["task.failed"],
        },
    )
    for body in (
        created,
        (await api.get(f"/v1/tasks/{created['id']}", headers=alice)).json(),
        (await api.get("/v1/tasks", headers=auth("root", "admin"))).json(),
    ):
        text = json.dumps(body)
        assert "whsec-super-secret" not in text and "key-123" not in text
    assert created["callback"] == {
        "url": "https://example.com/hooks/tq",
        "events": ["task.failed"],
        "has_secret": True,
        "header_names": ["X-Api-Key"],
    }
    # Stored as given, for the worker that will sign with it.
    stored = await queue.get(uuid.UUID(created["id"]))
    assert stored.callback["secret"] == "whsec-super-secret"


# --------------------------------------------------------------------------- lifecycle


async def test_edit_cancel_requeue(api, auth, queue, add_type):
    alice = auth("alice")
    task = await enqueue(api, alice, name="math.add", payload={"a": 1}, delay_seconds=60)
    url = f"/v1/tasks/{task['id']}"

    edited = await api.patch(
        url, json={"priority": 3, "payload": {"a": 2}, "metadata": {"x": 1}}, headers=alice
    )
    assert edited.status_code == 200
    assert (edited.json()["priority"], edited.json()["payload"]) == (3, {"a": 2})
    # An edited payload is validated like a new one, and unknown fields are refused.
    assert (await api.patch(url, json={"payload": {"a": "x"}}, headers=alice)).status_code == 422
    assert (await api.patch(url, json={"status": "succeeded"}, headers=alice)).status_code == 422
    # Bringing it forward makes it claimable.
    sooner = await api.patch(url, json={"run_after": "2020-01-01T00:00:00Z"}, headers=alice)
    assert sooner.json()["status"] == "pending"

    # Not while it is waiting or running...
    assert (await api.post(f"{url}/requeue", headers=alice)).status_code == 409
    assert len(await queue.claim(["math.add"], worker_id="w")) == 1
    running = await api.patch(url, json={"priority": 1}, headers=alice)
    assert running.status_code == 409
    assert "can no longer be changed" in running.json()["detail"]

    canceled = await api.post(f"{url}/cancel", headers=alice)
    assert canceled.json()["status"] == "canceled"
    assert (await api.post(f"{url}/cancel", headers=alice)).status_code == 409

    again = await api.post(f"{url}/requeue", json={"reset_progress": True}, headers=alice)
    assert again.status_code == 200
    assert (again.json()["status"], again.json()["attempt"]) == ("pending", 0)


async def test_long_poll_returns_when_the_task_finishes(api, auth, queue):
    alice = auth("alice")
    task = await enqueue(api, alice)

    async def finish_soon() -> None:
        await asyncio.sleep(0.4)
        (claimed,) = await queue.claim(["mail.send"], worker_id="w")
        await queue.succeed(claimed.id, worker_id="w", attempt=1, output={"sent": True})

    loop = asyncio.get_running_loop()
    started = loop.time()
    finisher = asyncio.create_task(finish_soon())
    response = await api.get(f"/v1/tasks/{task['id']}?wait=5", headers=alice)
    await finisher
    assert response.json()["status"] == "succeeded"
    assert response.json()["output"] == {"sent": True}
    assert 0.3 < loop.time() - started < 3

    # Time up with the task still waiting: answered with what there is.
    pending = await enqueue(api, alice)
    started = loop.time()
    response = await api.get(f"/v1/tasks/{pending['id']}?wait=0.5", headers=alice)
    assert response.json()["status"] == "pending"
    assert 0.4 < loop.time() - started < 2


async def test_listing_filters_and_pages(api, auth):
    admin = auth("root", "admin")
    ids = []
    for i in range(7):
        task = await enqueue(api, admin, metadata={"tenant": "acme" if i < 5 else "other", "i": i})
        ids.append(task["id"])
    await api.post(f"/v1/tasks/{ids[0]}/cancel", headers=admin)

    async def items(query: str) -> list[dict]:
        response = await api.get(f"/v1/tasks?{query}", headers=admin)
        assert response.status_code == 200, response.text
        return response.json()["items"]

    assert len(await items("status=pending")) == 6
    assert len(await items("status=pending&status=canceled")) == 7
    assert len(await items("status=canceled&name=mail.send&queue=default")) == 1
    assert len(await items('metadata={"tenant":"acme"}')) == 5
    assert len(await items('metadata={"tenant":"acme","i":3}')) == 1
    assert (await api.get("/v1/tasks?metadata=nope", headers=admin)).status_code == 422
    assert (await api.get("/v1/tasks?metadata=[1]", headers=admin)).status_code == 422
    assert (await api.get("/v1/tasks?status=bogus", headers=admin)).status_code == 422
    assert (await api.get("/v1/tasks?limit=500", headers=admin)).status_code == 422

    seen, before = [], None
    for _ in range(4):
        query = "limit=3" + (f"&before={before}" if before else "")
        page = (await api.get(f"/v1/tasks?{query}", headers=admin)).json()
        seen += [task["id"] for task in page["items"]]
        before = page["next_before"]
        if before is None:
            break
    assert seen == ids[::-1]


async def test_logs_can_be_followed(api, auth, queue):
    alice = auth("alice")
    task = await enqueue(api, alice)
    task_id = uuid.UUID(task["id"])
    await queue.append_logs(
        [{"task_id": task_id, "attempt": 1, "message": f"line {i}"} for i in range(3)]
    )
    page = (await api.get(f"/v1/tasks/{task['id']}/logs", headers=alice)).json()
    assert [line["message"] for line in page["items"]] == ["line 0", "line 1", "line 2"]

    await queue.append_logs([{"task_id": task_id, "attempt": 1, "message": "line 3"}])
    more = (
        await api.get(f"/v1/tasks/{task['id']}/logs?after_id={page['last_id']}", headers=alice)
    ).json()
    assert [line["message"] for line in more["items"]] == ["line 3"]
    # Nothing new: the cursor stays where it was.
    idle = (
        await api.get(f"/v1/tasks/{task['id']}/logs?after_id={more['last_id']}", headers=alice)
    ).json()
    assert idle == {"items": [], "last_id": more["last_id"]}


# --------------------------------------------------------------------------- observability


async def test_queues_can_be_paused_and_resumed(api, auth, queue):
    operator = auth("olga", "operator")
    await enqueue(api, operator, queue="mail")
    await enqueue(api, operator, queue="mail", delay_seconds=60)

    paused = await api.post(
        "/v1/queues/mail/pause", json={"note": "provider outage"}, headers=operator
    )
    assert paused.status_code == 200
    assert paused.json() == {
        "name": "mail",
        "paused": True,
        "note": "provider outage",
        "scheduled": 1,
        "held": 0,
        "pending": 1,
        "running": 0,
        "workers": 0,
        "slots": 0,
    }
    assert await queue.claim(["mail.send"], worker_id="w", queue="mail") == []
    # Enqueueing into a paused queue still works.
    await enqueue(api, operator, queue="mail")

    resumed = await api.post("/v1/queues/mail/resume", headers=operator)
    assert (resumed.json()["paused"], resumed.json()["pending"]) == (False, 2)
    assert len(await queue.claim(["mail.send"], worker_id="w", queue="mail", limit=5)) == 2


async def test_stats(api, auth, queue):
    operator = auth("olga", "operator")
    for _ in range(4):
        await enqueue(api, operator)
    await enqueue(api, operator, delay_seconds=60)
    done, failed, _running = await queue.claim(["mail.send"], worker_id="w", limit=3)
    await queue.succeed(done.id, worker_id="w", attempt=1)
    await queue.fail(failed.id, worker_id="w", attempt=1, error={"type": "E"}, retry_in=None)

    stats = (await api.get("/v1/stats?window_minutes=5", headers=operator)).json()
    assert stats["window_minutes"] == 5
    totals = stats["totals"]
    assert (totals["scheduled"], totals["pending"], totals["running"]) == (1, 1, 1)
    assert (totals["succeeded"], totals["failed"]) == (1, 1)
    (row,) = stats["tasks"]
    assert (row["queue"], row["name"], row["pending"]) == ("default", "mail.send", 1)
    assert row["oldest_pending_seconds"] >= 0
    assert (await api.get("/v1/stats?window_minutes=0", headers=operator)).status_code == 422


async def test_timeline_counts_what_finished_in_each_bucket(api, auth, queue, db):
    operator = auth("olga", "operator")
    for _ in range(3):
        await enqueue(api, operator)
    await enqueue(api, operator, name="other.task")
    done, failed, old = await queue.claim(["mail.send"], worker_id="w", limit=3)
    (other,) = await queue.claim(["other.task"], worker_id="w")
    await queue.succeed(done.id, worker_id="w", attempt=1)
    await queue.succeed(other.id, worker_id="w", attempt=1)
    await queue.fail(failed.id, worker_id="w", attempt=1, error={"type": "E"}, retry_in=None)
    await queue.cancel(old.id)
    # Move one into the past, to land in an earlier bucket, and one out of the window.
    async with db.transaction() as tx:
        await tx.execute(
            text("UPDATE tq_tasks SET completed_at = now() - interval '25 minutes' WHERE id = :id"),
            {"id": old.id},
        )
        await tx.execute(
            text("UPDATE tq_tasks SET completed_at = now() - interval '2 hours' WHERE id = :id"),
            {"id": other.id},
        )

    url = "/v1/stats/timeline?window_minutes=60&buckets=6"
    timeline = (await api.get(url, headers=operator)).json()
    assert (timeline["window_minutes"], timeline["bucket_seconds"]) == (60, 600)
    buckets = timeline["buckets"]
    assert len(buckets) == 6 and buckets == sorted(buckets, key=lambda b: b["start"])
    counts = [(b["succeeded"], b["failed"], b["canceled"]) for b in buckets]
    assert counts == [(0, 0, 0), (0, 0, 0), (0, 0, 0), (0, 0, 1), (0, 0, 0), (1, 1, 0)]

    # An ACL hides what it hides everywhere else; a role without statistics gets nothing.
    narrow = auth("nora", "viewer", task_names=["other.*"])
    hidden = (await api.get(url, headers=narrow)).json()["buckets"]
    assert all(b["succeeded"] + b["failed"] + b["canceled"] == 0 for b in hidden)
    wide = (await api.get(url + "0&window_minutes=180", headers=narrow)).json()["buckets"]
    assert sum(b["succeeded"] for b in wide) == 1
    assert (await api.get(url, headers=auth("sam"))).status_code == 403
    assert (await api.get(url + "00", headers=operator)).status_code == 422


async def test_task_types_and_workers(api, auth, db, add_type):
    monitor = Monitor(db)
    await monitor.worker_started(
        "worker-1",
        queue_name="default",
        hostname="host",
        pid=1,
        pools=[{"label": "ALL", "names": ["math.add"], "slots": 4}],
        slots=4,
        version="0.1.0",
    )
    viewer = auth("vic", "viewer")
    (kind,) = (await api.get("/v1/task-types", headers=viewer)).json()
    assert (kind["name"], kind["max_attempts"], kind["registered_by"]) == (
        "math.add",
        5,
        "worker-1",
    )
    assert kind["payload_schema"]["required"] == ["a"]
    # Limited to the names an account's ACL covers.
    narrow = auth("nina", "viewer", task_names=["mail.*"])
    assert (await api.get("/v1/task-types", headers=narrow)).json() == []

    (worker,) = (await api.get("/v1/workers", headers=viewer)).json()
    assert (worker["id"], worker["slots"], worker["alive"]) == ("worker-1", 4, True)
    await monitor.worker_stopped("worker-1", in_flight=0, counters={"succeeded": 3})
    assert (await api.get("/v1/workers", headers=viewer)).json() == []
    (stopped,) = (await api.get("/v1/workers?all=true", headers=viewer)).json()
    assert stopped["alive"] is False and stopped["counters"] == {"succeeded": 3}


# --------------------------------------------------------------------------- schedules

NIGHTLY = {"task": "report.build", "cron": "0 3 * * *", "timezone": "Europe/Berlin"}


async def test_schedules_are_created_replaced_paused_and_deleted(api, auth, db):
    olga = auth("olga", "operator")
    created = await api.put(
        "/v1/schedules/nightly",
        json={**NIGHTLY, "payload": {"full": True}, "queue": "reports", "priority": 5},
        headers=olga,
    )
    assert created.status_code == 201, created.text
    body = created.json()
    assert (body["name"], body["task"], body["queue"], body["source"], body["created_by"]) == (
        "nightly",
        "report.build",
        "reports",
        "api",
        "olga",
    )
    assert (body["cron"], body["timezone"], body["every_seconds"]) == (
        "0 3 * * *",
        "Europe/Berlin",
        None,
    )
    assert body["paused"] is False and body["last_task_id"] is None

    replaced = await api.put(
        "/v1/schedules/nightly", json={"task": "report.build", "every_seconds": 300}, headers=olga
    )
    assert replaced.status_code == 200
    assert (replaced.json()["cron"], replaced.json()["every_seconds"]) == (None, 300.0)
    assert replaced.json()["queue"] == "default" and replaced.json()["payload"] == {}

    paused = await api.post("/v1/schedules/nightly/pause", headers=olga)
    assert paused.json()["paused"] is True
    assert (await api.get("/v1/schedules/nightly", headers=olga)).json()["paused"] is True
    assert (await api.post("/v1/schedules/nightly/resume", headers=olga)).json()["paused"] is False
    assert [s["name"] for s in (await api.get("/v1/schedules", headers=olga)).json()] == ["nightly"]

    # What it enqueues belongs to whoever made the schedule.
    async with db.transaction() as tx:
        await tx.execute(text("UPDATE tq_schedules SET next_run_at = now()"))
    (fired,) = await Schedules(db).fire_due()
    task = (await api.get(f"/v1/tasks/{fired.task.id}", headers=olga)).json()
    assert (task["created_by"], task["metadata"]["schedule"]) == ("olga", "nightly")
    assert (await api.get("/v1/schedules/nightly", headers=olga)).json()["last_task_id"] == task[
        "id"
    ]

    assert (await api.delete("/v1/schedules/nightly", headers=olga)).status_code == 204
    assert (await api.get("/v1/schedules/nightly", headers=olga)).status_code == 404
    assert (await api.delete("/v1/schedules/nightly", headers=olga)).status_code == 404


@pytest.mark.parametrize(
    ("body", "complaint"),
    [
        ({"task": "t"}, "cron or every_seconds"),
        ({"task": "t", "cron": "0 * * * *", "every_seconds": 60}, "cron or every_seconds"),
        ({"task": "t", "cron": "61 * * * *"}, "minute"),
        ({"task": "t", "cron": "0 0 31 2 *"}, "never fires"),
        ({"task": "t", "cron": "0 * * * *", "timezone": "Mars/Olympus"}, "unknown timezone"),
        ({"task": "t", "every_seconds": 0.1}, "greater than or equal to 1"),
        ({"task": "tq.webhook", "every_seconds": 60}, "reserved"),
        ({"task": "t", "every_seconds": 60, "pyload": {}}, "Extra inputs"),
    ],
)
async def test_schedules_that_cannot_run_are_refused(api, auth, body, complaint):
    response = await api.put("/v1/schedules/s", json=body, headers=auth("olga", "operator"))
    assert response.status_code == 422, response.text
    assert complaint in response.text
    assert (await api.get("/v1/schedules", headers=auth("olga", "operator"))).json() == []


async def test_a_scheduled_payload_is_checked_like_an_enqueued_one(api, auth, add_type):
    olga = auth("olga", "operator")
    body = {"task": "math.add", "every_seconds": 60}
    bad = await api.put("/v1/schedules/add", json={**body, "payload": {"b": 1}}, headers=olga)
    assert bad.status_code == 422 and "does not match the schema" in bad.text
    good = await api.put("/v1/schedules/add", json={**body, "payload": {"a": 1}}, headers=olga)
    assert good.status_code == 201
    assert (
        await api.put("/v1/schedules/bad name", json=body | {"payload": {"a": 1}}, headers=olga)
    ).status_code == 422


async def test_schedules_need_their_permissions(api, auth):
    olga, vic, alice = auth("olga", "operator"), auth("vic", "viewer"), auth("alice")
    assert (await api.put("/v1/schedules/nightly", json=NIGHTLY, headers=olga)).status_code == 201

    # A viewer looks and does not touch.
    assert len((await api.get("/v1/schedules", headers=vic)).json()) == 1
    assert (await api.get("/v1/schedules/nightly", headers=vic)).status_code == 200
    for refused in (
        await api.put("/v1/schedules/nightly", json=NIGHTLY, headers=vic),
        await api.put("/v1/schedules/another", json=NIGHTLY, headers=vic),
        await api.post("/v1/schedules/nightly/pause", headers=vic),
        await api.post("/v1/schedules/nightly/resume", headers=vic),
        await api.delete("/v1/schedules/nightly", headers=vic),
    ):
        assert refused.status_code == 403
        assert "schedules:manage" in refused.json()["detail"]

    # A submitter enqueues its own tasks; a schedule is not one of those.
    for refused in (
        await api.get("/v1/schedules", headers=alice),
        await api.get("/v1/schedules/nightly", headers=alice),
        await api.put("/v1/schedules/mine", json=NIGHTLY, headers=alice),
    ):
        assert refused.status_code == 403
    assert (await api.get("/v1/schedules", headers={})).status_code == 401
    unchanged = (await api.get("/v1/schedules/nightly", headers=olga)).json()
    assert (unchanged["paused"], unchanged["cron"]) == (False, "0 3 * * *")


async def test_the_acl_covers_a_schedule_through_what_it_enqueues(api, auth):
    olga = auth("olga", "operator")
    mail_only = auth("mo", "operator", task_names=["mail.*"], queues=["default"])
    assert (await api.put("/v1/schedules/nightly", json=NIGHTLY, headers=olga)).status_code == 201
    digest = {"task": "mail.digest", "every_seconds": 3600}
    assert (
        await api.put("/v1/schedules/digest", json=digest, headers=mail_only)
    ).status_code == 201

    assert [s["name"] for s in (await api.get("/v1/schedules", headers=mail_only)).json()] == [
        "digest"
    ]
    # Outside the ACL is the same answer as not there.
    for hidden in (
        await api.get("/v1/schedules/nightly", headers=mail_only),
        await api.post("/v1/schedules/nightly/pause", headers=mail_only),
        await api.delete("/v1/schedules/nightly", headers=mail_only),
    ):
        assert hidden.status_code == 404
    # It may not schedule a task, or onto a queue, that it could not enqueue to.
    for body in (NIGHTLY, {**digest, "queue": "other"}):
        refused = await api.put("/v1/schedules/new", json=body, headers=mail_only)
        assert refused.status_code == 403 and "outside this account's ACL" in refused.text
    # Nor take over one it cannot see by pointing the name at a task it can.
    taken = await api.put("/v1/schedules/nightly", json=digest, headers=mail_only)
    assert taken.status_code == 409
    assert (await api.get("/v1/schedules/nightly", headers=olga)).json()["task"] == "report.build"


async def test_a_schedule_declared_in_code_can_be_paused_but_not_replaced(api, auth, db):
    olga = auth("olga", "operator")
    await Schedules(db).publish(
        [("report.build", Schedule(every=300))], queue="default", task_names=["report.build"]
    )
    (listed,) = (await api.get("/v1/schedules", headers=olga)).json()
    assert (listed["name"], listed["source"], listed["created_by"]) == (
        "report.build",
        "code",
        None,
    )

    replaced = await api.put("/v1/schedules/report.build", json=NIGHTLY, headers=olga)
    assert replaced.status_code == 409 and "declared in code" in replaced.json()["detail"]
    assert (await api.post("/v1/schedules/report.build/pause", headers=olga)).json()["paused"]
    assert (await api.get("/v1/schedules/report.build", headers=olga)).json()[
        "every_seconds"
    ] == 300
