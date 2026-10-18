"""The remote-worker protocol: claiming and settling over HTTP, with the same fencing a
worker connected to the database gets."""

from __future__ import annotations

import asyncio
import uuid

from tests.helpers import until

W = {"worker_id": "gpu-box-1"}


async def claim(api, headers, **body) -> list[dict]:
    response = await api.post(
        "/v1/worker/claim", json={**W, "names": ["render.frame"], **body}, headers=headers
    )
    assert response.status_code == 200, response.text
    return response.json()


async def test_full_cycle(api, auth, queue):
    worker = auth("render-farm", "worker")
    types = [
        {
            "name": "render.frame",
            "description": "Renders one frame.",
            "payload_schema": {"type": "object", "required": ["frame"]},
            "max_attempts": 4,
        }
    ]
    response = await api.put("/v1/worker/gpu-box-1/task-types", json=types, headers=worker)
    assert response.status_code == 204
    (kind,) = (await api.get("/v1/task-types", headers=auth("vic", "viewer"))).json()
    assert (kind["name"], kind["registered_by"]) == ("render.frame", "http:render-farm:gpu-box-1")

    # Registered a moment ago, and already what the API validates against.
    alice = auth("alice")
    bad = await api.post("/v1/tasks", json={"name": "render.frame"}, headers=alice)
    assert bad.status_code == 422
    created = await api.post(
        "/v1/tasks", json={"name": "render.frame", "payload": {"frame": 7}}, headers=alice
    )
    task_id = created.json()["id"]
    assert created.json()["max_attempts"] == 4

    (task,) = await claim(api, worker)
    assert (task["id"], task["status"], task["attempt"]) == (task_id, "running", 1)
    assert task["payload"] == {"frame": 7}
    # Owned by the account *and* the id it chose.
    assert task["worker_id"] == "http:render-farm:gpu-box-1"
    assert await claim(api, worker) == []

    beat = await api.post("/v1/worker/heartbeat", json={**W, "task_ids": [task_id]}, headers=worker)
    assert beat.json() == {"owned": [task_id]}

    own = {**W, "attempt": 1}
    progress = await api.post(
        f"/v1/worker/tasks/{task_id}/progress",
        json={**own, "progress": {"percent": 50}, "step": "shading"},
        headers=worker,
    )
    assert progress.json() == {"accepted": True, "status": "running"}
    logs = await api.post(
        f"/v1/worker/tasks/{task_id}/logs",
        json={
            **W,
            "attempt": 1,
            "lines": [
                {"message": "loading scene"},
                {"message": "low memory", "level": "warning", "data": {"free_mb": 120}},
                {"message": "odd level", "level": "shout"},
            ],
        },
        headers=worker,
    )
    assert logs.status_code == 204

    done = await api.post(
        f"/v1/worker/tasks/{task_id}/succeed",
        json={**own, "output": {"path": "s3://frames/7.png"}},
        headers=worker,
    )
    assert done.json() == {"accepted": True, "status": "succeeded"}

    final = (await api.get(f"/v1/tasks/{task_id}", headers=alice)).json()
    assert (final["status"], final["output"]) == ("succeeded", {"path": "s3://frames/7.png"})
    assert (final["progress"], final["step"]) == ({"percent": 50}, "shading")
    lines = (await api.get(f"/v1/tasks/{task_id}/logs", headers=alice)).json()["items"]
    assert [(line["message"], line["level"]) for line in lines] == [
        ("loading scene", "info"),
        ("low memory", "warning"),
        ("odd level", "info"),
    ]
    # Settling twice changes nothing.
    again = await api.post(f"/v1/worker/tasks/{task_id}/succeed", json=own, headers=worker)
    assert again.json() == {"accepted": False, "status": None}


async def test_only_worker_accounts_may_use_it(api, auth, queue):
    await queue.enqueue("render.frame")
    for role in ("submitter", "viewer", "operator"):
        response = await api.post(
            "/v1/worker/claim", json={**W, "names": ["render.frame"]}, headers=auth("x", role)
        )
        assert response.status_code == 403, role
        assert response.json()["detail"] == "this token lacks the 'tasks:work' permission"
    assert (
        await api.post("/v1/worker/claim", json={**W, "names": ["render.frame"]})
    ).status_code == 401
    assert len(await queue.list(status="pending")) == 1


async def test_acl_limits_what_a_worker_may_claim(api, auth, queue):
    await queue.enqueue("render.frame", queue="gpu")
    await queue.enqueue("billing.charge", queue="gpu")
    worker = auth("render-farm", "worker", queues=["gpu"], task_names=["render.*"])

    for body, complaint in (
        ({"names": ["billing.charge"], "queue": "gpu"}, "task name 'billing.charge'"),
        ({"names": ["render.frame"], "queue": "default"}, "queue 'default'"),
        # The built-in webhook task carries callback secrets; it is never handed out.
        ({"names": ["tq.webhook"], "queue": "gpu"}, "task name 'tq.webhook'"),
    ):
        response = await api.post("/v1/worker/claim", json={**W, **body}, headers=worker)
        assert response.status_code == 403
        assert complaint in response.json()["detail"]

    refused = await api.put(
        "/v1/worker/gpu-box-1/task-types", json=[{"name": "billing.charge"}], headers=worker
    )
    assert refused.status_code == 403

    (task,) = await claim(api, worker, queue="gpu")
    assert task["name"] == "render.frame"


async def test_failures_retry_until_attempts_run_out(api, auth, queue):
    worker = auth("render-farm", "worker")
    created = await queue.enqueue("render.frame", max_attempts=2)
    url = f"/v1/worker/tasks/{created.id}/fail"
    error = {"type": "CudaError", "message": "out of memory"}

    (task,) = await claim(api, worker)
    first = await api.post(
        url, json={**W, "attempt": 1, "error": error, "retry_in": 0}, headers=worker
    )
    assert first.json() == {"accepted": True, "status": "pending"}

    (task,) = await claim(api, worker)
    assert task["attempt"] == 2
    # The last attempt: failed for good, whatever the worker asked for.
    last = await api.post(url, json={**W, "attempt": 2, "error": error}, headers=worker)
    assert last.json() == {"accepted": True, "status": "failed"}
    stored = await queue.get(created.id)
    assert stored.error == {
        "type": "CudaError",
        "message": "out of memory",
        "attempt": 2,
        "worker": "http:render-farm:gpu-box-1",
    }

    # `retry: false` fails at once, with attempts to spare.
    another = await queue.enqueue("render.frame", max_attempts=5)
    await claim(api, worker)
    response = await api.post(
        f"/v1/worker/tasks/{another.id}/fail",
        json={**W, "attempt": 1, "error": error, "retry": False},
        headers=worker,
    )
    assert response.json()["status"] == "failed"

    # Default delay: the server's backoff.
    third = await queue.enqueue("render.frame", max_attempts=5)
    await claim(api, worker)
    response = await api.post(
        f"/v1/worker/tasks/{third.id}/fail",
        json={**W, "attempt": 1, "error": error},
        headers=worker,
    )
    assert response.json()["status"] == "scheduled"


async def test_a_worker_cannot_touch_what_it_does_not_hold(api, auth, queue):
    farm, rival = auth("render-farm", "worker"), auth("rival", "worker")
    created = await queue.enqueue("render.frame")
    task_id = str(created.id)
    await claim(api, farm)
    own = {**W, "attempt": 1}

    # Another account using the same worker id; the same account with another id; the
    # right worker with the wrong attempt.
    for headers, body in (
        (rival, own),
        (farm, {"worker_id": "gpu-box-2", "attempt": 1}),
        (farm, {**W, "attempt": 2}),
    ):
        for action, extra in (
            ("succeed", {"output": "forged"}),
            ("fail", {"error": {"type": "X"}}),
            ("progress", {"progress": {"percent": 99}}),
        ):
            response = await api.post(
                f"/v1/worker/tasks/{task_id}/{action}", json={**body, **extra}, headers=headers
            )
            assert response.json()["accepted"] is False, (action, body)

    beat = await api.post("/v1/worker/heartbeat", json={**W, "task_ids": [task_id]}, headers=rival)
    assert beat.json() == {"owned": []}
    logs = await api.post(
        f"/v1/worker/tasks/{task_id}/logs",
        json={**W, "attempt": 1, "lines": [{"message": "forged"}]},
        headers=rival,
    )
    assert logs.status_code == 404
    assert (await api.post("/v1/worker/release", json=W, headers=rival)).json() == {"released": 0}

    stored = await queue.get(created.id)
    assert (stored.status, stored.progress, stored.output) == ("running", {}, None)
    assert await queue.get_logs(created.id) == []


async def test_cancel_reaches_a_remote_worker_through_its_heartbeat(api, auth, queue):
    worker = auth("render-farm", "worker")
    kept = await queue.enqueue("render.frame")
    dropped = await queue.enqueue("render.frame")
    assert len(await claim(api, worker, limit=5)) == 2

    await queue.cancel(dropped.id)
    beat = await api.post(
        "/v1/worker/heartbeat",
        json={**W, "task_ids": [str(kept.id), str(dropped.id), str(uuid.uuid4())]},
        headers=worker,
    )
    assert beat.json() == {"owned": [str(kept.id)]}
    late = await api.post(
        f"/v1/worker/tasks/{dropped.id}/succeed", json={**W, "attempt": 1}, headers=worker
    )
    assert late.json()["accepted"] is False
    assert (await queue.get(dropped.id)).status == "canceled"


async def test_release_hands_tasks_back(api, auth, queue):
    worker = auth("render-farm", "worker")
    tasks = [await queue.enqueue("render.frame") for _ in range(3)]
    assert len(await claim(api, worker, limit=3)) == 3

    one = await api.post(
        "/v1/worker/release", json={**W, "task_ids": [str(tasks[0].id)]}, headers=worker
    )
    assert one.json() == {"released": 1}
    rest = await api.post("/v1/worker/release", json=W, headers=worker)
    assert rest.json() == {"released": 2}
    for task in tasks:
        stored = await queue.get(task.id)
        assert (stored.status, stored.attempt) == ("pending", 0)


async def test_claim_can_wait_for_work(api, auth, queue):
    worker = auth("render-farm", "worker")
    loop = asyncio.get_running_loop()

    started = loop.time()
    assert await claim(api, worker, wait_seconds=0.5) == []
    assert 0.4 < loop.time() - started < 2

    async def enqueue_soon() -> None:
        await asyncio.sleep(0.3)
        await queue.enqueue("render.frame")

    started = loop.time()
    producer = asyncio.create_task(enqueue_soon())
    tasks = await claim(api, worker, wait_seconds=5)
    await producer
    assert len(tasks) == 1 and loop.time() - started < 3


async def test_a_remote_worker_that_goes_quiet_loses_its_tasks(
    api, auth, queue, registry, worker_factory
):
    """Stale recovery does not care how a task was claimed."""

    @registry.task("local.only")
    async def local(ctx, payload): ...

    worker = auth("render-farm", "worker")
    created = await queue.enqueue("render.frame", max_attempts=3)
    await claim(api, worker)
    # Any database-connected worker runs the maintenance that notices.
    async with worker_factory():
        await until(lambda: _status(queue, created.id, "pending"), what="the task to be recovered")
    (again,) = await claim(api, worker)
    assert again["attempt"] == 2
    stale = await api.post(
        f"/v1/worker/tasks/{created.id}/succeed", json={**W, "attempt": 1}, headers=worker
    )
    assert stale.json()["accepted"] is False


async def _status(queue, task_id, status: str) -> bool:
    return (await queue.get(task_id)).status == status
