"""Callbacks: signed, delivered by the queue itself, retried like any other task."""

from __future__ import annotations

import json
import time

import pytest

from pgtaskqueue import NonRetryable
from pgtaskqueue.builtin.webhook import SIGNATURE_HEADER, check_url, sign, verify
from tests.helpers import Receiver, make_settings, running_worker, until, wait_for

SECRET = "whsec-test-secret"


# --------------------------------------------------------------------------- signature


def test_signature_round_trip():
    body = b'{"event":"task.succeeded"}'
    header = sign(SECRET, body)
    assert header.startswith("t=") and ",v1=" in header
    assert verify(SECRET, body, header)
    assert not verify("another-secret", body, header)
    assert not verify(SECRET, body + b" ", header)


def test_signature_rejects_replays_and_garbage():
    body = b"{}"
    old = sign(SECRET, body, int(time.time()) - 3600)
    assert not verify(SECRET, body, old)
    assert verify(SECRET, body, old, tolerance=7200)
    # The timestamp is part of what is signed, so it cannot be refreshed by hand.
    _, digest = old.split(",")
    assert not verify(SECRET, body, f"t={int(time.time())},{digest}")
    for garbage in ("", "v1=abc", "t=abc,v1=def", "nonsense"):
        assert not verify(SECRET, body, garbage)


# --------------------------------------------------------------------------- SSRF guard


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/hook",
        "http://localhost:8080/hook",
        "http://10.0.0.5/hook",
        "http://192.168.1.1/hook",
        "http://169.254.169.254/latest/meta-data/",
        "http://[::1]/hook",
    ],
)
async def test_private_addresses_are_refused(url):
    with pytest.raises(NonRetryable, match="non-public address"):
        await check_url(url, allow_private=False)
    await check_url(url, allow_private=True)


@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("ftp://example.com/x", "must be http"),
        ("file:///etc/passwd", "must be http"),
        ("http:///nohost", "must be http"),
        ("https://user:pass@example.com/hook", "credentials"),
    ],
)
async def test_urls_that_are_never_acceptable(url, message):
    with pytest.raises(NonRetryable, match=message):
        await check_url(url, allow_private=True)


# --------------------------------------------------------------------------- delivery


async def test_callback_is_delivered_and_signed(queue, registry, worker_factory):
    @registry.task("report.build")
    async def build(ctx, payload):
        return {"rows": 42}

    async with Receiver() as receiver, worker_factory():
        task = await queue.enqueue(
            "report.build",
            {"month": "2026-09"},
            metadata={"tenant": "acme"},
            callback={
                "url": receiver.url,
                "secret": SECRET,
                "headers": {"X-Tenant": "acme", "Content-Type": "text/plain", "Host": "evil"},
            },
        )
        await until(lambda: receiver.requests, what="the webhook")
        (delivery,) = await queue.list(name="tq.webhook")
        delivery = await wait_for(queue, delivery.id)

    (request,) = receiver.requests
    assert (request.method, request.path) == ("POST", "/hook")
    assert verify(SECRET, request.body, request.headers[SIGNATURE_HEADER.lower()])
    assert request.headers["x-taskqueue-event"] == "task.succeeded"
    assert request.headers["x-taskqueue-task"] == str(task.id)
    assert request.headers["x-taskqueue-delivery"] == str(delivery.id)
    # The caller's own header is passed on; the ones that describe the request are not theirs.
    assert request.headers["x-tenant"] == "acme"
    assert request.headers["content-type"] == "application/json"
    assert request.headers["host"].startswith("127.0.0.1")

    body = json.loads(request.body)
    assert (body["event"], body["attempt"], body["delivery_id"]) == (
        "task.succeeded",
        1,
        str(delivery.id),
    )
    sent = body["task"]
    assert (sent["id"], sent["status"], sent["output"]) == (
        str(task.id),
        "succeeded",
        {"rows": 42},
    )
    assert sent["metadata"] == {"tenant": "acme"}
    # The secret never travels: not in the body, not in any API response.
    assert SECRET not in request.body.decode()
    assert sent["callback"]["has_secret"] is True

    assert (delivery.status, delivery.output) == ("succeeded", {"status_code": 200})
    assert delivery.payload == {"task_id": str(task.id), "event": "task.succeeded"}


async def test_failure_is_reported_without_the_traceback(queue, registry, worker_factory):
    @registry.task("t.broken", max_attempts=1)
    async def broken(ctx, payload):
        raise ValueError("bad input")

    async with Receiver() as receiver, worker_factory():
        await queue.enqueue("t.broken", callback={"url": receiver.url})
        await until(lambda: receiver.requests, what="the webhook")

    request = receiver.requests[0]
    body = json.loads(request.body)
    assert body["event"] == "task.failed"
    assert body["task"]["error"]["message"] == "bad input"
    assert "traceback" not in body["task"]["error"]
    # No secret on the task and none configured: delivered unsigned.
    assert SIGNATURE_HEADER.lower() not in request.headers


async def test_global_secret_signs_when_the_task_names_none(queue, registry, db):
    @registry.task("t.ok")
    async def ok(ctx, payload): ...

    signed = make_settings(webhook_secret="the-deployment-wide-secret")
    async with Receiver() as receiver, running_worker(db, registry, signed):
        await queue.enqueue("t.ok", callback={"url": receiver.url})
        await until(lambda: receiver.requests, what="the webhook")
    request = receiver.requests[0]
    assert verify(
        "the-deployment-wide-secret", request.body, request.headers[SIGNATURE_HEADER.lower()]
    )


async def test_cancel_notifies_too(queue, registry, worker_factory):
    @registry.task("t.ok")
    async def ok(ctx, payload): ...

    async with Receiver() as receiver, worker_factory():
        task = await queue.enqueue("t.ok", delay=60, callback={"url": receiver.url})
        await queue.cancel(task.id)
        await until(lambda: receiver.requests, what="the webhook")
    assert json.loads(receiver.requests[0].body)["event"] == "task.canceled"


async def test_only_the_requested_events_are_sent(queue, registry, worker_factory):
    @registry.task("t.ok")
    async def ok(ctx, payload): ...

    async with Receiver() as receiver, worker_factory():
        task = await queue.enqueue(
            "t.ok", callback={"url": receiver.url, "events": ["task.failed"]}
        )
        await wait_for(queue, task.id)
    assert await queue.list(name="tq.webhook") == []
    assert receiver.requests == []


async def test_delivery_is_retried_until_the_receiver_accepts(queue, registry, worker_factory):
    @registry.task("t.ok")
    async def ok(ctx, payload): ...

    async with Receiver(statuses=[503, 500]) as receiver, worker_factory():
        await queue.enqueue("t.ok", callback={"url": receiver.url, "secret": SECRET})
        await until(lambda: len(receiver.requests) == 3, what="three deliveries")
        (delivery,) = await queue.list(name="tq.webhook")
        delivery = await wait_for(queue, delivery.id)

    assert (delivery.status, delivery.attempt) == ("succeeded", 3)
    # One delivery id across the retries, so a receiver can de-duplicate; a fresh
    # signature and attempt number each time.
    assert {r.headers["x-taskqueue-delivery"] for r in receiver.requests} == {str(delivery.id)}
    assert [json.loads(r.body)["attempt"] for r in receiver.requests] == [1, 2, 3]
    assert all(
        verify(SECRET, r.body, r.headers[SIGNATURE_HEADER.lower()]) for r in receiver.requests
    )
    logs = [line.message for line in await queue.get_logs(delivery.id)]
    assert sum("-> 503" in line or "-> 500" in line or "-> 200" in line for line in logs) == 3


async def test_retry_after_is_honoured(queue, registry, worker_factory):
    @registry.task("t.ok")
    async def ok(ctx, payload): ...

    busy = Receiver(statuses=[429], headers={"Retry-After": "120"})
    async with busy as receiver, worker_factory():
        await queue.enqueue("t.ok", callback={"url": receiver.url})
        await until(lambda: receiver.requests, what="the webhook")
        (delivery,) = await queue.list(name="tq.webhook")
        delivery = await wait_for(queue, delivery.id, ["scheduled"])
    assert 100 < (delivery.run_after - delivery.created_at).total_seconds() < 140


async def test_a_refusal_is_not_retried(queue, registry, worker_factory):
    @registry.task("t.ok")
    async def ok(ctx, payload): ...

    async with Receiver(statuses=[410]) as receiver, worker_factory():
        task = await queue.enqueue("t.ok", callback={"url": receiver.url})
        await until(lambda: receiver.requests, what="the webhook")
        (delivery,) = await queue.list(name="tq.webhook")
        delivery = await wait_for(queue, delivery.id)

    assert (delivery.status, delivery.attempt) == ("failed", 1)
    assert "410" in delivery.error["message"]
    # The task itself is unaffected by what happened to its notification...
    assert (await queue.get(task.id)).status == "succeeded"
    # ...and a failed delivery does not try to notify about itself.
    assert len(await queue.list(name="tq.webhook")) == 1


async def test_private_callback_urls_are_refused_by_default(queue, registry, db):
    @registry.task("t.ok")
    async def ok(ctx, payload): ...

    guarded = make_settings(webhook_allow_private=False)
    async with Receiver() as receiver, running_worker(db, registry, guarded):
        await queue.enqueue("t.ok", callback={"url": receiver.url})
        await until(lambda: queue.list(name="tq.webhook"), what="the delivery task")
        (delivery,) = await queue.list(name="tq.webhook")
        delivery = await wait_for(queue, delivery.id)

    assert receiver.requests == []
    assert (delivery.status, delivery.attempt) == ("failed", 1)
    assert "non-public address 127.0.0.1" in delivery.error["message"]


async def test_dedicated_webhook_worker(queue, registry, db):
    """`TQ_WEBHOOK_SLOTS=0` on the task workers, one worker started for deliveries only."""

    @registry.task("t.ok")
    async def ok(ctx, payload): ...

    from pgtaskqueue import Registry

    no_hooks = make_settings(webhook_slots=0)
    async with Receiver() as receiver:
        async with running_worker(db, registry, no_hooks) as worker:
            assert [pool.label for pool in worker.pools] == ["ALL"]
            task = await queue.enqueue("t.ok", callback={"url": receiver.url})
            await wait_for(queue, task.id)
            (delivery,) = await queue.list(name="tq.webhook")
            assert delivery.status == "pending" and receiver.requests == []
        async with running_worker(db, Registry(), make_settings(webhook_slots=4)) as hooks:
            assert [(pool.label, pool.slots) for pool in hooks.pools] == [("tq.webhook", 4)]
            await until(lambda: receiver.requests, what="the webhook")
