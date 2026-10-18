"""`tq.webhook`: tell a task's `callback.url` that the task has finished.

A delivery is an ordinary task, enqueued in the same transaction that finished the task it
reports on. So it cannot be lost between "the task succeeded" and "someone was told", it is
retried with backoff like anything else, and every attempt leaves a row and log lines.

The request:

    POST <callback.url>
    Content-Type: application/json
    X-TaskQueue-Event: task.succeeded
    X-TaskQueue-Delivery: <id of this delivery; the same across its retries>
    X-TaskQueue-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>.<body>">

    {"event": "...", "delivery_id": "...", "attempt": 1, "sent_at": "...", "task": {...}}

docs/api.md#webhooks shows how a receiver verifies the signature.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import ipaddress
import json
import time
import uuid
from datetime import UTC, datetime
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel

from pgtaskqueue._version import __version__
from pgtaskqueue.enums import WEBHOOK_TASK
from pgtaskqueue.errors import NonRetryable, Retry
from pgtaskqueue.executor import Executor, TaskContext
from pgtaskqueue.schemas import TaskOut

SIGNATURE_HEADER = "X-TaskQueue-Signature"
# Headers a callback may not set for itself: ours, and the ones that describe the request.
_PROTECTED_HEADERS = {"host", "content-length", "content-type", "transfer-encoding"}
_MAX_RETRY_AFTER = 3600.0


class WebhookPayload(BaseModel):
    task_id: uuid.UUID
    event: str


def sign(secret: str, body: bytes, timestamp: int | None = None) -> str:
    """The value of `X-TaskQueue-Signature` for `body`."""
    ts = int(time.time()) if timestamp is None else timestamp
    digest = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    return f"t={ts},v1={digest}"


def verify(secret: str, body: bytes, header: str, *, tolerance: float = 300.0) -> bool:
    """Whether `header` is a valid, recent signature of `body`. For receivers and tests."""
    try:
        parts = dict(item.split("=", 1) for item in header.split(","))
        ts = int(parts["t"])
    except (KeyError, ValueError):
        return False
    if abs(time.time() - ts) > tolerance:
        return False
    return hmac.compare_digest(sign(secret, body, ts), header)


async def check_url(url: str, *, allow_private: bool) -> None:
    """Refuse a callback URL that is not plain http(s) to a public address.

    A callback URL is supplied by whoever enqueued the task and fetched from inside your
    network, which is the shape of a server-side request forgery: without this check, a
    submitter could aim the worker at a metadata endpoint or an internal admin port.

    The host is resolved and every address it resolves to is checked. The connection that
    follows resolves the name again, so this is a guard against mistakes and casual misuse,
    not against an attacker who controls DNS for the name — for untrusted submitters, send
    worker egress through a proxy that enforces the same rule. See SECURITY.md.
    """
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise NonRetryable(f"callback url must be http(s): {url!r}")
    if parts.username or parts.password:
        raise NonRetryable("callback url must not carry credentials")
    if allow_private:
        return
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(parts.hostname, parts.port or 0)
    except OSError as exc:
        raise Retry(f"could not resolve {parts.hostname!r}: {exc}") from exc
    for info in infos:
        address = ipaddress.ip_address(info[4][0].split("%")[0])
        if not address.is_global:
            raise NonRetryable(
                f"callback host {parts.hostname!r} resolves to the non-public address "
                f"{address} (set TQ_WEBHOOK_ALLOW_PRIVATE=true to allow it)"
            )


class Webhook(Executor[WebhookPayload]):
    """Delivers a finished task's callback."""

    name = WEBHOOK_TASK
    payload_model = WebhookPayload

    async def run(self, ctx: TaskContext, payload: WebhookPayload) -> dict:
        settings = ctx.queue.settings
        task = await ctx.queue.get(payload.task_id)
        if task is None:
            raise NonRetryable(f"task {payload.task_id} no longer exists")
        callback = task.callback or {}
        url = callback.get("url")
        if not url:
            raise NonRetryable(f"task {payload.task_id} has no callback url")
        await check_url(url, allow_private=settings.webhook_allow_private)

        body = json.dumps(
            {
                "event": payload.event,
                "delivery_id": str(ctx.id),
                "attempt": ctx.attempt,
                "sent_at": datetime.now(UTC).isoformat(),
                # Without the traceback: it is for whoever operates the queue, and the
                # receiver can fetch the task if it is allowed to see one.
                "task": TaskOut.of(task, traceback=False).model_dump(mode="json"),
            },
            separators=(",", ":"),
        ).encode()

        headers = {
            str(key): str(value)
            for key, value in (callback.get("headers") or {}).items()
            if str(key).lower() not in _PROTECTED_HEADERS
            and not str(key).lower().startswith("x-taskqueue-")
        }
        headers.update(
            {
                "Content-Type": "application/json",
                "User-Agent": f"pgtaskqueue/{__version__}",
                "X-TaskQueue-Event": payload.event,
                "X-TaskQueue-Delivery": str(ctx.id),
                "X-TaskQueue-Task": str(task.id),
            }
        )
        if secret := callback.get("secret") or settings.webhook_secret:
            headers[SIGNATURE_HEADER] = sign(secret, body)

        try:
            # Redirects are not followed: the address that was checked is the one called.
            async with httpx.AsyncClient(
                timeout=settings.webhook_timeout_seconds, follow_redirects=False
            ) as client:
                response = await client.post(url, content=body, headers=headers)
        except httpx.HTTPError as exc:
            raise Retry(f"{type(exc).__name__}: {exc}") from exc

        status = response.status_code
        ctx.log(f"POST {url} -> {status}", data={"status_code": status})
        if 200 <= status < 300:
            return {"status_code": status}
        if status in (408, 425, 429) or status >= 500:
            raise Retry(f"receiver answered {status}", delay=_retry_after(response))
        raise NonRetryable(f"receiver answered {status}; not retrying")


def _retry_after(response: httpx.Response) -> float | None:
    try:
        return min(max(float(response.headers["retry-after"]), 0.0), _MAX_RETRY_AFTER)
    except (KeyError, ValueError):
        return None
