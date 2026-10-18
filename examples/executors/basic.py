"""Small tasks that each show one thing a worker does for you.

Run a worker over them, then enqueue:

    tq worker --executors examples.executors.basic

    tq enqueue demo.echo '{"message": "hello"}' --wait
    tq enqueue demo.sleep '{"seconds": 5}'
    tq enqueue demo.flaky '{"fail_times": 2}'
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import UTC, datetime

import httpx
from pydantic import BaseModel, Field

from pgtaskqueue import Executor, NonRetryable, Retry, Snooze, TaskContext, register, task

logger = logging.getLogger(__name__)


# --- the shortest possible task: a function --------------------------------------------


class EchoPayload(BaseModel):
    message: str = "hello"


@task("demo.echo")
async def echo(ctx: TaskContext, payload: EchoPayload) -> dict:
    """Returns its payload. The return value becomes the task's `output`."""
    return {"message": payload.message, "attempt": ctx.attempt}


# --- I/O-bound work: many at once in one process -----------------------------------------


class SleepPayload(BaseModel):
    seconds: float = Field(default=1.0, ge=0, le=3600)


@register
class Sleep(Executor[SleepPayload]):
    """Waits, reporting progress. Stands in for anything that spends its time awaiting —
    an HTTP call, a model API, a slow query — which is what one event loop is good at:
    a worker with 200 slots runs 200 of these on one core."""

    name = "demo.sleep"
    payload_model = SleepPayload
    timeout = 3700

    async def run(self, ctx: TaskContext, payload: SleepPayload) -> dict:
        steps = 10
        for i in range(steps):
            # Cancelling the task (or stopping the worker) raises CancelledError here, at
            # the await. Nothing to write for that: `finally` blocks run, the rest is skipped.
            await asyncio.sleep(payload.seconds / steps)
            await ctx.set_progress(percent=(i + 1) * 100 // steps)
        return {"slept": payload.seconds}


# --- failure and retry ---------------------------------------------------------------


class FlakyPayload(BaseModel):
    fail_times: int = 2


@register
class Flaky(Executor[FlakyPayload]):
    """Fails its first `fail_times` attempts, then succeeds. Watch `attempt`, `error` and
    `run_after` change on the row between attempts."""

    name = "demo.flaky"
    payload_model = FlakyPayload
    max_attempts = 5

    async def run(self, ctx: TaskContext, payload: FlakyPayload) -> dict:
        if ctx.attempt <= payload.fail_times:
            raise RuntimeError(f"attempt {ctx.attempt} failed on purpose")
        return {"succeeded_on_attempt": ctx.attempt}

    def retry_delay(self, attempt: int, error: BaseException) -> float | None:
        # A second per attempt instead of the default 5s, 10s, 20s… so the demo is quick.
        return float(attempt)


# --- choosing what a failure means -------------------------------------------------------


class FetchPayload(BaseModel):
    url: str


@register
class Fetch(Executor[FetchPayload]):
    """GETs a URL. Shows the three ways to fail, and that a library's own logging lands
    in the task's log: httpx logs each request at INFO, and the worker files that line
    under this task without the executor doing anything."""

    name = "demo.fetch"
    payload_model = FetchPayload
    max_attempts = 4
    timeout = 30

    async def run(self, ctx: TaskContext, payload: FetchPayload) -> dict:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            response = await client.get(payload.url)
        if response.status_code == 429:
            # Retried, and after exactly as long as the server asked for.
            raise Retry("rate limited", delay=float(response.headers.get("retry-after", 30)))
        if response.status_code >= 500:
            # Any other exception: retried with exponential backoff.
            response.raise_for_status()
        if response.status_code >= 400:
            # Asking again will not make a 404 a 200.
            raise NonRetryable(f"{payload.url} answered {response.status_code}")
        logger.info("fetched %d bytes", len(response.content))
        return {
            "status": response.status_code,
            "bytes": len(response.content),
            "sha256": hashlib.sha256(response.content).hexdigest(),
        }


# --- waiting without burning an attempt --------------------------------------------------


class WaitUntilPayload(BaseModel):
    ready_at: datetime


@task("demo.wait_until", max_attempts=1)
async def wait_until(ctx: TaskContext, payload: WaitUntilPayload) -> dict:
    """Checks a condition and, if it does not hold yet, goes back to the queue with
    `Snooze` — which, unlike `Retry`, does not use up an attempt. The pattern for polling
    another system for a result without holding a slot while waiting."""
    remaining = (payload.ready_at - datetime.now(UTC)).total_seconds()
    if remaining > 0:
        raise Snooze(min(remaining, 5), "not ready yet")
    return {"ready": True}


# --- CPU-bound work: a plain function, run in a thread --------------------------------------


class HashPayload(BaseModel):
    rounds: int = Field(default=200_000, ge=1, le=50_000_000)


@register
class Hash(Executor[HashPayload]):
    """A synchronous `run`. The worker calls it in a thread, so it does not stall the
    event loop the other tasks share. Give CPU-bound names a pool of their own with few
    slots (`--tasks demo.hash:2`): threads do not make Python code run in parallel. If the task
    is canceled, `TaskStopped` is raised in the loop below."""

    name = "demo.hash"
    payload_model = HashPayload

    def run(self, ctx: TaskContext, payload: HashPayload) -> dict:
        digest = b"pgtaskqueue"
        for i in range(payload.rounds):
            digest = hashlib.sha256(digest).digest()
            if i and i % 50_000 == 0:
                ctx.log(f"{i} rounds")  # safe from a thread
        return {"digest": digest.hex()}
