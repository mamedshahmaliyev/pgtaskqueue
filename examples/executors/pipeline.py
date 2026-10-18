"""A task made of steps, and a task that starts other tasks.

Run a worker over them:

    tq worker --executors examples.executors.pipeline

A report that fails in its middle step, then resumes there on the retry instead of starting
over:

    tq enqueue report.build '{"rows": 500, "fail_once_at": "transform"}' --wait
"""

from __future__ import annotations

import asyncio

from pydantic import BaseModel, Field

from pgtaskqueue import Executor, TaskContext, register


class ReportPayload(BaseModel):
    rows: int = Field(default=100, ge=1, le=100_000)
    # Raise in this step on the first attempt, to watch a retry resume.
    fail_once_at: str | None = None
    notify: bool = False


@register
class BuildReport(Executor[ReportPayload]):
    """extract → transform → load, each a durable step.

    `ctx.step(name, fn)` runs `fn` and stores its result on the task row. When a later step
    fails and the task is retried — or a worker dies, or someone requeues it next week —
    the steps that completed return their stored result and are not run again. So a step
    should be worth not repeating (slow, costly, or not idempotent) and return something
    small and JSON-serializable: a path, an id, a summary. Not the data itself.
    """

    name = "report.build"
    payload_model = ReportPayload
    max_attempts = 3
    timeout = 300

    async def run(self, ctx: TaskContext, payload: ReportPayload) -> dict:
        self.ctx, self.payload = ctx, payload
        extracted = await ctx.step("extract", self.extract)
        summary = await ctx.step("transform", lambda: self.transform(extracted))
        location = await ctx.step("load", lambda: self.load(summary))
        if payload.notify:
            # A follow-up task: same queue, with this task's id in its metadata.
            await ctx.enqueue("report.notify", {"location": location, "rows": summary["rows"]})
        return {"location": location, **summary}

    def retry_delay(self, attempt: int, error: BaseException) -> float | None:
        return 1.0

    def _maybe_fail(self, step: str) -> None:
        if self.payload.fail_once_at == step and self.ctx.attempt == 1:
            raise RuntimeError(f"{step} failed on purpose (attempt 1)")

    async def extract(self) -> dict:
        self._maybe_fail("extract")
        await asyncio.sleep(0.2)  # the slow query
        values = [(i * 37) % 101 for i in range(self.payload.rows)]
        self.ctx.log(f"extracted {len(values)} rows")
        # A real extract returns *where it put* the rows — an object key, a staging table —
        # and the next step reads them from there. These rows are cheap to describe, so
        # the description stands in for the handle.
        return {"rows": len(values), "total": sum(values), "max": max(values)}

    async def transform(self, extracted: dict) -> dict:
        self._maybe_fail("transform")
        await self.ctx.set_progress(percent=50)
        return {
            "rows": extracted["rows"],
            "mean": round(extracted["total"] / extracted["rows"], 3),
            "max": extracted["max"],
        }

    async def load(self, summary: dict) -> str:
        self._maybe_fail("load")
        await asyncio.sleep(0.1)  # the upload
        await self.ctx.set_progress(percent=100)
        return f"memory://reports/{self.ctx.id}.json"


class NotifyPayload(BaseModel):
    location: str
    rows: int


@register
class Notify(Executor[NotifyPayload]):
    name = "report.notify"
    payload_model = NotifyPayload

    async def run(self, ctx: TaskContext, payload: NotifyPayload) -> dict:
        parent = ctx.metadata.get("parent_task_id")
        ctx.log(f"report with {payload.rows} rows is at {payload.location} (from {parent})")
        return {"notified": True}


class FanoutPayload(BaseModel):
    count: int = Field(default=10, ge=1, le=10_000)
    seconds: float = 0.5


@register
class Fanout(Executor[FanoutPayload]):
    """Splits into `count` child tasks in one statement, then finishes. The children run
    wherever there is a free slot; find them with `metadata.parent_task_id`."""

    name = "report.fanout"
    payload_model = FanoutPayload

    async def run(self, ctx: TaskContext, payload: FanoutPayload) -> dict:
        from pgtaskqueue import NewTask

        children = await ctx.queue.enqueue_many(
            NewTask(
                "demo.sleep",
                {"seconds": payload.seconds},
                queue=ctx.task.queue_name,
                metadata={"parent_task_id": str(ctx.id), "index": i},
                # Safe to retry: a second attempt of this task re-creates no child whose
                # first copy is still waiting or running.
                dedupe_key=f"{ctx.id}:{i}",
                created_by=ctx.task.created_by,
            )
            for i in range(payload.count)
        )
        return {"children": len(children)}
