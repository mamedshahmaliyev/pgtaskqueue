"""Tasks that run on a schedule.

A schedule is declared next to the task. The worker that runs the task writes it to
`tq_schedules` when it starts, and from then on any worker enqueues the task when it is due:

    tq worker --executors examples.executors.periodic

    tq schedule list
    tq tasks --name demo.tick --limit 5
"""

from __future__ import annotations

from datetime import timedelta

from pydantic import BaseModel

from pgtaskqueue import Executor, Schedule, TaskContext, register, task

# --- an interval -------------------------------------------------------------------------


@task("demo.tick", schedule=Schedule(every=10))
async def tick(ctx: TaskContext, payload: dict) -> dict:
    """Runs every ten seconds. `ctx.metadata` says which schedule enqueued it and the
    time it was due, which is the time to use for "the period this run covers": the task
    itself can start late, or run twice."""
    return {"scheduled_for": ctx.metadata["scheduled_for"]}


# --- a cron expression, in a timezone ----------------------------------------------------


class DigestPayload(BaseModel):
    audience: str = "everyone"


@register
class Digest(Executor[DigestPayload]):
    """One task with two schedules. Each has a name of its own and its own payload, which
    is checked against the payload model when the worker starts, not at 08:00 on Monday."""

    name = "demo.digest"
    payload_model = DigestPayload
    schedules = (
        # 08:00 on working days, by the clock in Berlin, summer and winter.
        Schedule(
            "0 8 * * mon-fri",
            timezone="Europe/Berlin",
            name="demo.digest.weekday",
            payload={"audience": "team"},
        ),
        Schedule(
            every=timedelta(hours=6),
            name="demo.digest.everyone",
            priority=200,
        ),
    )

    async def run(self, ctx: TaskContext, payload: DigestPayload) -> dict:
        await ctx.log(f"digest for {payload.audience}, due {ctx.metadata['scheduled_for']}")
        return {"audience": payload.audience}
