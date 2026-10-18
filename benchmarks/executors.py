"""The tasks the benchmarks run: as close to nothing as a task can be, so what is measured
is the queue and not the work."""

from __future__ import annotations

import asyncio

from pgtaskqueue import TaskContext, task


@task("bench.noop")
async def noop(ctx: TaskContext, payload: dict) -> None:
    return None


@task("bench.sleep")
async def sleep(ctx: TaskContext, payload: dict) -> None:
    """Stands in for I/O-bound work: holds a slot, uses no CPU."""
    await asyncio.sleep(float(payload.get("seconds", 1)))
