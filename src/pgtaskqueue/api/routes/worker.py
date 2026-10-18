"""The remote-worker protocol: claim, heartbeat and settle over HTTP.

For workers that cannot (or should not) connect to Postgres. A worker inside your network
uses `tq worker`, which talks to the database directly and is both faster and simpler; this
is for the machine that is somewhere else. `examples/remote_worker.py` is a complete client
in about a hundred lines. Needs the `tasks:work` permission (role `worker`).
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, status

from pgtaskqueue.api.deps import State, Who
from pgtaskqueue.api.schemas import (
    ClaimRequest,
    FailRequest,
    HeartbeatRequest,
    HeartbeatResponse,
    LogsRequest,
    ProgressRequest,
    ReleaseRequest,
    Settled,
    SucceedRequest,
    TaskTypeIn,
    WorkerId,
)
from pgtaskqueue.models import Task
from pgtaskqueue.schemas import TaskOut

router = APIRouter(prefix="/worker", tags=["worker protocol"])


def _settled(task: Task | None) -> Settled:
    return Settled(accepted=task is not None, status=task.status if task else None)


@router.put("/{worker_id}/task-types", status_code=status.HTTP_204_NO_CONTENT)
async def register_task_types(
    worker_id: WorkerId, body: list[TaskTypeIn], who: Who, state: State
) -> None:
    """Publish what this worker can run, so the API can validate payloads for it."""
    await state.service.register_task_types(who, worker_id, body)


@router.post("/claim", response_model=list[TaskOut])
async def claim(body: ClaimRequest, who: Who, state: State) -> list[TaskOut]:
    """Take up to `limit` ready tasks. They come back `running`, owned by `worker_id`
    until it stops heartbeating. Keep each task's `attempt`: every later call needs it."""
    tasks = await state.service.claim(
        who, body.worker_id, body.names, queue=body.queue, limit=body.limit, wait=body.wait_seconds
    )
    return [TaskOut.of(task) for task in tasks]


@router.post("/heartbeat", response_model=HeartbeatResponse)
async def heartbeat(body: HeartbeatRequest, who: Who, state: State) -> HeartbeatResponse:
    """Call every few seconds with every task in hand — well inside TQ_STALE_AFTER_SECONDS,
    or the tasks are given to someone else."""
    owned = await state.service.heartbeat(who, body.worker_id, body.task_ids)
    return HeartbeatResponse(owned=sorted(owned))


@router.post("/tasks/{task_id}/succeed", response_model=Settled)
async def succeed(task_id: uuid.UUID, body: SucceedRequest, who: Who, state: State) -> Settled:
    return _settled(
        await state.service.succeed(who, task_id, body.worker_id, body.attempt, body.output)
    )


@router.post("/tasks/{task_id}/fail", response_model=Settled)
async def fail(task_id: uuid.UUID, body: FailRequest, who: Who, state: State) -> Settled:
    """Report a failed attempt. The server decides between retrying and failing for good,
    from the task's `max_attempts`; `status` in the answer says which."""
    return _settled(
        await state.service.fail(
            who,
            task_id,
            body.worker_id,
            body.attempt,
            body.error,
            retry=body.retry,
            retry_in=body.retry_in,
        )
    )


@router.post("/tasks/{task_id}/progress", response_model=Settled)
async def progress(task_id: uuid.UUID, body: ProgressRequest, who: Who, state: State) -> Settled:
    accepted = await state.service.progress(
        who, task_id, body.worker_id, body.attempt, body.progress, body.step
    )
    return Settled(accepted=accepted, status="running" if accepted else None)


@router.post("/tasks/{task_id}/logs", status_code=status.HTTP_204_NO_CONTENT)
async def logs(task_id: uuid.UUID, body: LogsRequest, who: Who, state: State) -> None:
    await state.service.append_logs(
        who, task_id, body.worker_id, body.attempt, [line.model_dump() for line in body.lines]
    )


@router.post("/release")
async def release(body: ReleaseRequest, who: Who, state: State) -> dict[str, int]:
    """Hand tasks back untouched (attempt not counted) — what a worker does on shutdown."""
    return {"released": await state.service.release(who, body.worker_id, body.task_ids)}
