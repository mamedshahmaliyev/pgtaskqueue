from __future__ import annotations

import json
import uuid
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query, Response, status

from pgtaskqueue.api.deps import State, Who
from pgtaskqueue.api.schemas import (
    LogPage,
    PauseRequest,
    QueueOut,
    RequeueRequest,
    TaskBatchCreate,
    TaskBatchCreated,
    TaskCreate,
    TaskCreated,
    TaskPage,
    TaskPatch,
    TaskTypeOut,
    Timeline,
    WorkerOut,
)
from pgtaskqueue.auth.principal import Principal
from pgtaskqueue.auth.roles import Permission
from pgtaskqueue.enums import TaskStatus
from pgtaskqueue.models import Task
from pgtaskqueue.schemas import TaskLogOut, TaskOut

router = APIRouter()


def _out(task: Task, who: Principal) -> TaskOut:
    # A traceback shows file paths and code. It is for whoever operates the queue; someone
    # who can only see their own tasks gets the error's type and message.
    return TaskOut.of(task, traceback=who.has(Permission.TASKS_READ_ALL))


# --------------------------------------------------------------------------- tasks


@router.post(
    "/tasks", response_model=TaskCreated, status_code=status.HTTP_201_CREATED, tags=["tasks"]
)
async def enqueue(body: TaskCreate, response: Response, who: Who, state: State) -> TaskCreated:
    """Add a task. Answers 201, or 200 with `created: false` when `dedupe_key` matched a
    task that is already waiting or running (that task is returned)."""
    task = await state.service.enqueue(who, body)
    if not task.created:
        response.status_code = status.HTTP_200_OK
    return TaskCreated(**_out(task, who).model_dump(), created=task.created)


@router.post(
    "/tasks/batch",
    response_model=TaskBatchCreated,
    status_code=status.HTTP_201_CREATED,
    tags=["tasks"],
)
async def enqueue_batch(body: TaskBatchCreate, who: Who, state: State) -> TaskBatchCreated:
    """Add up to 1000 tasks in one statement. All are validated before any is written."""
    created = await state.service.enqueue_batch(who, body.tasks)
    return TaskBatchCreated(
        created=[task.id for task in created], skipped=len(body.tasks) - len(created)
    )


@router.get("/tasks", response_model=TaskPage, tags=["tasks"])
async def list_tasks(
    who: Who,
    state: State,
    queue: str | None = None,
    name: str | None = None,
    status_: Annotated[list[TaskStatus] | None, Query(alias="status")] = None,
    created_by: str | None = None,
    worker_id: str | None = None,
    metadata: Annotated[
        str | None,
        Query(description='JSON object the task\'s metadata must contain, e.g. {"tenant":"acme"}'),
    ] = None,
    before: Annotated[
        uuid.UUID | None, Query(description="`next_before` of the previous page")
    ] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> TaskPage:
    """Tasks, newest first. Without `tasks:read_all`, only the caller's own."""
    labels: dict[str, Any] | None = None
    if metadata:
        try:
            labels = json.loads(metadata)
        except json.JSONDecodeError as exc:
            raise HTTPException(422, "metadata must be JSON") from exc
        if not isinstance(labels, dict):
            raise HTTPException(422, "metadata must be a JSON object")
    tasks = await state.service.list_tasks(
        who,
        queue=queue,
        name=name,
        status=[s.value for s in status_] if status_ else None,
        created_by=created_by,
        worker_id=worker_id,
        metadata=labels,
        before=before,
        limit=limit,
    )
    return TaskPage(
        items=[_out(task, who) for task in tasks],
        next_before=tasks[-1].id if len(tasks) == limit else None,
    )


@router.get("/tasks/{task_id}", response_model=TaskOut, tags=["tasks"])
async def get_task(
    task_id: uuid.UUID,
    who: Who,
    state: State,
    wait: Annotated[
        float,
        Query(ge=0, description="Hold the request up to this many seconds for the task to finish"),
    ] = 0,
) -> TaskOut:
    """One task. `?wait=20` long-polls: the answer comes when the task finishes or the time
    is up, whichever is first — check `status` to tell which."""
    return _out(await state.service.get_task(who, task_id, wait), who)


@router.get("/tasks/{task_id}/logs", response_model=LogPage, tags=["tasks"])
async def task_logs(
    task_id: uuid.UUID,
    who: Who,
    state: State,
    after_id: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 500,
) -> LogPage:
    """A task's log lines, oldest first. Poll with `after_id=<last_id>` to follow it."""
    logs = await state.service.task_logs(who, task_id, after_id=after_id, limit=limit)
    return LogPage(
        items=[TaskLogOut.of(line) for line in logs], last_id=logs[-1].id if logs else after_id
    )


@router.post("/tasks/{task_id}/cancel", response_model=TaskOut, tags=["tasks"])
async def cancel(task_id: uuid.UUID, who: Who, state: State) -> TaskOut:
    """Cancel a task that has not finished. A running task is interrupted at its worker's
    next heartbeat (or at once, where LISTEN/NOTIFY is on). 409 if it had finished."""
    return _out(await state.service.cancel(who, task_id), who)


@router.post("/tasks/{task_id}/requeue", response_model=TaskOut, tags=["tasks"])
async def requeue(
    task_id: uuid.UUID, who: Who, state: State, body: RequeueRequest | None = None
) -> TaskOut:
    """Run a finished task again, in place: same id, attempts start over, completed steps
    are kept unless `reset_progress`."""
    reset = body.reset_progress if body else False
    return _out(await state.service.requeue(who, task_id, reset), who)


@router.patch("/tasks/{task_id}", response_model=TaskOut, tags=["tasks"])
async def update_task(task_id: uuid.UUID, body: TaskPatch, who: Who, state: State) -> TaskOut:
    """Change a task that has not started. 409 once a worker has claimed it."""
    return _out(await state.service.update(who, task_id, body), who)


# --------------------------------------------------------------------------- observability


@router.get("/task-types", response_model=list[TaskTypeOut], tags=["observability"])
async def task_types(who: Who, state: State) -> list[Any]:
    """What workers have said they can run: names, payload schemas, defaults."""
    return await state.service.task_types(who)


@router.get("/queues", response_model=list[QueueOut], tags=["observability"])
async def queues(who: Who, state: State) -> list[dict[str, Any]]:
    return await state.service.queues(who)


@router.post("/queues/{queue}/pause", response_model=QueueOut, tags=["observability"])
async def pause(
    queue: str, who: Who, state: State, body: PauseRequest | None = None
) -> dict[str, Any]:
    """Stop workers claiming from a queue. Enqueueing still works and running tasks finish."""
    return await state.service.set_paused(who, queue, True, body.note if body else None)


@router.post("/queues/{queue}/resume", response_model=QueueOut, tags=["observability"])
async def resume(queue: str, who: Who, state: State) -> dict[str, Any]:
    return await state.service.set_paused(who, queue, False)


@router.get("/workers", response_model=list[WorkerOut], tags=["observability"])
async def workers(who: Who, state: State, all: bool = False) -> list[dict[str, Any]]:
    """Workers that are alive; `?all=true` includes those that stopped in the last week."""
    return await state.service.workers(who, alive_only=not all)


@router.get("/stats", tags=["observability"])
async def stats(
    who: Who, state: State, window_minutes: Annotated[int, Query(ge=1, le=10080)] = 60
) -> dict[str, Any]:
    """Queue depth now, and throughput and timings over the window, per queue and name."""
    return await state.service.stats(who, window_minutes)


@router.get("/stats/timeline", response_model=Timeline, tags=["observability"])
async def timeline(
    who: Who,
    state: State,
    window_minutes: Annotated[int, Query(ge=1, le=10080)] = 60,
    buckets: Annotated[int, Query(ge=1, le=200)] = 30,
) -> dict[str, Any]:
    """Tasks finished over the window, cut into `buckets` equal slices that end now: how
    many succeeded, failed and were canceled in each. What a chart of throughput draws."""
    return await state.service.timeline(who, window_minutes, buckets)
