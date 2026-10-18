"""How a task looks from outside: the shape the API, the MCP tools and webhooks all return.

In the core package, not the `api` extra, because the webhook executor — which runs in a
worker, without FastAPI installed — sends the same representation, and the `tq` command
prints it.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from pgtaskqueue.models import ScheduleRow, Task, TaskLog


class CallbackOut(BaseModel):
    """A task's callback as it is shown back: where it goes, never what authenticates it."""

    url: str
    events: list[str] | None = None
    has_secret: bool = False
    header_names: list[str] = Field(default_factory=list)


class TaskOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    queue: str
    name: str
    status: str
    priority: int
    payload: dict[str, Any]
    metadata: dict[str, Any]
    progress: dict[str, Any]
    step: str | None = None
    output: Any | None = None
    error: dict[str, Any] | None = None
    attempt: int
    max_attempts: int
    timeout_seconds: float | None = None
    run_after: datetime
    dedupe_key: str | None = None
    callback: CallbackOut | None = None
    created_by: str | None = None
    # Held back by its account's running limit while `scheduled`. See docs/auth.md#limits.
    capped: bool = False
    worker_id: str | None = None
    spent_seconds: float
    created_at: datetime
    started_at: datetime | None = None
    heartbeat_at: datetime | None = None
    completed_at: datetime | None = None

    @classmethod
    def of(cls, task: Task, *, traceback: bool = True) -> TaskOut:
        error = task.error
        if error and not traceback:
            error = {k: v for k, v in error.items() if k != "traceback"}
        callback = None
        if task.callback and task.callback.get("url"):
            callback = CallbackOut(
                url=task.callback["url"],
                events=task.callback.get("events"),
                has_secret=bool(task.callback.get("secret")),
                header_names=sorted(task.callback.get("headers") or {}),
            )
        return cls(
            id=task.id,
            queue=task.queue_name,
            name=task.name,
            status=task.status,
            priority=task.priority,
            payload=task.payload or {},
            metadata=task.meta or {},
            progress=task.progress or {},
            step=task.step,
            output=task.output,
            error=error,
            attempt=task.attempt,
            max_attempts=task.max_attempts,
            timeout_seconds=task.timeout_seconds,
            run_after=task.run_after,
            dedupe_key=task.dedupe_key,
            callback=callback,
            created_by=task.created_by,
            worker_id=task.worker_id,
            spent_seconds=task.spent_seconds or 0.0,
            created_at=task.created_at,
            started_at=task.started_at,
            heartbeat_at=task.heartbeat_at,
            completed_at=task.completed_at,
        )


class TaskLogOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    task_id: uuid.UUID
    attempt: int
    level: str
    step: str | None = None
    message: str
    data: dict[str, Any] | None = None
    created_at: datetime

    @classmethod
    def of(cls, log: TaskLog) -> TaskLogOut:
        return cls.model_validate(log)


class ScheduleOut(BaseModel):
    name: str
    task: str
    queue: str
    payload: dict[str, Any]
    # One of the two is set.
    cron: str | None = None
    every_seconds: float | None = None
    timezone: str
    priority: int | None = None
    max_attempts: int | None = None
    timeout_seconds: float | None = None
    paused: bool
    # 'code' (declared next to an executor) or 'api'.
    source: str
    created_by: str | None = None
    next_run_at: datetime
    # The last fire that enqueued a task, and that task.
    last_run_at: datetime | None = None
    last_task_id: uuid.UUID | None = None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def of(cls, schedule: ScheduleRow) -> ScheduleOut:
        return cls(
            name=schedule.name,
            task=schedule.task_name,
            queue=schedule.queue_name,
            payload=schedule.payload or {},
            cron=schedule.cron,
            every_seconds=schedule.every_seconds,
            timezone=schedule.timezone,
            priority=schedule.priority,
            max_attempts=schedule.max_attempts,
            timeout_seconds=schedule.timeout_seconds,
            paused=schedule.paused,
            source=schedule.source,
            created_by=schedule.created_by,
            next_run_at=schedule.next_run_at,
            last_run_at=schedule.last_run_at,
            last_task_id=schedule.last_task_id,
            created_at=schedule.created_at,
            updated_at=schedule.updated_at,
        )
