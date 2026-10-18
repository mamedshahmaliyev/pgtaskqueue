"""Request and response bodies. `TaskOut` and `TaskLogOut` live in `pgtaskqueue.schemas`,
because webhooks send them too."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Any

from pydantic import AnyHttpUrl, AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from pgtaskqueue.enums import TaskEvent
from pgtaskqueue.schemas import TaskLogOut, TaskOut

TaskName = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:\-]{0,127}$")]
QueueName = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._\-]{0,63}$")]
# `priority` is a smallint; lower runs first.
Priority = Annotated[int, Field(ge=0, le=32767)]


class Strict(BaseModel):
    # A misspelled field is an error, not a silently ignored option.
    model_config = ConfigDict(extra="forbid")


# --------------------------------------------------------------------------- tasks


class CallbackIn(Strict):
    """Where to POST when the task finishes. See docs/api.md#webhooks."""

    url: AnyHttpUrl
    # Signs the delivery (X-TaskQueue-Signature). Falls back to TQ_WEBHOOK_SECRET.
    secret: str | None = Field(default=None, min_length=8, max_length=256)
    headers: dict[str, str] | None = Field(default=None, max_length=20)
    # Which outcomes to report. Default: all three.
    events: list[TaskEvent] | None = None


class TaskCreate(Strict):
    name: TaskName
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: QueueName = "default"
    priority: Priority | None = None
    run_after: AwareDatetime | None = None
    delay_seconds: float | None = Field(default=None, ge=0)
    max_attempts: int | None = Field(default=None, ge=1, le=100)
    timeout_seconds: float | None = Field(default=None, gt=0)
    dedupe_key: str | None = Field(default=None, min_length=1, max_length=256)
    metadata: dict[str, Any] = Field(default_factory=dict)
    callback: CallbackIn | None = None

    @model_validator(mode="after")
    def _one_schedule(self) -> TaskCreate:
        if self.run_after is not None and self.delay_seconds is not None:
            raise ValueError("give run_after or delay_seconds, not both")
        return self


class TaskCreated(TaskOut):
    # False when `dedupe_key` matched a task that was already waiting or running; the task
    # returned is that one.
    created: bool = True


class TaskBatchCreate(Strict):
    tasks: list[TaskCreate] = Field(min_length=1, max_length=1000)


class TaskBatchCreated(BaseModel):
    created: list[uuid.UUID]
    # Tasks whose dedupe_key matched one that is already active.
    skipped: int


class TaskPatch(Strict):
    """Changes to a task that has not started. Only the fields sent are changed."""

    priority: Priority | None = None
    run_after: AwareDatetime | None = None
    payload: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    max_attempts: int | None = Field(default=None, ge=1, le=100)
    timeout_seconds: float | None = Field(default=None, gt=0)


class TaskPage(BaseModel):
    items: list[TaskOut]
    # Pass as `before` to get the next (older) page. None on the last page.
    next_before: uuid.UUID | None = None


class LogPage(BaseModel):
    items: list[TaskLogOut]
    # Pass as `after_id` to get what was written since.
    last_id: int


class RequeueRequest(Strict):
    # Forget completed steps, so the task starts from the beginning.
    reset_progress: bool = False


# --------------------------------------------------------------------------- schedules


class ScheduleIn(Strict):
    """A periodic schedule, whole: `PUT` replaces every field here. See docs/schedules.md."""

    # The task to enqueue.
    task: TaskName
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: QueueName = "default"
    # Five fields (`0 3 * * *`) or an alias (`@hourly`), read in `timezone`.
    cron: str | None = Field(default=None, min_length=1, max_length=200)
    every_seconds: float | None = Field(default=None, ge=1)
    timezone: str = Field(default="UTC", min_length=1, max_length=64)
    priority: Priority | None = None
    max_attempts: int | None = Field(default=None, ge=1, le=100)
    timeout_seconds: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _one_timing(self) -> ScheduleIn:
        if (self.cron is None) == (self.every_seconds is None):
            raise ValueError("give cron or every_seconds, one of them")
        return self


# --------------------------------------------------------------------------- observability


class TaskTypeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    description: str | None = None
    payload_schema: dict[str, Any] | None = None
    max_attempts: int | None = None
    timeout_seconds: float | None = None
    registered_by: str | None = None
    updated_at: datetime


class QueueOut(BaseModel):
    name: str
    paused: bool
    note: str | None = None
    scheduled: int
    # Of `scheduled`, the tasks held back by their account's running limit.
    held: int = 0
    pending: int
    running: int
    workers: int
    slots: int


class PauseRequest(Strict):
    note: str | None = Field(default=None, max_length=500)


class WorkerOut(BaseModel):
    id: str
    queue: str
    hostname: str
    pid: int
    pools: list[dict[str, Any]]
    slots: int
    in_flight: int
    counters: dict[str, Any]
    version: str | None = None
    alive: bool
    started_at: datetime
    heartbeat_at: datetime
    stopped_at: datetime | None = None


class TimelineBucket(BaseModel):
    # The bucket covers `bucket_seconds` from here.
    start: datetime
    succeeded: int
    failed: int
    canceled: int


class Timeline(BaseModel):
    window_minutes: int
    bucket_seconds: float
    # Oldest first. The last bucket ends at the moment of the request.
    buckets: list[TimelineBucket]


# --------------------------------------------------------------------------- auth


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"  # noqa: S105
    expires_in: int
    refresh_token: str | None = None
    scope: str | None = None


class Me(BaseModel):
    subject: str
    role: str
    permissions: list[str]
    queues: list[str]
    task_names: list[str]


# An account's limit: None takes the TQ_ACCOUNT_* default, 0 means no limit.
Limit = Annotated[int | None, Field(ge=0, le=1_000_000_000)]
PriorityLimit = Annotated[int | None, Field(ge=0, le=32767)]


class LimitsIn(Strict):
    """See docs/auth.md#limits."""

    # Tasks scheduled or pending at once.
    max_waiting: Limit = None
    # Tasks pending or running at once; the rest are held back until there is room.
    max_running: Limit = None
    # Tasks created in any 60 seconds.
    enqueue_per_minute: Limit = None
    # The most urgent priority the account may ask for.
    min_priority: PriorityLimit = None


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    username: str
    kind: str
    role: str
    queues: list[str]
    task_names: list[str]
    is_active: bool
    max_waiting: int | None = None
    max_running: int | None = None
    enqueue_per_minute: int | None = None
    min_priority: int | None = None
    created_at: datetime
    last_login_at: datetime | None = None
    # In a listing: whether the caller may change this account. See docs/auth.md.
    manageable: bool | None = None


class UserCreate(LimitsIn):
    username: str
    password: str
    role: str = "submitter"
    kind: str = "user"
    queues: list[str] = ["*"]
    task_names: list[str] = ["*"]


class UserPatch(LimitsIn):
    """Only the fields sent are changed. A limit sent as null goes back to the default."""

    role: str | None = None
    queues: list[str] | None = None
    task_names: list[str] | None = None
    is_active: bool | None = None


class RoleOut(BaseModel):
    name: str
    permissions: list[str]
    # Whether the caller may give this role to an account.
    assignable: bool


class LimitValues(BaseModel):
    max_waiting: int
    max_running: int
    enqueue_per_minute: int
    min_priority: int


class OwnLimits(BaseModel):
    max_waiting: int | None
    max_running: int | None
    enqueue_per_minute: int | None
    min_priority: int | None


class LimitUsage(BaseModel):
    # Scheduled or pending; held tasks are among them.
    waiting: int
    held: int
    running: int
    # Tasks created in the last 60 seconds.
    last_minute: int


class AccountLimitsOut(BaseModel):
    username: str
    # What applies: the account's own where it sets one, else the default. 0 is no limit.
    effective: LimitValues
    account: OwnLimits
    defaults: LimitValues
    usage: LimitUsage


class PasswordChange(Strict):
    password: str


# --------------------------------------------------------------------------- worker protocol

WorkerId = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:\-]{0,99}$")]


class TaskTypeIn(Strict):
    name: TaskName
    description: str | None = None
    payload_schema: dict[str, Any] | None = None
    max_attempts: int | None = Field(default=None, ge=1, le=100)
    timeout_seconds: float | None = Field(default=None, gt=0)


class ClaimRequest(Strict):
    worker_id: WorkerId
    names: list[TaskName] = Field(min_length=1, max_length=200)
    queue: QueueName = "default"
    limit: int = Field(default=1, ge=1, le=100)
    # Hold the request open this long if nothing is ready (long polling).
    wait_seconds: float = Field(default=0, ge=0)


class HeartbeatRequest(Strict):
    worker_id: WorkerId
    task_ids: list[uuid.UUID] = Field(max_length=1000)


class HeartbeatResponse(BaseModel):
    # The tasks the worker still owns. Stop working on any that are missing.
    owned: list[uuid.UUID]


class Owned(Strict):
    worker_id: WorkerId
    # The `attempt` the task had when it was claimed.
    attempt: int


class SucceedRequest(Owned):
    output: Any = None


class FailRequest(Owned):
    error: dict[str, Any]
    # False fails the task for good, whatever attempts remain.
    retry: bool = True
    # Seconds until the next attempt. Default: exponential backoff.
    retry_in: float | None = Field(default=None, ge=0)


class ProgressRequest(Owned):
    progress: dict[str, Any] = Field(default_factory=dict)
    step: str | None = None


class LogLine(Strict):
    message: str
    level: str = "info"
    step: str | None = None
    data: dict[str, Any] | None = None


class LogsRequest(Strict):
    worker_id: WorkerId
    attempt: int = 0
    lines: list[LogLine] = Field(min_length=1, max_length=1000)


class ReleaseRequest(Strict):
    worker_id: WorkerId
    task_ids: list[uuid.UUID] | None = None


class Settled(BaseModel):
    # False when the task was no longer this worker's: it was canceled or reassigned, and
    # nothing was written.
    accepted: bool
    status: str | None = None
