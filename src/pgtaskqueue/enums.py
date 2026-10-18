from __future__ import annotations

from enum import StrEnum


class TaskStatus(StrEnum):
    """Where a task is in its life. See docs/architecture.md for the transitions."""

    # Waiting for `run_after`. Not claimable; the promote loop moves it to `pending`.
    SCHEDULED = "scheduled"
    # Ready, waiting for a free slot. The only status the claim query reads.
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


ACTIVE_STATUSES: tuple[TaskStatus, ...] = (
    TaskStatus.SCHEDULED,
    TaskStatus.PENDING,
    TaskStatus.RUNNING,
)
TERMINAL_STATUSES: tuple[TaskStatus, ...] = (
    TaskStatus.SUCCEEDED,
    TaskStatus.FAILED,
    TaskStatus.CANCELED,
)


class LogLevel(StrEnum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class TaskEvent(StrEnum):
    """What a webhook reports. One per terminal status."""

    SUCCEEDED = "task.succeeded"
    FAILED = "task.failed"
    CANCELED = "task.canceled"


# NOTIFY channels. Payloads are a queue name and a task id respectively.
CHANNEL_ENQUEUE = "tq_enqueue"
CHANNEL_CANCEL = "tq_cancel"

# The built-in task that delivers a `callback`. Reserved prefix: `tq.`
WEBHOOK_TASK = "tq.webhook"
