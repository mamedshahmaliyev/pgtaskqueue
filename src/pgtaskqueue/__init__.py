"""A PostgreSQL-backed task queue for asyncio.

    from pgtaskqueue import Database, TaskQueue, Executor, TaskContext, register

    queue = TaskQueue(Database())
    await queue.enqueue("email.send", {"to": "ada@example.com"})

The README is the tour; `docs/` has the detail.
"""

from pgtaskqueue._version import __version__
from pgtaskqueue.archive import Archive
from pgtaskqueue.db import Database
from pgtaskqueue.enums import TaskEvent, TaskStatus
from pgtaskqueue.errors import (
    DuplicateTaskError,
    NonRetryable,
    QueueConfigError,
    Retry,
    ScheduleConflict,
    Snooze,
    TaskQueueError,
    TaskStopped,
    TaskTimeout,
)
from pgtaskqueue.executor import Executor, Registry, TaskContext, register, registry, task
from pgtaskqueue.models import Task, TaskLog
from pgtaskqueue.monitor import Monitor
from pgtaskqueue.queue import NewTask, TaskQueue
from pgtaskqueue.schedules import Schedule, Schedules
from pgtaskqueue.settings import Settings, get_settings
from pgtaskqueue.worker import Pool, PoolSpec, Worker, run_worker

__all__ = [
    "Archive",
    "Database",
    "DuplicateTaskError",
    "Executor",
    "Monitor",
    "NewTask",
    "NonRetryable",
    "Pool",
    "PoolSpec",
    "QueueConfigError",
    "Registry",
    "Retry",
    "Schedule",
    "ScheduleConflict",
    "Schedules",
    "Settings",
    "Snooze",
    "Task",
    "TaskContext",
    "TaskEvent",
    "TaskLog",
    "TaskQueue",
    "TaskQueueError",
    "TaskStatus",
    "TaskStopped",
    "TaskTimeout",
    "Worker",
    "__version__",
    "get_settings",
    "register",
    "registry",
    "run_worker",
    "task",
]
