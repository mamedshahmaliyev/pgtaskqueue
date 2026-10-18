"""Per-task logs: who is writing, and how lines reach the database.

Many tasks share one event loop, so "which task is this line about" cannot be a global. It
is a `ContextVar`: asyncio gives every task its own copy, `asyncio.to_thread` carries it into
worker threads, and so a line written anywhere under an executor's `run()` — its own code, a
library it calls, a thread it started — is attributed to that task and to no other.

Lines are buffered and inserted in batches. A task that logs in a tight loop therefore costs
one INSERT per flush interval, not one per line.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pgtaskqueue.jsonutil import clip, to_jsonable

if TYPE_CHECKING:
    from pgtaskqueue.executor import TaskContext
    from pgtaskqueue.queue import TaskQueue

logger = logging.getLogger("pgtaskqueue.logs")

# The task the current coroutine (or thread) is executing, if any.
current_task: ContextVar[TaskContext | None] = ContextVar("tq_current_task", default=None)

_LEVELS = {"debug": 10, "info": 20, "warning": 30, "error": 40}


def level_name(levelno: int) -> str:
    if levelno >= 40:
        return "error"
    if levelno >= 30:
        return "warning"
    if levelno >= 20:
        return "info"
    return "debug"


class LogBuffer:
    """Log lines waiting to be written, for every task a worker is running."""

    def __init__(self, queue: TaskQueue, *, max_chars: int = 8000, max_pending: int = 50_000):
        self._queue = queue
        self._max_chars = max_chars
        # deque.append and popleft are atomic, which is all the thread-safety this needs:
        # lines arrive from executor threads, and are drained from the event loop.
        self._pending: deque[dict[str, Any]] = deque(maxlen=max_pending)
        # One write at a time, so batches reach the table in the order they were buffered.
        self._writing = asyncio.Lock()
        # Strong references: a shielded write outlives the caller that started it, and the
        # event loop alone would let it be garbage-collected halfway.
        self._writes: set[asyncio.Task[None]] = set()

    def add(
        self,
        task_id: Any,
        attempt: int,
        message: str,
        *,
        level: str = "info",
        step: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        if level not in _LEVELS:
            level = "info"
        try:
            data = to_jsonable(data) if data else None
        except TypeError:
            data = {"repr": clip(repr(data), 2000)}
        self._pending.append(
            {
                "task_id": task_id,
                "attempt": attempt,
                "level": level,
                "step": step,
                "message": clip(str(message).replace("\x00", ""), self._max_chars),
                "data": data,
                "created_at": datetime.now(UTC),
            }
        )

    def __len__(self) -> int:
        return len(self._pending)

    async def flush(self) -> None:
        """Write everything buffered so far. Never raises: a log line is not worth a task.

        The write is shielded from the caller's cancellation. A flush cancelled halfway —
        the flush loop, when its worker stops — cannot know whether its INSERT had already
        committed: putting the batch back writes it twice, dropping it loses it. So the
        write runs to completion whatever happens to the caller, and the next flush (the
        worker's last one, on its way out) waits for it before writing its own.
        """
        if not self._pending and not self._writing.locked():
            return
        write = asyncio.ensure_future(self._write())
        self._writes.add(write)
        write.add_done_callback(self._writes.discard)
        await asyncio.shield(write)

    async def _write(self) -> None:
        async with self._writing:
            if not self._pending:
                return
            batch = []
            while self._pending:
                batch.append(self._pending.popleft())
            try:
                await self._queue.append_logs(batch)
            except Exception:
                # Most likely the task row was deleted, or the database is briefly away.
                # The lines are dropped rather than retried forever; say so where an
                # operator looks.
                logger.warning("dropped %d task log lines", len(batch), exc_info=True)


class TaskLogHandler(logging.Handler):
    """Routes `logging` records emitted while a task runs into that task's log.

    Installed on the root logger by the worker. Records emitted outside any task — the
    worker's own lifecycle messages — pass through untouched.
    """

    # The queue's own machinery logs while it is serving a task (SQL echo, connection
    # events). Capturing those would write a log line for every log line written.
    _IGNORED = ("pgtaskqueue", "sqlalchemy", "asyncpg", "asyncio")

    def emit(self, record: logging.LogRecord) -> None:
        ctx = current_task.get()
        if ctx is None or record.name.startswith(self._IGNORED):
            return
        try:
            message = record.getMessage()
            if record.exc_info and record.exc_info[1] is not None:
                message = f"{message}\n{logging.Formatter().formatException(record.exc_info)}"
            ctx.log(message, level=level_name(record.levelno), data={"logger": record.name})
        except Exception:  # a logging handler must never raise
            self.handleError(record)
