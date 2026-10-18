from __future__ import annotations


class TaskQueueError(Exception):
    """Base class for everything this package raises on purpose."""


class DuplicateTaskError(TaskQueueError):
    """Another active task already holds this `dedupe_key`."""


class ScheduleConflict(TaskQueueError):
    """The schedule name is taken by a schedule from the other source: one declared in code
    cannot be replaced through the API, and the other way round."""


class QueueConfigError(TaskQueueError):
    """A worker was asked for a shape it cannot run (unknown names, nothing to consume)."""


class TaskTimeout(TaskQueueError):
    """An attempt ran past the task's `timeout_seconds`. Recorded by the worker, and retried
    like any other failure; executors do not raise it."""


class TaskStopped(BaseException):
    """Raised inside a thread running blocking task code when its run is over: the task was
    canceled, timed out or given to another worker, or the worker is stopping.

    A `BaseException`, like `asyncio.CancelledError`, so that `except Exception` in task code
    does not swallow it. Catch it to clean up, then re-raise.
    """


# --- raised by executors to steer what happens to their task -------------------------------


class Retry(TaskQueueError):
    """Fail this attempt and run again after `delay` seconds (default: the normal backoff).

    Counts as an attempt, so it ends in `failed` once `max_attempts` is used up.
    """

    def __init__(self, reason: str = "", *, delay: float | None = None) -> None:
        super().__init__(reason or "retry requested")
        self.delay = delay


class Snooze(TaskQueueError):
    """Run again after `delay` seconds **without** using up an attempt.

    For "not ready yet" rather than "went wrong": polling another system for a result, or
    waiting for a precondition. A task can snooze indefinitely, so pair it with a deadline
    of your own.
    """

    def __init__(self, delay: float, reason: str = "") -> None:
        super().__init__(reason or f"snoozed for {delay}s")
        self.delay = delay


class NonRetryable(TaskQueueError):
    """Fail the task now, whatever attempts remain. For errors a retry cannot fix."""
