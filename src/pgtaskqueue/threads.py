"""Blocking code in a thread that can be stopped.

`asyncio.to_thread` cannot stop what it started: cancel the await and the function runs on
to its end, its result thrown away. For a task that was canceled, timed out or given to
another worker, that is work nobody wants, and sometimes work that does harm.

`run_in_thread` gives each call a thread of its own and, when the await is cancelled, raises
`TaskStopped` inside that thread with CPython's `PyThreadState_SetAsyncExc`. The exception
arrives when the thread next executes Python bytecode, so:

* a loop in Python stops within a few instructions;
* one long call into C (`time.sleep(600)`, a socket read with no timeout, a native library)
  finishes first, and the exception is raised when it returns;
* it can arrive anywhere, including inside a `finally` or between two statements that were
  meant to happen together. Code that must not be torn there has to catch `TaskStopped`.

The awaiting coroutine does not wait for the thread: it is cancelled at once, as before, and
the thread ends shortly after or, in the second case, when its call returns.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import ctypes
import threading
from collections.abc import Callable
from typing import Any

from pgtaskqueue.errors import TaskStopped

# Only CPython has it. Elsewhere a stopped thread runs to its end, as it always did.
_set_async_exc = getattr(getattr(ctypes, "pythonapi", None), "PyThreadState_SetAsyncExc", None)

CAN_STOP_THREADS = _set_async_exc is not None


class _Call:
    """One function running in one thread."""

    def __init__(self, loop: asyncio.AbstractEventLoop, fn: Callable[..., Any], args: Any) -> None:
        self.loop = loop
        self.future: asyncio.Future[Any] = loop.create_future()
        # Guards `finished` against `stop`: an exception must never be sent to a thread
        # that has left the function, because thread ids are reused once a thread ends.
        self._lock = threading.Lock()
        self._finished = False
        self._stopped = False
        # The copy carries the context variables in, as `asyncio.to_thread` does, so the
        # thread's log lines belong to the task. A daemon, so that a thread stuck in a C
        # call cannot keep a stopping worker's process alive.
        self.thread = threading.Thread(
            target=contextvars.copy_context().run,
            args=(self._body, fn, args),
            name=f"tq-call:{getattr(fn, '__qualname__', fn)}",
            daemon=True,
        )

    def _body(self, fn: Callable[..., Any], args: Any) -> None:
        result, error = None, None
        try:
            try:
                result = fn(*args)
            finally:
                with self._lock:
                    self._finished = True
        except BaseException as exc:
            error = exc
        # The loop is closed if the worker has gone; there is then nobody to tell.
        with contextlib.suppress(RuntimeError):
            self.loop.call_soon_threadsafe(self._resolve, result, error)

    def _resolve(self, result: Any, error: BaseException | None) -> None:
        if self.future.done():
            return  # cancelled: the outcome is no longer wanted
        if isinstance(error, TaskStopped):
            # Not from `stop`, or the future would be cancelled. A BaseException must not
            # reach the worker's TaskGroup, where it would end every other run.
            error = RuntimeError("the function raised TaskStopped itself")
        if error is not None:
            self.future.set_exception(error)
        else:
            self.future.set_result(result)

    def stop(self) -> None:
        with self._lock:
            if self._finished or self._stopped or _set_async_exc is None:
                return
            self._stopped = True
            # While `finished` is false the thread is still inside `_body`, so its id is
            # its own.
            _set_async_exc(ctypes.c_ulong(self.thread.ident), ctypes.py_object(TaskStopped))


async def run_in_thread[T](fn: Callable[..., T], /, *args: Any) -> T:
    """Call `fn(*args)` in a thread and wait for it. Cancel the wait and `TaskStopped` is
    raised in the thread."""
    call = _Call(asyncio.get_running_loop(), fn, args)
    call.thread.start()
    try:
        return await call.future
    except asyncio.CancelledError:
        call.stop()
        raise
