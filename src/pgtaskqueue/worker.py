"""The worker: one process, one event loop, many tasks at a time.

Everything a worker does is a coroutine in one `asyncio.TaskGroup`:

    pool loops ──── claim what fits in the free slots, start a run for each task
    runs ────────── one per task being executed; never raise (see `_execute`)
    heartbeat ───── one UPDATE for every running task; cancels the runs it no longer owns
    promote ─────── scheduled → pending when `run_after` passes; held tasks → pending as
                    their accounts' running limits allow
    schedule ────── enqueues the periodic tasks that are due
    maintenance ─── recover tasks of dead workers, prune, refresh the worker's own row
    archive ─────── move old finished tasks to the archive database, if there is one
    log flush ───── batches of buffered task log lines
    listen ──────── LISTEN on a dedicated connection; wakes pools, hastens cancellations

Nothing here holds a database connection for the length of a task: a run takes one for the
few milliseconds of each write and gives it back.

docs/workers.md is the operator's guide; docs/architecture.md explains the guarantees.
"""

from __future__ import annotations

import asyncio
import contextlib
import fnmatch
import inspect
import logging
import os
import random
import signal
import socket
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from functools import partial
from typing import Any

from pgtaskqueue._version import __version__
from pgtaskqueue.archive import Archive, open_archive_database
from pgtaskqueue.db import Database
from pgtaskqueue.enums import CHANNEL_CANCEL, CHANNEL_ENQUEUE, WEBHOOK_TASK, TaskStatus
from pgtaskqueue.errors import NonRetryable, QueueConfigError, Retry, Snooze, TaskTimeout
from pgtaskqueue.executor import Executor, Registry, TaskContext
from pgtaskqueue.executor import registry as default_registry
from pgtaskqueue.jsonutil import error_dict, to_jsonable
from pgtaskqueue.logs import LogBuffer, TaskLogHandler, current_task
from pgtaskqueue.models import Task
from pgtaskqueue.monitor import Monitor
from pgtaskqueue.queue import LOCK_MAINTENANCE, TaskQueue
from pgtaskqueue.schedules import Schedules
from pgtaskqueue.settings import Settings, get_settings

logger = logging.getLogger("pgtaskqueue.worker")

ALL = "ALL"
_GLOB_CHARS = frozenset("*?[")


# --------------------------------------------------------------------------- pools


@dataclass(frozen=True, slots=True)
class PoolSpec:
    """One entry of `--tasks`: which names, and how many of them at once."""

    pattern: str
    # None: use the worker's default slot count. 0: exclude these names.
    slots: int | None = None

    @classmethod
    def parse(cls, raw: str) -> PoolSpec:
        raw = raw.strip()
        pattern, sep, count = raw.rpartition(":")
        if sep and count.strip().lstrip("-").isdigit():
            slots = int(count)
            if slots < 0:
                raise QueueConfigError(f"{raw!r}: slots cannot be negative")
            return cls(pattern.strip(), slots)
        return cls(raw)

    @property
    def is_glob(self) -> bool:
        return not _GLOB_CHARS.isdisjoint(self.pattern)


@dataclass(frozen=True, slots=True)
class Pool:
    """Names that share a concurrency limit: at most `slots` of them run at once."""

    label: str
    names: tuple[str, ...]
    slots: int


def resolve_pools(
    specs: Iterable[str | PoolSpec], names: Sequence[str], default_slots: int
) -> list[Pool]:
    """Turn `--tasks` specs into pools over the registered task `names`.

    Each name lands in at most one pool. Exact names are matched first, then globs in the
    order written, then `ALL` takes what is left — so `image.resize:1,image.*:4,ALL:20`
    means what it reads as. With no specs, every name shares one pool of `default_slots`.
    """
    parsed = [s if isinstance(s, PoolSpec) else PoolSpec.parse(s) for s in specs]
    parsed = [s for s in parsed if s.pattern]
    if not parsed:
        return [Pool(ALL, tuple(names), default_slots)] if names else []

    unassigned = list(names)
    pools: list[Pool] = []

    def take(spec: PoolSpec, matched: list[str]) -> None:
        for name in matched:
            unassigned.remove(name)
        slots = default_slots if spec.slots is None else spec.slots
        if matched and slots > 0:
            pools.append(Pool(spec.pattern, tuple(matched), slots))

    exact = [s for s in parsed if not s.is_glob and s.pattern != ALL]
    globs = [s for s in parsed if s.is_glob]
    rest = [s for s in parsed if s.pattern == ALL]
    if len(rest) > 1:
        raise QueueConfigError(f"{ALL} may be given once")

    for spec in exact:
        if spec.pattern not in names:
            raise QueueConfigError(
                f"no executor is registered for {spec.pattern!r} "
                f"(registered: {', '.join(names) or 'none'})"
            )
        if spec.pattern not in unassigned:
            raise QueueConfigError(f"{spec.pattern!r} is listed twice")
        take(spec, [spec.pattern])
    for spec in globs:
        matched = [n for n in unassigned if fnmatch.fnmatchcase(n, spec.pattern)]
        if not matched and not any(fnmatch.fnmatchcase(n, spec.pattern) for n in names):
            raise QueueConfigError(f"{spec.pattern!r} matches no registered task name")
        take(spec, matched)
    for spec in rest:
        take(spec, list(unassigned))
    return pools


# --------------------------------------------------------------------------- runtime state


@dataclass(slots=True, eq=False)
class _PoolState:
    pool: Pool
    running: set[uuid.UUID] = field(default_factory=set)
    # Set to make the pool's loop look again now: a notification, a freed slot, a stop.
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    # Whether the last claim came back full. If it did there may be a backlog, and a freed
    # slot is worth a claim at once; if it did not, the queue was empty a moment ago and a
    # freed slot can wait for a notification or the next poll.
    maybe_more: bool = True


@dataclass(slots=True, eq=False)
class _Run:
    task: Task
    ctx: TaskContext
    pool: _PoolState
    atask: asyncio.Task[None] | None = None
    # Why the run was cancelled from outside: "lost" (the task is no longer ours) or
    # "shutdown". None while it has not been.
    cancel_reason: str | None = None
    # True once the executor has returned and its outcome is being written. From then on a
    # heartbeat that does not list the task is expected, not a reason to cancel.
    settling: bool = False

    def cancel(self, reason: str) -> None:
        if self.cancel_reason is None and not self.settling and self.atask is not None:
            self.cancel_reason = reason
            self.atask.cancel()


@dataclass(slots=True)
class _Outcome:
    kind: str  # succeeded | retry | failed | snoozed
    output: Any = None
    error: dict[str, Any] | None = None
    delay: float = 0.0


def new_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


# --------------------------------------------------------------------------- the worker


class Worker:
    """Claims and runs tasks until told to stop.

        worker = Worker(queue_name="default", pools=["image.*:2", "ALL:20"])
        reason = await worker.run()      # "signal", "idle", "max_tasks" or "stop"

    `registry` defaults to the one `@register` and `@task` write to.
    """

    def __init__(
        self,
        db: Database | None = None,
        *,
        registry: Registry | None = None,
        queue_name: str | None = None,
        pools: Sequence[str | PoolSpec] | None = None,
        slots: int | None = None,
        settings: Settings | None = None,
        worker_id: str | None = None,
        handle_signals: bool = True,
        capture_logging: bool = True,
    ) -> None:
        self.settings = settings or get_settings()
        self.id = worker_id or new_worker_id()
        self._owns_db = db is None
        self.db = db or Database(settings=self.settings, application_name=f"tq-worker {self.id}")
        self.queue = TaskQueue(self.db, self.settings)
        self.monitor = Monitor(self.db, self.settings)
        self.schedules = Schedules(self.db, self.settings)
        # Opened lazily, by the first batch: a worker whose archive is unreachable still runs
        # tasks, and says so each interval.
        self.archive: Archive | None = None
        if self.settings.archive_database_url:
            self.archive = Archive(
                self.db,
                open_archive_database(self.settings, f"tq-archive {self.id}"),
                self.settings,
            )
        self.queue_name = queue_name or self.settings.worker_queue
        self.registry = registry if registry is not None else default_registry
        self.logs = LogBuffer(self.queue, max_chars=self.settings.log_message_max_chars)

        self._pool_specs = list(pools if pools is not None else self.settings.worker_tasks)
        self._default_slots = slots if slots is not None else self.settings.worker_slots
        self._handle_signals = handle_signals
        self._capture_logging = capture_logging

        self._executors: dict[str, type[Executor]] = {}
        self._pools: list[_PoolState] = []
        self._running: dict[uuid.UUID, _Run] = {}
        self._tg: asyncio.TaskGroup | None = None
        self._stop = asyncio.Event()
        self._force = asyncio.Event()
        self._stopping = False
        self._stop_reason = ""
        self._heartbeat_now = asyncio.Event()
        self._log_handler: TaskLogHandler | None = None
        self._last_activity = time.monotonic()
        # Claims left before TQ_WORKER_MAX_TASKS is reached. None: no limit.
        self._budget: int | None = self.settings.worker_max_tasks or None
        self.counters: dict[str, int] = dict.fromkeys(
            (
                "claimed",
                "succeeded",
                "failed",
                "retried",
                "snoozed",
                "canceled",
                "lost",
                "released",
            ),
            0,
        )

    # ------------------------------------------------------------------ public

    @property
    def pools(self) -> list[Pool]:
        return [state.pool for state in self._pools]

    @property
    def in_flight(self) -> int:
        return len(self._running)

    def stop(self, reason: str = "stop") -> None:
        """Ask the worker to stop: no new claims, running tasks get the grace period, and
        whatever is still running after it is released back to the queue."""
        if not self._stop.is_set():
            self._stop_reason = reason
            self._stop.set()

    def kill(self) -> None:
        """Stop without the grace period: cancel what is running and release it now."""
        self.stop("stop")
        self._force.set()

    async def run(self) -> str:
        """Run until stopped. Returns why: "signal", "idle", "max_tasks" or "stop"."""
        await self._startup()
        try:
            async with asyncio.TaskGroup() as tg:
                self._tg = tg
                pool_loops = [
                    tg.create_task(self._pool_loop(state), name=f"tq-pool:{state.pool.label}")
                    for state in self._pools
                ]
                services = [
                    tg.create_task(coro, name=name)
                    for name, coro in (
                        ("tq-heartbeat", self._heartbeat_loop()),
                        ("tq-promote", self._promote_loop()),
                        ("tq-schedule", self._schedule_loop()),
                        ("tq-maintenance", self._maintenance_loop()),
                        ("tq-archive", self._archive_loop()),
                        ("tq-log-flush", self._log_flush_loop()),
                        ("tq-listen", self._listen_loop()),
                    )
                ]
                try:
                    await self._supervise(pool_loops)
                finally:
                    for service in (*pool_loops, *services):
                        service.cancel()
        finally:
            await self._cleanup()
        return self._stop_reason

    # ------------------------------------------------------------------ start and stop

    async def _startup(self) -> None:
        s = self.settings
        reserved = [name for name in self.registry.names() if name.startswith("tq.")]
        if reserved:
            raise QueueConfigError(f"task names starting with 'tq.' are reserved: {reserved}")
        self._executors = {name: self.registry.get(name) for name in self.registry.names()}
        pools = resolve_pools(self._pool_specs, self.registry.names(), self._default_slots)
        consumed = [name for pool in pools for name in pool.names]
        # Before anything is written, so two schedules under one name stop the worker here.
        declared = self.registry.schedules(consumed)
        if s.webhook_slots > 0:
            from pgtaskqueue.builtin.webhook import Webhook

            self._executors[WEBHOOK_TASK] = Webhook
            pools.append(Pool(WEBHOOK_TASK, (WEBHOOK_TASK,), s.webhook_slots))
        if not pools:
            raise QueueConfigError(
                "nothing to run: no executors are registered (pass --executors or set "
                "TQ_WORKER_EXECUTORS) and TQ_WEBHOOK_SLOTS is 0"
            )
        self._pools = [_PoolState(pool) for pool in pools]

        await self.monitor.register_task_types(
            self.registry.definitions(consumed), registered_by=self.id
        )
        for name in await self.schedules.publish(
            declared, queue=self.queue_name, task_names=consumed
        ):
            logger.warning(
                "schedule %r was not published: a schedule created through the API has that "
                "name. Delete it (`tq schedule delete %s`) or rename this one.",
                name,
                name,
            )
        await self.monitor.worker_started(
            self.id,
            queue_name=self.queue_name,
            hostname=socket.gethostname(),
            pid=os.getpid(),
            pools=[{"label": p.label, "names": list(p.names), "slots": p.slots} for p in pools],
            slots=sum(p.slots for p in pools),
            version=__version__,
        )
        if self._capture_logging and s.capture_log_level:
            self._log_handler = TaskLogHandler(level=s.capture_log_level.upper())
            logging.getLogger().addHandler(self._log_handler)
        if self._handle_signals:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                # Not available on Windows, or outside the main thread: stop() still works.
                with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                    loop.add_signal_handler(sig, self._on_signal, sig)
        self._last_activity = time.monotonic()
        logger.info(
            "worker %s started on queue %r: %s",
            self.id,
            self.queue_name,
            "; ".join(f"{p.label}×{p.slots} ({', '.join(p.names)})" for p in pools),
        )

    def _on_signal(self, sig: signal.Signals) -> None:
        if self._stop.is_set():
            logger.warning("%s again: cancelling running tasks now", sig.name)
            self._force.set()
        else:
            logger.info("%s: finishing running tasks, claiming no more", sig.name)
            self.stop("signal")

    async def _supervise(self, pool_loops: list[asyncio.Task[None]]) -> None:
        """Wait for a reason to stop, then drain."""
        s = self.settings
        while not self._stop.is_set():
            await _wait(self._stop, 0.5)
            if self._stop.is_set():
                break
            finished = self.counters["claimed"] - len(self._running)
            if s.worker_max_tasks and finished >= s.worker_max_tasks:
                self.stop("max_tasks")
            elif (
                s.worker_idle_exit_seconds > 0
                and not self._running
                # A worker that has run nothing has nothing to give back by restarting, so
                # an idle exit recycles a *used* process rather than cycling an unused one.
                and self.counters["claimed"] > 0
                and time.monotonic() - self._last_activity >= s.worker_idle_exit_seconds
            ):
                self.stop("idle")

        self._stopping = True
        for state in self._pools:
            state.wake.set()
        # A claim that was in flight when the stop arrived is finished (and released)
        # before the count of running tasks means anything.
        await asyncio.wait(pool_loops)

        pending = {run.atask for run in self._running.values() if run.atask is not None}
        if pending:
            grace = 0.0 if self._force.is_set() else s.worker_shutdown_grace_seconds
            logger.info(
                "stopping (%s): %d task(s) running, waiting up to %gs",
                self._stop_reason,
                len(pending),
                grace,
            )
            forced = asyncio.ensure_future(self._force.wait())
            try:
                deadline = time.monotonic() + grace
                while pending and not self._force.is_set():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    done, _ = await asyncio.wait(
                        pending | {forced},
                        timeout=remaining,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    pending -= done
            finally:
                forced.cancel()
            if pending:
                logger.warning("releasing %d task(s) that did not finish in time", len(pending))
                for run in list(self._running.values()):
                    run.cancel("shutdown")
                # An executor that swallows cancellation cannot be waited for forever. What
                # it holds is released in bulk by _cleanup, and fencing discards whatever
                # it writes afterwards.
                await asyncio.wait(pending, timeout=10)
        else:
            logger.info("stopping (%s): nothing running", self._stop_reason)

    async def _cleanup(self) -> None:
        if self._log_handler is not None:
            logging.getLogger().removeHandler(self._log_handler)
            self._log_handler = None
        if self._handle_signals:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                    loop.remove_signal_handler(sig)
        # Each step is best-effort and independent: a database that has gone away must not
        # turn a shutdown into a traceback, and stale recovery covers whatever is missed.
        for step in (self._release_leftovers, self.logs.flush, self._mark_stopped):
            try:
                await asyncio.shield(step())
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("shutdown step %s failed", step.__name__, exc_info=True)
        if self._owns_db:
            await self.db.dispose()
        if self.archive is not None:
            await self.archive.archive_db.dispose()
        logger.info(
            "worker %s stopped (%s): %s", self.id, self._stop_reason or "error", self.counters
        )

    async def _release_leftovers(self) -> None:
        released = await self.queue.release(self.id)
        if released:
            self.counters["released"] += released
            logger.warning("released %d task(s) still marked as running here", released)

    async def _mark_stopped(self) -> None:
        await self.monitor.worker_stopped(self.id, in_flight=0, counters=self.counters)

    # ------------------------------------------------------------------ claiming

    async def _pool_loop(self, state: _PoolState) -> None:
        pool = state.pool
        failures = 0
        while not self._stopping:
            # Cleared before the claim, not after: a wake-up that arrives while the claim
            # is in flight must survive it, or a task enqueued in that window waits a poll.
            state.wake.clear()
            free = pool.slots - len(state.running)
            # Under TQ_WORKER_MAX_TASKS, what is asked for is taken from the budget before
            # the claim and what did not come back is returned after it, so two pools
            # claiming at the same moment cannot overshoot between them.
            want = free if self._budget is None else min(free, self._budget)
            if want > 0:
                self._spend(want)
                try:
                    tasks = await self.queue.claim(
                        pool.names, worker_id=self.id, queue=self.queue_name, limit=want
                    )
                    failures = 0
                except Exception as exc:
                    self._spend(-want)
                    failures += 1
                    delay = min(2.0**failures, 30.0)
                    logger.warning(
                        "claim failed (%s: %s); retrying in %gs", type(exc).__name__, exc, delay
                    )
                    await _wait(self._stop, delay)
                    continue
                if self._stopping:
                    if tasks:
                        await self.queue.release(self.id, [task.id for task in tasks])
                    return
                self._spend(len(tasks) - want)
                for task in tasks:
                    self._start(state, task)
                state.maybe_more = len(tasks) == want
            # Full, or the queue had nothing more: sleep until a slot frees, a notification
            # arrives, or — the fallback that makes notifications optional — the poll
            # interval passes. Jittered so a fleet started together does not poll together.
            await _wait(state.wake, self.settings.poll_interval_seconds * random.uniform(0.8, 1.2))

    def _spend(self, count: int) -> None:
        if self._budget is not None:
            self._budget -= count

    def _start(self, state: _PoolState, task: Task) -> None:
        assert self._tg is not None
        run = _Run(task, TaskContext(task, self.queue, self.id, self.logs), state)
        self._running[task.id] = run
        state.running.add(task.id)
        self.counters["claimed"] += 1
        run.atask = self._tg.create_task(self._execute(run), name=f"tq-run:{task.name}:{task.id}")

    def _wake_pools(self) -> None:
        for state in self._pools:
            state.wake.set()

    # ------------------------------------------------------------------ running one task

    async def _execute(self, run: _Run) -> None:
        """Run one claimed task to an outcome and record it.

        This is a child of the worker's TaskGroup, where an exception that escapes would
        cancel every other task the worker is running. So nothing escapes: one task's
        failure, however odd, stays that task's.
        """
        task = run.task
        # Each run is its own asyncio task with its own context, so this is visible to
        # everything the executor calls and to no other run.
        current_task.set(run.ctx)
        try:
            try:
                outcome = await self._attempt(run)
            except asyncio.CancelledError:
                outcome = None
            run.settling = True
            if outcome is None:
                await self._cancelled(run)
            else:
                await self._settle(run, outcome)
        except asyncio.CancelledError:
            # Cancelled a second time, while recording. Nothing more to do here: the task
            # is released in bulk at shutdown, or recovered as stale.
            pass
        except Exception:
            logger.exception("task %s (%s): outcome could not be recorded", task.id, task.name)
        finally:
            self._running.pop(task.id, None)
            run.pool.running.discard(task.id)
            self._last_activity = time.monotonic()
            if run.pool.maybe_more:
                run.pool.wake.set()
            limit = self.settings.worker_max_tasks
            if limit and not self._running and self.counters["claimed"] >= limit:
                self.stop("max_tasks")

    async def _attempt(self, run: _Run) -> _Outcome:
        """Call the executor and turn whatever happens into an outcome."""
        task, ctx = run.task, run.ctx
        cls = self._executors[task.name]
        executor: Executor | None = None
        scope: asyncio.Timeout | None = None
        try:
            executor = cls()
            payload = cls.parse_payload(task.payload or {})
            async with asyncio.timeout(task.timeout_seconds) as scope:
                if inspect.iscoroutinefunction(executor.run):
                    output = await executor.run(ctx, payload)
                else:
                    # A plain `def run`: off the event loop, so it cannot stall the other
                    # tasks. The thread inherits this run's context, so its logs are too,
                    # and it is stopped if this await is cancelled or times out.
                    output = await ctx.to_thread(executor.run, ctx, payload)
            try:
                return _Outcome("succeeded", output=to_jsonable(output))
            except TypeError as exc:
                raise NonRetryable(f"the executor's return value is not JSON: {exc}") from exc
        except Snooze as exc:
            ctx.log(f"snoozed for {exc.delay:g}s: {exc}")
            return _Outcome("snoozed", delay=max(float(exc.delay), 0.0))
        except Exception as exc:
            if isinstance(exc, TimeoutError) and scope is not None and scope.expired():
                exc = TaskTimeout(f"exceeded its timeout of {task.timeout_seconds:g}s")
            error = error_dict(exc, attempt=task.attempt, worker_id=self.id)
            if isinstance(exc, NonRetryable) or task.attempt >= task.max_attempts:
                ctx.log(f"failed: {type(exc).__name__}: {exc}", level="error")
                return _Outcome("failed", error=error)
            delay = exc.delay if isinstance(exc, Retry) else None
            if delay is None and executor is not None:
                delay = executor.retry_delay(task.attempt, exc)
            if delay is None:
                delay = self.queue.backoff(task.attempt)
            ctx.log(
                f"attempt {task.attempt}/{task.max_attempts} failed: {type(exc).__name__}: "
                f"{exc} — retrying in {delay:.1f}s",
                level="warning",
            )
            return _Outcome("retry", error=error, delay=max(float(delay), 0.0))
        finally:
            # Whatever the executor left running (a thread of its own, a helper waiting on
            # this event) learns here that there is no run to work for any more.
            ctx.stopped.set()

    async def _settle(self, run: _Run, outcome: _Outcome) -> None:
        task = run.task
        owned = {"worker_id": self.id, "attempt": task.attempt}
        write: Callable[[], Awaitable[Task | None]]
        match outcome.kind:
            case "succeeded":
                write = partial(self.queue.succeed, task.id, output=outcome.output, **owned)
            case "snoozed":
                write = partial(self.queue.snooze, task.id, delay=outcome.delay, **owned)
            case "retry":
                write = partial(
                    self.queue.fail,
                    task.id,
                    error=outcome.error or {},
                    retry_in=outcome.delay,
                    **owned,
                )
            case _:
                write = partial(self.queue.fail, task.id, error=outcome.error or {}, **owned)
        try:
            settled = await self._write(write)
        except Exception as exc:
            # The outcome itself could not be stored (an output Postgres refuses, say).
            # Leaving the row `running` would have it recovered and run again, to the same
            # end; say what happened instead, if the database will take that.
            logger.error(
                "task %s (%s): %s could not be recorded: %s", task.id, task.name, outcome.kind, exc
            )
            error = error_dict(exc, attempt=task.attempt, worker_id=self.id)
            error["message"] = f"the task's outcome ({outcome.kind}) could not be recorded: {exc}"
            settled = await self.queue.fail(task.id, error=error, **owned)
            outcome = _Outcome("failed", error=error)
        if settled is None:
            # Canceled, or recovered as stale and re-claimed, while it ran. Fencing rejected
            # the write, so the outcome is dropped rather than overwriting someone else's.
            run.ctx.log(
                f"finished ({outcome.kind}) after the task was canceled or reassigned; "
                "the result was discarded",
                level="warning",
            )
            self.counters["lost"] += 1
            return
        key = {"retry": "retried"}.get(outcome.kind, outcome.kind)
        self.counters[key] += 1

    async def _cancelled(self, run: _Run) -> None:
        """The run was cancelled from outside. Why decides what happens to the task."""
        task = run.task
        if run.cancel_reason == "lost":
            # The heartbeat found the task is no longer ours; there is nothing to write.
            current = await self.queue.get(task.id)
            if current is not None and current.status == TaskStatus.CANCELED:
                run.ctx.log("canceled while running; stopped", level="warning")
                self.counters["canceled"] += 1
            else:
                run.ctx.log(
                    "stopped: the task was reassigned after a missed heartbeat", level="warning"
                )
                self.counters["lost"] += 1
            return
        # Shutdown (or the worker itself failing): hand the task back with its attempt.
        released = await asyncio.shield(self.queue.release(self.id, [task.id]))
        if released:
            run.ctx.log("worker is stopping; released back to the queue", level="warning")
            self.counters["released"] += 1

    async def _write(self, op: Callable[[], Awaitable[Task | None]]) -> Task | None:
        """Run a settle statement, riding out a brief database interruption."""
        for delay in (0.5, 2.0, 5.0):
            try:
                return await op()
            except Exception as exc:
                logger.warning(
                    "settle failed (%s: %s); retrying in %gs", type(exc).__name__, exc, delay
                )
                await asyncio.sleep(delay)
        return await op()

    # ------------------------------------------------------------------ service loops

    async def _heartbeat_loop(self) -> None:
        """Keep every running task's `heartbeat_at` fresh, and learn what was taken away.

        The reply is the set of tasks this worker still owns. One that is missing was
        canceled, or presumed lost and given to another worker; either way its run is
        cancelled here, so the work stops instead of racing its replacement.
        """
        interval = self.settings.heartbeat_interval_seconds
        while True:
            await _wait(self._heartbeat_now, interval)
            self._heartbeat_now.clear()
            sent = dict(self._running)
            if not sent:
                continue
            try:
                owned = await self.queue.heartbeat(self.id, list(sent))
            except Exception as exc:
                logger.warning("heartbeat failed (%s: %s)", type(exc).__name__, exc)
                continue
            for task_id, run in sent.items():
                if task_id not in owned and self._running.get(task_id) is run:
                    run.cancel("lost")

    async def _promote_loop(self) -> None:
        interval = self.settings.promote_interval_seconds
        while True:
            await asyncio.sleep(interval * random.uniform(0.8, 1.2))
            try:
                promoted = await self.queue.promote_due()
                if await self.queue.release_capped() or promoted:
                    self._wake_pools()
            except Exception as exc:
                logger.warning("promote failed (%s: %s)", type(exc).__name__, exc)

    async def _schedule_loop(self) -> None:
        """Fire the periodic schedules that are due: every schedule, not only those of the
        tasks this worker runs. Firing only inserts a task, which needs no executor, so
        any worker can do it and whichever gets to a row first does."""
        interval = self.settings.schedule_interval_seconds
        while True:
            await asyncio.sleep(interval * random.uniform(0.8, 1.2))
            try:
                fired = await self.schedules.fire_due()
            except Exception as exc:
                logger.warning("firing schedules failed (%s: %s)", type(exc).__name__, exc)
                continue
            for fire in fired:
                if fire.task is None:
                    logger.info(
                        "schedule %s: skipped, its previous task has not finished", fire.schedule
                    )
                else:
                    logger.debug("schedule %s: enqueued task %s", fire.schedule, fire.task.id)
            if any(fire.task is not None for fire in fired):
                self._wake_pools()

    async def _maintenance_loop(self) -> None:
        interval = self.settings.maintenance_interval_seconds
        while True:
            try:
                await self.monitor.worker_heartbeat(
                    self.id, in_flight=len(self._running), counters=self.counters
                )
                async with self.queue.try_lock(LOCK_MAINTENANCE) as mine:
                    if mine:
                        for task in await self.queue.recover_stale():
                            logger.warning(
                                "task %s (%s): worker %s stopped heartbeating; %s",
                                task.id,
                                task.name,
                                task.worker_id,
                                "re-queued" if task.status == TaskStatus.PENDING else "failed",
                            )
                        if pruned := await self.queue.prune():
                            logger.info("pruned %d finished task(s)", pruned)
                        await self.monitor.reap_workers()
            except Exception as exc:
                logger.warning("maintenance failed (%s: %s)", type(exc).__name__, exc)
            await asyncio.sleep(interval * random.uniform(0.8, 1.2))

    async def _archive_loop(self) -> None:
        """Move finished tasks past TQ_ARCHIVE_AFTER_DAYS to the archive database. Its own
        loop rather than a step of maintenance: the first run over a long history can take
        a while, and recovering lost tasks must not wait for it. Every worker tries; a lock
        taken in each batch lets one archive at a time."""
        if self.archive is None:
            return
        interval = self.settings.archive_interval_seconds
        while True:
            await asyncio.sleep(interval * random.uniform(0.8, 1.2))
            try:
                if moved := await self.archive.move():
                    logger.info("archived %d finished task(s)", moved)
            except Exception as exc:
                logger.warning("archiving failed (%s: %s)", type(exc).__name__, exc)

    async def _log_flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.log_flush_interval_seconds)
            await self.logs.flush()

    async def _listen_loop(self) -> None:
        """Turn NOTIFYs into wake-ups. Only ever an accelerator: with this loop dead the
        worker still finds every task at its next poll and every cancel at its next
        heartbeat."""
        if not self.settings.notify:
            return
        if not self.db.can_listen:
            logger.info(
                "behind a pooler with no TQ_LISTEN_DATABASE_URL: no LISTEN, polling every %ss",
                self.settings.poll_interval_seconds,
            )
            return
        delay = 1.0
        while True:
            try:
                async with self.db.dedicated_connection(f"tq-listen {self.id}") as conn:
                    closed = asyncio.Event()
                    conn.add_termination_listener(_setter(closed))
                    await conn.add_listener(CHANNEL_ENQUEUE, self._on_enqueue)
                    await conn.add_listener(CHANNEL_CANCEL, self._on_cancel)
                    # Whatever was enqueued while nobody was listening.
                    self._wake_pools()
                    delay = 1.0
                    while not closed.is_set():
                        await _wait(closed, 30.0)
                        if not closed.is_set():
                            # A connection cut without a FIN never reports itself closed.
                            await conn.execute("SELECT 1", timeout=10)
                logger.warning("LISTEN connection closed; polling until it is back")
            except Exception as exc:
                logger.warning(
                    "LISTEN unavailable (%s: %s); polling meanwhile", type(exc).__name__, exc
                )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)

    def _on_enqueue(self, _conn: Any, _pid: int, _channel: str, payload: str) -> None:
        if payload == self.queue_name:
            self._wake_pools()

    def _on_cancel(self, _conn: Any, _pid: int, _channel: str, payload: str) -> None:
        # A hint, not an instruction: the heartbeat it triggers asks the database what this
        # worker still owns, and that answer is what cancels the run.
        with contextlib.suppress(ValueError):
            if uuid.UUID(payload) in self._running:
                self._heartbeat_now.set()


def _setter(event: asyncio.Event) -> Callable[[Any], None]:
    return lambda _conn: event.set()


async def _wait(event: asyncio.Event, timeout: float) -> bool:
    """Wait for `event` for at most `timeout` seconds. Returns whether it is set."""
    with contextlib.suppress(TimeoutError):
        async with asyncio.timeout(timeout):
            await event.wait()
    return event.is_set()


async def run_worker(
    *,
    executors: Sequence[str] | None = None,
    queue_name: str | None = None,
    tasks: Sequence[str] | None = None,
    slots: int | None = None,
    settings: Settings | None = None,
) -> str:
    """Import the executor modules and run a worker over them. What `tq worker` calls."""
    settings = settings or get_settings()
    default_registry.load(executors if executors is not None else settings.worker_executors)
    worker = Worker(queue_name=queue_name, pools=tasks, slots=slots, settings=settings)
    return await worker.run()
