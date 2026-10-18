"""Executors: the code a task name runs, and what that code is handed.

    from pgtaskqueue import Executor, TaskContext, register

    class ResizePayload(BaseModel):
        url: str
        width: int = 800

    @register
    class Resize(Executor[ResizePayload]):
        name = "image.resize"
        payload_model = ResizePayload
        max_attempts = 5
        timeout = 120

        async def run(self, ctx: TaskContext, payload: ResizePayload) -> dict:
            ctx.log(f"resizing {payload.url}")
            ...
            return {"width": payload.width}

Or, for a task that is one function:

    @task("math.add")
    async def add(ctx: TaskContext, payload: AddPayload) -> dict:
        return {"sum": payload.a + payload.b}

docs/executors.md is the guide.
"""

from __future__ import annotations

import asyncio
import fnmatch
import importlib
import inspect
import threading
import uuid
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, get_type_hints

from pydantic import BaseModel, ValidationError

from pgtaskqueue.errors import NonRetryable, QueueConfigError
from pgtaskqueue.models import Task
from pgtaskqueue.schedules import Schedule
from pgtaskqueue.threads import run_in_thread

if TYPE_CHECKING:
    from pgtaskqueue.logs import LogBuffer
    from pgtaskqueue.queue import TaskQueue


class TaskContext:
    """What an executor knows about the task it is running, and how it reports back."""

    def __init__(self, task: Task, queue: TaskQueue, worker_id: str, logs: LogBuffer) -> None:
        self.task = task
        self.queue = queue
        self.worker_id = worker_id
        self._logs = logs
        self._step: str | None = task.step
        # A local copy: what earlier attempts recorded, plus what this one adds.
        self.progress: dict[str, Any] = dict(task.progress or {})
        # Set when this run is over, however it ended. A `threading.Event`, not an asyncio
        # one, because the code that needs to ask is code running in a thread.
        self.stopped = threading.Event()
        try:
            self._loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None

    @property
    def id(self) -> uuid.UUID:
        return self.task.id

    @property
    def name(self) -> str:
        return self.task.name

    @property
    def attempt(self) -> int:
        """1 on the first run."""
        return self.task.attempt

    @property
    def is_last_attempt(self) -> bool:
        return self.task.attempt >= self.task.max_attempts

    @property
    def metadata(self) -> dict[str, Any]:
        return self.task.meta or {}

    def log(self, message: str, *, level: str = "info", data: dict[str, Any] | None = None) -> None:
        """Add a line to this task's log. Not awaited: lines are buffered and written in
        batches, and are safe to add from a thread."""
        self._logs.add(
            self.task.id, self.task.attempt, message, level=level, step=self._step, data=data
        )

    async def set_progress(self, **fields: Any) -> None:
        """Merge `fields` into the task's `progress`, visible to anyone reading the task.

            await ctx.set_progress(percent=40, message="encoding")

        Top-level keys are merged; `steps` is reserved for `ctx.step`. If the task is no
        longer this worker's (it was canceled, or presumed lost) the write changes nothing,
        and the heartbeat cancels the run shortly after.
        """
        self.progress.update(fields)
        await self.queue.set_progress(
            self.task.id, fields, worker_id=self.worker_id, attempt=self.task.attempt
        )

    async def step[T](self, name: str, fn: Callable[[], Awaitable[T]]) -> T:
        """Run `fn` once per task, across attempts.

        The first time, `fn` runs and its (JSON-serializable) result is stored under
        `progress.steps.<name>`. On a later attempt — after a failure further down, a
        worker crash, a manual requeue — the stored result is returned and `fn` is not
        called. That is what makes a retry resume instead of repeat:

            video = await ctx.step("download", lambda: download(payload.url))
            text = await ctx.step("transcribe", lambda: transcribe(video["path"]))
        """
        done: dict[str, Any] = self.progress.setdefault("steps", {})
        if name in done:
            self.log(f"step {name!r}: reusing the result of an earlier attempt")
            return done[name]
        self._step = name
        await self.queue.set_progress(
            self.task.id, {}, worker_id=self.worker_id, attempt=self.task.attempt, step=name
        )
        self.log(f"step {name!r}: started")
        result = await fn()
        await self.queue.save_step(
            self.task.id, name, result, worker_id=self.worker_id, attempt=self.task.attempt
        )
        done[name] = result
        self.log(f"step {name!r}: done")
        return result

    def from_thread[T](self, coro: Coroutine[Any, Any, T]) -> T:
        """From a synchronous executor (a plain `def run`, which runs in a thread): run one
        of this context's coroutines on the worker's event loop and wait for it.

            ctx.from_thread(ctx.set_progress(percent=40))
        """
        if self._loop is None:
            raise RuntimeError("this context was not created inside an event loop")
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    async def to_thread[T](self, fn: Callable[..., T], /, *args: Any) -> T:
        """Like `asyncio.to_thread`, for blocking work in an `async` executor, except that
        the thread is stopped when the task is: `TaskStopped` is raised inside `fn`.

            result = await ctx.to_thread(render, payload.width, payload.height)
        """
        return await run_in_thread(fn, *args)

    async def enqueue(
        self, name: str, payload: dict[str, Any] | None = None, **options: Any
    ) -> Task:
        """Enqueue a follow-up task: same queue and creator unless `options` say otherwise,
        with this task's id in its metadata as `parent_task_id`."""
        options.setdefault("queue", self.task.queue_name)
        options.setdefault("created_by", self.task.created_by)
        metadata = {"parent_task_id": str(self.task.id), **(options.pop("metadata", None) or {})}
        return await self.queue.enqueue(name, payload, metadata=metadata, **options)


class Executor[P]:
    """Base class for the code behind one task name. One instance is created per run."""

    name: ClassVar[str] = ""
    description: ClassVar[str] = ""
    # A Pydantic model the payload is validated into. None hands `run` the raw dict.
    payload_model: ClassVar[type[BaseModel] | None] = None
    # Defaults for tasks of this name, published to `tq_task_types` when a worker starts and
    # applied at enqueue. None leaves the global default (TQ_DEFAULT_MAX_ATTEMPTS, no timeout).
    max_attempts: ClassVar[int | None] = None
    timeout: ClassVar[float | None] = None
    # When tasks of this name enqueue themselves. Written to `tq_schedules` when a worker
    # that runs the name starts. See docs/schedules.md.
    schedules: ClassVar[Sequence[Schedule]] = ()

    async def run(self, ctx: TaskContext, payload: P) -> Any:
        """Do the work. The return value is stored as the task's `output`.

        Raise to fail the attempt: any exception is retried with backoff until
        `max_attempts`; `NonRetryable` fails at once; `Retry(delay=...)` picks the delay;
        `Snooze(delay)` waits without using an attempt.
        """
        raise NotImplementedError

    def retry_delay(self, attempt: int, error: BaseException) -> float | None:
        """Seconds before the next attempt, or None for the default exponential backoff."""
        return None

    @classmethod
    def parse_payload(cls, raw: dict[str, Any]) -> Any:
        if cls.payload_model is None:
            return raw
        try:
            return cls.payload_model.model_validate(raw)
        except ValidationError as exc:
            # The payload will be exactly as invalid on the next attempt.
            raise NonRetryable(f"invalid payload for {cls.name}: {exc}") from exc

    @classmethod
    def definition(cls) -> dict[str, Any]:
        """What is published about this executor in `tq_task_types`."""
        return {
            "name": cls.name,
            # The class's own docstring: `inspect.getdoc` would hand an undocumented
            # executor the docstring of `Executor` itself.
            "description": (
                cls.description or inspect.cleandoc(cls.__dict__.get("__doc__") or "")
            ).strip()
            or None,
            "payload_schema": cls.payload_model.model_json_schema() if cls.payload_model else None,
            "max_attempts": cls.max_attempts,
            "timeout_seconds": cls.timeout,
        }


def _qualified(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


class Registry:
    """Task name → executor class. A worker runs what its registry holds, and nothing else."""

    def __init__(self) -> None:
        self._executors: dict[str, type[Executor]] = {}

    def register[E: type[Executor]](self, cls: E) -> E:
        """Class decorator. The class must set `name`."""
        if not cls.name:
            raise QueueConfigError(f"{cls.__name__} must set a task `name`")
        existing = self._executors.get(cls.name)
        # A module imported twice (a reload) registers its classes again under the same
        # qualified name, which is fine; two different classes claiming one name is not.
        if existing is not None and _qualified(existing) != _qualified(cls):
            raise QueueConfigError(
                f"task name {cls.name!r} is registered by both {_qualified(existing)} "
                f"and {_qualified(cls)}"
            )
        for schedule in cls.schedules:
            try:
                schedule.validate()
                # A scheduled payload that the executor would refuse fails every run. Find
                # out when the module is imported instead.
                cls.parse_payload(schedule.payload or {})
            except (ValueError, NonRetryable) as exc:
                raise QueueConfigError(f"schedule of task {cls.name!r}: {exc}") from exc
        self._executors[cls.name] = cls
        return cls

    def task(
        self,
        name: str,
        *,
        description: str = "",
        max_attempts: int | None = None,
        timeout: float | None = None,
        schedule: Schedule | Sequence[Schedule] | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Function decorator: register `async def fn(ctx, payload)` as a task. A plain
        `def fn(ctx, payload)` is registered too, and runs in a thread.

        The payload model is read from the second parameter's annotation, when it is a
        Pydantic model. `schedule` makes the task periodic: one `Schedule`, or several.
        """
        schedules = (schedule,) if isinstance(schedule, Schedule) else tuple(schedule or ())

        def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
            params = list(inspect.signature(fn).parameters)
            try:
                hints = get_type_hints(fn)
            except NameError as exc:
                # Typically a payload model defined inside a function, where annotations
                # that are strings (`from __future__ import annotations`) cannot find it.
                raise QueueConfigError(
                    f"task {name!r}: cannot resolve the annotations of {fn.__qualname__} "
                    f"({exc}); define the payload model at module level"
                ) from exc
            annotation = hints.get(params[1]) if len(params) > 1 else None
            model = (
                annotation
                if inspect.isclass(annotation) and issubclass(annotation, BaseModel)
                else None
            )

            # A plain `def` stays a plain `def run`, which the worker calls in a thread.
            if inspect.iscoroutinefunction(fn):

                async def run(self: Executor, ctx: TaskContext, payload: Any) -> Any:
                    return await fn(ctx, payload)
            else:

                def run(self: Executor, ctx: TaskContext, payload: Any) -> Any:
                    return fn(ctx, payload)

            cls = type(
                fn.__name__,
                (Executor,),
                {
                    "name": name,
                    "description": description or inspect.getdoc(fn) or "",
                    "payload_model": model,
                    "max_attempts": max_attempts,
                    "timeout": timeout,
                    "schedules": schedules,
                    "run": run,
                    "__module__": fn.__module__,
                    "__qualname__": fn.__qualname__,
                    "__doc__": fn.__doc__,
                },
            )
            self.register(cls)
            return fn

        return decorate

    def get(self, name: str) -> type[Executor] | None:
        return self._executors.get(name)

    def names(self) -> list[str]:
        return sorted(self._executors)

    def match(self, pattern: str) -> list[str]:
        """Registered names matching a glob (`image.*`)."""
        return [name for name in self.names() if fnmatch.fnmatchcase(name, pattern)]

    def definitions(self, names: Iterable[str] | None = None) -> list[dict[str, Any]]:
        chosen = self.names() if names is None else names
        return [self._executors[name].definition() for name in chosen]

    def schedules(self, names: Iterable[str] | None = None) -> list[tuple[str, Schedule]]:
        """`(task name, schedule)` for every schedule the chosen executors declare."""
        chosen = self.names() if names is None else names
        declared = [(name, s) for name in chosen for s in self._executors[name].schedules]
        seen: dict[str, str] = {}
        for task_name, schedule in declared:
            name = schedule.name or task_name
            if name in seen:
                raise QueueConfigError(
                    f"two schedules are named {name!r} (tasks {seen[name]!r} and "
                    f"{task_name!r}); give one of them a `name`"
                )
            seen[name] = task_name
        return declared

    def load(self, modules: Iterable[str]) -> None:
        """Import modules for the executors they register on import."""
        for module in modules:
            importlib.import_module(module)

    def __contains__(self, name: str) -> bool:
        return name in self._executors

    def __len__(self) -> int:
        return len(self._executors)


# The registry `@register` and `@task` write to, and the one a worker reads unless told
# otherwise. Tests build their own to stay isolated from it.
registry = Registry()
register = registry.register
task = registry.task
