# Writing tasks

An **executor** is the code behind a task name. A worker imports the modules you point it
at, and runs the executors those modules register.

- [The two forms](#the-two-forms)
- [Payloads](#payloads)
- [Output](#output)
- [Failing on purpose](#failing-on-purpose)
- [Timeouts and cancellation](#timeouts-and-cancellation)
- [Progress](#progress)
- [Steps: resuming instead of repeating](#steps-resuming-instead-of-repeating)
- [Logs](#logs)
- [Blocking and CPU-bound work](#blocking-and-cpu-bound-work)
- [Starting other tasks](#starting-other-tasks)
- [Running on a schedule](#running-on-a-schedule)
- [Idempotency](#idempotency)
- [Testing an executor](#testing-an-executor)
- [Reference](#reference)

Runnable versions of everything here are in
[examples/executors/](../examples/executors/).

## The two forms

A function, for a task that is one function:

```python
from pydantic import BaseModel
from pgtaskqueue import TaskContext, task


class AddPayload(BaseModel):
    a: int
    b: int


@task("math.add", max_attempts=5, timeout=30)
async def add(ctx: TaskContext, payload: AddPayload) -> dict:
    """Adds two numbers."""
    return {"sum": payload.a + payload.b}
```

A class, when you want helper methods or a custom retry delay:

```python
from pgtaskqueue import Executor, TaskContext, register


@register
class Resize(Executor[ResizePayload]):
    """Resizes an image to a given width."""

    name = "image.resize"
    payload_model = ResizePayload
    max_attempts = 5
    timeout = 120

    async def run(self, ctx: TaskContext, payload: ResizePayload) -> dict:
        ctx.log(f"resizing {payload.url}")
        ...
        return {"width": payload.width}

    def retry_delay(self, attempt: int, error: BaseException) -> float | None:
        return 2.0 * attempt
```

One instance is created per run, so `self` is a safe place for per-run state.

Names are dotted by convention (`image.resize`), which is what makes globs like `image.*`
useful for pools and ACLs. Names starting with `tq.` are reserved.

`max_attempts`, `timeout`, the docstring and the payload's JSON Schema are published to the
`tq_task_types` table when a worker starts. That is how the API validates payloads and
applies defaults without importing your code, and how `GET /v1/task-types` and the MCP
`list_task_types` tool can describe your tasks.

Those defaults are applied when a task is enqueued, from what is registered at that moment.
Two consequences:

- A task enqueued before any worker has ever started with its executor gets
  `TQ_DEFAULT_MAX_ATTEMPTS` (3) and no timeout. Start the worker once before you enqueue a
  new kind of task, or pass `max_attempts` and `timeout` to `enqueue`.
- Changing an executor's `max_attempts` or `timeout` affects tasks enqueued after the
  worker has restarted, not the ones already waiting.

## Payloads

With a Pydantic model, the payload is validated before `run` is called and arrives as a
model instance. A payload that does not validate fails the task at once without retrying,
since it would be exactly as invalid next time.

Without a model (`payload_model = None`, or an untyped second parameter), `run` receives the
raw `dict`.

With `@task`, the model is read from the second parameter's annotation. **Define payload
models at module level.** A model defined inside a function cannot be resolved when the
module uses `from __future__ import annotations`, and registration fails with a message
saying so.

Payloads are stored as `jsonb` in the task row. Keep them small: pass a reference (an id, an
object key, a path) rather than the data. The API refuses payloads over
`TQ_MAX_PAYLOAD_BYTES` (1 MB).

## Output

Whatever `run` returns is stored as the task's `output`, readable from the row, the API and
any webhook. It must be JSON-serializable. Pydantic models, dataclasses, datetimes, UUIDs,
Decimals, paths, enums and sets are converted for you. Anything else fails the task with a
message naming the type.

The same advice as for payloads applies: return where the result is, not the result.

## Failing on purpose

```python
from pgtaskqueue import NonRetryable, Retry, Snooze

raise Retry("rate limited", delay=30)  # failed attempt; run again in 30s
raise Snooze(60, "export not ready")  # not a failure; run again in 60s, attempt handed back
raise NonRetryable("no such customer")  # failed for good, now
raise ValueError("anything else")  # failed attempt; retried with backoff
```

| | Counts as an attempt | Ends in `failed` when |
|---|---|---|
| Any exception | Yes | `max_attempts` is used up |
| `Retry` | Yes | `max_attempts` is used up |
| `Snooze` | No | Never. Pair it with a deadline of your own. |
| `NonRetryable` | Yes | Immediately |

`ctx.attempt` is 1 on the first run. `ctx.is_last_attempt` is true when a failure now would
be final, which is the place for cleanup that should happen only once the task has given up.

The default delay between attempts is exponential: 5s, 10s, 20s, … up to 10 minutes, with
jitter (`TQ_RETRY_BACKOFF_*`). Override it for one task type with `retry_delay`, or for one
failure with `Retry(delay=…)`.

`Snooze` is for "not ready yet": polling another system for a result, waiting for a
precondition. The task goes back to the queue and holds no slot while it waits.

## Timeouts and cancellation

`timeout` is seconds per attempt. When it passes, the attempt is cancelled and recorded as a
`TaskTimeout` failure, then retried like any other.

A task is cancelled from outside in three cases: someone canceled it, its timeout passed, or
the worker is stopping and the grace period ran out. In all three, `asyncio.CancelledError`
is raised at the `await` your code is at. So:

- Put cleanup in `finally` or a context manager, as you would anywhere in asyncio.
- Do not catch `CancelledError` and carry on. If you catch it to clean up, re-raise.
- Code that never awaits is not reached by `CancelledError`. If it runs in a thread the
  worker started, `TaskStopped` is raised in the thread instead. See
  [Blocking and CPU-bound work](#blocking-and-cpu-bound-work).

A `TimeoutError` your own code raises (from an inner `asyncio.timeout`, or an HTTP client) is
an ordinary failure with its own type. Only the task-level timeout is recorded as
`TaskTimeout`.

## Progress

```python
await ctx.set_progress(percent=40, message="encoding")
```

Merges the given keys into the task's `progress` column, which anyone reading the task sees.
Each call is one small `UPDATE`, so call it at milestones rather than in a tight loop.
`progress` survives retries. The key `steps` is reserved for the next section.

## Steps: resuming instead of repeating

```python
async def run(self, ctx: TaskContext, payload: Payload) -> dict:
    video = await ctx.step("download", lambda: download(payload.url))
    audio = await ctx.step("extract-audio", lambda: extract(video["path"]))
    text = await ctx.step("transcribe", lambda: transcribe(audio["path"]))
    return {"transcript": text["key"]}
```

`ctx.step(name, fn)` runs `fn` and stores its result on the task row. On a later attempt,
whether after a failure further down, a worker crash, or a manual requeue next week, a step
that completed returns its stored result and `fn` is not called.

If `transcribe` fails, the retry does not download the video again.

Guidelines:

- A step should be worth not repeating: slow, expensive, or not idempotent.
- A step's result must be small and JSON-serializable. It is stored in the row. Return a
  path, an id or a summary, not the data.
- Step names must be unique within a task and stable across deploys. Renaming a step makes
  it run again.
- A step is recorded after it returns. If the worker dies during a step, that step runs
  again, so each step should still tolerate being started twice.
- `tq requeue <id> --reset-progress` (or `reset_progress` in the API) forgets completed
  steps and starts from the beginning.

The step a task is in is shown in its `step` column and on each log line.

## Logs

```python
ctx.log("fetched 3 pages")
ctx.log("quota is low", level="warning", data={"remaining": 12})
```

`ctx.log` is not awaited. Lines are buffered and written in batches, and it is safe to call
from a thread.

Standard `logging` works too. While a task runs, records at `TQ_CAPTURE_LOG_LEVEL` (INFO)
or above, from your code or any library it calls, are stored as that task's log lines:

```python
logger = logging.getLogger(__name__)
logger.info("this lands in the task's log")
```

Two things to know:

- The `logging` level still applies. `tq worker` configures the root logger at INFO. If you
  embed a `Worker` in your own process, make sure the root logger's level allows the records
  you want captured.
- Records from `pgtaskqueue`, `sqlalchemy`, `asyncpg` and `asyncio` loggers are not captured,
  or the queue's own SQL logging would write a log line for every log line.

Lines longer than `TQ_LOG_MESSAGE_MAX_CHARS` (8,000) are clipped. Read a task's log with
`tq show <id> --logs`, `GET /v1/tasks/{id}/logs`, or SQL on `tq_task_logs`.

## Blocking and CPU-bound work

All of a worker's tasks share one event loop. A task that blocks it (a synchronous HTTP
client, a tight CPU loop, `time.sleep`) stalls every other task **and the heartbeat**. Block
for longer than `TQ_STALE_AFTER_SECONDS` and the worker's tasks are presumed lost and given
to another worker.

Two ways to keep blocking work off the loop:

**A plain `def`.** The worker calls a synchronous `run` (or a synchronous `@task`
function) in a thread:

```python
@register
class Hash(Executor[HashPayload]):
    name = "demo.hash"
    payload_model = HashPayload

    def run(self, ctx: TaskContext, payload: HashPayload) -> dict:
        ...  # blocking code is fine here
        ctx.log("halfway")  # safe from a thread
        ctx.from_thread(ctx.set_progress(percent=50))  # run a coroutine on the loop
        return {"digest": digest}
```

**`ctx.to_thread` for part of an async `run`:**

```python
result = await ctx.to_thread(render, payload.width, payload.height)
```

Each call gets a thread of its own, so a pool's slots are the only limit on how many run
at once.

### Stopping a thread

When a task is canceled, times out, is given to another worker, or its worker stops, the
worker raises `TaskStopped` inside the thread. This covers a plain `def` executor and
anything started with `ctx.to_thread`.

```python
from pgtaskqueue import TaskStopped


def run(self, ctx, payload):
    workdir = make_workdir()
    try:
        for chunk in chunks(payload):  # stopped here, between two instructions
            process(chunk, workdir)
    except TaskStopped:
        ctx.log("stopped; cleaning up")
        raise  # always re-raise
    finally:
        shutil.rmtree(workdir)
```

What to know about it:

- **It arrives when the thread next runs Python code.** A loop in Python stops within a
  few instructions. One long call into C finishes first: `time.sleep(600)`, a socket read
  with no timeout, a native library call. The exception is raised when that call returns.
  Give blocking calls a timeout of their own, or break a long wait into short ones.
- **It can arrive anywhere**, including inside a `finally` block or between two statements
  meant to happen together. Keep what must not be torn short, or catch `TaskStopped` there.
- **It is a `BaseException`**, like `asyncio.CancelledError`, so `except Exception` does not
  swallow it. Do not catch it and carry on.
- **The worker does not wait for the thread.** The task is settled and its slot is free as
  soon as the await is cancelled; the thread ends shortly after. Whatever it returns
  is discarded.
- **`ctx.stopped`** is a `threading.Event` set when the run is over, however it ended.
  `ctx.stopped.wait(30)` is a sleep that ends early, and a thread your own code started
  can check `ctx.stopped.is_set()`. The worker does not stop threads it did not start,
  which includes `asyncio.to_thread`.
- **CPython only.** It uses `PyThreadState_SetAsyncExc`. On an interpreter without it a
  thread runs to its end, as it would with `asyncio.to_thread`. Not tested on PyPy or on a
  free-threaded build.
- Threads are daemon threads: one stuck in a C call does not keep a stopping worker's
  process alive.

Other limits to plan around:

- **Threads do not make Python code parallel.** Pure-Python CPU work in threads shares one
  core under the GIL. Libraries that release it (NumPy, Pillow, most I/O) do run in
  parallel.
- For CPU-bound task types, use a pool of their own with few slots, and scale by running
  more worker processes. See [workers.md](workers.md#sizing-slots).

For work that needs real isolation (a native library that can crash, memory that must be
returned), run it as a subprocess from the executor with `asyncio.create_subprocess_exec`.
A subprocess can be killed when the task is cancelled, and a crash inside it cannot take
the worker down.

## Starting other tasks

```python
await ctx.enqueue("report.notify", {"location": location})
```

Enqueues a follow-up task on the same queue, with the same `created_by`, and with this
task's id in its metadata as `parent_task_id`. Any `enqueue` option can be passed.

For many children at once, use `ctx.queue.enqueue_many` with a `dedupe_key` per child, so
that a retry of the parent does not create them twice:

```python
await ctx.queue.enqueue_many(
    NewTask(
        "demo.sleep",
        {"seconds": 1},
        dedupe_key=f"{ctx.id}:{i}",
        metadata={"parent_task_id": str(ctx.id)},
    )
    for i in range(100)
)
```

There is no built-in "wait for all children". Find them by
`metadata.parent_task_id`, or have the last child enqueue the next stage.

## Running on a schedule

```python
from pgtaskqueue import Schedule, TaskContext, task


@task("report.daily", schedule=Schedule("0 3 * * *", timezone="Europe/Berlin"))
async def daily_report(ctx: TaskContext, payload: dict) -> dict:
    day = ctx.metadata["scheduled_for"]
    ...
```

`Schedule(every=300)` is an interval instead. The worker that runs the task publishes the
schedule when it starts, and from then on a task is enqueued each time it is due.
`ctx.metadata["scheduled_for"]` is the time the run was due, as an ISO 8601 string; use it
rather than the clock, because the task can start late or be retried. A fire is skipped
while the schedule's previous task has not finished.

[schedules.md](schedules.md) has cron syntax, timezones, what happens to missed runs, and
schedules created without code.

## Idempotency

Execution is **at least once**. A task can run twice if a worker dies after doing the work
and before recording it. Design for that:

- Make the effect idempotent: upsert rather than insert, `PUT` rather than `POST`, pass an
  idempotency key to APIs that take one. `str(ctx.id)` is a good key: it is the same on
  every attempt.
- Use `ctx.step` around the parts that are not idempotent, to narrow the window.
- Check before acting: if the output already exists, return it.

## Testing an executor

An executor is a class with an async method, so the simplest test calls it directly with a
stand-in context. To test it end to end (payload validation, retries, logs), run a real
worker against a test database, as this project's own tests do. The fixtures in
[tests/conftest.py](../tests/conftest.py) and [tests/helpers.py](../tests/helpers.py) are a
small, copyable example:

```python
from tests.helpers import wait_for


async def test_add(queue, registry, worker_factory):
    @registry.task("math.add")
    async def add(ctx, payload):
        return {"sum": payload["a"] + payload["b"]}

    async with worker_factory(slots=2):
        task = await queue.enqueue("math.add", {"a": 2, "b": 3})
        done = await wait_for(queue, task.id)
    assert done.status == "succeeded"
    assert done.output == {"sum": 5}
```

Give each test its own `Registry()` rather than the global one, so tests do not see each
other's tasks.

## Reference

### `TaskContext`

| Member | |
|---|---|
| `ctx.id` | The task's id (`uuid.UUID`). The same on every attempt. |
| `ctx.name` | The task name. |
| `ctx.attempt` | 1 on the first run. |
| `ctx.is_last_attempt` | Whether a failure now would be final. |
| `ctx.metadata` | The labels the task was enqueued with. |
| `ctx.task` | The whole row as claimed: `payload`, `priority`, `queue_name`, `created_by`, … |
| `ctx.progress` | `progress` as of this attempt, including earlier attempts' steps. |
| `ctx.log(message, *, level="info", data=None)` | Add a log line. Not awaited. |
| `await ctx.set_progress(**fields)` | Merge fields into `progress`. |
| `await ctx.step(name, fn)` | Run `fn` once per task, across attempts. |
| `await ctx.enqueue(name, payload, **options)` | Enqueue a follow-up task. |
| `ctx.from_thread(coro)` | From a synchronous `run`: run a coroutine on the loop and wait. |
| `await ctx.to_thread(fn, *args)` | Run blocking code in a thread that is stopped when the task is. |
| `ctx.stopped` | A `threading.Event`, set when this run is over. |
| `ctx.queue` | The `TaskQueue`, for anything else. |

### `Executor`

| Attribute | Default | |
|---|---|---|
| `name` | required | The task name. |
| `description` | the docstring | Shown in task-type listings. |
| `payload_model` | `None` | A Pydantic model, or `None` for the raw dict. |
| `max_attempts` | `None` | `None` uses `TQ_DEFAULT_MAX_ATTEMPTS` (3). |
| `timeout` | `None` | Seconds per attempt. `None` is no limit. |
| `schedules` | `()` | [`Schedule`](schedules.md#schedule) objects: when the task enqueues itself. |
| `run(ctx, payload)` | required | `async def`, or `def` to run in a thread. |
| `retry_delay(attempt, error)` | `None` | Seconds before the next attempt; `None` for backoff. |

### `@task(name, *, description="", max_attempts=None, timeout=None, schedule=None)`

Registers `async def fn(ctx, payload)`, or a plain `def fn(ctx, payload)` to run in a
thread. The decorated function is returned unchanged, so it can still be called directly in
tests. `schedule` is one [`Schedule`](schedules.md#schedule) or a list of them.

### `Registry`

`register` and `task` at the top level of `pgtaskqueue` write to a global registry, which is
what `tq worker` reads. Create your own `Registry()` to keep sets of executors apart, and
pass it to `Worker(registry=…)`.
