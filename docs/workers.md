# Running workers

A worker is a process that claims tasks and runs them. This is the operator's guide: how to
start one, how to shape it, and what it does when it stops.

- [Starting a worker](#starting-a-worker)
- [Slots and pools](#slots-and-pools)
- [Sizing slots](#sizing-slots)
- [One worker per kind of work](#one-worker-per-kind-of-work)
- [Queues](#queues)
- [Stopping, and why workers exit on purpose](#stopping-and-why-workers-exit-on-purpose)
- [When a worker dies](#when-a-worker-dies)
- [Seeing what workers are doing](#seeing-what-workers-are-doing)
- [Webhook deliveries](#webhook-deliveries)
- [Periodic tasks](#periodic-tasks)
- [Embedding a worker in your own process](#embedding-a-worker-in-your-own-process)
- [Workers that cannot reach the database](#workers-that-cannot-reach-the-database)
- [Troubleshooting](#troubleshooting)

## Starting a worker

```bash
tq worker --executors myapp.tasks,myapp.reports
```

`--executors` is a comma-separated list of modules to import. Importing them registers
their executors (see [executors.md](executors.md)), and the worker consumes every task name
it now has an executor for. Modules are resolved from the current directory, as with
`python -m`.

| Flag | Environment | Default | |
|---|---|---|---|
| `--executors` | `TQ_WORKER_EXECUTORS` | none | Modules to import. |
| `--queue` | `TQ_WORKER_QUEUE` | `default` | The queue to consume. |
| `--slots` | `TQ_WORKER_SLOTS` | `10` | Tasks at once, for names given no count. |
| `--tasks` | `TQ_WORKER_TASKS` | all names | Pools. See below. |
| `--idle-exit` | `TQ_WORKER_IDLE_EXIT_SECONDS` | `0` (never) | Exit after this long idle. |
| `--max-tasks` | `TQ_WORKER_MAX_TASKS` | `0` (never) | Exit after this many tasks. |

On start, the worker logs what it will run:

```
worker web-1-4127-9c1e02 started on queue 'default': image.*×2 (image.render, image.thumbnail); ALL×20 (demo.echo, demo.sleep); tq.webhook×2 (tq.webhook)
```

It refuses to start if a name in `--tasks` matches no registered executor, since a typo
there would otherwise mean a worker that silently does nothing.

## Slots and pools

A **slot** is one task running. A **pool** is a set of task names that share a number of
slots. With no `--tasks`, every name shares one pool of `--slots`.

`--tasks` splits that up. Each entry is `NAME`, `NAME:N`, a glob, or `ALL:N`:

```bash
tq worker --executors myapp.tasks --tasks 'video.encode:1,image.*:4,ALL:20'
```

| Entry | Meaning |
|---|---|
| `video.encode:1` | At most one `video.encode` at a time. |
| `image.*:4` | At most four tasks whose names match `image.*`, between them. |
| `ALL:20` | Twenty slots for every registered name not matched above. |
| `report.build` | This name, with `--slots` slots. |
| `mail.send:0` | Do not consume this name here, even though its executor is loaded. |

Each name lands in exactly one pool: exact names first, then globs in the order written,
then `ALL`. Without `ALL`, names that match nothing are not consumed.

Pools are independent. A backlog of slow `video.encode` tasks cannot occupy the slots that
`mail.send` needs, which is the reason to have more than one.

## Sizing slots

A slot costs almost nothing by itself: a coroutine and a few kilobytes. In the reference
benchmark, one worker process held 10,000 tasks sleeping at once (see
[benchmarks.md](benchmarks.md)). What limits a pool is what its tasks do:

| The task mostly… | Slots | Why |
|---|---|---|
| awaits I/O: HTTP calls, model APIs, queries | 20 to several hundred | One event loop runs many of these on one core. The limit is usually the other side's rate limit or connection pool. |
| runs CPU-bound code in a thread | about the number of cores, often fewer | Pure-Python threads share one core under the GIL. Scale with more processes instead. |
| uses a scarce resource: a GPU, a license, a rate-limited API | what the resource allows, often 1 | The pool is the limiter. |
| uses a lot of memory | memory ÷ peak per task | Slots multiply the worst case. |

Two things do not scale with slots:

- **Database connections.** A worker holds a connection only for the few milliseconds of
  each queue operation, never for the length of a task. The default pool (10, plus 10
  overflow) serves hundreds of slots. Raise `TQ_DB_POOL_SIZE` only if tasks are very short
  and the worker is mostly doing queue writes.
- **Throughput for trivial tasks.** One process runs one event loop on one core. For many
  tiny tasks per second, add processes, not slots.

To scale out, run more copies of the same worker (`docker compose up -d --scale worker=4`).
They coordinate through the database and need no configuration to know about each other.

## One worker per kind of work

Different kinds of work want different things: different dependencies, different
concurrency, sometimes different hardware. Give each its own worker.

The compose file has two, built from one [Dockerfile](../docker/Dockerfile):

```yaml
worker:         # general I/O-bound tasks, 20 at a time, core dependencies only
  command: ["tq", "worker", "--executors", "examples.executors.basic,examples.executors.pipeline"]

worker-image:   # CPU-bound image tasks, 2 at a time, with Pillow installed
  build:
    args:
      REQUIREMENTS: examples/workers/image/requirements.txt
  command: ["tq", "worker", "--executors", "examples.executors.image", "--tasks", "image.*:2"]
```

Each worker image installs its own `requirements.txt` on top of the core package. The API
image has none of them and the other workers do not either. This works because nothing
outside a worker needs to import its executors: the worker publishes its task names, payload
schemas and defaults to `tq_task_types` when it starts, and the API reads them from there.

A task is only ever claimed by a worker that has an executor for its name. If no such worker
is running, the task waits as `pending`.

To add a kind of work: write the executors, add a requirements file if they need one, and
copy the `worker-image` service. [infrastructure.md](infrastructure.md#images) has the
details.

## Queues

A queue is a name on a task (`default` unless you say otherwise). A worker consumes one
queue. Use queues to separate work that shares task names but not workers: per environment,
per tenant tier, per region.

```bash
tq enqueue report.build '{"rows": 500}' --queue bulk
tq worker --executors myapp.reports --queue bulk
```

Within a queue, use **pools** to separate concurrency by task name and **priority** to order
tasks. Reach for a second queue when you need separate workers.

A queue can be paused. Workers then claim nothing from it; enqueueing still works and
running tasks finish:

```bash
tq pause default
tq resume default
```

## Stopping, and why workers exit on purpose

**SIGTERM or SIGINT** (what `docker stop` and Ctrl-C send) starts a graceful stop:

1. The worker stops claiming.
2. Running tasks get `TQ_WORKER_SHUTDOWN_GRACE_SECONDS` (30s) to finish.
3. Whatever is still running is cancelled and **released**: back to `pending`, with its
   attempt handed back. Another worker picks it up, and the interruption does not count
   against `max_attempts`.
4. Buffered logs are flushed and the worker's row is marked stopped.

A second signal skips the grace period.

Give the container longer to stop than the grace period, or it is killed in the middle of
step 3. In compose that is `stop_grace_period: 45s` against the default 30s grace.

**Idle exit and max tasks** make a worker stop by itself:

```bash
tq worker --executors myapp.tasks --idle-exit 900    # exit after 15 minutes with nothing running
tq worker --executors myapp.tasks --max-tasks 1000   # exit after 1,000 task runs
```

Combined with a restart policy, this recycles the process:

```yaml
worker:
  restart: unless-stopped
  environment:
    TQ_WORKER_IDLE_EXIT_SECONDS: 900
```

The worker exits with status 0; Docker starts a new one. The fresh process has released
whatever memory the old one had accumulated, has new database connections, and runs whatever
code is now in the image. Long-lived Python processes that run varied work tend to grow, and
this is the cheap, reliable fix.

Details:

- `unless-stopped`, not `always`, so `docker compose stop worker` stays stopped.
- An idle exit happens only after the worker has run at least one task. A worker that has
  done nothing has nothing to give back, and would otherwise restart in a loop on a quiet
  system.
- `--max-tasks` counts task runs, including retries and snoozes, and is exact: the worker
  never claims more than the limit, then finishes what it has and exits.
- The exit reason is logged: `exit: idle`, `exit: max_tasks` or `exit: signal`.

Under Kubernetes or systemd the same settings work with their restart policies
(`restartPolicy: Always`, `Restart=always`).

## When a worker dies

A worker that is killed (`SIGKILL`, out of memory, the host going away) cannot release
anything. Its tasks stay `running` with a `heartbeat_at` that stops advancing. After
`TQ_STALE_AFTER_SECONDS` (60s), another worker's maintenance loop notices and puts them back
to `pending`, or to `failed` if they were on their last attempt. The error recorded is
`WorkerLost`.

Unlike a graceful release, this **does** use up the attempt. A task that reliably kills its
worker therefore ends as `failed` after `max_attempts` rather than taking workers down
forever.

Recovery needs at least one worker alive on any queue. It does not have to be one that can
run the task.

## Seeing what workers are doing

```bash
tq workers            # alive workers: pools, running/slots, counters
tq workers --all      # including those that stopped in the last week
tq stats              # depth and throughput per task name
```

The same is at `GET /v1/workers` and `GET /v1/stats`, and in the `tq_workers` table. Each
worker's counters (`claimed`, `succeeded`, `failed`, `retried`, `snoozed`, `canceled`,
`lost`, `released`) are since that process started.

The worker logs to stderr. `--log-level DEBUG` (before the subcommand: `tq --log-level DEBUG
worker …`) is verbose; the default INFO logs start, stop and anything that went wrong.

## Webhook deliveries

Task callbacks are delivered by the built-in `tq.webhook` task. Every worker reserves
`TQ_WEBHOOK_SLOTS` (2) for it, so a worker also delivers webhooks for its queue without
those deliveries competing with your tasks.

- A worker that should make no outbound calls: `TQ_WEBHOOK_SLOTS=0`.
- A dedicated delivery worker: `TQ_WEBHOOK_SLOTS=0` everywhere else, and one worker with
  `--tasks tq.webhook:20`.

If every worker on a queue has `TQ_WEBHOOK_SLOTS=0`, callbacks for that queue are queued and
never sent.

## Periodic tasks

Every worker does two things for [periodic tasks](schedules.md):

- When it starts, it writes the schedules its executors declare to `tq_schedules`, and
  deletes the ones declared earlier for its task names that are gone from the code.
- While it runs, it looks once a second (`TQ_SCHEDULE_INTERVAL_SECONDS`) for schedules that
  are due and enqueues their tasks. It fires any schedule, not only those of the tasks it
  runs, on any queue.

So schedules fire as long as one worker of any kind is running, and stop when none is.
Nothing needs to be configured, and there is no worker to single out as "the scheduler".

```bash
tq schedule list
```

A worker logs a skipped fire at INFO: `schedule demo.tick: skipped, its previous task has
not finished`.

## Embedding a worker in your own process

`tq worker` is a thin wrapper. The same thing in code:

```python
import asyncio
import logging

from pgtaskqueue import Worker

import myapp.tasks  # registers its executors


async def main() -> None:
    logging.basicConfig(level=logging.INFO)  # needed for logging capture; see below
    worker = Worker(queue_name="default", pools=["image.*:2", "ALL:20"])
    reason = await worker.run()  # "signal", "idle", "max_tasks" or "stop"


asyncio.run(main())
```

- `worker.stop()` asks for a graceful stop from elsewhere in the process; `worker.kill()`
  skips the grace period.
- `handle_signals=False` if your application installs its own signal handlers.
- `registry=` to run a specific `Registry` instead of the global one.
- `db=` to share a `Database` (and its connection pool) with the rest of your application.
- The worker adds a handler to the root logger to capture task logs, but does not change
  logger levels. If the root level is WARNING (Python's default), INFO records are dropped
  before the handler sees them.

## Workers that cannot reach the database

For a machine in another network (a GPU box, a customer's server), the API exposes the same
operations over HTTP: claim, heartbeat, succeed, fail, release. The worker needs an account
with the `worker` role and nothing else.
[examples/remote_worker.py](../examples/remote_worker.py) is a complete client in one file,
and [api.md](api.md#remote-workers) documents the protocol.

Prefer `tq worker` wherever the database is reachable. It is faster, wakes on NOTIFY, and
has less to go wrong.

## Troubleshooting

**Tasks stay `pending`.**
Check, in order: is a worker running on that queue (`tq workers`)? Does it list that task
name in its pools? Is the queue paused (`tq stats`, `GET /v1/queues`)? Are all the pool's
slots taken by long-running tasks?

**Tasks stay `scheduled` past their time.**
The promote loop runs inside workers. With no worker running anywhere, nothing promotes.
A task the UI calls *held*, or that has `"capped": true`, is waiting for its account to
have fewer tasks pending or running than its [running limit](auth.md#limits);
`GET /v1/auth/limits` shows the account's count. Workers with
`TQ_ACCOUNT_MAX_RUNNING` set differently from the API release by their own value.

**A task ran twice.**
Execution is at-least-once. Look at the task's log and its `error` between attempts.
`WorkerLost` means the first worker stopped heartbeating; if that worker was alive, something
blocked its event loop for longer than `TQ_STALE_AFTER_SECONDS`. See
[Blocking and CPU-bound work](executors.md#blocking-and-cpu-bound-work).

**"finished after the task was canceled or reassigned; the result was discarded".**
The same cause seen from the other side: the worker finished a task it no longer owned.
Fencing dropped its result.

**`LISTEN unavailable … polling meanwhile`.**
The worker could not open or keep its `LISTEN` connection. Tasks still run, at the poll
interval, and the worker keeps trying to reconnect. If `TQ_LISTEN_DATABASE_URL` is set,
check that it is reachable from the worker.

**Tasks start late and never within milliseconds, behind PgBouncer.**
`LISTEN` through a transaction-mode pooler connects and then hears nothing. Set
`TQ_DB_PGBOUNCER=true`, and give workers `TQ_LISTEN_DATABASE_URL`
([infrastructure.md](infrastructure.md#connection-poolers)).

**`prepared statement "…" does not exist` or `already exists`.**
The process is connecting through a transaction-mode pooler without `TQ_DB_PGBOUNCER=true`.

**A worker restarts every few seconds.**
It is failing at start-up: read its log. The usual causes are a module in `--executors`
that cannot be imported, a name in `--tasks` that matches no executor, or a database that is
not migrated.

**The worker's task logs are empty.**
Lines are flushed every second, so the last moments of a running task may not be there yet.
For `logging` records, check the logger level (see above).
