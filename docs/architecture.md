# Architecture

How the queue works, and why each piece is the way it is. Read this before changing
[queue.py](../src/pgtaskqueue/queue.py) or [worker.py](../src/pgtaskqueue/worker.py).

- [The pieces](#the-pieces)
- [A task's life](#a-tasks-life)
- [The claim](#the-claim)
- [Ownership and fencing](#ownership-and-fencing)
- [Heartbeats and lost workers](#heartbeats-and-lost-workers)
- [Scheduling](#scheduling)
- [Periodic tasks](#periodic-tasks)
- [Retries](#retries)
- [Deduplication](#deduplication)
- [Inside a worker](#inside-a-worker)
- [Wake-ups: LISTEN/NOTIFY](#wake-ups-listennotify)
- [Logs](#logs)
- [Webhooks are tasks](#webhooks-are-tasks)
- [What is guaranteed](#what-is-guaranteed)
- [The schema](#the-schema)

## The pieces

```
  your application ──┐
  tq enqueue ────────┤ INSERT                       ┌──────────────┐
  POST /v1/tasks ────┤                              │  tq worker   │  one process,
  MCP enqueue_task ──┘                              │  ┌────────┐  │  one event loop,
                     ▼                              │  │ pool A │  │  N tasks at once
            ┌────────────────────┐    claim         │  │ pool B │  │
            │     PostgreSQL     │◄─────────────────┤  └────────┘  │
            │                    │    heartbeat     │  heartbeat   │
            │  tq_tasks          │◄─────────────────┤  promote     │
            │  tq_task_logs      │    settle        │  maintenance │
            │  tq_task_types     │◄─────────────────┤  log flush   │
            │  tq_workers        │    NOTIFY        │  listen      │
            │  tq_queues         ├─────────────────►│  schedules   │
            │  tq_users          │                  └──────────────┘
            │  tq_schedules      │
            └─────────▲──────────┘
                      │ the same statements, over HTTP
            ┌─────────┴──────────┐                  ┌──────────────┐
            │  tq api (FastAPI)  │◄─────────────────┤ remote worker│
            │  REST · MCP · JWT  │  /v1/worker/*    │ (any language)│
            └────────────────────┘                  └──────────────┘
```

There is no broker and no result backend. PostgreSQL is the only moving part besides the
processes themselves. (The API can use a Redis for request rate limits; no queue state is
kept there, and nothing below depends on it.)

- **`TaskQueue`** ([queue.py](../src/pgtaskqueue/queue.py)) is every operation on a task, each
  one a single SQL statement.
- **`Worker`** ([worker.py](../src/pgtaskqueue/worker.py)) claims tasks and runs executors.
- **`Monitor`** ([monitor.py](../src/pgtaskqueue/monitor.py)) is everything around the tasks:
  workers, task types, paused queues, statistics.
- **`Schedules`** ([schedules.py](../src/pgtaskqueue/schedules.py)) is periodic tasks: rows
  that say when to enqueue one.
- **The API** ([api/](../src/pgtaskqueue/api/)) is an optional front door. It runs no tasks and
  imports no executor code.

Three rules hold in every statement the queue runs:

1. **A state change is one conditional statement.** The status a transition expects is in its
   `WHERE`. Two writers racing for a row are settled by the row lock, and the loser changes
   nothing. Nothing is read first and written second.
2. **A worker owns a task only while `status = 'running' AND worker_id = <it> AND attempt =
   <the attempt it claimed>`.** Every write a worker makes carries all three.
3. **Time is the database's.** `now()` decides what is due and what is stale, so hosts with
   drifting clocks cannot disagree.

## A task's life

```
                 run_after in the future
    enqueue ───────────────────────────────► scheduled
       │                                        │ promote loop, when run_after passes
       │ run_after is now                       ▼
       └───────────────────────────────────► pending ◄──────────────┐
                                                │ claim             │ release (worker stopping)
                                                ▼                   │ stale recovery
                                             running ───────────────┤ retry with no delay
                                                │                   │
              ┌──────────────┬──────────────────┼───────────────┐   │
              ▼              ▼                  ▼               ▼   │
          succeeded        failed            canceled        scheduled (retry or snooze
                                                              with a delay) ──► pending
```

| Status | Meaning |
|---|---|
| `scheduled` | Waiting for `run_after`. Not claimable. |
| `pending` | Ready. The only status the claim query reads. |
| `running` | Claimed by a worker that is heartbeating for it. |
| `succeeded` | The executor returned; its return value is in `output`. |
| `failed` | Out of attempts, or raised `NonRetryable`. The last error is in `error`. |
| `canceled` | Canceled by a caller before it finished. |

The last three are terminal. A terminal task changes again only through `requeue`, which
runs it again in place: same id, attempts start over, completed steps kept unless asked
otherwise.

## The claim

The statement every worker runs to take work. It is the reason the design holds up, so here
it is in full:

```sql
WITH picked AS (
    SELECT c.id
    FROM (
        SELECT u.name
        FROM unnest(CAST(:names AS text[])) AS u(name)
        WHERE NOT EXISTS (SELECT 1 FROM tq_queues q WHERE q.name = :queue AND q.paused)
    ) AS n
    CROSS JOIN LATERAL (
        SELECT t.id, t.priority, t.run_after
        FROM tq_tasks t
        WHERE t.queue_name = :queue AND t.name = n.name AND t.status = 'pending'
        ORDER BY t.priority, t.run_after, t.id
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    ) AS c
    ORDER BY c.priority, c.run_after, c.id
    LIMIT :limit
)
UPDATE tq_tasks t
SET status = 'running', attempt = t.attempt + 1, worker_id = :worker_id,
    started_at = now(), heartbeat_at = now()
FROM picked
WHERE t.id = picked.id
RETURNING t.*
```

What each part is for:

**A partial index that matches the query exactly.**

```sql
CREATE INDEX tq_tasks_claim_idx ON tq_tasks (queue_name, name, priority, run_after, id)
    WHERE status = 'pending';
```

Only pending rows are in it, in the order the claim wants them. A claim is a short walk from
the front of that index which stops after `limit` rows. Its cost depends on how many tasks
are ready, not on how many the table holds. Ten million finished tasks cost a claim nothing,
which is what makes it reasonable to keep them. [benchmarks.md](benchmarks.md) measures this.

**One `LATERAL` subquery per task name.** A worker asks for the names it has executors for.
`name = ANY(:names)` in a single scan could not use the index's ordering across names;
one index walk per name can, and the outer `ORDER BY … LIMIT` merges them.

**`FOR UPDATE SKIP LOCKED`, next to the status test.** `SKIP LOCKED` makes a claimer step
over rows another claimer is holding right now, so concurrent claims do not queue behind
each other. The `status = 'pending'` test in the same subquery is what Postgres re-checks
(under READ COMMITTED) when a row was pending in this statement's snapshot but was claimed
and committed before the lock was reached. Both halves are needed. With the lock on an outer
`WHERE id = (subquery)` instead, that re-check has no condition to fail on, and the same
task can be handed to two workers. `test_concurrent_claims_never_deliver_twice` in
[tests/test_queue.py](../tests/test_queue.py) runs 16 claimers against one backlog to hold
this.

**A paused queue yields no names**, so nothing is scanned or locked.

**`attempt + 1` in the same statement** is the fencing token described next.

**Ordering** is `priority` (lower first), then `run_after`, then `id`. Ids are UUIDv7,
generated by the client and time-ordered, so the tie-break is first-in, first-out. A task
that was retried with a delay gets a later `run_after` and goes behind newer tasks of the
same priority.

## Ownership and fencing

A worker can be wrong about whether it still owns a task. It may have been paused by the
kernel, cut off from the database, or stuck in a blocking call long enough to be presumed
dead and have its task given to another worker. When it wakes up, it must not overwrite the
other worker's result.

So every write a worker makes is conditional on all of:

```sql
WHERE id = :id AND status = 'running' AND worker_id = :worker_id AND attempt = :attempt
```

`attempt` is incremented by the claim, so each claim of a task has a number no earlier claim
had. A stale worker's `succeed` matches zero rows; the queue methods return `None`, the
worker logs that the result was discarded, and nothing else happens. The same condition
guards `fail`, `snooze`, `set_progress` and `save_step`.

## Heartbeats and lost workers

Every `TQ_HEARTBEAT_INTERVAL_SECONDS` (5s) a worker sends one `UPDATE` for all the tasks it
is running:

```sql
UPDATE tq_tasks SET heartbeat_at = now()
WHERE id = ANY(:ids) AND status = 'running' AND worker_id = :worker_id
RETURNING id
```

The reply is the set of tasks the worker still owns. A task that is missing was canceled, or
was recovered and possibly claimed by someone else. The worker cancels that run. This is how
a cancel reaches a running task, and it is why an executor should be cancellable at its
`await` points.

`heartbeat_at` is in no index, on purpose. The table has `fillfactor = 85`, so a heartbeat
is a HOT update: no index is written at all.

Each worker's maintenance loop, once every `TQ_MAINTENANCE_INTERVAL_SECONDS` (15s), tries a
transaction-scoped advisory lock. The one that gets it recovers stale tasks:

```sql
UPDATE tq_tasks
SET status = CASE WHEN attempt < max_attempts THEN 'pending' ELSE 'failed' END, ...
WHERE status = 'running' AND heartbeat_at < now() - :stale
```

A task whose heartbeat is older than `TQ_STALE_AFTER_SECONDS` (60s) goes back to `pending`
if it has attempts left and to `failed` if it does not. That bound matters: a task that
kills its worker (out of memory, a segfault in a native library) would otherwise do so
forever. The error recorded is `WorkerLost`.

`TQ_STALE_AFTER_SECONDS` must be at least three heartbeat intervals; the settings refuse to
load otherwise, because one slow heartbeat would re-queue live work.

There is no separate reaper process. Any worker can do this job, and one of them does.

## Scheduling

A task enqueued with `delay` or a future `run_after` is stored as `scheduled`. A promote
loop in every worker moves due tasks to `pending` once a second
(`TQ_PROMOTE_INTERVAL_SECONDS`), under an advisory lock so one worker does it at a time.

Two statuses instead of a `run_after <= now()` test in the claim: the claim index then holds
only tasks that can run now. A million tasks scheduled for next week add nothing to a claim,
and the claim needs no time comparison at all. The price is that a delayed task can start up
to one promote interval late.

`scheduled` or `pending` is decided inside the `INSERT`, by comparing `run_after` to the
database's `now()`. A client whose clock runs ahead cannot enqueue "now" as a past time and
jump the line.

## Account limits

An account's waiting, rate and priority [limits](auth.md#limits) are checked by the API
before it inserts, and are covered there. The running limit is the queue's, and is built
from the same two statuses as scheduling.

The `INSERT` looks up the task's `created_by` in `tq_users`. If that account has a running
limit (its own `max_running`, or `TQ_ACCOUNT_MAX_RUNNING`), the task is written as
`scheduled` with `capped = true`, whenever it is due. The promote step skips capped tasks.
After it, in the same loop, a **release** step moves each account's due capped tasks to
`pending`, oldest first, as many as the account has room for: its limit minus its tasks
`pending` and `running`.

- **The claim does not change.** It knows nothing of accounts, so `tq_tasks_claim_idx`
  and the claim query stay as they are, and a held task costs a claim nothing, like a
  delayed one.
- **`pending` counts toward the limit**, not only `running`. Otherwise an account at its
  limit would keep all its other tasks claimable, and the next free slot anywhere would go
  to it.
- **One worker releases at a time**, under an advisory lock (`LOCK_RELEASE`) taken in the
  release's own transaction. Two releases at once would each count the same room and
  together release twice the limit.
- **It finds the accounts with a loose index scan** of `tq_tasks_held_idx`: a recursive
  query that jumps from one account to the next, so it reads one row per account with held
  tasks, not every held task.
- **A task that comes back** (requeued, or edited while waiting) goes back to `scheduled`
  if it is capped, so it waits its turn again.
- **The limit is a ceiling, not an exact count.** A task whose worker died is `running`
  until it is recovered, and counts. A task that was already `pending` when the limit
  was lowered stays pending. Tasks enqueued before the account had a limit were never
  held, but while they are pending or running they use up its room.

The release runs once a promote interval, not when a task settles, so a held task can
start up to `TQ_PROMOTE_INTERVAL_SECONDS` after its account has room.

## Periodic tasks

A schedule is a row in `tq_schedules` with a cron expression or an interval, and a
`next_run_at`. A loop in every worker fires the rows that are due, once a second
(`TQ_SCHEDULE_INTERVAL_SECONDS`). Firing is one transaction:

1. `SELECT … WHERE NOT paused AND next_run_at <= now() … FOR UPDATE SKIP LOCKED`, which
   also returns `now()`.
2. One `INSERT` of the tasks, with the dedupe key `tq.schedule:<name>`.
3. One `UPDATE` that sets each row's `next_run_at` to the first fire time after that
   `now()`.

The row lock makes a fire time fire once, with any number of workers looking: a second
worker steps over a locked row, and once the lock is free the row is no longer due. That
is why this loop needs no advisory lock, unlike promote. The task, its `NOTIFY` and the new
`next_run_at` commit together, so a worker that dies in the middle leaves the row due.

The next fire time is counted from the database's `now()`, not from the time that was
missed. However long nothing was looking, a due row enqueues one task and moves into the
future. The dedupe key means a schedule has one active task at a time; a fire that finds
the previous task unfinished moves the row on and enqueues nothing.

Schedules declared in code reach the table the way task types do: the worker writes them
when it starts. A `source` column keeps those apart from the ones created through the API,
and neither kind overwrites the other. [schedules.md](schedules.md) is the guide.

## Retries

When an executor raises:

| Raised | What happens |
|---|---|
| `Retry(reason, delay=…)` | Failed attempt. Runs again after `delay`, or the default backoff. |
| `Snooze(delay, reason)` | Not a failure. Runs again after `delay`; the attempt is handed back. |
| `NonRetryable(…)` | `failed` now, whatever attempts remain. |
| Timeout (`timeout_seconds`) | Recorded as `TaskTimeout`, then treated like any exception. |
| Any other exception | Failed attempt. Retried with backoff until `max_attempts`. |

The default delay before attempt *n+1* is `min(base × 2^(n−1), max)` plus up to 20% jitter:
5s, 10s, 20s, … capped at 10 minutes. `Executor.retry_delay(attempt, error)` overrides it
per task type.

A retry with a delay is the task going back to `scheduled` with a new `run_after`. There is
no separate retry queue or dead-letter queue: a task that ran out of attempts is a row with
`status = 'failed'`, its last `error`, and its log, and `requeue` runs it again.

`max_attempts` and the timeout come from the enqueue call, else from what the task type's
worker registered (see [The schema](#the-schema)), else from `TQ_DEFAULT_MAX_ATTEMPTS`.
That lookup happens inside the `INSERT`, which is how the API applies an executor's defaults
without being able to import the executor.

## Deduplication

```sql
CREATE UNIQUE INDEX tq_tasks_dedupe_idx ON tq_tasks (queue_name, name, dedupe_key)
    WHERE dedupe_key IS NOT NULL AND status IN ('scheduled', 'pending', 'running');
```

At most one *active* task per `(queue, name, dedupe_key)`. Enqueueing a second one returns
the first, with `created = False`. The key is free again the moment the task finishes, so it
means "do not queue this twice", not "do this once ever". For the second meaning, make the
executor idempotent or record completion in your own tables.

## Inside a worker

One process, one event loop. Everything is a coroutine in one `asyncio.TaskGroup`:

| Coroutine | Does |
|---|---|
| pool loops | One per pool. Claims what fits in the free slots; starts a run per task. |
| runs | One per task being executed. |
| heartbeat | One `UPDATE` for every running task; cancels the runs it no longer owns. |
| promote | `scheduled` → `pending` when `run_after` passes; then the release of tasks held by an [account limit](#account-limits). |
| schedules | Enqueues a task for each [periodic schedule](#periodic-tasks) that is due. |
| maintenance | Recovers tasks of dead workers, prunes, refreshes the worker's own row. |
| archive | Moves old finished tasks to the [archive database](analytics.md#the-archive), when there is one. |
| log flush | Writes buffered task log lines in batches. |
| listen | `LISTEN` on a dedicated connection; wakes pools, hastens cancels. |

A `TaskGroup` cancels all its children when one fails. A worker must not behave that way for
task failures, so a run never raises: `_execute` turns every outcome, including an exception
it has never seen, into a row update. The group's all-or-nothing behavior is kept for what
it is good for, which is taking everything down together when the worker stops.

A pool is a set of task names sharing a concurrency limit. A pool loop claims `free slots`
tasks, starts them, and sleeps until a slot frees, a notification arrives, or the poll
interval passes. No database connection is held while a task runs. A run takes one from the
pool for the few milliseconds of each write and gives it back, so a worker with 1,000 slots
does not need 1,000 connections.

Stopping (SIGTERM, idle exit, max tasks): pools stop claiming, running tasks get
`TQ_WORKER_SHUTDOWN_GRACE_SECONDS` to finish, and whatever is still running is cancelled and
*released*: back to `pending` with its attempt handed back. A deploy does not cost tasks an
attempt. [workers.md](workers.md) covers this from the operator's side.

## Wake-ups: LISTEN/NOTIFY

Polling alone is correct. Notifications make it fast.

- `tq_enqueue` (payload: queue name) is sent whenever a task becomes `pending`. Idle pools
  claim immediately instead of at their next poll.
- `tq_cancel` (payload: task id) is sent on cancel. The worker running that task heartbeats
  immediately. The notification is a hint; the heartbeat's reply is what cancels the run.

`NOTIFY` is transactional, so a notification is delivered only when the enqueue commits, and
a worker never wakes for a task it cannot see yet.

Each worker holds one extra connection for `LISTEN`, outside its pool. If that connection
drops, the worker logs it, keeps polling, and reconnects with backoff. Behind a
transaction-mode pooler such as PgBouncer a session cannot `LISTEN`, so a worker there
either gets a direct connection for it (`TQ_LISTEN_DATABASE_URL`) or polls, and pickup
latency becomes the poll interval. [benchmarks.md](benchmarks.md) has the numbers for both,
and [infrastructure.md](infrastructure.md#connection-poolers) the setup.

## Logs

Many tasks share one event loop, so "which task is this line about" cannot be a global. It
is a `ContextVar`. asyncio gives every task its own copy and the threads a run starts (`ctx.to_thread`, `asyncio.to_thread`) are given
it, so a line written anywhere under an executor's `run()` is attributed to that
task: its own `ctx.log(...)`, a library's `logging` call, a thread it started.

Lines are buffered in memory and inserted in batches every
`TQ_LOG_FLUSH_INTERVAL_SECONDS` (1s). A task logging in a tight loop costs one `INSERT` per
second, not one per line. The price: a worker killed with `SIGKILL` loses up to a second of
log lines. The task itself is recovered; only those lines are gone.

## Webhooks are tasks

A task with a `callback` gets a notification when it finishes. That notification is itself a
task, named `tq.webhook`, inserted **in the same transaction** that settles the original
task. So:

- It cannot be lost between "the task succeeded" and "someone was told". Either both rows
  are committed or neither is.
- It is retried with the same backoff as anything else (`TQ_WEBHOOK_MAX_ATTEMPTS`, 6).
- Every delivery attempt is a row and log lines you can query.
- The delivery id (the `tq.webhook` task's id) is the same across retries, so a receiver can
  de-duplicate.

Every worker reserves `TQ_WEBHOOK_SLOTS` (2) for deliveries, so they do not compete with
your tasks for slots. See [api.md](api.md#webhooks) for the request format and signature.

## What is guaranteed

**At-least-once execution.** A task that was enqueued and committed will be run until it
succeeds, fails for good, or is canceled, as long as some worker that has its executor is
running. It may run **more than once**: if a worker dies after doing the work but before
recording it, the task is recovered and run again. Write executors to be idempotent, or use
`ctx.step` so that completed steps are not repeated.

**No concurrent double delivery.** The claim never hands one task to two workers. The
exception is a worker that is presumed dead while it is in fact alive and unreachable: its
task is re-queued and can start elsewhere while the first run is still going. Fencing means
the first run's result is discarded, but its side effects happened. A stale threshold well
above your worst heartbeat delay keeps this rare.

**Transactional enqueue.** With `session=`, a task is enqueued in your transaction and
becomes visible if and only if that transaction commits.

**Not guaranteed:** strict ordering. Priority and FIFO order the claims, but tasks run
concurrently and retries move a task back. If two tasks must not overlap, give them a pool
with one slot, or have the first enqueue the second.

## The schema

Seven tables, all prefixed `tq_`, so they can live in an application's own database. Their
migration history is tracked in `tq_alembic_version`, separate from the application's.
[models.py](../src/pgtaskqueue/models.py) is the reference.

| Table | Holds |
|---|---|
| `tq_tasks` | Every task: waiting, running and finished. |
| `tq_task_logs` | Log lines, by task and attempt. Deleted with their task. |
| `tq_task_types` | What workers say they can run: description, payload JSON Schema, defaults. |
| `tq_workers` | One row per worker process: pools, slots, counters, last heartbeat. |
| `tq_queues` | Pause switches. A queue needs no row to exist. |
| `tq_schedules` | Periodic tasks: what to enqueue, when, and when it is next due. |
| `tq_users` | API accounts, with role, ACL and limits. |

The archive's two tables, `tq_archive_tasks` and `tq_archive_task_logs`, are in
`TQ_ARCHIVE_DATABASE_URL` and have a migration history of their own
(`tq_archive_alembic_version`). Their columns are those of `tq_tasks` and `tq_task_logs`,
plus `archived_at`. [analytics.md](analytics.md#the-archive) explains how tasks get there.

`tq_task_types` is how the API knows about tasks without importing them. A worker publishes
the definitions of its executors when it starts; the API validates payloads against the
published JSON Schema and applies the published defaults. Each worker image can then have
its own dependencies, and the API image has none of them.

Indexes on `tq_tasks`, each with one job:

| Index | For |
|---|---|
| `tq_tasks_claim_idx` (partial: `pending`) | The claim. |
| `tq_tasks_due_idx` on `run_after` (partial: `scheduled`, not capped) | The promote loop. |
| `tq_tasks_held_idx` on `created_by, run_after` (partial: `scheduled`, capped) | The release step, [account limits](#account-limits). |
| `tq_tasks_active_by_idx` on `created_by, status` (partial: active, with an account) | Counting an account's tasks against its limits. |
| `tq_tasks_running_idx` on `worker_id` (partial: `running`) | Heartbeats, release, recovery. |
| `tq_tasks_dedupe_idx` (partial unique: active with a key) | Deduplication. |
| `tq_tasks_status_idx`, `tq_tasks_name_idx` | Listing and filtering, newest first. |
| `tq_tasks_created_by_idx` (partial) | "My tasks" for accounts that see only their own. |
| `tq_tasks_completed_idx` (partial) | Retention pruning; statistics over a recent window. |

The table is created with `fillfactor = 85` and aggressive autovacuum thresholds (2% instead
of the default 20%), because a queue table is the worst case for the defaults: every row is
updated a few times and then never again.
