# Analytics: the queue is a table

Every task that was ever enqueued is a row in `tq_tasks`, with its payload, its outcome, its
timings and its error. Every log line is a row in `tq_task_logs`. There is no broker to
drain and no result backend that expires. Questions about your background work are SQL
queries, and they join against your own tables.

This is the main reason to run a queue in PostgreSQL rather than Redis.

- [What the columns mean](#what-the-columns-mean)
- [Right now](#right-now)
- [Throughput and duration](#throughput-and-duration)
- [Failures](#failures)
- [By tenant, customer or anything else](#by-tenant-customer-or-anything-else)
- [Workers](#workers)
- [Logs and steps](#logs-and-steps)
- [Webhooks and child tasks](#webhooks-and-child-tasks)
- [Joining with your own data](#joining-with-your-own-data)
- [Dashboards](#dashboards)
- [Keeping analytics from hurting the queue](#keeping-analytics-from-hurting-the-queue)
- [Retention and size](#retention-and-size)
- [The archive](#the-archive)

The built-in summary is `tq stats`, `GET /v1/stats` and `Monitor.stats()`. Everything below
is what to run when that is not enough. All of it is plain SQL against the documented
schema; run it with `psql`, a BI tool, or from your application.

```bash
docker compose exec db psql -U tq task_queue
```

## What the columns mean

A few columns need care when aggregating:

| Column | Meaning |
|---|---|
| `status` | `scheduled`, `pending`, `running`, `succeeded`, `failed`, `canceled`. |
| `attempt` | How many times it has been claimed. 1 for a task that ran once. |
| `created_at` | When it was enqueued. |
| `run_after` | When it became eligible. Later than `created_at` for delayed tasks and retries. |
| `started_at` | When the **latest** attempt started. |
| `completed_at` | When it reached a terminal status. NULL until then. |
| `spent_seconds` | Execution time **summed over all attempts**. |
| `worker_id` | The worker that ran the **latest** attempt. Kept after the task finishes. |
| `error` | The **latest** failure: `{type, message, traceback, attempt, worker}`. Cleared on success. |
| `metadata` | Your labels, as `jsonb`. |
| `created_by` | The API account that enqueued it, or `cli`, or whatever your code passed. |

So: time waiting in the queue is `started_at - run_after` for a task with `attempt = 1`, and
is not recoverable from the row for a retried task. Each attempt's history is in the log.

Ids are UUIDv7, which are time-ordered. `ORDER BY id` is creation order, and no extra index
is needed for "newest first".

## Right now

What is in the queue, by task:

```sql
SELECT queue_name, name, status, count(*)
FROM tq_tasks
WHERE status IN ('scheduled', 'pending', 'running')
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
```

How long the head of each line has been waiting. This is the number to alert on:

```sql
SELECT queue_name, name, count(*) AS pending,
       round(extract(epoch FROM now() - min(run_after))) AS oldest_seconds
FROM tq_tasks
WHERE status = 'pending'
GROUP BY 1, 2
ORDER BY oldest_seconds DESC;
```

What is running, longest first, and whether its worker is still heartbeating:

```sql
SELECT id, name, worker_id, attempt, step,
       now() - started_at   AS running_for,
       now() - heartbeat_at AS since_heartbeat,
       progress - 'steps'   AS progress
FROM tq_tasks
WHERE status = 'running'
ORDER BY started_at;
```

These three read only the partial indexes for active statuses, so they stay fast whatever
the table's size.

## Throughput and duration

Tasks finished per hour over the last day, by outcome:

```sql
SELECT date_trunc('hour', completed_at) AS hour,
       count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
       count(*) FILTER (WHERE status = 'failed')    AS failed,
       count(*) FILTER (WHERE status = 'canceled')  AS canceled
FROM tq_tasks
WHERE completed_at >= now() - interval '24 hours'
GROUP BY 1
ORDER BY 1;
```

Duration percentiles by task name, last 7 days:

```sql
SELECT name,
       count(*)                                                                    AS runs,
       round(avg(spent_seconds)::numeric, 3)                                       AS avg_s,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY spent_seconds)::numeric, 3) AS p50_s,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY spent_seconds)::numeric, 3) AS p95_s,
       round(max(spent_seconds)::numeric, 3)                                       AS max_s
FROM tq_tasks
WHERE status = 'succeeded' AND completed_at >= now() - interval '7 days'
GROUP BY name
ORDER BY p95_s DESC;
```

Time spent waiting for a free slot, for tasks that ran once:

```sql
SELECT name,
       round(percentile_cont(0.5)  WITHIN GROUP (
           ORDER BY extract(epoch FROM started_at - run_after))::numeric, 3) AS p50_wait_s,
       round(percentile_cont(0.95) WITHIN GROUP (
           ORDER BY extract(epoch FROM started_at - run_after))::numeric, 3) AS p95_wait_s
FROM tq_tasks
WHERE attempt = 1 AND started_at IS NOT NULL
  AND completed_at >= now() - interval '24 hours'
GROUP BY name
ORDER BY p95_wait_s DESC;
```

A rising p95 wait with a flat duration means you need more slots or more workers for that
task. The reverse means the tasks themselves got slower.

Total compute by task name, which is where the money goes:

```sql
SELECT name, count(*) AS runs, round(sum(spent_seconds)::numeric / 3600, 2) AS hours
FROM tq_tasks
WHERE completed_at >= now() - interval '30 days'
GROUP BY name
ORDER BY hours DESC;
```

The slowest individual tasks this week:

```sql
SELECT id, name, spent_seconds, attempt, completed_at, payload
FROM tq_tasks
WHERE status = 'succeeded' AND completed_at >= now() - interval '7 days'
ORDER BY spent_seconds DESC
LIMIT 20;
```

## Failures

Failure rate by task name:

```sql
SELECT name,
       count(*) FILTER (WHERE status = 'failed') AS failed,
       count(*)                                  AS finished,
       round(100.0 * count(*) FILTER (WHERE status = 'failed') / count(*), 2) AS failed_pct
FROM tq_tasks
WHERE completed_at >= now() - interval '24 hours'
GROUP BY name
HAVING count(*) FILTER (WHERE status = 'failed') > 0
ORDER BY failed_pct DESC;
```

What is failing, grouped by exception type:

```sql
SELECT name, error->>'type' AS error_type, count(*),
       max(completed_at) AS last_seen,
       left(max(error->>'message'), 120) AS example
FROM tq_tasks
WHERE status = 'failed' AND completed_at >= now() - interval '24 hours'
GROUP BY 1, 2
ORDER BY count(*) DESC;
```

`WorkerLost` means a worker died or was cut off while running the task. `TaskTimeout` means
an attempt ran past its timeout.

Tasks that needed retries but got there in the end. A healthy system has a few; a growing
number is an early warning:

```sql
SELECT name, attempt, count(*)
FROM tq_tasks
WHERE status = 'succeeded' AND attempt > 1
  AND completed_at >= now() - interval '24 hours'
GROUP BY 1, 2
ORDER BY 1, 2;
```

Tasks that are retrying right now, with the reason:

```sql
SELECT id, name, attempt, max_attempts, run_after,
       error->>'type' AS error_type, error->>'message' AS message
FROM tq_tasks
WHERE status IN ('scheduled', 'pending') AND attempt > 0
ORDER BY run_after;
```

The full traceback of one failure:

```sql
SELECT error->>'traceback' FROM tq_tasks WHERE id = '0192e3a1-…';
```

Requeue everything that failed with a particular error after you have fixed the cause. This
is a write, so use the API or CLI, which keep the invariants, driven by a query:

```bash
psql -Atc "SELECT id FROM tq_tasks WHERE status = 'failed' AND error->>'type' = 'ConnectError'
           AND completed_at >= now() - interval '2 hours'" | xargs -n1 tq requeue
```

Do not `UPDATE tq_tasks` by hand to change a status. The queue's statements carry
conditions that keep workers from overwriting each other, and a hand-written update does
not.

## By tenant, customer or anything else

`metadata` is a `jsonb` column you fill at enqueue with whatever you will want to group by:

```python
await queue.enqueue("report.build", {...}, metadata={"tenant": "acme", "plan": "pro"})
```

Usage per tenant:

```sql
SELECT metadata->>'tenant' AS tenant,
       count(*) AS tasks,
       round(sum(spent_seconds)::numeric, 1) AS seconds,
       count(*) FILTER (WHERE status = 'failed') AS failed
FROM tq_tasks
WHERE completed_at >= date_trunc('month', now())
GROUP BY 1
ORDER BY seconds DESC;
```

One tenant's recent tasks:

```sql
SELECT id, name, status, created_at
FROM tq_tasks
WHERE metadata @> '{"tenant": "acme"}'
ORDER BY id DESC
LIMIT 50;
```

`metadata` is **not indexed by default**, because an index on it costs every insert and
many deployments never filter on it. If you filter by metadata often (the API's `metadata=`
filter and `TaskQueue.list(metadata=…)` use the `@>` operator shown above), add one:

```sql
CREATE INDEX CONCURRENTLY tq_tasks_metadata_idx
    ON tq_tasks USING gin (metadata jsonb_path_ops);
```

or, for one hot key, a smaller and faster expression index:

```sql
CREATE INDEX CONCURRENTLY tq_tasks_tenant_idx ON tq_tasks ((metadata->>'tenant'), id);
```

Indexes you add are yours: the migrations do not know about them and will not drop them.

By API account, which is indexed:

```sql
SELECT created_by, count(*), max(created_at) AS last_enqueued
FROM tq_tasks
WHERE created_by IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;
```

## Workers

Who is alive, and how busy:

```sql
SELECT id, queue_name, in_flight, slots,
       round(100.0 * in_flight / nullif(slots, 0)) AS busy_pct,
       now() - started_at AS up_for,
       counters
FROM tq_workers
WHERE stopped_at IS NULL AND heartbeat_at > now() - interval '1 minute'
ORDER BY started_at;
```

Work done per worker today:

```sql
SELECT worker_id, count(*) AS tasks, round(sum(spent_seconds)::numeric, 1) AS seconds,
       count(*) FILTER (WHERE status = 'failed') AS failed
FROM tq_tasks
WHERE completed_at >= date_trunc('day', now())
GROUP BY 1
ORDER BY 2 DESC;
```

A worker with a much higher failure rate than its peers is a host problem, not a task
problem.

How often workers restart, and why they were running (a worker that exits on idle or after
`max_tasks` is replaced by a new row):

```sql
SELECT date_trunc('hour', started_at) AS hour, count(*) AS workers_started,
       sum((counters->>'claimed')::int) AS tasks_claimed,
       sum((counters->>'lost')::int)    AS lost
FROM tq_workers
GROUP BY 1
ORDER BY 1 DESC
LIMIT 24;
```

Worker rows are kept for 7 days after a worker stops.

## Logs and steps

One task's log, as `tq show <id> --logs` prints it:

```sql
SELECT created_at, attempt, level, step, message
FROM tq_task_logs
WHERE task_id = '0192e3a1-…'
ORDER BY id;
```

Recent error lines across all tasks, with the task they belong to:

```sql
SELECT l.created_at, t.name, l.task_id, l.attempt, left(l.message, 200) AS message
FROM tq_task_logs l
JOIN tq_tasks t ON t.id = l.task_id
WHERE l.level = 'error' AND l.created_at >= now() - interval '1 hour'
ORDER BY l.id DESC
LIMIT 100;
```

How long each [step](executors.md#steps-resuming-instead-of-repeating) takes, from the lines
`ctx.step` writes when a step starts and finishes:

```sql
WITH steps AS (
    SELECT t.name, l.task_id, l.attempt, l.step,
           min(l.created_at) FILTER (WHERE l.message LIKE 'step %: started') AS started,
           max(l.created_at) FILTER (WHERE l.message LIKE 'step %: done')    AS done
    FROM tq_task_logs l
    JOIN tq_tasks t ON t.id = l.task_id
    WHERE l.step IS NOT NULL AND l.created_at >= now() - interval '7 days'
    GROUP BY 1, 2, 3, 4
)
SELECT name, step, count(*) AS runs,
       round(avg(extract(epoch FROM done - started))::numeric, 3) AS avg_s,
       round(max(extract(epoch FROM done - started))::numeric, 3) AS max_s
FROM steps
WHERE done IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, avg_s DESC;
```

`tq_task_logs` is indexed by `(task_id, id)`, which serves "the log of this task". Queries
across all tasks by time or level scan the table. That is fine for occasional use on a
table of millions of rows; if you search logs constantly, add an index for it
(`CREATE INDEX CONCURRENTLY … ON tq_task_logs (created_at)`), or ship logs to a system built
for search.

## Webhooks and child tasks

Webhook deliveries are tasks named `tq.webhook`, with the task they report in their
metadata. Deliveries that gave up:

```sql
SELECT w.id AS delivery, w.metadata->>'for_name' AS task_name,
       w.metadata->>'for_task' AS task_id, w.attempt,
       w.error->>'message' AS reason, w.completed_at
FROM tq_tasks w
WHERE w.name = 'tq.webhook' AND w.status = 'failed'
ORDER BY w.completed_at DESC
LIMIT 50;
```

After fixing the receiver, `tq requeue <delivery>` sends it again.

Tasks started by another task carry `parent_task_id` in their metadata:

```sql
SELECT status, count(*)
FROM tq_tasks
WHERE metadata->>'parent_task_id' = '0192e3a1-…'
GROUP BY 1;
```

## Joining with your own data

When the queue's tables are in your application's database, tasks join to your rows. Put the
key in `metadata` (or read it from `payload`) and:

```sql
-- Orders whose confirmation e-mail failed, with the reason.
SELECT o.id, o.customer_email, t.error->>'message' AS reason, t.completed_at
FROM orders o
JOIN tq_tasks t ON t.metadata->>'order_id' = o.id::text
WHERE t.name = 'mail.order_confirmation' AND t.status = 'failed';
```

Enqueueing in the same transaction as the business write makes the reverse guarantee: an
order row exists if and only if its task does.

```python
async with db.transaction() as tx:
    order = await create_order(tx, ...)
    await queue.enqueue(
        "mail.order_confirmation",
        {"order_id": order.id},
        metadata={"order_id": str(order.id)},
        session=tx,
    )
```

## Dashboards

Any tool that speaks PostgreSQL works: Grafana, Metabase, Superset, Redash. Point it at the
database through a read-only role (below) and use the queries above. A view gives the tool
one flat table of finished work to chart, without teaching it about `jsonb`:

```sql
CREATE VIEW tq_finished AS
SELECT id, queue_name, name, status, attempt, created_by, worker_id,
       metadata->>'tenant'  AS tenant,
       error->>'type'       AS error_type,
       created_at, started_at, completed_at, spent_seconds,
       CASE WHEN attempt = 1
            THEN extract(epoch FROM started_at - run_after) END AS wait_seconds
FROM tq_tasks
WHERE completed_at IS NOT NULL;
```

Views you create are yours, like indexes: the migrations neither know about them nor drop
them. A view that selects named columns keeps working when a later version adds a column.

For a time-series panel, group by a time bucket and let the tool supply the range. In
Grafana:

```sql
SELECT date_trunc('minute', completed_at) AS time, name, count(*) AS finished
FROM tq_tasks
WHERE $__timeFilter(completed_at)
GROUP BY 1, 2
ORDER BY 1;
```

For alerting, the two numbers that matter are the age of the oldest pending task (the
second query under [Right now](#right-now)) and the failure rate. `GET /v1/stats` returns
both as JSON if your monitoring prefers HTTP to SQL.

## Keeping analytics from hurting the queue

Analytics and the queue share one database, so a careless report can slow the workers down.
Four habits prevent that.

**Use a read-only role.** It cannot change a task by accident, and it can carry its own
limits:

```sql
CREATE ROLE tq_readonly LOGIN PASSWORD 'change-me';
GRANT USAGE ON SCHEMA public TO tq_readonly;
GRANT SELECT ON tq_tasks, tq_task_logs, tq_task_types, tq_workers, tq_queues, tq_schedules
  TO tq_readonly;
ALTER ROLE tq_readonly SET statement_timeout = '30s';
ALTER ROLE tq_readonly SET default_transaction_read_only = on;
```

`tq_users` is left out on purpose: it holds password hashes.

**Always bound the time range.** `completed_at >= now() - interval '…'` uses
`tq_tasks_completed_idx`, and `ORDER BY id DESC LIMIT n` uses the primary key. A query with
neither reads the whole table. That is harmless at a hundred thousand rows and a problem at
a hundred million.

**Do not hold a transaction open.** A session that sits idle inside a transaction stops
vacuum from cleaning up rows behind it, and a queue table is rewritten constantly. Dead
rows pile up, the partial indexes grow, and claims get slower. Set
`idle_in_transaction_session_timeout` on the reporting role, and prefer tools that run each
query in autocommit.

**Send heavy reports to a replica.** A streaming replica has the same tables. Point
dashboards at it and the primary serves only the queue. Long queries on a replica either
delay replay or get canceled, depending on `max_standby_streaming_delay`; neither affects
the workers.

If you report on old history often, a nightly rollup is cheaper than re-aggregating raw rows
on every dashboard load, and it survives the retention policy:

```sql
CREATE TABLE IF NOT EXISTS tq_daily (
    day date, queue_name text, name text, status text,
    tasks bigint, spent_seconds double precision,
    PRIMARY KEY (day, queue_name, name, status)
);

INSERT INTO tq_daily
SELECT completed_at::date, queue_name, name, status, count(*), sum(spent_seconds)
FROM tq_tasks
WHERE completed_at >= current_date - 1 AND completed_at < current_date
GROUP BY 1, 2, 3, 4
ON CONFLICT (day, queue_name, name, status)
DO UPDATE SET tasks = EXCLUDED.tasks, spent_seconds = EXCLUDED.spent_seconds;
```

Run it from cron, or as a [periodic task](schedules.md): an executor that runs this
statement, with `schedule=Schedule("10 0 * * *")`.

## Retention and size

By default nothing is ever deleted (`TQ_RETENTION_DAYS=0`), because the history is the point.
The cost is disk, not speed: claims read a partial index over pending rows only, and the
[benchmarks](benchmarks.md#table-size) show a claim taking the same 2 ms with a million
finished rows behind it as with ten thousand.

How big it is, and how fast it grows:

```sql
SELECT relname,
       pg_size_pretty(pg_table_size(oid))   AS table_size,
       pg_size_pretty(pg_indexes_size(oid)) AS index_size,
       reltuples::bigint                    AS approx_rows
FROM pg_class
WHERE relname IN ('tq_tasks', 'tq_task_logs');

SELECT completed_at::date AS day, count(*) AS tasks
FROM tq_tasks
WHERE completed_at >= now() - interval '14 days'
GROUP BY 1
ORDER BY 1;
```

As a rough guide from the benchmark, a task with a small payload costs about 260 bytes of
table and 170 bytes of index, so a million finished tasks is around 420 MB before logs.
Large payloads, outputs and tracebacks add to that directly. Logs are usually the bigger
table: measure yours.

When you do want a limit, set `TQ_RETENTION_DAYS=90`. A worker's maintenance loop then
deletes finished tasks older than that, in batches, along with their logs. Active tasks are
never deleted, whatever their age.

To keep the numbers and drop the rows, run the rollup above before the retention window
reaches them. To keep the rows but not in the queue's database, use
[the archive](#the-archive) instead of retention.

Whether autovacuum is keeping up, which is the thing to check if claims ever slow down:

```sql
SELECT relname, n_live_tup, n_dead_tup, last_autovacuum, last_autoanalyze
FROM pg_stat_user_tables
WHERE relname LIKE 'tq\_%';
```

A dead-tuple count that stays far above the live count means something is holding vacuum
back: usually a long transaction (see above) or an abandoned replication slot.

## The archive

The archive keeps the history and moves it out of the queue's database. Workers move
finished tasks, with their logs, to another PostgreSQL database once they are
`TQ_ARCHIVE_AFTER_DAYS` (30) old. The queue's table then holds only recent work, and old
tasks stay queryable in the archive.

The archive database must exist; `tq migrate` creates its tables. With compose:

```bash
docker compose up -d db
docker compose exec db createdb -U tq task_queue_archive
```

```bash
# in .env; compose passes it to `migrate` and to the workers
TQ_ARCHIVE_DATABASE_URL=postgresql+asyncpg://tq:tq@db:5432/task_queue_archive
```

```bash
docker compose up -d --build   # migrate creates the archive's tables; workers start moving
```

That URL names the compose network's `db`, so run `tq archive` in a container
(`docker compose exec worker tq archive run`), or from the host with the published port:
`TQ_ARCHIVE_DATABASE_URL=postgresql+asyncpg://tq:tq@localhost:54320/task_queue_archive`.

It can be a database on the same server, as here, or on another one. It can also be the
queue's own database, since its tables have names of their own (`tq_archive_tasks`,
`tq_archive_task_logs`), but then the history stays on the same disk. A transaction-mode
pooler in front of the archive is not tested.

Every `TQ_ARCHIVE_INTERVAL_SECONDS` (60), one worker moves tasks in batches of
`TQ_ARCHIVE_BATCH_SIZE` (1000) until none are left. To move a long history at once, or with
no workers running, use `tq archive run`:

```bash
tq archive run --older-than 90
tq archive tasks --status failed --limit 5
tq archive show 0192e3a1-7c1e-7d3a-9b52-0f6d1c2e4a77 --logs
```

### How a batch moves

Two databases cannot commit together, so each batch runs in an order that may repeat a
step but never loses a task:

1. In the queue's database, lock the batch with `SELECT … FOR UPDATE SKIP LOCKED`: finished
   tasks whose `completed_at` is past the threshold. The transaction stays open.
2. In the archive, write the tasks and their log lines, and commit.
3. In the queue's database, delete the tasks (their logs go with them), and commit.

What follows from that:

- **A task leaves the queue only once the archive has committed it.** If the archive is
  down, batches fail, the worker logs a warning each interval, and the tasks stay where
  they are. Nothing else in the queue is affected.
- **A task can be in both databases for a while.** That happens if step 3 fails after
  step 2. The next batch archives the task again, and the new copy replaces the old one,
  logs included. If you count across both databases, count each `id` once and prefer the
  queue's copy.
- **A requeue during a batch waits for it.** Then it finds the task gone, and returns "no
  such task".
- **One worker archives at a time**, under an advisory lock taken in each batch's
  transaction. No transaction in the queue's database lasts longer than one batch.

### What changes for an archived task

It is no longer in the queue's database, so the HTTP API, the MCP server, the web UI,
`tq show`, `tq stats` and `Monitor.stats()` do not see it, and it cannot be requeued. Read it
with `tq archive`, `Archive.get()` in Python, or SQL against the archive. The archive has
the same columns as `tq_tasks`, plus `archived_at`:

```sql
SELECT date_trunc('month', completed_at) AS month, name, status,
       count(*) AS tasks, round(sum(spent_seconds)) AS spent_seconds
FROM tq_archive_tasks
WHERE completed_at >= now() - interval '1 year'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

SELECT l.created_at, l.attempt, l.level, l.message
FROM tq_archive_task_logs l
WHERE l.task_id = '0192e3a1-7c1e-7d3a-9b52-0f6d1c2e4a77'
ORDER BY l.id;
```

Nothing is ever deleted from the archive. To limit it, delete there yourself; log lines go
with their task:

```sql
DELETE FROM tq_archive_tasks WHERE completed_at < now() - interval '2 years';
```

With an archive, `TQ_RETENTION_DAYS` must be `0` or more than `TQ_ARCHIVE_AFTER_DAYS`.
Otherwise retention would delete tasks before they were archived, so the settings refuse
to load.

The archive's schema has its own migration history, in `tq_archive_alembic_version`, and
`tq migrate` applies it whenever `TQ_ARCHIVE_DATABASE_URL` is set. A release that adds a
column to `tq_tasks` adds it to the archive in the same release. During a rolling deploy, a
new worker's batches fail until the archive has been migrated, and the tasks wait in the
queue.
