# Benchmarks

How many tasks a second, how many at once, how fast a task starts, and what a large table
costs. Measured with the scripts in [benchmarks/](../benchmarks/), which you can run
yourself.

- [Summary](#summary)
- [Environment](#environment)
- [Enqueue](#enqueue)
- [Throughput](#throughput)
- [Pickup latency](#pickup-latency)
- [Concurrency in one process](#concurrency-in-one-process)
- [Table size](#table-size)
- [What these numbers do not tell you](#what-these-numbers-do-not-tell-you)
- [Running them yourself](#running-them-yourself)

## Summary

On one laptop, with PostgreSQL in Docker on the same machine:

| Question | Answer |
|---|---|
| How fast can tasks be written? | 2,600/s one at a time from 32 concurrent callers; 23,600/s in batches |
| How fast do they get through? | 1,200/s with one worker process; 4,100/s with four |
| How long before an idle worker starts a new task? | 8 ms (median) |
| How many can one process run at once? | 10,000, using 129 MB |
| Does a big table slow the queue down? | No: a claim takes 2 ms at 10,000 rows and at 1,000,000 |

These are the numbers for tasks that do nothing, so they measure the queue itself. They are
a ceiling for the queue's own overhead, not a prediction for your workload. A task that
calls an API for 300 ms spends 300 ms doing that and about 1 ms being queued.

Read [What these numbers do not tell you](#what-these-numbers-do-not-tell-you) before
quoting any of them.

## Environment

| | |
|---|---|
| Machine | Apple M2 Max, 12 cores, 32 GB RAM, macOS 27.0.1 |
| PostgreSQL | 18.6, in Docker Desktop 29.8.2 (Linux VM with 12 CPUs, 16 GB) |
| PostgreSQL settings | The ones in [compose.yml](../compose.yml): `shared_buffers=512MB`, `max_connections=200`, `synchronous_commit=on`, `fsync=on` |
| Python | 3.13.12 |
| pgtaskqueue | 0.1.0, default settings unless a row says otherwise |
| Workers | Real `tq worker` processes on the host, connecting to PostgreSQL through Docker's published port |

Workers, PostgreSQL and the benchmark script all share the same 12 cores.

Each figure is from one run. An earlier full run agreed with it to within about 10% on
every row the two share, and to within 3% on most, so treat differences smaller than that
as noise.

## Enqueue

How fast tasks can be written, with nothing consuming them.

| How | Tasks | Seconds | Tasks/s |
|---|---|---|---|
| `enqueue()`, one caller | 2,000 | 3.73 | 536 |
| `enqueue()`, 32 concurrent callers | 20,000 | 7.56 | 2,644 |
| `enqueue()`, 32 concurrent callers, `TQ_NOTIFY=false` | 20,000 | 6.40 | 3,124 |
| `enqueue_many()`, batches of 100 | 100,000 | 6.33 | 15,809 |
| `enqueue_many()`, batches of 1,000 | 200,000 | 8.76 | 22,825 |
| `enqueue_many()`, batches of 5,000 | 200,000 | 8.46 | 23,649 |

What to take from it:

- **One `enqueue()` is one transaction**, and takes about 2 ms. A single caller in a loop
  is therefore limited to about 500 a second. That is the cost of each task being durable
  the moment the call returns.
- **Concurrent callers scale**, because PostgreSQL commits them together. This is the shape
  of an API under load.
- **Batches are an order of magnitude faster.** `enqueue_many()` writes a batch as one
  statement in one transaction. Use it (or `POST /v1/tasks/batch`) whenever you have more
  than a handful to add. Beyond 1,000 per batch there is nothing more to gain.
- **The wake-up notification costs about 15%** of single-row enqueue throughput. It is on
  by default because it is what makes pickup immediate (see
  [Pickup latency](#pickup-latency)).

## Throughput

Tasks a second through the whole path: claimed, run, result recorded. 20,000 tasks that do
nothing are enqueued first; then worker processes start together and empty the queue. The
rate is the task count divided by the time from the first task starting to the last one
finishing, read from the table. Every run also checks that all 20,000 succeeded and none
ran twice.

| Worker processes | Slots each | Settings | Tasks/s |
|---|---|---|---|
| 1 | 10 | defaults | 1,178 |
| 1 | 50 | defaults | 1,219 |
| 1 | 200 | defaults | 1,799 |
| 2 | 100 | defaults | 2,872 |
| 4 | 100 | defaults | 4,057 |
| 8 | 100 | defaults | 4,103 |
| 4 | 100 | `TQ_DB_POOL_PRE_PING=false` | 4,663 |
| 4 | 100 | `TQ_NOTIFY=false` | 4,019 |
| 4 | 100 | `TQ_DB_POOL_SIZE=30`, `TQ_DB_MAX_OVERFLOW=0` | 4,948 |
| 4 | 100 | PostgreSQL `synchronous_commit=off` | 3,994 |
| 8 | 100 | PostgreSQL `synchronous_commit=off` | 4,473 |

What to take from it:

- **One worker process moves about 1,200 no-op tasks a second**, and more slots help only a
  little. A process is one event loop on one core, and for a task that does nothing, the
  loop's own bookkeeping is the work.
- **More processes scale until about four**, then stop. Eight processes are no faster than
  four here. On this machine the workers, PostgreSQL and the benchmark share the cores, so
  this run cannot say whether the ceiling near 4,000 to 5,000 a second is the database or
  the machine.
- **The ceiling is not the disk**, on this setup. Turning `synchronous_commit` off changed
  nothing measurable. See the caveat about Docker Desktop below before relying on that.
- **Database round trips are what a task costs.** Recording a result is one transaction per
  task. Skipping the connection check before each use (`TQ_DB_POOL_PRE_PING=false`) gave
  15%, and giving each process 30 connections instead of the default 20 gave 22%, because
  with 100 slots and 20 connections, finished tasks wait for a connection to report on.
- **The notification is free on this path.** Workers that are already busy do not wait for
  one.

If throughput matters to you, in order of effect: run several worker processes; raise
`TQ_DB_POOL_SIZE` towards the number of slots that finish at the same moment, within
PostgreSQL's connection limit (see
[infrastructure.md](infrastructure.md#connections)); and consider
`TQ_DB_POOL_PRE_PING=false` if your network does not drop idle connections.

For scale: 4,000 tasks a second is 345 million a day. If your tasks take 100 ms or more,
you will run out of slots long before the queue's own overhead shows up, and the number
that matters to you is in [Concurrency in one process](#concurrency-in-one-process).

### Through PgBouncer

The same benchmark, run twice back to back: once with direct connections, once with every
worker connecting through PgBouncer 1.26 in transaction mode (`TQ_DB_PGBOUNCER=true`,
`default_pool_size=40`, a direct `LISTEN` connection per worker).

| Worker processes | Slots each | Settings | Direct, tasks/s | Through PgBouncer, tasks/s |
|---|---|---|---|---|
| 1 | 10 | defaults | 1,125 | 787 |
| 1 | 50 | defaults | 1,224 | 1,168 |
| 1 | 200 | defaults | 1,722 | 1,323 |
| 2 | 100 | defaults | 2,791 | 1,851 |
| 4 | 100 | defaults | 4,178 | 2,019 |
| 8 | 100 | defaults | 4,078 | 1,815 |
| 4 | 100 | `TQ_DB_POOL_PRE_PING=false` | 4,837 | 2,985 |

- **The ceiling roughly halved**, from about 4,100 to about 2,000 tasks a second. PgBouncer
  is one more hop for every statement and runs on a single thread. One worker process
  loses less, because the worker itself is then the limit.
- **The connection check costs more here**: turning it off gave 48%, against 16% direct.
  Through a pooler each check is a transaction of its own.
- Two things make this setup unkind to the pooler, so read the ratio and not the figures.
  The workers ran on the host and reached PgBouncer through Docker Desktop's port
  forwarding, and no-op tasks are all queue traffic. With tasks that do real work the
  database is not what limits a worker, and the pooler's cost shrinks with it.
- One worker logged one failed connection attempt to PgBouncer during the run and retried.
  All 20,000 tasks of every run succeeded, and none ran twice.

The direct column is a second run of the table above and differs from it by a few percent,
which is the run-to-run spread to keep in mind.

## Pickup latency

How long a task waits before it starts, when a worker is idle and has a free slot. 100
tasks are enqueued one at a time, 137 ms apart, and the time from `created_at` to
`started_at` is read from the table.

| How the worker finds out | p50 | p95 | max |
|---|---|---|---|
| `LISTEN`/`NOTIFY` (default) | 8.13 ms | 13.17 ms | 16.50 ms |
| Polling only, every 2 s (`TQ_NOTIFY=false`) | 1,026 ms | 2,059 ms | 2,229 ms |
| Polling only, every 0.25 s | 127 ms | 247 ms | 290 ms |

With notifications a task starts within about 10 ms of being enqueued. Without them, the
wait is uniform between zero and the poll interval, so half the interval on average.

Polling alone is always correct, and is what you get behind a transaction-mode connection
pooler unless workers are given a direct connection for `LISTEN`
([infrastructure.md](infrastructure.md#connection-poolers)). If you are in that position and latency matters,
lower `TQ_POLL_INTERVAL_SECONDS`; an idle worker's poll is one cheap indexed query per pool.

## Concurrency in one process

How many tasks a single worker process holds running at the same time. N tasks that each
sleep for 20 seconds are enqueued, and one worker starts with N slots. A task that sleeps
stands in for one that waits on a network call: it holds a slot and uses no CPU.

| Slots | Peak running | Time to peak | All done after | Memory idle | Memory at peak | Ran twice |
|---|---|---|---|---|---|---|
| 100 | 100 | 0.2 s | 20.3 s | 68 MB | 69 MB | 0 |
| 1,000 | 1,000 | 0.2 s | 20.6 s | 67 MB | 73 MB | 0 |
| 5,000 | 5,000 | 0.2 s | 22.9 s | 67 MB | 93 MB | 0 |
| 10,000 | 10,000 | 0.4 s | 25.6 s | 67 MB | 129 MB | 0 |

What to take from it:

- **One process ran 10,000 tasks at once**, in one event loop, with 20 database
  connections. Tasks do not hold a connection while they run.
- **A running task costs about 6 KB** of memory in the worker, on top of whatever the task
  itself allocates.
- **All 10,000 were claimed within half a second.** The extra 5.6 seconds at the end is the
  time to record 10,000 results, at the single-process rate from the section above.
- **Nothing ran twice.** At 10,000 running tasks the worker still heartbeats all of them on
  time, so none is mistaken for lost. A heartbeat is one statement for all of a worker's
  tasks, not one per task.

This is the reason to run I/O-bound work in an async worker: the limit on concurrent tasks
is what the tasks wait on (an API's rate limit, a database's connections), not the worker.

It applies to tasks that wait. A task that computes holds the event loop, or a thread, or a
core. For those, the number of slots that makes sense is close to the number of cores, and
you scale with processes. [workers.md](workers.md#sizing-slots) covers how to choose.

## Table size

What a large `tq_tasks` table costs. The table is filled directly with SQL, vacuumed and
analyzed, and then operations are timed one at a time against it: 200 claims of one task,
20 claims of 100, 200 single enqueues, and one call each of `stats()` and `list(limit=50)`.

Two shapes are measured. A **pending backlog** is that many tasks all waiting to run. A
**finished history** is that many completed tasks, spread evenly over 30 days, with 5,000
pending in front of them.

| Rows | Shape | Claim 1, p50 | Claim 1, p95 | Claim 100, p50 | Enqueue, p50 | `stats()` | `list(50)` | Table | Indexes |
|---|---|---|---|---|---|---|---|---|---|
| 10,000 | pending backlog | 1.90 ms | 3.38 ms | 6.09 ms | 2.52 ms | 5.3 ms | 4.7 ms | 3 MB | 2 MB |
| 10,000 | finished history | 2.18 ms | 2.84 ms | 5.32 ms | 2.00 ms | 4.0 ms | 3.3 ms | 4 MB | 3 MB |
| 100,000 | pending backlog | 2.17 ms | 2.70 ms | 5.56 ms | 2.31 ms | 19.7 ms | 3.8 ms | 23 MB | 19 MB |
| 100,000 | finished history | 2.22 ms | 2.66 ms | 5.33 ms | 2.62 ms | 4.9 ms | 5.8 ms | 27 MB | 18 MB |
| 1,000,000 | pending backlog | 1.85 ms | 3.51 ms | 5.67 ms | 2.72 ms | 70.8 ms | 8.3 ms | 224 MB | 182 MB |
| 1,000,000 | finished history | 2.25 ms | 3.10 ms | 5.85 ms | 2.60 ms | 5.5 ms | 3.7 ms | 254 MB | 165 MB |

What to take from it:

- **Claiming does not get slower as the table grows.** It takes 2 ms whether the table
  holds ten thousand rows or a million, and whether those rows are waiting or finished. A
  claim reads the front of an index that contains only pending tasks, in the order they
  will run; it never looks at the rest. [architecture.md](architecture.md#the-claim)
  explains the query.
- **Claiming 100 costs three times what claiming one does**, not a hundred times. Workers
  claim as many as they have free slots for in one statement.
- **Enqueueing does not get slower either.**
- **`stats()` scales with the backlog, not with the history.** It counts pending rows, so a
  million of them takes 71 ms. A million finished rows costs nothing, because the
  throughput figures read only the rows finished inside the window (the last 60 minutes by
  default).
- **A task costs about 430 bytes**, table and indexes together, with a payload of a few
  bytes. A million finished tasks is about 420 MB. Your payloads, outputs and log lines add
  to that.

Keeping every task forever is therefore a question of disk space, not of queue speed.
[analytics.md](analytics.md#retention-and-size) covers measuring and limiting it.

## What these numbers do not tell you

**They come from a laptop, not a server.** PostgreSQL ran inside Docker Desktop's virtual
machine, competing with the workers for the same cores. A dedicated database server will
give different numbers, most likely higher ones. Nothing here was measured on Linux
hardware.

**They say little about your disk.** Docker Desktop's virtual disk does not necessarily
push each commit to physical storage the way a server's does. The finding that
`synchronous_commit=off` makes no difference should be re-measured on your storage before
you believe it. Do not turn `synchronous_commit` off for a queue in any case unless losing
the last moments of acknowledged tasks in a crash is acceptable.

**The network was a loopback.** Every task costs a few round trips to the database. Here a
round trip took a fraction of a millisecond. With workers in another availability zone it
may take a millisecond or more, and throughput per process falls accordingly. Concurrency
and the flat claim time are unaffected.

**The tasks did nothing, and their payloads were a few bytes.** Real payloads and outputs
are written and read as `jsonb`; a 100 KB payload costs what writing 100 KB costs.

**The largest table was a million rows.** The design reason claims stay flat applies at any
size, but ten million and a hundred million rows were not measured. At those sizes
autovacuum tuning and index bloat over weeks of churn matter, and a ten-minute benchmark
cannot show them.

**Each run lasted seconds, not days.** No soak test was done.

**One configuration was tested:** PostgreSQL 18, Python 3.13, direct connections, plus the
one throughput run through PgBouncer above. No replica, no other PostgreSQL version.

**Logging was almost idle.** The no-op tasks write no log lines. Tasks that log heavily
add batched inserts into `tq_task_logs`.

If a number here matters to a decision you are making, run the benchmark on the hardware
you will use. It takes about ten minutes.

## Running them yourself

The benchmarks need the development environment and a PostgreSQL they can reach:

```bash
uv sync --all-extras
docker compose up -d db
```

They run against a database of their own, named after the configured one with `_bench`
appended (`task_queue_bench`), which they create, migrate and **empty**. They refuse to run
against a database whose name does not end in `_bench`. To point them elsewhere:

```bash
export TQ_BENCH_DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/anything_bench
```

Then:

```bash
uv run python -m benchmarks all                 # everything, about ten minutes
uv run python -m benchmarks throughput          # one of them
uv run python -m benchmarks all --json benchmarks/results/mine.json
```

| Benchmark | Measures | Options |
|---|---|---|
| `enqueue` | Writes a second: single, concurrent, batched | `--tasks` |
| `throughput` | End-to-end tasks a second by processes × slots | `--tasks` (default 20,000) |
| `latency` | Enqueued → started on an idle worker | `--samples` (default 100) |
| `concurrency` | Tasks running at once in one process | `--slots 100,1000,5000,10000`, `--seconds 20` |
| `depth` | Timings by table size | `--depths 10000,100000,1000000`, `--repeats 200` |

Each prints its tables as Markdown, preceded by the environment it ran in.

To get numbers you can compare:

- Stop everything else that uses the database, including the compose workers
  (`docker compose stop worker worker-image api`).
- The throughput benchmark starts up to 8 worker processes with 20 connections each.
  PostgreSQL's `max_connections` must allow for that; the compose file sets 200.
- To benchmark a remote PostgreSQL, run the script from a machine next to the workers'
  intended location, so that the network between them is the real one.

The worker processes the benchmarks start are ordinary `tq worker` processes over the two
executors in [benchmarks/executors.py](../benchmarks/executors.py). Nothing is mocked.
