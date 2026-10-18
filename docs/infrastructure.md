# Infrastructure

What runs, how it is built, what PostgreSQL needs, and how to operate it.

- [What runs](#what-runs)
- [The compose project](#the-compose-project)
- [Images](#images)
- [Adding a worker for a new kind of work](#adding-a-worker-for-a-new-kind-of-work)
- [Postgres](#postgres)
- [Connections](#connections)
- [Using a database you already have](#using-a-database-you-already-have)
- [Deploying and upgrading](#deploying-and-upgrading)
- [Scaling](#scaling)
- [Health and monitoring](#health-and-monitoring)
- [Backups and restores](#backups-and-restores)
- [Connection poolers](#connection-poolers)
- [Beyond compose](#beyond-compose)
- [Production checklist](#production-checklist)

## What runs

```
                      ┌──────────────────────────────┐
   HTTP clients ────► │ api                          │
   MCP clients        │ FastAPI: REST, JWT, MCP      │──┐
                      │ no executor code             │  │
                      └──────────────────────────────┘  │
                                                        ▼
   your code ── TaskQueue.enqueue() ──────────►  ┌─────────────┐
                                                 │ PostgreSQL  │
                      ┌────────────────────┐     │ tq_* tables │
                      │ worker             │◄───►│             │
                      │ executors A, B     │     └─────────────┘
                      └────────────────────┘            ▲
                      ┌────────────────────┐            │
                      │ worker-image       │◄───────────┘
                      │ executors C + deps │
                      └────────────────────┘
```

PostgreSQL is the only stateful part and the only thing the processes share. There is no
broker and no scheduler process. The API and the workers never talk to each other; each
talks to the database. The API may also use a Redis for its
[request rate limits](auth.md#request-rate-limits); it holds counters only, and the
workers never touch it.

| Process | Needs | State |
|---|---|---|
| PostgreSQL | A disk | Everything |
| `tq api` | The database, the JWT key; Redis for rate limits, optionally | None. Run as many as you like. |
| Redis | Memory | Rate-limit counters, which may be lost |
| `tq worker` | The database, its executors' code and dependencies | None. Run as many as you like. |
| `tq migrate` | The database, with permission to create tables | Runs once per deploy and exits. |

Workers also do the housekeeping (making delayed tasks ready, recovering tasks from dead
workers, deleting old ones, delivering webhooks), taking turns through advisory locks.
**At least one worker must be running for those to happen.** The API is optional: a system
that enqueues from Python and never uses HTTP does not need it.

## The compose project

[compose.yml](../compose.yml) runs all of it on one host.

```bash
cp .env.example .env
# set TQ_JWT_SECRET (openssl rand -hex 32) and TQ_BOOTSTRAP_ADMIN_PASSWORD
docker compose up -d --build
```

| Service | Image | Does |
|---|---|---|
| `db` | `postgres:18` | The database. Data in the `db-data` volume. Published on `127.0.0.1:54320`. |
| `redis` | `redis:8` | The API's rate-limit counters. No persistence, 64 MB, least recently used evicted. Published on `127.0.0.1:63790` for the tests. |
| `migrate` | `pgtaskqueue-api` | Runs `tq migrate`, creates the first admin, exits. Everything else waits for it. |
| `api` | `pgtaskqueue-api` | The HTTP API and MCP server. Published on `127.0.0.1:8010`. |
| `worker` | `pgtaskqueue-worker` | The general example tasks, 20 at a time. |
| `worker-image` | `pgtaskqueue-worker-image` | The image tasks, 2 at a time, with Pillow installed. |

Things in it that are deliberate:

- **Ports are bound to 127.0.0.1.** Nothing is reachable from other machines until you put
  a reverse proxy with TLS in front of the API. Do not publish the database port on a
  server.
- **Each service gets only the variables it needs**, listed under `environment:`. The
  workers never receive the JWT secret; the API never receives the webhook secret.
  Consequently, adding a line to `.env` is not enough to set a variable a service does not
  already list. Add it to the service in `compose.override.yml`.
- **`migrate` gates everything.** `api` and the workers start only after it has exited
  successfully, so no process sees a half-migrated schema.
- **Workers have `restart: unless-stopped` and exit when idle** for 15 minutes
  (`TQ_WORKER_IDLE_EXIT_SECONDS=900`), so they are replaced by fresh processes.
  [workers.md](workers.md#stopping-and-why-workers-exit-on-purpose) explains why.
- **The API starts without waiting for Redis to be healthy**, and runs without it: the rate
  limits are skipped while it is down. Losing its data resets the counters and nothing
  else, so it keeps none on disk.
- **`stop_grace_period: 45s`** is longer than the worker's own 30-second grace period, so
  Docker does not kill a worker while it is handing its tasks back.

Local changes go in `compose.override.yml`, which compose merges automatically and git
ignores. [compose.override.example.yml](../compose.override.example.yml) has the common
ones: live code reload, extra settings, more workers.

Everyday commands:

```bash
docker compose ps
docker compose logs -f worker
docker compose exec api tq stats
docker compose exec api tq user create alice --role operator --generate
docker compose stop worker            # stays stopped
docker compose up -d --build worker   # rebuild and replace after a code change
docker compose down                   # stop everything; keeps the data
docker compose down -v                # … and delete the data
```

## Images

One [Dockerfile](../docker/Dockerfile) builds every image. Two build arguments choose what
goes in:

| Build argument | Result |
|---|---|
| `EXTRAS=api` | The API image: the core plus FastAPI, PyJWT, argon2, Jinja2 and the MCP SDK. Serves the [web UI](ui.md) too. |
| neither | A worker image with the core only: SQLAlchemy, asyncpg, Alembic, Pydantic, httpx. |
| `REQUIREMENTS=path/to/requirements.txt` | A worker image with that file installed on top. |

The split is the point. A worker that renders images needs Pillow; one that transcribes
audio needs a gigabyte of model libraries; the API needs neither, and should not be rebuilt
when either changes. Because the API never imports executor code (it reads task names and
payload schemas from the `tq_task_types` table, which workers fill in when they start),
each kind of work can have its own image with its own dependencies, even ones that conflict
with each other.

The Dockerfile installs dependencies before copying the source, so editing code rebuilds
only the last layers. The processes run as an unprivileged user (uid 10001).

`PYTHON_IMAGE` is a third argument, defaulting to `python:3.13-slim`, for a different
Python version or a base image with system libraries your executors need.

## Adding a worker for a new kind of work

1. **Write the executors** in a module. See [executors.md](executors.md).

   ```python
   # myapp/tasks/audio.py
   from pgtaskqueue import TaskContext, task


   @task("audio.transcribe", timeout=1800, max_attempts=2)
   async def transcribe(ctx: TaskContext, payload: dict) -> dict: ...
   ```

2. **List what they need** in a requirements file of their own.

   ```
   # myapp/workers/audio/requirements.txt
   faster-whisper>=1.1
   ```

3. **Add a service**, copying `worker-image`:

   ```yaml
   worker-audio:
     <<: *worker
     build:
       context: .
       dockerfile: docker/Dockerfile
       args:
         REQUIREMENTS: myapp/workers/audio/requirements.txt
     image: myapp-worker-audio
     command: ["tq", "worker", "--executors", "myapp.tasks.audio", "--tasks", "audio.*:1"]
     environment:
       <<: *env
       TQ_WEBHOOK_SLOTS: 0
   ```

4. `docker compose up -d --build worker-audio`. The new task names appear in
   `GET /v1/task-types` once it has started, and tasks enqueued under them before that have
   been waiting as `pending`.

The Dockerfile here copies `examples/` because that is where this repository's executors
live. In your own project, copy your package instead, or start from your own Dockerfile:
anything that installs `pgtaskqueue` and your code, and runs `tq worker`, is a worker.

A worker that needs a GPU, a different base image, or a different machine is the same
thing with a different `build:` or `deploy:` section. Workers on other hosts need only a
route to the database.

## Postgres

Developed and tested on **PostgreSQL 18**. Nothing in the design is known to require 18
(it relies on `FOR UPDATE SKIP LOCKED`, `LATERAL`, partial indexes, `jsonb` and
`LISTEN`/`NOTIFY`, all of which are much older), but no earlier version has been tested.

The compose file starts PostgreSQL with settings sized for a laptop:

| Setting | In compose | For a server |
|---|---|---|
| `max_connections` | `200` | See [Connections](#connections). |
| `shared_buffers` | `512MB` | About a quarter of the machine's memory. |
| `effective_cache_size` | `1GB` | About three quarters of it. |
| `work_mem` | `16MB` | Fine as it is for the queue. Raise it for heavy analytics queries. |
| `maintenance_work_mem` | `256MB` | 1GB or more on a large table, so vacuum is quick. |
| `wal_compression` | `on` | Keep. |
| `max_wal_size` | `4GB` | Raise if checkpoints are frequent under load. |
| `checkpoint_timeout` | `15min` | Keep. |
| `autovacuum_naptime` | `15s` | Keep. A queue table changes constantly. |

A queue table is unusual: every row is inserted, updated a few times in quick succession,
and then never touched again. The migration sets storage options on `tq_tasks` for that:

```sql
ALTER TABLE tq_tasks SET (
    fillfactor = 85,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_cost_limit = 2000
);
```

Free space on each page lets heartbeats and progress updates happen without touching any
index, and vacuum runs after 2% of the table has changed instead of the default 20%. You do
not need to set these yourself.

What keeps a queue in PostgreSQL healthy is vacuum being able to do its work. The one thing
that stops it is a **long-running transaction** anywhere in the same database: a forgotten
`psql` session inside `BEGIN`, a long report, an abandoned replication slot. Set
`idle_in_transaction_session_timeout` (a minute is reasonable) on the database or on the
roles that people and reporting tools use. [analytics.md](analytics.md#keeping-analytics-from-hurting-the-queue)
has more on running reports safely.

A managed PostgreSQL (RDS, Cloud SQL, Azure, Supabase, Neon and the like) needs nothing
special over a direct connection. Through a pooled endpoint, see
[Connection poolers](#connection-poolers).

## Connections

Every process holds a small pool. A connection is used for the length of one queue
operation, not for the length of a task, so the pool does not grow with the number of
slots.

| Process | Connections at most |
|---|---|
| `tq worker` | `TQ_DB_POOL_SIZE` + `TQ_DB_MAX_OVERFLOW` + 1 for `LISTEN` = **21** by default |
| `tq api` | (`TQ_DB_POOL_SIZE` + `TQ_DB_MAX_OVERFLOW`) × `--workers` = **20** by default |
| `tq` commands | 1 or 2, briefly |

The pool starts empty and grows on demand, and the overflow connections are closed again
when the load passes, so these are ceilings, not what an idle process holds.

PostgreSQL's `max_connections` must cover the sum, with room left for you:

```
max_connections  ≥  workers × 21  +  API processes × 20  +  10 for psql, migrations, monitoring
```

The compose project is 2 workers and 1 API process: 62 of the 200 allowed. Eight workers
would be 188, which is the point to either lower `TQ_DB_MAX_OVERFLOW` or raise
`max_connections`.

Two adjustments, in opposite directions:

- **Many small workers** (say fifty workers of 2 slots each): set `TQ_DB_POOL_SIZE=3` and
  `TQ_DB_MAX_OVERFLOW=2` on them. A worker that runs two tasks at a time does not need
  twenty connections.
- **A worker with hundreds of slots and short tasks**: raise `TQ_DB_POOL_SIZE` towards the
  number of tasks that finish in the same instant. The
  [benchmark](benchmarks.md#throughput) gained 22% going from 20 to 30 with 100 slots.

Each PostgreSQL connection is a process on the server and costs a few megabytes. Past a few
hundred, put [PgBouncer](#connection-poolers) in front.

Connections identify themselves, which makes `pg_stat_activity` readable:

```sql
SELECT application_name, state, count(*)
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY 1, 2
ORDER BY 1, 2;
```

Workers appear as `tq-worker <id>` and `tq-listen <id>`, the API as `tq-api`, and commands
as `tq-cli`.

## Using a database you already have

The queue does not need a database of its own. Its seven tables are all prefixed `tq_`, and
its migration history lives in `tq_alembic_version`, so it does not collide with your
application's tables or with your own Alembic history.

Point `TQ_DATABASE_URL` at your application's database and run `tq migrate`.

That is the arrangement to prefer, because it buys two things a separate database cannot:

- **Enqueueing inside your own transaction.** Pass your session to `enqueue(session=…)`
  and the task is committed, or rolled back, together with the rows that caused it.
- **Joins.** Tasks can be joined to your tables in reports
  ([analytics.md](analytics.md#joining-with-your-own-data)).

The role that runs `tq migrate` needs to create tables in the schema. The role the API and
workers use needs only to read and write them:

```sql
GRANT USAGE ON SCHEMA public TO tq_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON
    tq_tasks, tq_task_logs, tq_task_types, tq_workers, tq_queues, tq_users TO tq_app;
```

## Deploying and upgrading

The order is always the same:

1. `tq migrate`
2. Replace the API and the workers.

The compose file enforces it with `depends_on`. Elsewhere, run the migration as a release
step or a one-off job before rolling out the new processes.

`tq migrate` does nothing when the schema is already current, and is safe to start from
several places at once: the runs take turns on an advisory lock. The lock is held on a
connection of its own, not the one migrating, because a migration that builds an index
with `CREATE INDEX CONCURRENTLY` commits partway through. Run it on every deploy.

**Upgrading to the account limits (migration `0003`).** It builds three indexes on
`tq_tasks` concurrently, which takes a while on a large table but blocks no claim. Until
every worker runs the new code, an old worker's promote loop does not know about held
tasks and releases them as soon as they are due, so a running limit is not kept during the
rollout. Set limits after it.

**Replacing workers loses no work.** A worker that receives SIGTERM stops claiming, gives
its running tasks 30 seconds (`TQ_WORKER_SHUTDOWN_GRACE_SECONDS`) to finish, and hands back
whatever is still running. Those tasks are picked up by another worker without using up one
of their attempts. A task that takes longer than the grace period is therefore restarted
from the beginning, or from its last completed [step](executors.md#steps-resuming-instead-of-repeating),
on every deploy. Give long tasks steps, or a longer grace period with a matching
`stop_grace_period`.

**A task type's code changes under tasks already queued.** A task enqueued before a deploy
may run on the new code, and during a rolling deploy a retry may run on a different version
than the first attempt. Keep payload changes backward compatible for one release: add
fields with defaults, and remove a field only after no queued task carries it.

**Downgrading** the schema is `tq migrate --revision <rev>`. Take a backup first; a
downgrade drops what the newer revision added. Below `0003`, accounts lose their limits,
`manager` accounts become `operator`, and held tasks become ordinary scheduled tasks.

## Scaling

**More throughput for one kind of work:** run more copies of its worker.

```bash
docker compose up -d --scale worker=4
```

Workers coordinate through the database and nothing else, so copies need no configuration,
on one host or on many. Each claims different tasks; none waits on another.

**More concurrency for I/O-bound work:** raise the slots instead. One process can hold
thousands of tasks that are waiting on the network
([benchmarks.md](benchmarks.md#concurrency-in-one-process)). More processes help only when
the work uses CPU, or when one event loop is saturated.

**CPU-bound work:** slots close to the number of cores the container has, and more
containers for more. [workers.md](workers.md#sizing-slots) goes through the choice.

**The API** is stateless. Run several behind a load balancer, or `tq api --workers N` for
several processes in one container. Use `/health/ready` as the health check.

**The database** is the part that scales up rather than out. One PostgreSQL moved about
4,000 no-op tasks a second on a laptop, which is 345 million a day; most workloads are
limited by what their tasks do long before that. If you do reach it: batch your enqueues,
raise the pool size, move reporting to a replica, and only then consider splitting
unrelated workloads into separate databases.

**Isolation between workloads** is what queues are for. A worker consumes one queue
(`--queue`). Put work that must not be delayed by a backlog elsewhere in a queue of its
own, with its own workers. Queues can be paused individually (`tq pause QUEUE`).

## Health and monitoring

**The API** has two endpoints that need no token:

| Endpoint | Says |
|---|---|
| `GET /health/live` | The process is up. For a liveness probe. |
| `GET /health/ready` | The database answers. For a load balancer or readiness probe. Returns 503 otherwise. |

**Workers** have no HTTP port. A worker is healthy if its row in `tq_workers` has a recent
heartbeat, which `tq workers` and `GET /v1/workers` show. The restart policy handles a
worker that has exited; a worker that is hung stops heartbeating, and its tasks are
recovered by the others after `TQ_STALE_AFTER_SECONDS`.

**The queue.** Three numbers are worth an alert:

| Alert when | Means | Get it from |
|---|---|---|
| The oldest pending task is older than you can accept | Not enough workers, or none for that task name, or the queue is paused | `oldest_pending_seconds` in `tq stats --json` or `GET /v1/stats` |
| The failure rate rises | A dependency is down, or a deploy broke a task | `window.failed` against `window.succeeded`, per task name |
| No worker is alive | Nothing is being run, and delayed tasks are not being made ready | `GET /v1/workers` returns none |

```bash
docker compose exec api tq stats --json | jq '.totals'
```

The [web UI](ui.md) shows the same numbers. For dashboards of your own, query the tables:
[analytics.md](analytics.md).

**Logs.** Every process logs to standard output, one line per event, at `INFO` by default
(`tq --log-level DEBUG worker …` for more). What a *task* logs is stored in the database
with the task (`tq show ID --logs`), so a task's history does not depend on your log
pipeline's retention.

## Backups and restores

The queue is ordinary tables. Whatever backs up the database backs up the queue: `pg_dump`,
base backups with WAL archiving, or your provider's snapshots. There is nothing else to
save.

```bash
docker compose exec db pg_dump -U tq -Fc pgtaskqueue > pgtaskqueue.dump
```

After a restore, the queue is as it was at the moment of the backup, which has two
consequences:

- **Tasks that were `running` then are run again.** No worker is heartbeating for them, so
  after `TQ_STALE_AFTER_SECONDS` they are recovered and retried.
- **Tasks that were waiting then are waiting again**, including the ones that went on to
  run and finish after the backup was taken. Those run a second time.
- **Tasks enqueued after the backup are gone.**

This is the at-least-once guarantee meeting a time machine. It is why executors should be
[idempotent](executors.md#idempotency). If a restore would repeat something that must not
be repeated, pause the queues before starting workers (`tq pause default`), inspect what is
pending, and cancel what should not run.

A streaming replica gives you a standby to fail over to and a place to run reports. After a
failover, workers reconnect by themselves (with `TQ_DB_POOL_PRE_PING` on, which is the
default), and the same caveat applies to anything the replica had not yet received.

## Connection poolers

PgBouncer in **transaction** mode is supported. You do not need it for a handful of
processes, since each already holds a small pool. It earns its place when the processes
number in the hundreds, or when your platform only offers a pooled endpoint.

Two settings:

| Variable | Set it on | What it does |
|---|---|---|
| `TQ_DB_PGBOUNCER=true` | Every process whose `TQ_DATABASE_URL` points at the pooler: API, workers, `tq` commands | Stops the driver caching prepared statements, which do not survive a connection that changes server between transactions. Without it, statements fail with `prepared statement … does not exist` or `already exists`. |
| `TQ_LISTEN_DATABASE_URL=<direct URL>` | Workers | A direct URL to the same database, used for the one `LISTEN` connection each worker keeps. Optional. |

`LISTEN` needs a session that stays on one server connection, which transaction pooling
does not give. So a worker behind the pooler does one of two things:

- **With `TQ_LISTEN_DATABASE_URL`**: it listens on that one direct connection and does
  everything else through the pooler. Pickup stays at milliseconds. Cost: one direct
  PostgreSQL connection per worker process.
- **Without it**: it does not listen, says so once in its log, and finds tasks by polling
  every `TQ_POLL_INTERVAL_SECONDS`. A cancel reaches a running task at its next heartbeat.
  This costs latency ([measured](benchmarks.md#pickup-latency)) and nothing else. If no
  worker listens, `TQ_NOTIFY=false` on every process saves the unheard notifications.

Everything else is compatible as it stands: row locks and advisory locks are
transaction-scoped, never session-scoped, and no session state is set after connecting.

### Trying it with compose

[compose.pgbouncer.yml](../compose.pgbouncer.yml) adds a `pgbouncer` service and points the
API and both workers at it, with a direct `LISTEN` connection for each worker:

```bash
docker compose -f compose.yml -f compose.pgbouncer.yml up -d --build
docker compose -f compose.yml -f compose.pgbouncer.yml exec worker \
  tq enqueue demo.echo '{"message": "via pgbouncer"}' --wait
```

The `migrate` service keeps its direct connection. `tq migrate` does work through the
pooler, but a migration is one process running once, so there is nothing to pool, and a
direct connection keeps it clear of the pooler's timeouts.

### Sizing

- `max_client_conn` must cover every process's pool: the sums in [Connections](#connections),
  less the `LISTEN` connections that go direct.
- `default_pool_size` is how many server connections PgBouncer opens for the database. It
  is the number of queue operations that can be in flight at once; the rest wait their
  turn inside PgBouncer. Start at 20 to 40 and raise it if `SHOW POOLS` shows clients
  waiting.
- PostgreSQL's `max_connections` then needs `default_pool_size`, plus one per worker that
  has a direct `LISTEN` connection, plus what migrations and people use.
- `TQ_DB_POOL_SIZE` and `TQ_DB_MAX_OVERFLOW` keep their meaning: how many queue operations
  one process runs at once. The pooler does not change what a process needs.
- A pooler is not free. In [our benchmark](benchmarks.md#through-pgbouncer) the throughput
  ceiling for no-op tasks roughly halved behind it, and `TQ_DB_POOL_PRE_PING=false` won a
  good part of that back.

In `pg_stat_activity`, a pooled server connection shows the `application_name` of the
client that used it last, so `tq-worker <id>` rows no longer map one-to-one to workers.

### What was tested

- PgBouncer 1.26 in transaction mode in front of PostgreSQL 18. The whole test suite passes
  through it, with PgBouncer's own prepared-statement support on (`max_prepared_statements`
  at its default) and off (`0`), with and without `TQ_LISTEN_DATABASE_URL`.
- The compose overlay above, running the example tasks.
- Throughput through it: [benchmarks.md](benchmarks.md#through-pgbouncer).

**Not tested:** other poolers (PgCat, Odyssey, Supavisor, RDS Proxy), other PgBouncer
versions, statement mode, and TLS between the processes and the pooler. Session mode should
need neither setting, and was not tested either.

To run the tests through a pooler yourself:

```bash
docker compose -f compose.yml -f compose.pgbouncer.yml up -d db pgbouncer
TQ_TEST_DATABASE_URL=postgresql+asyncpg://tq:tq@localhost:64320/task_queue_test \
TQ_LISTEN_DATABASE_URL=postgresql+asyncpg://tq:tq@localhost:54320/task_queue_test \
TQ_DB_PGBOUNCER=true uv run pytest -q
```

## Beyond compose

Nothing in the design depends on Docker Compose. The pieces map directly onto any
orchestrator. These mappings have not been tested in this repository.

| Compose | Kubernetes | systemd |
|---|---|---|
| `migrate` service | A Job, or an init container, run before the rollout | `ExecStartPre=tq migrate` on one unit |
| `api` service | A Deployment and a Service; readiness probe on `/health/ready` | A unit running `tq api` |
| Each worker service | A Deployment per kind of work | A unit, or a template unit `tq-worker@.service` |
| `restart: unless-stopped` | `restartPolicy: Always` (the default for a Deployment) | `Restart=always` |
| `stop_grace_period: 45s` | `terminationGracePeriodSeconds: 45` | `TimeoutStopSec=45` |
| `--scale worker=4` | `replicas: 4` | Four instances of the template |

Two notes for Kubernetes:

- A worker that exits on purpose is restarted by the kubelet, as intended. Exits that come
  less than about ten minutes apart are delayed with an increasing back-off, as crashes
  are, even though the exit status is 0. Keep `TQ_WORKER_IDLE_EXIT_SECONDS` above ten
  minutes and `TQ_WORKER_MAX_TASKS` high enough that a worker lives at least that long.
- To scale workers on queue depth, the number to scale on is the count of pending tasks for
  the names that worker runs, which is one SQL query
  ([analytics.md](analytics.md#right-now)).

## Production checklist

- [ ] `TQ_JWT_SECRET` is generated (`openssl rand -hex 32`), not copied from an example.
- [ ] The bootstrap admin password is changed, or the account replaced, after first login.
- [ ] The API is behind TLS. Tokens are bearer credentials.
- [ ] The database port is not published. The API port is published only to the proxy.
- [ ] The database password is not `tq`.
- [ ] `max_connections` covers every process ([Connections](#connections)).
- [ ] `idle_in_transaction_session_timeout` is set.
- [ ] Backups run, and a restore has been tried.
- [ ] At least one worker is always running, and something alerts when none is.
- [ ] Something alerts on the age of the oldest pending task.
- [ ] `stop_grace_period` is longer than `TQ_WORKER_SHUTDOWN_GRACE_SECONDS`.
- [ ] `TQ_WEBHOOK_ALLOW_PRIVATE` is off, unless every account that can enqueue is trusted.
- [ ] `TQ_MCP_ALLOWED_HOSTS` and `TQ_CORS_ORIGINS` name your hosts, if you use them.
- [ ] `TQ_REDIS_URL` is set, so sign-ins are [rate-limited](auth.md#request-rate-limits),
      and `FORWARDED_ALLOW_IPS` names your proxy, so the limits see client addresses.
- [ ] `TQ_RETENTION_DAYS` is a decision, not an accident: forever is the default.
      [`TQ_ARCHIVE_DATABASE_URL`](analytics.md#the-archive) keeps the history and moves it
      off the queue's table.
- [ ] Executors are idempotent, or you know which ones are not.

The security side of this list is expanded in [auth.md](auth.md#hardening-checklist) and
[SECURITY.md](../SECURITY.md).
