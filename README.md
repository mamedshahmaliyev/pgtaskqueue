# pgtaskqueue

A task queue for Python that lives in the PostgreSQL you already run.

Tasks are rows. Workers claim them with `FOR UPDATE SKIP LOCKED` and run them with
`asyncio`. There is no broker, no result backend and no scheduler process: one database,
your workers, and optionally an HTTP API.

To try it without installing anything, use the [public demo](#try-the-demo).

```python
from pgtaskqueue import TaskContext, task


@task("report.build", max_attempts=5, timeout=300)
async def build_report(ctx: TaskContext, payload: dict) -> dict:
    ctx.log(f"building for {payload['tenant']}")
    # A step's result is saved on the task: a retry resumes after it, not before it.
    path = await ctx.step("extract", lambda: extract_to_file(payload["tenant"]))
    count = await ctx.step("load", lambda: load(path))
    return {"rows": count}
```

```python
await queue.enqueue("report.build", {"tenant": "acme"}, metadata={"tenant": "acme"})
```

```bash
tq worker --executors myapp.tasks --slots 50
```

```sql
SELECT metadata->>'tenant' AS tenant, count(*), avg(spent_seconds)
FROM tq_tasks WHERE name = 'report.build' AND completed_at > now() - interval '7 days'
GROUP BY 1;
```

## Try the demo

A running copy, with the web UI, the HTTP API, the MCP server and two workers:

| | |
|---|---|
| Web UI | <https://pgtaskqueue.mmdsh.org> |
| API reference | <https://pgtaskqueue.mmdsh.org/docs> |
| MCP server | `https://pgtaskqueue.mmdsh.org/mcp` |

Sign in with one of these accounts. The password for all of them is **`demo-password`**.

| Account | Role | What it can do |
|---|---|---|
| `demo-manager` | manager | What an operator can, and manage accounts no wider than its own |
| `demo-operator` | operator | See and manage every task, pause queues, manage schedules |
| `demo-submitter` | submitter | Enqueue tasks, and see and manage only its own |
| `demo-viewer` | viewer | See everything, change nothing |

From a terminal:

```bash
URL=https://pgtaskqueue.mmdsh.org
TOKEN=$(curl -s $URL/v1/auth/token -d grant_type=password \
  -d username=demo-submitter -d password=demo-password \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["access_token"])')

curl -s $URL/v1/tasks -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name": "demo.flaky", "payload": {"fail_times": 1}}'
```

Things to know before you try it:

- **It is public and shared.** Everyone uses the same accounts and sees the same tasks. Put
  nothing private in a payload.
- **It is reset every hour.** Your tasks, schedules and accounts are deleted, and the
  demo data is seeded again.
- **It is limited.** Each account may have only a few tasks waiting and running, and enqueue
  only a few a minute. A **429** means wait a minute.
- **It is not a benchmark.** It runs on a small shared server. Please do not load-test it;
  run your own copy with the [quick start](#quick-start) instead.

[docs/demo.md](docs/demo.md) has the limits, the task names you can run, what to try in each
role, webhooks and MCP.

## Why keep the queue in PostgreSQL

The point is **manageability, control and analytics**, not speed.

- **You can see everything.** Each task is a row that stays: payload, status, attempts,
  timings, output, error with traceback, the worker that ran it, who enqueued it. Each log
  line is a row. Questions about your background work are SQL queries, joined to your own
  tables.
- **You can change things while they run.** Reprioritise, reschedule or edit a waiting
  task. Cancel a running one and have it stop. Pause a queue. Requeue last night's failures
  once the cause is fixed.
- **Tasks commit with your data.** Enqueue in the same transaction as the business write,
  and either both happen or neither does. No outbox, no lost message.
- **Nothing new to operate.** No broker to size, secure, monitor and back up. Your
  PostgreSQL backups already cover the queue.

What it costs is raw throughput: about 4,000 tasks a second end to end on a laptop, where a
Redis-backed queue does far more. If your tasks do real work, you will not notice. If you
need to move tens of thousands of tiny messages a second, use a broker.
[docs/comparison.md](docs/comparison.md) is an honest account of when to choose something
else, including a list of what this project does not have.

## What is in it

**The queue**

- Atomic claims with `SKIP LOCKED` over a partial index: claim time stays at 2 ms from ten
  thousand rows to a million
- Priorities, delays and scheduled times, deduplication keys, batch enqueue
- Periodic tasks: a cron expression with a timezone, or a fixed interval, declared next
  to the task or created through the API. No scheduler process: workers fire them
- Retries with exponential backoff and jitter; `Retry`, `Snooze` and `NonRetryable` to
  decide per failure; per-task timeouts
- Heartbeats, and recovery of tasks whose worker died
- Cancellation that reaches a running task
- At-least-once delivery, with fencing so a worker that was presumed dead cannot overwrite
  the task's next attempt

**Workers**

- One `asyncio` event loop per process, structured with `TaskGroup`; thousands of
  concurrent I/O-bound tasks per process
- Slots and pools: `--tasks 'image.*:2,ALL:20'` gives each kind of work its own limit
- Plain `def` executors run in a thread, which is stopped when the task is canceled
- Each kind of work can have its own image and its own `requirements.txt`; the API never
  imports task code
- Graceful shutdown that hands unfinished tasks back
- Workers that exit when idle (or after N tasks) and are restarted fresh by
  `restart: unless-stopped`
- A small HTTP protocol for workers that cannot reach the database, in any language

**Per task**

- Logs stored with the task, including what libraries log while it runs
- Progress, and named steps whose results survive a retry
- Child tasks
- Signed webhook callbacks when it finishes, delivered with retries

**API and access**

- FastAPI REST API with OpenAPI docs, long-polling for results
- OAuth2 token endpoint, JWT access and refresh tokens, HS256 or key pairs, or your own
  identity provider
- Five roles, plus per-account ACLs on queue names and task names
- An MCP server at `/mcp`, so an AI assistant can use the queue with the same permissions
- A web UI at `/ui`: a dashboard, the task list with filters, enqueue, cancel, kill and
  restart, one task with its live log, and statistics. No build step

**Operations**

- `tq` command line: migrate, run, enqueue, inspect, cancel, requeue, pause, schedule,
  stats, and `tq seed` for demonstration data
- Docker Compose project: PostgreSQL, migrations, API, two example workers
- Works behind PgBouncer in transaction mode
- Alembic migrations that coexist with your application's own
- Benchmarks you can re-run

## Quick start

You need Docker. This starts PostgreSQL, the API and two workers with example tasks.

```bash
git clone https://github.com/mamedshahmaliyev/pgtaskqueue.git
cd pgtaskqueue
cp .env.example .env
```

Edit `.env` and set the two required values:

```bash
TQ_JWT_SECRET=<output of: openssl rand -hex 32>
TQ_BOOTSTRAP_ADMIN_PASSWORD=<a password for the first admin account>
```

Start it:

```bash
docker compose up -d --build
docker compose ps
```

Run a task and wait for its result:

```bash
docker compose exec worker tq enqueue demo.echo '{"message": "hello"}' --wait
```

```json
{
  "status": "succeeded",
  "output": {
    "attempt": 1,
    "message": "hello"
  },
  "error": null
}
```

Run one that fails halfway, is retried, and resumes at the step that failed:

```bash
docker compose exec worker tq enqueue report.build '{"rows": 500, "fail_once_at": "transform"}' --wait
docker compose exec worker tq tasks --name report.build --limit 1     # copy the id
docker compose exec worker tq show <id> --logs
```

```
08:39:28.761393 #1 info    [extract] step 'extract': started
08:39:28.966322 #1 info    [extract] extracted 500 rows
08:39:28.970236 #1 info    [extract] step 'extract': done
08:39:28.972667 #1 info    [transform] step 'transform': started
08:39:28.973673 #1 warning [transform] attempt 1/3 failed: RuntimeError: transform failed on purpose (attempt 1) — retrying in 1.0s
08:39:30.076241 #2 info    [transform] step 'extract': reusing the result of an earlier attempt
08:39:30.078463 #2 info    [transform] step 'transform': started
08:39:30.081190 #2 info    [transform] step 'transform': done
08:39:30.082206 #2 info    [load] step 'load': started
08:39:30.198403 #2 info    [load] step 'load': done
```

A task that only the second worker can run, because only its image has Pillow:

```bash
docker compose exec worker tq enqueue image.render '{"width": 1600, "height": 900}' --wait
```

See what happened:

```bash
docker compose exec worker tq stats
docker compose exec worker tq workers
docker compose exec db psql -U tq task_queue -c \
  "SELECT name, status, count(*), round(avg(spent_seconds)::numeric, 3) AS avg_seconds
   FROM tq_tasks GROUP BY 1, 2 ORDER BY 1, 2"
```

### Over HTTP

The API is on <http://localhost:8010>, with interactive documentation at
<http://localhost:8010/docs> and the [web UI](docs/ui.md) at <http://localhost:8010/ui>.
Sign in there with the admin account from your `.env`.

```bash
TOKEN=$(curl -s http://localhost:8010/v1/auth/token \
  -d grant_type=password -d username=admin -d password='<your admin password>' \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')

curl -s http://localhost:8010/v1/tasks \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"name": "demo.echo", "payload": {"message": "over http"}}'

# Wait up to 20 seconds for it to finish, then return it:
curl -s "http://localhost:8010/v1/tasks/<id>?wait=20" -H "Authorization: Bearer $TOKEN"
```

To stop: `docker compose down` (add `-v` to delete the data as well).

## Using it in your own project

It is not on PyPI yet. Install from the repository:

```bash
pip install "pgtaskqueue @ git+https://github.com/mamedshahmaliyev/pgtaskqueue.git"          # queue + worker
pip install "pgtaskqueue[api] @ git+https://github.com/mamedshahmaliyev/pgtaskqueue.git"     # + HTTP API, auth, MCP
```

Point it at a database and create its tables. They are all prefixed `tq_`, so your
application's own database is a fine place for them:

```bash
export TQ_DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/myapp
tq migrate
```

Write a task:

```python
# myapp/tasks.py
import httpx
from pydantic import BaseModel

from pgtaskqueue import Retry, TaskContext, task


class Welcome(BaseModel):
    user_id: int
    email: str


@task("mail.welcome", max_attempts=5, timeout=30)
async def send_welcome(ctx: TaskContext, payload: Welcome) -> dict:
    async with httpx.AsyncClient() as http:
        response = await http.post("https://mail.example.com/send", json=payload.model_dump())
    if response.status_code == 429:
        raise Retry("rate limited", delay=60)
    response.raise_for_status()
    ctx.log(f"sent to {payload.email}")
    return {"message_id": response.json()["id"]}
```

Run a worker for it:

```bash
tq worker --executors myapp.tasks --slots 50
```

Enqueue from your application:

```python
from pgtaskqueue import Database, TaskQueue

db = Database()  # reads TQ_DATABASE_URL
queue = TaskQueue(db)

task = await queue.enqueue("mail.welcome", {"user_id": 7, "email": "ada@example.com"})

# or inside your own transaction, so the task exists if and only if the user does:
async with db.transaction() as tx:
    ...  # insert the user with tx
    await queue.enqueue("mail.welcome", {"user_id": 7, "email": "ada@example.com"}, session=tx)
```

[examples/](examples/) has runnable versions of all of this: executors, a second worker
with its own dependencies, an API client, a webhook receiver, and a worker that talks only
HTTP.

## How it performs

Measured on one laptop (Apple M2 Max), with PostgreSQL 18 in Docker on the same machine and
tasks that do nothing, so the numbers are the queue's own overhead:

| | |
|---|---|
| Enqueue | 2,600/s one at a time from concurrent callers; 23,600/s in batches |
| End to end | 1,200/s with one worker process; 4,100/s with four |
| Pickup latency, idle worker | 8 ms median |
| Concurrent tasks in one process | 10,000, using 129 MB |
| Claim time at 10,000 rows and at 1,000,000 rows | 2 ms and 2 ms |

These are not server numbers, and they are not a promise for your workload.
[docs/benchmarks.md](docs/benchmarks.md) has the method, the full tables, what was not
measured, and how to run the same benchmarks on your hardware in ten minutes.

## Documentation

| | |
|---|---|
| [Architecture](docs/architecture.md) | How it works and why: the claim query, fencing, heartbeats, guarantees, the schema |
| [Writing tasks](docs/executors.md) | Executors, payloads, retries, steps, logs, blocking work, testing |
| [Periodic tasks](docs/schedules.md) | Cron expressions and intervals, when a schedule fires, missed runs, timezones |
| [Running workers](docs/workers.md) | Slots and pools, one worker per kind of work, stopping, idle exit |
| [HTTP API](docs/api.md) | Endpoints, webhooks, the remote-worker protocol |
| [Auth](docs/auth.md) | Tokens, roles, ACLs, signing keys, external identity providers |
| [MCP server](docs/mcp.md) | Connecting an AI assistant |
| [Web UI](docs/ui.md) | The pages, what each role sees, demo data, how the browser session works |
| [Public demo](docs/demo.md) | The demo's accounts, limits and resets, and how to host one |
| [Infrastructure](docs/infrastructure.md) | Compose, images, PostgreSQL settings, connections, deploys, backups |
| [Configuration](docs/configuration.md) | Every `TQ_` setting and every `tq` command |
| [Analytics](docs/analytics.md) | SQL for throughput, durations, failures, tenants, dashboards |
| [Benchmarks](docs/benchmarks.md) | Numbers, method and limits |
| [Comparison](docs/comparison.md) | Against Celery and Redis, other PostgreSQL queues, workflow engines |
| [AGENTS.md](AGENTS.md) | For contributors and coding agents: layout, invariants, how to change things |

## Status

Version 0.1. New, and not yet proven in production.

- Developed and tested on PostgreSQL 18 and Python 3.13. The test suite also passes on
  Python 3.12 and 3.14. No other PostgreSQL version has been tested.
- Works behind PgBouncer in transaction mode (tested with 1.26). Other poolers, and
  Kubernetes, have not been tested.
- The API and the schema may still change before 1.0. Schema changes come as migrations.

It does not limit how often a kind of task runs, and it has no workflow graphs.
See [what this project does not have](docs/comparison.md#what-this-project-does-not-have).

## Development

```bash
uv sync --all-extras
docker compose up -d db
uv run pytest -q
uv run ruff check . && uv run ruff format --check .
```

The tests run against a real PostgreSQL, in a database of their own (`task_queue_test`).
[CONTRIBUTING.md](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md) have the rest. To report a
security problem, see [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
