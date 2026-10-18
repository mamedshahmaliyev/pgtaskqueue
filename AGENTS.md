# AGENTS.md

Instructions for AI coding agents, and a fast orientation for anyone, working in this
repository. [README.md](README.md) is the tour for users; this file is for whoever changes
the code.

## What this is

A task queue whose only infrastructure is PostgreSQL. Tasks are rows in `tq_tasks`; workers
claim them with `FOR UPDATE SKIP LOCKED` and run them in one asyncio event loop. An
optional FastAPI application exposes the queue over HTTP and MCP with JWT auth and roles.

Read [docs/architecture.md](docs/architecture.md) before changing `queue.py` or `worker.py`.
It is short, and those two files are where a plausible-looking change breaks a guarantee.

## Commands

```bash
uv sync --all-extras                 # install everything, once
docker compose up -d db redis        # PostgreSQL on localhost:54320, Redis on :63790
cp .env.example .env                 # then set TQ_JWT_SECRET; the tests do not need it

uv run pytest -q                     # all tests, about 40 seconds
uv run pytest -q tests/test_queue.py -k claim
uv run ruff check .                  # lint
uv run ruff format .                 # format (also formats Python blocks in the docs)

uv run tq migrate                    # create or upgrade the schema in TQ_DATABASE_URL
uv run tq worker --executors examples.executors.basic,examples.executors.pipeline
uv run tq api --reload
uv run python -m benchmarks throughput
```

Before you call a change done: `uv run ruff format . && uv run ruff check . && uv run pytest -q`.
All three must pass.

## Layout

```
src/pgtaskqueue/
  queue.py         TaskQueue: every operation on a task, each a single SQL statement
  worker.py        Worker: pools, slots, the TaskGroup of loops, shutdown
  executor.py      Executor, @task, @register, Registry, TaskContext (ctx.log, ctx.step, …)
  monitor.py       Monitor: workers, task types, paused queues, statistics
  schedules.py     Schedule, Schedules: periodic tasks, and the transaction that fires them
  archive.py       Archive: moves old finished tasks to TQ_ARCHIVE_DATABASE_URL; reads them
  cron.py          Cron: parsing an expression, finding its next fire time
  models.py        The schema and its indexes, with the reasons for each
  settings.py      Every TQ_ setting, in one class
  db.py            Database: the async engine, sessions, the dedicated LISTEN connection
  threads.py       Blocking task code in a thread that can be stopped (TaskStopped)
  logs.py          Buffered per-task logs; the logging handler that captures them
  errors.py        Retry, Snooze, NonRetryable, TaskTimeout
  cli.py           The `tq` command
  migrate.py       Runs the packaged Alembic migrations
  migrations/      Alembic environment and versions/
  migrations_archive/   The archive's own history (tq_archive_* tables, another database)
  builtin/         webhook.py: the `tq.webhook` executor that delivers callbacks
  auth/            passwords, roles and permissions, JWTs, accounts   (api extra)
    limits.py      Per-account limits the API checks at enqueue (waiting, rate, priority)
  api/             FastAPI app, routes, service layer, MCP server     (api extra)
    ratelimit.py   Request rate limits in Redis: per account, per address, sign-ins
    ui.py          The web UI's routes and its cookie session
    assets.py      The ?v= on static URLs and imports; `python -m pgtaskqueue.api.assets bump`
    templates/     Jinja2 page shells
    static/        Plain JavaScript modules (custom elements) and one stylesheet
  seed.py          `tq seed`: demonstration accounts and tasks
tests/             pytest, against a real PostgreSQL
examples/          Executors, workers, an API client, a remote worker, a webhook receiver
benchmarks/        python -m benchmarks …
docker/Dockerfile  One Dockerfile for the API image and every worker image
compose.yml        db, migrate, api, worker, worker-image
compose.pgbouncer.yml   Overlay that puts PgBouncer between them and the database
docs/              One file per topic; see the index in README.md
```

## Rules that must keep holding

These are what make the queue correct. A change that breaks one is a bug even if every test
still passes, so if you touch them, add a test that would have caught the break.

1. **A state change is one conditional `UPDATE`.** The status a transition expects is in its
   `WHERE`. Never `SELECT` a task, decide in Python, then `UPDATE`: two processes will both
   decide.
2. **Every write a worker makes to a task is fenced** with
   `status = 'running' AND worker_id = :me AND attempt = :claimed` (`TaskQueue._owned`). A
   worker that was presumed dead must not be able to overwrite the task's next attempt.
3. **Time comes from the database** (`now()`), never from the host, for anything stored or
   compared: `run_after`, `heartbeat_at`, staleness.
4. **The claim query and `tq_tasks_claim_idx` are one design.** The index is partial on
   `status = 'pending'` and ordered exactly as the query orders. Change one and you must
   change the other, then re-run `python -m benchmarks depth` to confirm claims are still
   flat as the table grows.
5. **A heartbeat touches no indexed column**, so PostgreSQL can apply it as a HOT update.
   Do not index `heartbeat_at` or `progress`.
6. **No database connection is held while a task runs.** A connection is checked out for
   one operation and returned. Do not pass a session into an executor or keep a transaction
   open across an `await` on user code.
7. **What a status change triggers is written in the same transaction as the change**: the
   webhook delivery task, the `NOTIFY`. See `TaskQueue._settled`.
8. **The API never imports executor code.** It learns task names and payload schemas from
   `tq_task_types`, which workers write on start. Keep `api/` free of imports from user
   task modules, and keep `fastapi`, `jwt` and `mcp` imports out of the core modules: a
   worker image installs only the core dependencies.
9. **Delivery is at-least-once.** Do not add anything that promises exactly-once, and do
   not describe it that way in docs.
10. **Task names starting `tq.` are reserved** for built-ins.
11. **Nothing depends on session state**, so the queue works through a transaction-mode
    pooler: no `SET` without `LOCAL`, no session-level advisory locks, no temporary tables,
    no `LISTEN` on a pooled connection. Every engine is built with `db.connect_args`.

## Conventions

- Python 3.12+, `asyncio` throughout, `TaskGroup` for structured concurrency. No
  `asyncio.gather` over tasks that must be cancelled together, no bare `create_task`
  without keeping the reference.
- SQLAlchemy 2.x Core for the queue's statements (the SQL is the product, so keep it
  visible), ORM models only for the schema and simple rows.
- Line length 100. `ruff` decides formatting; do not hand-format against it.
- Comments say **why**, not what. The code base has many, on purpose: most lines of
  `queue.py` are there for a concurrency reason that is not visible in the line. Keep that
  up, and do not add comments that restate the code.
- Errors meant for users are short sentences that say what to do.
- Settings are added in `settings.py` only, with a comment, and documented in
  [docs/configuration.md](docs/configuration.md). If compose services need the new setting,
  add it under the right service's `environment:` in `compose.yml`; containers receive
  only the variables listed there.
- Dependencies: the core list in `pyproject.toml` is what every worker image installs.
  Anything only the API needs goes in the `api` extra. Think twice before adding to
  either.

## Tests

Tests run against a real PostgreSQL. The queue *is* its SQL, so nothing is mocked: not the
database, not the worker, not the HTTP server.

- The database is `TQ_TEST_DATABASE_URL`, or `TQ_DATABASE_URL` with `_test` appended to the
  database name. It is created and migrated automatically, and emptied before each test.
- All tests share one event loop (`asyncio_default_test_loop_scope = "session"`). Async
  fixtures and tests need no decorator.
- [tests/conftest.py](tests/conftest.py) has the fixtures: `db`, `queue`, `settings`,
  `registry` (a fresh `Registry` per test), `worker_factory`.
- [tests/helpers.py](tests/helpers.py) has `wait_for(queue, task_id)`, `until(predicate)`,
  `running_worker(...)`, `serving(db, settings)` (the API on a real socket), `bearer(...)`
  and `Receiver` (a webhook receiver).

```python
from tests.helpers import wait_for


async def test_my_task(queue, registry, worker_factory):
    @registry.task("my.task")
    async def my_task(ctx, payload):
        return {"ok": True}

    task = await queue.enqueue("my.task", {})
    async with worker_factory(slots=2):
        done = await wait_for(queue, task.id)
    assert done.status == "succeeded"
```

Write tests that fail for the right reason. For anything concurrent, test the race itself
(see `test_concurrent_claims_never_deliver_twice`), and check that the test fails when the
protection is removed.

Never wait with a bare `asyncio.sleep(n)` for something to happen; use `wait_for` or
`until`, which poll and time out with a useful message.

The archive's tests use a second database, `TQ_TEST_ARCHIVE_DATABASE_URL`, or the test
database's name with `_archive` appended, created and migrated the same way.

The rate-limit tests (`tests/test_ratelimit.py`) use `TQ_TEST_REDIS_URL`, by default the
compose Redis's database 15 (`redis://localhost:63790/15`), and empty it before each test.
Never point it at a Redis anything else uses. Without a reachable Redis those tests are
skipped, with the reason in the summary.

Run one test run at a time against a given database. Every test empties the `tq_` tables
first, so two runs sharing `task_queue_test` (two Python versions side by side, say) fail
each other at random. Give the second run its own database with `TQ_TEST_DATABASE_URL`.

## Migrations

The schema is defined twice, and a test keeps the two in agreement:

- [src/pgtaskqueue/models.py](src/pgtaskqueue/models.py) is what the code reads.
- [src/pgtaskqueue/migrations/versions/](src/pgtaskqueue/migrations/versions/) is what users
  run.

`test_models_and_migrations_describe_the_same_schema` fails if they differ.

The archive (`tq_archive_tasks`, `tq_archive_task_logs`, in `TQ_ARCHIVE_DATABASE_URL`) is a
second history in [migrations_archive/](src/pgtaskqueue/migrations_archive/), tracked in
`tq_archive_alembic_version`. Its columns are copied from `tq_tasks` and `tq_task_logs` in
`models.py`, so **a column added to either needs an archive migration too**:
`test_archive_models_and_migrations_describe_the_same_schema` fails until it has one. Write
it as above, against the archive's own history:

```bash
uv run tq migrate        # with TQ_ARCHIVE_DATABASE_URL set, migrates both
uv run alembic --name archive revision --autogenerate -m "add tq_archive_tasks.something"
```

To change the schema:

1. Change the model.
2. Generate a migration against a database that is at the current head:

   ```bash
   uv run tq migrate
   uv run alembic revision --autogenerate -m "add tq_tasks.something"
   ```

   [alembic.ini](alembic.ini) exists only for this command. The URL comes from
   `TQ_DATABASE_URL`.
3. Rename the file and set `revision` to the next number (`0002`, `0003`, …), matching
   `0001_initial_schema.py`. Read what autogenerate wrote: it does not see storage
   parameters, partial-index predicates it considers equal, or data migrations. Write those
   by hand. Write `downgrade()` too.
4. Run the tests. `test_upgrade_downgrade_upgrade` exercises both directions.

Rules:

- **Never edit a migration that has been released.** Add a new one.
- **Migrations must be safe on a live queue.** Workers and the API keep running during a
  deploy. Add columns as nullable or with a constant default; create indexes on `tq_tasks`
  with `CREATE INDEX CONCURRENTLY` (which needs `autocommit_block()` in Alembic); never
  take a lock that blocks claims for longer than a moment.
- **Old code runs against the new schema** during a rolling deploy, and new code may meet
  rows written by old code. Removing or renaming a column takes two releases.
- Tables are prefixed `tq_`. The version table is `tq_alembic_version`, not Alembic's
  default, so the queue can live in a database that has its own migrations. Autogenerate is
  restricted to `tq_` tables for the same reason (`_include_object` in `env.py`).
- `tq migrate` takes an advisory lock, so concurrent runs take turns.

## Adding things

**A setting.** `settings.py` (with a comment) → `docs/configuration.md` → `compose.yml` /
`.env.example` if it is commonly set → a test if it changes behaviour.

**An API endpoint.** The route in `api/routes/`, the logic in `api/service.py` (routes stay
thin; the MCP tools call the same service methods), request and response models in
`api/schemas.py`, a permission from `auth/roles.py` checked in the service (`Service.need`), a test in
`tests/test_api.py` for the happy path **and** for a caller who lacks the permission or
whose ACL excludes the task, then `docs/api.md`. An account that may not see a task gets
404, not 403.

**An MCP tool.** `api/mcp.py`, calling the service method the REST route calls, with
`readOnlyHint` / `destructiveHint` annotations, a test in `tests/test_mcp.py`, and a row in
`docs/mcp.md`.

**Something in the web UI.** A page is a template in `api/templates/` that names one custom
element, a route in `api/ui.py`, and a module in `api/static/` that defines the element.
The page gets its data from `/v1` through `api()` in `core.js`; if the data is not there,
add the API endpoint first, as above. Rules that keep it safe and small:

- No build step, no framework, no script or style from another host, nothing inline: the
  Content-Security-Policy forbids it and `test_ui.py` checks the header.
- Build DOM with `h()`. Never `innerHTML`: task names, payloads and logs are user input.
- The UI decides no permission. It may hide a button (`can("tasks:manage")`); the service
  layer is what refuses.
- A request the cookie authenticates must not change anything without the `X-TQ-UI`
  header (`deps.get_principal`). Do not add a route that reads the cookie and skips it.
- A chart follows the status colours in `app.css`, with a legend, a tooltip and a table.

After any change in `api/static/`, run `uv run python -m pgtaskqueue.api.assets bump` and
commit what it changed: `api/static_version.json` and the `?v=` on every import in
`api/static/*.js`. Write a new import without `?v=` and let the bump add it; never edit the
stamps by hand. Browsers keep a version's files for a year, and
`test_static_version_matches_the_files` fails until the bump is made.

Look at the page in a browser, light and dark, before calling it done; the tests cover the
session and the routes, not the rendering. Then `docs/ui.md`.

**A CLI command.** `cli.py`, a test in `tests/test_cli.py`, a row in
`docs/configuration.md`.

**Something a worker does periodically.** A loop in `Worker`, started in the `TaskGroup`
with the others. If only one worker should do it at a time, guard it with
`TaskQueue.try_lock`.

**Something about periodic tasks.** `schedules.py` and `cron.py`. A fire is one
transaction: lock the due rows, enqueue, move `next_run_at`. Keep the three together
(`test_concurrent_fires_use_each_fire_time_once` fails if the lock goes), and keep the
next fire time computed from the `now()` the query returned.

**A benchmark.** `benchmarks/__main__.py`. Benchmarks start real `tq worker` processes and
refuse to run against a database whose name does not end in `_bench`.

## Documentation

Docs are part of the change. A pull request that changes behaviour changes the page that
describes it.

| File | Covers |
|---|---|
| [README.md](README.md) | The tour and the quick start |
| [docs/architecture.md](docs/architecture.md) | How it works and why; guarantees; the schema |
| [docs/executors.md](docs/executors.md) | Writing tasks |
| [docs/schedules.md](docs/schedules.md) | Periodic tasks: cron, intervals, when they fire |
| [docs/workers.md](docs/workers.md) | Running workers: slots, pools, stopping |
| [docs/api.md](docs/api.md) | HTTP API, webhooks, the remote-worker protocol |
| [docs/auth.md](docs/auth.md) | JWT, roles, ACLs, accounts |
| [docs/mcp.md](docs/mcp.md) | The MCP server |
| [docs/ui.md](docs/ui.md) | The web UI, its session, `tq seed` |
| [docs/infrastructure.md](docs/infrastructure.md) | Compose, images, PostgreSQL, operating it |
| [docs/configuration.md](docs/configuration.md) | Every setting and CLI command |
| [docs/analytics.md](docs/analytics.md) | SQL for the tables |
| [docs/benchmarks.md](docs/benchmarks.md) | Measured numbers and how to reproduce them |
| [docs/comparison.md](docs/comparison.md) | Against Celery, other PostgreSQL queues, workflow engines |

Rules for the docs:

- **State only what has been run.** Run every command, query and code sample you add or
  change. If something is untested (a PostgreSQL version, a pooler, an orchestrator), say
  "not tested" in the text, as the existing pages do.
- **Numbers come from `python -m benchmarks`**, with the machine they came from. Do not
  round them up, and do not copy a figure from one section to another without checking it.
- State limits as plainly as features. [docs/comparison.md](docs/comparison.md) has a list
  of what this project does not have; keep it true.
- Plain words, short sentences, no marketing.

## Things not to do

- Do not commit `.env`, tokens, or real callback URLs.
- Do not run the benchmarks or the tests against a database you care about. Both empty the
  tables they use. The guards (`_test`, `_bench` suffixes) are there for that reason; do
  not weaken them.
- Do not `UPDATE tq_tasks` by hand in code or in docs examples to change a status. Use
  `TaskQueue` methods, which carry the conditions.
- Do not make the worker depend on the API, or the API on a worker being up.
- Do not add a broker, a cache or a second datastore. One PostgreSQL is the premise. The
  one exception is the API's optional Redis (`TQ_REDIS_URL`, `api/ratelimit.py`), which
  holds request rate-limit counters and nothing else. Keep it that way: no queue state,
  no tasks, no accounts in Redis; the API must work without it; workers never use it, so
  `redis` stays in the `api` extra.
