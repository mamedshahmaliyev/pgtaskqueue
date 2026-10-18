# Configuration and CLI reference

Every setting, with its default, and every `tq` command.

- [How settings are read](#how-settings-are-read)
- [Database](#database)
- [Queue behaviour](#queue-behaviour)
- [Archive](#archive)
- [Worker](#worker)
- [Webhooks](#webhooks)
- [API and authentication](#api-and-authentication)
- [Settings that must agree](#settings-that-must-agree)
- [The `tq` command](#the-tq-command)

## How settings are read

Settings are environment variables prefixed `TQ_`. The queue, the worker, the API and the
CLI all read the same set, from the same class
([settings.py](../src/pgtaskqueue/settings.py)).

They are read from, in order of precedence:

1. Command-line flags, for the few `tq worker` options that have one.
2. The process environment.
3. A `.env` file in the directory the process starts in.
4. The defaults below.

Lists are comma-separated: `TQ_WORKER_TASKS=image.*:2,report.build:4`. Booleans accept
`true`/`false`, `1`/`0`. Unknown `TQ_` variables are ignored, so check spelling when a
setting appears to do nothing. `tq worker` logs the pools it started with.

In code, pass a `Settings` object instead:

```python
from pgtaskqueue import Database, Settings, TaskQueue

settings = Settings(database_url="postgresql+asyncpg://…", default_max_attempts=5)
db = Database(settings=settings)
queue = TaskQueue(db, settings)
```

**With Docker Compose**, a container receives only the variables its service lists under
`environment:` in [compose.yml](../compose.yml), not the whole `.env` file. That is
deliberate: the workers never see the JWT secret. To set a variable the service does not
already list, add it in `compose.override.yml`
([example](../compose.override.example.yml)).

## Database

| Variable | Default | |
|---|---|---|
| `TQ_DATABASE_URL` | `postgresql+asyncpg://tq:tq@localhost:5432/task_queue` | SQLAlchemy URL. The driver must be `asyncpg`. |
| `TQ_DB_POOL_SIZE` | `10` | Connections each process keeps open. |
| `TQ_DB_MAX_OVERFLOW` | `10` | Extra connections a process may open under load and close afterwards. |
| `TQ_DB_POOL_PRE_PING` | `true` | Test a pooled connection before each use, and replace it if it died while idle. Costs one round trip per operation. |
| `TQ_DB_ECHO` | `false` | Log every SQL statement. For debugging. |
| `TQ_DB_PGBOUNCER` | `false` | Set to `true` when `TQ_DATABASE_URL` points at PgBouncer in transaction mode. Turns off the driver's prepared-statement caches. |
| `TQ_LISTEN_DATABASE_URL` | none | A direct URL to the same database for a worker's one `LISTEN` connection, when `TQ_DATABASE_URL` is a pooler. Without it, a worker behind a pooler polls. |

A connection is held for the length of one queue operation, never for the length of a task.
A worker with 200 slots does not need 200 connections; it needs enough for the tasks that
start and finish at the same moment. The default of 20 per process is right for most
workers. [infrastructure.md](infrastructure.md#connections) has the arithmetic for
PostgreSQL's `max_connections`, and [benchmarks.md](benchmarks.md#throughput) measures what
the pool size and the pre-ping cost.

## Queue behaviour

| Variable | Default | |
|---|---|---|
| `TQ_DEFAULT_PRIORITY` | `100` | Priority of a task enqueued without one. Lower runs first. |
| `TQ_DEFAULT_MAX_ATTEMPTS` | `3` | Attempts for a task when neither the enqueue call nor its executor says. |
| `TQ_RETRY_BACKOFF_BASE_SECONDS` | `5` | Delay before the second attempt. Doubles for each attempt after that. |
| `TQ_RETRY_BACKOFF_MAX_SECONDS` | `600` | Upper limit of that delay. |
| `TQ_RETRY_BACKOFF_JITTER` | `0.2` | Random extra, as a fraction of the delay, so that tasks which failed together do not retry together. |
| `TQ_STALE_AFTER_SECONDS` | `60` | A running task whose heartbeat is older than this is presumed lost with its worker, and is queued again (or failed, if it has no attempts left). |
| `TQ_RETENTION_DAYS` | `0` | Delete finished tasks, and their logs, this many days after they finish. `0` keeps everything. |
| `TQ_NOTIFY` | `true` | Use PostgreSQL `LISTEN`/`NOTIFY` so idle workers start new tasks immediately and running tasks hear a cancel immediately. |

With the defaults, a task that keeps failing is retried after about 5 s and then about
10 s, and fails for good on its third attempt. An executor can override the delay per task
([executors.md](executors.md#failing-on-purpose)).

With `TQ_NOTIFY=false`, workers find tasks by polling, and a cancel reaches a running task
at its next heartbeat. Behind PgBouncer see
[infrastructure.md](infrastructure.md#connection-poolers): a worker there stops listening by
itself unless it is given `TQ_LISTEN_DATABASE_URL`.

`TQ_RETENTION_DAYS` is applied by workers, so it must be set on them, not on the API.

## Archive

Moving finished tasks, with their logs, to another database. Read by workers, which move
them, and by `tq migrate` and `tq archive`. [analytics.md](analytics.md#the-archive) explains
how it works and how to read the archive.

| Variable | Default | |
|---|---|---|
| `TQ_ARCHIVE_DATABASE_URL` | none | SQLAlchemy URL of the archive database (`asyncpg` driver). Unset or empty: nothing is archived. It may be the queue's own database. |
| `TQ_ARCHIVE_AFTER_DAYS` | `30` | Archive a finished task this many days after it finished. Fractions are allowed. |
| `TQ_ARCHIVE_BATCH_SIZE` | `1000` | Tasks moved per transaction. A requeue of a task in the batch waits until the batch is done. |
| `TQ_ARCHIVE_INTERVAL_SECONDS` | `60` | How often a worker looks for tasks to archive. |

With an archive set, `TQ_RETENTION_DAYS` must be `0` or more than `TQ_ARCHIVE_AFTER_DAYS`;
the settings refuse to load otherwise, because retention would delete tasks before they were
archived.

## Worker

| Variable | Flag | Default | |
|---|---|---|---|
| `TQ_WORKER_EXECUTORS` | `--executors` | empty | Modules to import for the executors they register. Comma-separated. |
| `TQ_WORKER_QUEUE` | `--queue` | `default` | The queue this worker consumes. One per worker. |
| `TQ_WORKER_TASKS` | `--tasks` | empty | Which task names to run and how many of each at once: `NAME:N`, a glob (`image.*:4`), `ALL:N`, or `NAME:0` to exclude. Empty means every registered name, sharing `TQ_WORKER_SLOTS`. |
| `TQ_WORKER_SLOTS` | `--slots` | `10` | Tasks at once, for names given no count of their own. |
| `TQ_WORKER_IDLE_EXIT_SECONDS` | `--idle-exit` | `0` | Exit after this long with nothing to run, having run at least one task. `0` never exits. |
| `TQ_WORKER_MAX_TASKS` | `--max-tasks` | `0` | Exit after this many tasks. `0` never exits. |
| `TQ_WORKER_SHUTDOWN_GRACE_SECONDS` | | `30` | On SIGTERM, how long to let running tasks finish before cancelling them and handing them back. |
| `TQ_POLL_INTERVAL_SECONDS` | | `2` | How often an idle worker looks for tasks when no notification arrives. |
| `TQ_HEARTBEAT_INTERVAL_SECONDS` | | `5` | How often a worker reports its running tasks alive. |
| `TQ_PROMOTE_INTERVAL_SECONDS` | | `1` | How often delayed tasks whose time has come are made ready. This is the resolution of `delay` and `run_at`. |
| `TQ_SCHEDULE_INTERVAL_SECONDS` | | `1` | How often a worker looks for [periodic schedules](schedules.md) that are due. A schedule can fire up to this much late. |
| `TQ_MAINTENANCE_INTERVAL_SECONDS` | | `15` | How often lost tasks are recovered, worker rows refreshed and old tasks pruned. |
| `TQ_LOG_FLUSH_INTERVAL_SECONDS` | | `1` | How often buffered task log lines are written. |
| `TQ_CAPTURE_LOG_LEVEL` | | `INFO` | `logging` records at or above this level, emitted while a task runs, are stored as that task's log. Empty turns the capture off; `ctx.log()` always works. |
| `TQ_LOG_MESSAGE_MAX_CHARS` | | `8000` | Longer log messages are cut to this. |

The exit settings exist to be combined with a restart policy: the worker leaves, and Docker
starts a fresh one with released memory and whatever code is now in the image.
[workers.md](workers.md) explains slots, pools and stopping in full.

## Webhooks

Read by workers, which deliver the notifications. The API needs none of these.

| Variable | Default | |
|---|---|---|
| `TQ_WEBHOOK_SLOTS` | `2` | Slots each worker adds for delivering webhooks, besides its own. `0` leaves delivery to other workers. |
| `TQ_WEBHOOK_TIMEOUT_SECONDS` | `10` | Time allowed for the receiver to answer. |
| `TQ_WEBHOOK_MAX_ATTEMPTS` | `6` | Deliveries attempted before giving up. |
| `TQ_WEBHOOK_SECRET` | empty | Signs deliveries for tasks whose callback names no secret of its own. Empty sends those unsigned. |
| `TQ_WEBHOOK_ALLOW_PRIVATE` | `false` | Allow callback URLs that resolve to loopback, private or link-local addresses. |

A callback URL is checked when it is delivered, not when the task is enqueued. A refused
URL shows up as a failed `tq.webhook` task with the reason in its error.

Leave `TQ_WEBHOOK_ALLOW_PRIVATE` off unless every account that can enqueue is trusted: with
it on, a caller can make a worker send requests to addresses inside your network.
[api.md](api.md#webhooks) describes the delivery format and retries.

## API and authentication

Read by `tq api`. Workers need none of these.

| Variable | Default | |
|---|---|---|
| `TQ_JWT_ALGORITHM` | `HS256` | `HS256`/`HS384`/`HS512` with a shared secret, or an asymmetric algorithm (`RS256`, `ES256`, `EdDSA`, …) with a key pair. |
| `TQ_JWT_SECRET` | empty | The shared secret for `HS*`. At least 32 bytes. The API refuses to start without it. |
| `TQ_JWT_PRIVATE_KEY` | empty | For asymmetric algorithms: the PEM private key, inline or as a file path. Leave empty to only verify tokens issued elsewhere. |
| `TQ_JWT_PUBLIC_KEY` | empty | The PEM public key, inline or as a file path. Derived from the private key if omitted. |
| `TQ_JWT_ISSUER` | `pgtaskqueue` | The `iss` claim tokens are issued with and must carry. |
| `TQ_JWT_AUDIENCE` | `pgtaskqueue-api` | The `aud` claim, likewise. |
| `TQ_ACCESS_TOKEN_MINUTES` | `15` | Lifetime of an access token. |
| `TQ_REFRESH_TOKEN_DAYS` | `7` | Lifetime of a refresh token. |
| `TQ_BOOTSTRAP_ADMIN_USERNAME` | empty | Read by `tq migrate`: the admin account to create when there are no accounts. `admin` if only the password is set. |
| `TQ_BOOTSTRAP_ADMIN_PASSWORD` | empty | Its password. Empty creates no account. |
| `TQ_CORS_ORIGINS` | empty | Browser origins allowed to call the API. Comma-separated. |
| `TQ_UI_ENABLED` | `true` | Serve the [web UI](ui.md) at `/ui`, and accept its session cookie on `/v1`. |
| `TQ_MCP_ENABLED` | `true` | Serve the MCP endpoint at `/mcp`. |
| `TQ_MCP_ALLOWED_HOSTS` | empty | `Host` headers the MCP endpoint accepts, e.g. `tasks.example.com,localhost:*`. Empty accepts any. |
| `TQ_API_REQUIRE_REGISTERED_TASKS` | `false` | Refuse to enqueue a task name that no worker has registered. |
| `TQ_LONG_POLL_MAX_SECONDS` | `30` | Upper limit of `GET /v1/tasks/{id}?wait=`. |
| `TQ_MAX_PAYLOAD_BYTES` | `1000000` | Largest task payload the API accepts. |

Generate a secret with `openssl rand -hex 32`. [auth.md](auth.md) covers key pairs,
external identity providers, roles and ACLs.

A key given inline may have its newlines written as `\n`, which is what most secret stores
and `.env` files need.

## Account limits

The [limits](auth.md#limits) of an account that has not set its own. `0` is no limit, and
all four default to it. They apply to a task's `created_by`, so a task enqueued without an
account (from Python, or `tq enqueue`) is never limited.

| Variable | Default | Read by | |
|---|---|---|---|
| `TQ_ACCOUNT_MAX_WAITING` | `0` | API | Tasks an account may have `scheduled` or `pending` at once. More is refused with 429. |
| `TQ_ACCOUNT_MAX_RUNNING` | `0` | API and workers | Tasks of an account that may be `pending` or `running` at once. The rest are held and released as others finish. |
| `TQ_ACCOUNT_ENQUEUE_PER_MINUTE` | `0` | API | Tasks an account may create in any 60 seconds. More is refused with 429 and `Retry-After`. |
| `TQ_ACCOUNT_MIN_PRIORITY` | `0` | API | The most urgent priority an account may ask for (lower runs first). Below it is 403. |

## Request rate limits

Read by `tq api`. How fast clients may call the API, counted in Redis so every replica
shares one count. Without `TQ_REDIS_URL` none of them applies, whatever they are set to.
[auth.md](auth.md#request-rate-limits) explains each.

A rate is `N/second`, `N/minute`, `N/hour` or `N/day`, or empty for no limit. Up to N
requests may arrive at once; after that one is let through every period/N. A rate that
cannot be read stops the API at start-up.

| Variable | Default | |
|---|---|---|
| `TQ_REDIS_URL` | empty | Redis for the limits, e.g. `redis://redis:6379/0`. Compose sets it to its own Redis. Empty turns every limit off. |
| `TQ_RATE_LIMIT` | empty | Requests per account, by the token's subject. Past it, 429 with `Retry-After`. |
| `TQ_RATE_LIMIT_ANONYMOUS` | empty | Requests without a valid token, per client address. |
| `TQ_LOGIN_RATE_LIMIT` | `30/minute` | Sign-in attempts per client address: password and client-credentials grants, and the UI's login form. |
| `TQ_LOGIN_FAILURE_LIMIT` | `10/hour` | Failed sign-ins per account name, from any address. |
| `TQ_RATE_LIMIT_FAIL_OPEN` | `true` | While Redis cannot be reached, let requests through unlimited. `false` answers 503 instead. |

## Settings that must agree

A few settings describe one fact that several processes rely on. Give every process that
shares a database the same value.

| Setting | Why |
|---|---|
| `TQ_DATABASE_URL` | They are one queue only if they are one database. |
| `TQ_STALE_AFTER_SECONDS`, `TQ_HEARTBEAT_INTERVAL_SECONDS` | One worker decides, with its own setting, when another worker's tasks are lost. `TQ_STALE_AFTER_SECONDS` must be at least three times `TQ_HEARTBEAT_INTERVAL_SECONDS`, and start-up fails otherwise, so that one slow heartbeat does not put a live task back in the queue. |
| `TQ_NOTIFY` | A process with it off sends no notifications, so workers with it on would still wait for the poll. |
| `TQ_WEBHOOK_SECRET`, `TQ_WEBHOOK_ALLOW_PRIVATE` | Any worker with webhook slots may deliver any task's notification. |
| `TQ_JWT_*` | Every API replica must sign and verify the same way. |
| `TQ_RETENTION_DAYS` | The worker that runs maintenance at a given moment applies its own value. |
| `TQ_ARCHIVE_*` | Any worker may run the next batch, with its own values. A worker without `TQ_ARCHIVE_DATABASE_URL` archives nothing, and the others still do. |
| `TQ_ACCOUNT_MAX_RUNNING` | Whoever enqueues decides with its value whether a task is held, and the worker that releases held tasks applies its own. With the API at 5 and the workers at 0, tasks are held and released in one batch, without a limit. |
| `TQ_ACCOUNT_*` | Every API replica checks enqueues against its own values. |
| `TQ_REDIS_URL`, `TQ_RATE_LIMIT*`, `TQ_LOGIN_*` | Replicas count in the Redis they are given, against their own rates. Two Redis URLs are two separate counts, each allowing the full rate. |

## The `tq` command

Installed with the package. `python -m pgtaskqueue` is the same thing.

Every command except `tq api` talks to PostgreSQL directly, using `TQ_DATABASE_URL`. It is
an operator's tool: there is no login, because whoever can run it already has the database.
For access with roles and limits, use the [HTTP API](api.md).

```
tq [--log-level LEVEL] [--version] COMMAND …
```

### Running things

| Command | |
|---|---|
| `tq migrate [--revision REV] [--no-bootstrap]` | Create or upgrade the schema, and the archive's if `TQ_ARCHIVE_DATABASE_URL` is set (always to its latest revision), then create the first admin account if `TQ_BOOTSTRAP_ADMIN_PASSWORD` is set and no account exists. Safe to run on every deploy. |
| `tq worker [--executors MODS] [--queue Q] [--tasks SPEC] [--slots N] [--idle-exit S] [--max-tasks N]` | Run a worker until it is stopped. See [workers.md](workers.md). |
| `tq api [--host H] [--port P] [--workers N] [--reload]` | Run the HTTP API and MCP server. Needs the `api` extra. Listens on `127.0.0.1:8000` unless told otherwise. |

`tq worker` exits with status 0 when it stops on purpose (a signal, idle exit, the task
limit), and non-zero if it crashed.

### Tasks

| Command | |
|---|---|
| `tq enqueue NAME [JSON] [--queue Q] [--priority N] [--delay S] [--max-attempts N] [--timeout S] [--dedupe-key K] [--wait]` | Add a task. `--wait` blocks until it finishes, prints the result, and exits 0 only if it succeeded. |
| `tq tasks [--queue Q] [--name NAME] [--status S] [--limit N] [--json]` | List tasks, newest first. |
| `tq show ID [--logs [N]]` | One task in full. `--logs` adds its log (the first 500 lines, or N). |
| `tq cancel ID` | Cancel a task that has not finished. A running task is stopped. |
| `tq requeue ID [--reset-progress]` | Run a finished task again. Completed steps are kept unless `--reset-progress`. |

```bash
tq enqueue demo.echo '{"message": "hello"}' --wait
tq tasks --status failed --limit 5
tq show 0192e3a1-7c1e-7d3a-9b52-0f6d1c2e4a77 --logs
```

### Queues and workers

| Command | |
|---|---|
| `tq stats [--window MINUTES] [--json]` | Depth, the age of the oldest pending task, and throughput and durations over the window (60 minutes by default). |
| `tq workers [--all] [--json]` | Workers that are alive; `--all` includes those that stopped in the last 7 days. |
| `tq pause QUEUE` | Stop workers claiming from a queue. Running tasks finish. |
| `tq resume QUEUE` | Undo. |

### Periodic tasks

| Command | |
|---|---|
| `tq schedule list [--json]` | Every schedule: task, timing, next fire time (local time), source, and whether it is paused. |
| `tq schedule set NAME TASK [JSON] (--cron EXPR \| --every SECONDS) [--timezone TZ] [--queue Q] [--priority N] [--max-attempts N] [--timeout S]` | Create a schedule, or replace it whole. Exits 1 if the expression or timezone is wrong, or if the name belongs to a schedule declared in code. |
| `tq schedule pause NAME` | Stop it firing. |
| `tq schedule resume NAME` | Undo. A fire time that passed while it was paused fires once. |
| `tq schedule delete NAME` | Delete it. Tasks it enqueued stay. |

```bash
tq schedule set nightly-echo demo.echo '{"message": "good night"}' --cron '30 2 * * *' --timezone Europe/Berlin
tq schedule pause nightly-echo
```

`tq schedule set` does not check the payload against the task's schema; the API does.
[schedules.md](schedules.md) explains when a schedule fires.

### Archive

These need `TQ_ARCHIVE_DATABASE_URL`, and exit 1 without it.

| Command | |
|---|---|
| `tq archive run [--older-than DAYS] [--batch N]` | Archive finished tasks older than `TQ_ARCHIVE_AFTER_DAYS` (or `DAYS`) now, until none are left. Workers do the same every `TQ_ARCHIVE_INTERVAL_SECONDS`; this is for a first run over a long history, or a database with no workers. |
| `tq archive tasks [--queue Q] [--name NAME] [--status S] [--limit N] [--json]` | List archived tasks, newest first. |
| `tq archive show ID [--logs [N]]` | One archived task in full. `--logs` adds its log (the first 500 lines, or N). Exits 1 if the archive does not have it. |

```bash
tq archive run --older-than 90
tq archive show 0192e3a1-7c1e-7d3a-9b52-0f6d1c2e4a77 --logs
```

### Accounts

These need the `api` extra installed, since they hash passwords and sign tokens.

| Command | |
|---|---|
| `tq user create NAME [--role ROLE] [--service] [--queues GLOBS] [--task-names GLOBS] [--password P \| --generate]` | Create an account. The role defaults to `submitter`. Prompts for the password unless given one or told to generate it. `--service` makes a service account, which signs in with client credentials. |
| `tq user set-password NAME [--password P \| --generate]` | Change a password. Ends every session the account has. |
| `tq user set-limits NAME [--max-waiting N] [--max-running N] [--enqueue-per-minute N] [--min-priority N]` | Set an account's [limits](auth.md#limits). Each takes a number, `0` for no limit, or `default` for the `TQ_ACCOUNT_*` value. Limits not given are left alone. |
| `tq user list` | Accounts, with their roles, ACLs and any limits they set. |
| `tq token NAME [--minutes N \| --days N] [--scope "PERMS"]` | Print an access token for an account, signed with the API's key. For clients that cannot refresh a token. It cannot be revoked before it expires. |

```bash
tq user create ci --service --role submitter --task-names 'report.*' --generate
tq user set-limits ci --max-waiting 500 --max-running 10
tq token assistant --days 30 --scope 'tasks:read stats:read'
```

### Demonstration data

| Command | |
|---|---|
| `tq seed [--tasks N] [--hours H] [--no-users] [--password P] [--force]` | Fill an empty database with finished tasks (400 over 24 hours by default), tasks waiting, scheduled and running, a paused queue, and five demo accounts with the password `demo-password`. For trying the [web UI](ui.md#demonstration-data). Exits 1 if the database already holds tasks, unless `--force`. The accounts need the `api` extra; `--no-users` skips them. |

Not for a database anyone else can reach: the password is published.

`tq token` needs the signing key (`TQ_JWT_SECRET` or `TQ_JWT_PRIVATE_KEY`), so with compose
run it in the API container: `docker compose exec api tq token assistant --days 30`.
