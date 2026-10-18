# The public demo

A running copy of pgtaskqueue, to try without installing anything:

**https://pgtaskqueue.mmdsh.org**

It is the whole system: the web UI, the HTTP API, the MCP server, and two workers running
the [example executors](../examples/executors). It runs on one small shared server, and
everyone who tries it uses the same few accounts.

- **It is public.** Whatever you enqueue, other visitors can see. Put nothing private in a
  payload, a log or a webhook header.
- **It is reset every hour.** Tasks, schedules and accounts you made are deleted, and the
  demo data is seeded again.
- **It is not a benchmark.** It runs on a small shared machine with limits on purpose.
  The measured numbers are in [benchmarks.md](benchmarks.md), with how to reproduce them.
- **Please do not load-test it.** To push the queue hard, run your own copy: the
  [quick start](../README.md) is one `docker compose up`.

## Accounts

Every account has the password **`demo-password`**.

| Account | Role | What it can do |
|---|---|---|
| `demo-manager` | manager | Everything an operator can, plus the **Accounts** page: create, change and delete accounts that reach no further than its own |
| `demo-operator` | operator | See and manage every task, enqueue, cancel, kill, restart, pause queues, manage schedules, read statistics |
| `demo-submitter` | submitter | Enqueue tasks, and see and manage only its own |
| `demo-viewer` | viewer | See every task, the statistics and the workers. Change nothing |

There is no `admin` and no `worker` account. [auth.md](auth.md#roles) has every role and
permission.

### Limits

Each account is limited, and everyone signed in as it shares its limits. When a limit
stops you, someone else may be using the account; wait a minute.

| Account | Waiting at once | Running at once | Enqueued per minute | Most urgent priority |
|---|---|---|---|---|
| `demo-manager` | 50 | 4 | 30 | 100 |
| `demo-operator` | 50 | 4 | 30 | 100 |
| `demo-submitter` | 30 | 3 | 20 | 100 |
| `demo-viewer` | (enqueues nothing) | | | |

What you see when you reach one ([auth.md](auth.md#limits) has the details):

| You | The answer |
|---|---|
| Enqueue while the account already has 50 (or 30) tasks waiting | **429**, "this account has … tasks waiting" |
| Enqueue more than 30 (or 20) tasks within a minute | **429**, with a `Retry-After` header |
| Send a batch larger than either limit | **413** |
| Ask for a priority below 100 (lower runs first) | **403** |
| Have more than 4 (or 3) tasks pending or running | Not refused. The rest wait as **held**, and start as the earlier ones finish |

There are also limits on requests per account and per address. A **429** from any page or
call means slow down.

The account limits apply to accounts the manager makes, too. A manager may not give an
account limits looser than its own, so `demo-manager` can create an account with a
running limit of 2, but not 10 and not "none".

### What is put back, and when

- **Every hour**, the queue is emptied: tasks, logs, paused queues, schedules made through
  the API, and every account visitors made. Then the demo data is seeded again: a few
  hundred finished tasks over the last 24 hours, some waiting and scheduled, three running,
  and a paused queue named `bulk`.
- **Every minute**, the four accounts get their password, role, ACL and limits back. If
  someone changes the password or disables an account, wait a minute and sign in again.
  Visitors may add up to 10 accounts and 10 schedules; past that, the newest are removed.

## Task names

The demo accounts may enqueue and see these task names. Payloads are JSON objects; the
UI's enqueue form shows each name's fields.

| Name | Payload | What it shows |
|---|---|---|
| `demo.echo` | `{"message": "hello"}` | A task that returns its payload |
| `demo.sleep` | `{"seconds": 30}` (up to 3600) | Progress, and cancelling or killing a running task |
| `demo.flaky` | `{"fail_times": 2}` | Retries with backoff: fails twice, then succeeds |
| `demo.wait_until` | `{"ready_at": "…"}`, a UTC time a minute from now, such as `2026-10-18T18:00:00Z` | Snooze: back to the queue without using an attempt, until that time |
| `demo.hash` | `{"rounds": 2000000}` | CPU work in a thread, with log lines as it goes |
| `report.build` | `{"rows": 1000, "fail_once_at": "transform", "notify": true}` | Durable steps: the retry resumes at `transform`, then a follow-up `report.notify` |
| `image.render` | `{"width": 1280, "height": 720}` | A worker of its own with its own dependencies, two at a time |

`demo.fetch` (it fetches any URL) and `report.fanout` (it creates up to 10,000 tasks) are
in the examples but not in the demo accounts' ACL.

## The web UI

Open https://pgtaskqueue.mmdsh.org and sign in. [ui.md](ui.md) describes every page.

1. **As `demo-viewer`**: the dashboard, the task list, a task's page with its log, and
   **Statistics**. There are no buttons; the role cannot change anything.
2. **As `demo-submitter`**: **Tasks → Enqueue**, `demo.sleep` with `{"seconds": 60}`.
   Open the task and watch its progress, then cancel it. Only your own tasks are listed.
3. **As `demo-operator`**: every task. Pause and resume the queue `bulk` and watch its
   tasks wait. Restart a failed task. Enqueue 8 `demo.sleep` tasks and see 4 run while the
   others are **held**.
4. **As `demo-manager`**: **Accounts**. Create an account with the role `submitter` and
   limits no looser than your own, then sign in as it.

## The HTTP API

The API is under `/v1`. Its interactive reference is at
https://pgtaskqueue.mmdsh.org/docs, and [api.md](api.md) describes it.

Get a token (access tokens last 15 minutes):

```bash
URL=https://pgtaskqueue.mmdsh.org
TOKEN=$(curl -s $URL/v1/auth/token -d grant_type=password \
  -d username=demo-submitter -d password=demo-password \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["access_token"])')
```

Enqueue a task, then read it until it has finished:

```bash
ID=$(curl -s $URL/v1/tasks -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name": "demo.flaky", "payload": {"fail_times": 1}}' \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["id"])')

curl -s $URL/v1/tasks/$ID -H "Authorization: Bearer $TOKEN"
curl -s $URL/v1/tasks/$ID/logs -H "Authorization: Bearer $TOKEN"
```

See the account's limits and how close it is to them:

```bash
curl -s $URL/v1/auth/limits -H "Authorization: Bearer $TOKEN"
```

### Webhooks

A task can POST its outcome to a URL when it finishes. Use a public receiver, such as a
URL from webhook.site:

```bash
curl -s $URL/v1/tasks -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name": "demo.echo", "payload": {"message": "hi"},
       "callback": {"url": "https://webhook.site/<your-id>"}}'
```

Callbacks to private and loopback addresses are refused. [api.md](api.md#webhooks) has the
body and how to check its signature.

## MCP

The MCP server is at `https://pgtaskqueue.mmdsh.org/mcp`, with the same token. In Claude
Code:

```bash
claude mcp add --transport http pgtaskqueue https://pgtaskqueue.mmdsh.org/mcp \
  --header "Authorization: Bearer $TOKEN"
```

Then ask, for example, "enqueue demo.sleep for 20 seconds and tell me when it is done".
The token expires after 15 minutes; add the server again with a new one. [mcp.md](mcp.md)
lists the tools.

## Running a demo like this

This section is for whoever hosts a demo. Two files in [examples/demo](../examples/demo)
make it:

- `seed.py`: the four accounts, their limits and ACL, the hourly reset and the
  minute-by-minute repair.
- `reset.py`: the process that runs them. It refuses to start unless
  `DEMO_RESET_DATABASE` names the database `TQ_DATABASE_URL` points at, because it empties
  the queue.

Add it to `compose.override.yml`, next to the compose services. It uses the API's image,
which has the examples in it:

```yaml
services:
  demo-reset:
    image: pgtaskqueue-api
    command: ["python", "-m", "examples.demo.reset"]
    environment:
      TQ_DATABASE_URL: postgresql+asyncpg://${POSTGRES_USER}:${POSTGRES_PASSWORD}@db:5432/${POSTGRES_DB}
      DEMO_RESET_DATABASE: ${POSTGRES_DB}
    depends_on:
      migrate:
        condition: service_completed_successfully
    restart: unless-stopped
```

It resets once when it starts, so starting or recreating the service resets at once.
`docker compose stop demo-reset` ends the cycle.

Settings the demo host also sets in `.env`, beyond the required ones:

```bash
# Defaults for an account with no limits of its own, such as one a manager made and left unset.
TQ_ACCOUNT_MAX_WAITING=30
TQ_ACCOUNT_MAX_RUNNING=3
TQ_ACCOUNT_ENQUEUE_PER_MINUTE=20
TQ_ACCOUNT_MIN_PRIORITY=100
# Requests per account, shared by everyone signed in as it, and per address.
TQ_RATE_LIMIT=1200/minute
TQ_RATE_LIMIT_ANONYMOUS=600/minute
TQ_LOGIN_RATE_LIMIT=60/minute
# Failed sign-ins are counted per account name. At the default, ten wrong passwords for a
# shared demo account would lock it for everyone for an hour.
TQ_LOGIN_FAILURE_LIMIT=1000/hour
```

Behind a proxy that does not pass on the client's address, as here, every visitor behind
one proxy address shares the per-address limits. So those limits are set loosely, and the
account limits do the work.
