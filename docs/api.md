# HTTP API

The API is one of four ways to reach the queue, next to the Python `TaskQueue` class, the
`tq` command and the [MCP server](mcp.md). The [web UI](ui.md) is a client of it. It is for callers that should not have database
credentials: other services, browsers, scripts, remote workers.

- [Basics](#basics)
- [Tasks](#tasks)
- [Queues, workers and statistics](#queues-workers-and-statistics)
- [Schedules](#schedules)
- [Webhooks](#webhooks)
- [Remote workers](#remote-workers)
- [Accounts](#accounts)
- [Errors](#errors)

The interactive reference, generated from the code, is at `/docs` on a running server
(`/openapi.json` for the schema). This page explains what the reference cannot.

## Basics

```bash
tq api --host 0.0.0.0 --port 8000      # needs: pip install 'pgtaskqueue[api]'
```

With the compose file, the API is at `http://localhost:8010`.

Every route under `/v1` needs `Authorization: Bearer <access token>`. Get a token:

```bash
TOKEN=$(curl -s http://localhost:8010/v1/auth/token \
  -d grant_type=password -d username=admin -d password="$TQ_BOOTSTRAP_ADMIN_PASSWORD" \
  | python3 -c 'import sys, json; print(json.load(sys.stdin)["access_token"])')

curl -s http://localhost:8010/v1/auth/me -H "Authorization: Bearer $TOKEN"
```

The [web UI](ui.md) sends the same access token in a cookie instead; the API accepts that
from a browser, and nothing else about a request changes.

What a token may do depends on its account's **role** and **ACL**. [auth.md](auth.md)
covers both, the other grant types, and token lifetimes.

Conventions:

- Bodies are JSON, except the token endpoint, which is form-encoded as OAuth 2.0 requires.
- Unknown fields in a request body are rejected with 422, so a misspelled option is an
  error rather than silently ignored.
- Timestamps are ISO 8601 with a timezone. Ids are UUIDv7.
- A task you are not allowed to see answers **404**, the same as a task that does not exist.

Health checks need no token: `GET /health/live` (the process is up) and `GET /health/ready`
(the database answers; 503 if not).

## Tasks

| Method and path | Does | Permission |
|---|---|---|
| `POST /v1/tasks` | Enqueue one task | `tasks:enqueue` |
| `POST /v1/tasks/batch` | Enqueue up to 1,000 | `tasks:enqueue` |
| `GET /v1/tasks` | List, newest first | `tasks:read` |
| `GET /v1/tasks/{id}` | Read one; `?wait=` long-polls | `tasks:read` |
| `GET /v1/tasks/{id}/logs` | Its log lines | `tasks:read` |
| `PATCH /v1/tasks/{id}` | Change a task that has not started | `tasks:manage` |
| `POST /v1/tasks/{id}/cancel` | Cancel | `tasks:manage` |
| `POST /v1/tasks/{id}/requeue` | Run a finished task again | `tasks:manage` |
| `GET /v1/task-types` | What can be enqueued | `tasks:enqueue` or `tasks:read` |

Without `tasks:read_all` / `tasks:manage_all`, an account sees and manages only the tasks
it created.

### Enqueue

```bash
curl -s http://localhost:8010/v1/tasks \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{
    "name": "report.build",
    "payload": {"rows": 500},
    "priority": 50,
    "dedupe_key": "report-2026-10",
    "metadata": {"tenant": "acme"}
  }'
```

| Field | Default | |
|---|---|---|
| `name` | required | The task name. Names starting with `tq.` are reserved. |
| `payload` | `{}` | A JSON object, up to `TQ_MAX_PAYLOAD_BYTES` (1 MB). |
| `queue` | `default` | |
| `priority` | `100` | 0 to 32767. Lower runs first. |
| `run_after` | now | Do not start before this time. Must carry a timezone. |
| `delay_seconds` | | The same, measured from now. Give one or neither. |
| `max_attempts` | the task type's, else 3 | 1 to 100. |
| `timeout_seconds` | the task type's, else none | Per attempt. |
| `dedupe_key` | | At most one active task per queue, name and key. |
| `metadata` | `{}` | Your labels. Never interpreted; there to filter on. |
| `callback` | | Where to POST when it finishes. See [Webhooks](#webhooks). |

The answer is the task, with `201 Created`. If `dedupe_key` matched a task that is already
waiting or running, the answer is **that** task with `200 OK` and `"created": false`. This
makes a retried request harmless.

If a worker has registered the task name, the payload is validated against the JSON Schema
it published, and a mismatch is a 422 that says which field. A name no worker has registered
is accepted by default, so the API can be deployed before the workers for a new task. Set
`TQ_API_REQUIRE_REGISTERED_TASKS=true` to refuse unknown names instead.

`GET /v1/task-types` lists the registered names with their descriptions, payload schemas
and defaults.

An account may have [limits](auth.md#limits). Past its waiting or rate limit, enqueue is a
429; the rate limit adds `Retry-After` in seconds. A priority below its floor is a 403.
With a running limit, the task is written as `scheduled` with `"capped": true` and becomes
`pending` when the account has room:

```bash
curl -s http://localhost:8010/v1/auth/limits -H "Authorization: Bearer $TOKEN"
# {"username": "svc-reports",
#  "effective": {"max_waiting": 1000, "max_running": 20, "enqueue_per_minute": 600, "min_priority": 0},
#  "account":   {"max_waiting": 1000, "max_running": 20, "enqueue_per_minute": 600, "min_priority": null},
#  "defaults":  {"max_waiting": 0, "max_running": 0, "enqueue_per_minute": 0, "min_priority": 0},
#  "usage": {"waiting": 3, "held": 0, "running": 2, "last_minute": 5}}
```

### Enqueue many

```bash
curl -s http://localhost:8010/v1/tasks/batch \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{
    "tasks": [
      {"name": "demo.echo", "payload": {"message": "one"}},
      {"name": "demo.echo", "payload": {"message": "two"}}
    ]
  }'
# {"created": ["01928f…", "01928f…"], "skipped": 0}
```

One `INSERT` for the whole batch. Every task is validated before any is written, so a batch
is accepted or refused as a whole. `skipped` counts tasks whose `dedupe_key` matched an
active task. A batch counts as its length toward the account's limits; one larger than the
limit itself is a 413, since waiting would not help.

### Read and wait

```bash
curl -s "http://localhost:8010/v1/tasks/$ID" -H "Authorization: Bearer $TOKEN"
curl -s "http://localhost:8010/v1/tasks/$ID?wait=20" -H "Authorization: Bearer $TOKEN"
```

`?wait=20` holds the request until the task finishes or 20 seconds pass, whichever is
first. Check `status` to tell which. The maximum is `TQ_LONG_POLL_MAX_SECONDS` (30); loop to
wait longer. For anything that does not need the answer inline, prefer a
[webhook](#webhooks).

A task looks like this:

```json
{
  "id": "0192e3a1-5f0c-7d3e-9b1a-4c2f8e6d7a10",
  "queue": "default",
  "name": "report.build",
  "status": "succeeded",
  "priority": 50,
  "payload": {"rows": 500},
  "metadata": {"tenant": "acme"},
  "progress": {"percent": 100, "steps": {"extract": {"rows": 500, "total": 24850, "max": 100}}},
  "step": "load",
  "output": {"location": "memory://reports/0192e3a1….json", "rows": 500, "mean": 49.7, "max": 100},
  "error": null,
  "attempt": 1,
  "max_attempts": 3,
  "timeout_seconds": 300.0,
  "run_after": "2026-10-13T09:15:02.118Z",
  "dedupe_key": "report-2026-10",
  "callback": null,
  "created_by": "admin",
  "worker_id": "worker-1-17-3fa9c2",
  "spent_seconds": 0.34,
  "created_at": "2026-10-13T09:15:02.118Z",
  "started_at": "2026-10-13T09:15:02.127Z",
  "heartbeat_at": "2026-10-13T09:15:02.127Z",
  "completed_at": "2026-10-13T09:15:02.467Z"
}
```

`error`, when set, is `{type, message, attempt, worker}` plus `traceback` for callers with
`tasks:read_all`. A traceback shows file paths and code, so it is for whoever operates the
queue, not for every submitter. A `callback` is shown as `{url, events, has_secret,
header_names}`: where it goes, never the secret or header values.

### List

```bash
curl -s -G http://localhost:8010/v1/tasks -H "Authorization: Bearer $TOKEN" \
  -d status=failed -d status=canceled -d name=report.build \
  --data-urlencode 'metadata={"tenant":"acme"}' -d limit=50
```

Filters: `queue`, `name`, `status` (repeatable), `created_by`, `worker_id`, and `metadata`
(a JSON object the task's metadata must contain). `limit` is 1 to 200.

The answer is `{"items": [...], "next_before": "<id>"}`. Pass `next_before` as `before` for
the next, older page; it is `null` on the last page. Paging is by id, so it stays correct
and fast while new tasks arrive.

### Logs

```bash
curl -s "http://localhost:8010/v1/tasks/$ID/logs?after_id=0" -H "Authorization: Bearer $TOKEN"
# {"items": [{"id": 8812, "task_id": "0199…", "attempt": 1, "level": "info", "step": "extract",
#             "message": "extracted 500 rows", "data": null, "created_at": "…"}], "last_id": 8812}
```

Oldest first. To follow a running task, poll with `after_id=<last_id>`. Workers write log
lines in batches about once a second.

### Change, cancel, requeue

```bash
# Change a task nothing has claimed yet. 409 once it has started.
curl -s -X PATCH "http://localhost:8010/v1/tasks/$ID" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"priority": 10}'

# Cancel. 409 if it had already finished.
curl -s -X POST "http://localhost:8010/v1/tasks/$ID/cancel" -H "Authorization: Bearer $TOKEN"

# Run a finished task again. 409 if it has not finished.
curl -s -X POST "http://localhost:8010/v1/tasks/$ID/requeue" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"reset_progress": false}'
```

`PATCH` accepts `priority`, `run_after`, `payload`, `metadata`, `max_attempts` and
`timeout_seconds`.

Cancelling a `running` task marks it `canceled` at once. Its worker is told immediately
where LISTEN/NOTIFY is on, and at its next heartbeat otherwise (within 5 seconds by
default), and cancels the coroutine. Code the worker runs in a thread has `TaskStopped` raised in it;
see [executors.md](executors.md#stopping-a-thread) for when that arrives.

Requeue runs the task again **in place**: same id, attempts start over, and completed
[steps](executors.md#steps-resuming-instead-of-repeating) are kept unless `reset_progress`
is true.

## Queues, workers and statistics

| Method and path | Does | Permission |
|---|---|---|
| `GET /v1/queues` | Queues with depth, workers and pause state | `stats:read` |
| `POST /v1/queues/{queue}/pause` | Stop workers claiming from it | `queues:manage` |
| `POST /v1/queues/{queue}/resume` | Undo | `queues:manage` |
| `GET /v1/workers` | Alive workers; `?all=true` includes recently stopped | `workers:read` |
| `GET /v1/stats` | Depth now; throughput and timings over a window | `stats:read` |
| `GET /v1/stats/timeline` | Tasks finished over a window, in equal slices of time | `stats:read` |

`GET /v1/stats?window_minutes=60` answers, per queue and task name: how many are
`scheduled`, `pending` and `running` now, how long the oldest has waited, and for the window:
counts by outcome, average and p50/p95 execution time, average wait and average attempts.
`held` counts the `scheduled` tasks waiting on their account's
[running limit](auth.md#limits); they are included in `scheduled` too, and `/v1/queues`
reports them the same way.

`totals.oldest_pending_seconds` is the number to alert on. It is how long the task at the
head of the line has been ready and not started. A queue can be deep and healthy; it cannot
have an old head and be healthy.

`GET /v1/stats/timeline?window_minutes=60&buckets=30` cuts the window into `buckets` equal
slices (1 to 200, 30 by default) and counts what finished in each:

```bash
curl -s "http://localhost:8010/v1/stats/timeline?window_minutes=60&buckets=4" \
  -H "Authorization: Bearer $TOKEN"
# {"window_minutes": 60, "bucket_seconds": 900.0, "buckets": [
#   {"start": "2026-10-13T11:28:06.727424Z", "succeeded": 3, "failed": 0, "canceled": 0},
#   {"start": "2026-10-13T11:43:06.727424Z", "succeeded": 7, "failed": 3, "canceled": 1},
#   …]}
```

Buckets are oldest first. The last one ends at the moment of the request, by the database's
clock, so they do not fall on round times. It reads the same index as the window of
`GET /v1/stats`, and like it, it counts rows: a long window over a very large table is slow.

A paused queue keeps accepting tasks, and tasks already running finish. Pause takes an
optional `{"note": "why"}`.

All of these are filtered by the caller's ACL.

## Schedules

[Periodic tasks](schedules.md): a cron expression or an interval that enqueues a task.

| Method and path | Does | Permission |
|---|---|---|
| `GET /v1/schedules` | Every schedule the caller may see | `schedules:read` |
| `GET /v1/schedules/{name}` | One | `schedules:read` |
| `PUT /v1/schedules/{name}` | Create it, or replace it whole | `schedules:manage` |
| `POST /v1/schedules/{name}/pause` | Stop it firing | `schedules:manage` |
| `POST /v1/schedules/{name}/resume` | Undo | `schedules:manage` |
| `DELETE /v1/schedules/{name}` | Delete it. Tasks it enqueued stay. | `schedules:manage` |

```bash
curl -s -X PUT http://localhost:8010/v1/schedules/cleanup \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"task": "demo.echo", "payload": {"message": "sweep"}, "cron": "*/15 * * * *"}'
```

```json
{
  "name": "cleanup",
  "task": "demo.echo",
  "queue": "default",
  "payload": {"message": "sweep"},
  "cron": "*/15 * * * *",
  "every_seconds": null,
  "timezone": "UTC",
  "priority": null,
  "max_attempts": null,
  "timeout_seconds": null,
  "paused": false,
  "source": "api",
  "created_by": "docs-admin",
  "next_run_at": "2026-10-13T12:15:00Z",
  "last_run_at": null,
  "last_task_id": null,
  "created_at": "2026-10-13T12:01:18.775192Z",
  "updated_at": "2026-10-13T12:01:18.775192Z"
}
```

The body of `PUT`:

| Field | |
|---|---|
| `task` | Required. The task name to enqueue. |
| `cron` or `every_seconds` | Exactly one. A five-field [cron expression](schedules.md#cron-expressions), or seconds (at least 1). |
| `timezone` | The IANA zone for `cron`. `UTC` if left out. |
| `payload` | Checked against the task's published schema, as for enqueue. |
| `queue`, `priority`, `max_attempts`, `timeout_seconds` | As for enqueue. Left out, the task's defaults apply. |

`PUT` answers 201 when it created the schedule and 200 when it replaced one. Replacing is
whole: a field left out goes back to its default. The pause state is kept, and so is
`next_run_at` unless `cron`, `every_seconds` or `timezone` changed.

`source` is `code` for a schedule that a worker published from its executors, and `api`
otherwise. A `code` schedule can be paused, resumed and deleted here, but not replaced:
`PUT` on its name answers 409. A deleted `code` schedule comes back when a worker that
declares it starts.

The caller's ACL covers a schedule through its queue and its task name. A schedule outside
it is not listed and answers 404. `PUT` answers 403 for a task or queue outside the ACL, and
409 for a name held by a schedule the caller may not see.

## Webhooks

A task with a `callback` gets an HTTP POST when it finishes:

```json
{
  "name": "report.build",
  "payload": {"rows": 500},
  "callback": {
    "url": "https://app.example.com/hooks/tasks",
    "secret": "whsec_3f1c9a7e5b2d",
    "events": ["task.succeeded", "task.failed"],
    "headers": {"X-Tenant": "acme"}
  }
}
```

| Field | |
|---|---|
| `url` | Required. `http` or `https`, no credentials in the URL. |
| `secret` | Signs the request. 8 to 256 characters. Falls back to `TQ_WEBHOOK_SECRET`. |
| `events` | Which outcomes to report: `task.succeeded`, `task.failed`, `task.canceled`. Default: all. |
| `headers` | Up to 20 extra request headers. |

### The request

```http
POST /hooks/tasks HTTP/1.1
Content-Type: application/json
User-Agent: pgtaskqueue/0.1.0
X-TaskQueue-Event: task.succeeded
X-TaskQueue-Delivery: 0192e3a1-6a4f-7c11-8e02-91b7d5c3e4f2
X-TaskQueue-Task: 0192e3a1-5f0c-7d3e-9b1a-4c2f8e6d7a10
X-TaskQueue-Signature: t=1791882902,v1=5d41402abc4b2a76b9719d911017c592…

{"event":"task.succeeded","delivery_id":"0192e3a1-6a4f-…","attempt":1,
 "sent_at":"2026-10-13T09:15:02.471+00:00","task":{ … the task, as GET returns it … }}
```

The `task` object is the same as `GET /v1/tasks/{id}` returns, without the error traceback.

### Delivery

A delivery is itself a task (`tq.webhook`), written in the same transaction that finished
the task it reports. So it is durable the moment the result is, and it is retried:

| The receiver answers | Then |
|---|---|
| 2xx | Delivered. |
| 408, 425, 429, or 5xx | Retried with backoff. `Retry-After` is honored, up to an hour. |
| Any other status, including a redirect | Not retried. |
| No answer within `TQ_WEBHOOK_TIMEOUT_SECONDS` (10s), or a connection error | Retried. |

Up to `TQ_WEBHOOK_MAX_ATTEMPTS` (6) in total. Redirects are not followed.

Delivery is **at least once**. `X-TaskQueue-Delivery` is the same on every retry of one
delivery, so store it and ignore repeats. Answer quickly with a 2xx and do slow work
afterwards.

Each delivery is a row you can inspect like any other task:

```bash
curl -s -G http://localhost:8010/v1/tasks -H "Authorization: Bearer $TOKEN" \
  -d name=tq.webhook --data-urlencode "metadata={\"for_task\":\"$ID\"}"
```

### Verifying the signature

`X-TaskQueue-Signature: t=<unix seconds>,v1=<hex>` where

```
v1 = HMAC-SHA256(key = secret, message = "<t>." + <raw request body>)
```

```python
import hashlib, hmac, time


def verify(secret: str, body: bytes, header: str, tolerance: float = 300) -> bool:
    try:
        parts = dict(item.split("=", 1) for item in header.split(","))
        timestamp = int(parts["t"])
    except (KeyError, ValueError):
        return False
    if abs(time.time() - timestamp) > tolerance:  # a replayed old request
        return False
    expected = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
    return hmac.compare_digest(expected.hexdigest(), parts.get("v1", ""))
```

Three things to get right:

- Verify the **raw bytes** you received, not a re-serialized copy of the parsed JSON.
- Compare in constant time (`hmac.compare_digest`).
- Reject old timestamps, or a recorded request can be replayed forever.

[examples/webhook_receiver.py](../examples/webhook_receiver.py) is a complete receiver using
only the standard library. Without a secret (neither on the callback nor
`TQ_WEBHOOK_SECRET`), requests are sent unsigned.

### Where callbacks may go

A callback URL is supplied by whoever enqueues the task and is fetched from inside your
network. By default, workers refuse URLs whose host resolves to a loopback, private or
link-local address, so a submitter cannot aim a worker at an internal service or a cloud
metadata endpoint. Set `TQ_WEBHOOK_ALLOW_PRIVATE=true` on the workers if your receivers are
on a private network, as they are when calling another compose service.

This check guards against mistakes and casual misuse. It does not stop an attacker who
controls DNS for the hostname. If you accept callback URLs from untrusted parties, send
worker egress through a proxy that enforces the rule. See [SECURITY.md](../SECURITY.md).

## Remote workers

For a worker that cannot connect to PostgreSQL: a GPU machine in another network, a
customer's server, a worker written in another language. It does over HTTP what `tq worker`
does over SQL, with the same guarantees, because each call runs the same conditional
statement.

It needs an account with the `worker` role, which can do nothing else:

```bash
tq user create gpu-1 --service --role worker --task-names 'render.*' --generate
```

[examples/remote_worker.py](../examples/remote_worker.py) is a complete worker in about 150
lines of plain `httpx`, and is the best starting point for a port.

### The loop

```
sign in ──► publish task types ──► ┌─► claim (long poll) ──► run ──► succeed / fail ─┐
                                   └───────────────────────────────────────────────────┘
                 every few seconds: heartbeat with every task in hand
                 on exit: release
```

| Method and path | Body | Answer |
|---|---|---|
| `PUT /v1/worker/{worker_id}/task-types` | `[{name, description?, payload_schema?, max_attempts?, timeout_seconds?}]` | 204 |
| `POST /v1/worker/claim` | `{worker_id, names, queue?, limit?, wait_seconds?}` | `[task, …]` |
| `POST /v1/worker/heartbeat` | `{worker_id, task_ids}` | `{"owned": [ids]}` |
| `POST /v1/worker/tasks/{id}/succeed` | `{worker_id, attempt, output?}` | `{accepted, status}` |
| `POST /v1/worker/tasks/{id}/fail` | `{worker_id, attempt, error, retry?, retry_in?}` | `{accepted, status}` |
| `POST /v1/worker/tasks/{id}/progress` | `{worker_id, attempt, progress?, step?}` | `{accepted, status}` |
| `POST /v1/worker/tasks/{id}/logs` | `{worker_id, attempt, lines: [{message, level?, step?, data?}]}` | 204 |
| `POST /v1/worker/release` | `{worker_id, task_ids?}` | `{"released": n}` |

### Rules

**`worker_id`** is a name you choose for this process, such as hostname plus pid. Use the
same one in every call. On the server it is namespaced by account
(`http:<account>:<worker_id>`), so one account's worker cannot settle a task claimed by
another's.

**Claim** returns tasks already `running` and owned by you. `limit` is 1 to 100.
`wait_seconds` holds the request open while nothing is ready, up to
`TQ_LONG_POLL_MAX_SECONDS` (30), so an idle worker costs one request per long poll and still
starts work promptly. Set your HTTP client's timeout above it.

**Keep each task's `attempt`** from the claim and send it with every later call. It is the
fencing token: if the task was taken from you and claimed again, your writes carry a stale
attempt and are refused.

**Heartbeat** every few seconds with every task you hold, well inside
`TQ_STALE_AFTER_SECONDS` (60). The answer lists the tasks you still own. **Stop working on
any that are missing**: it was canceled or given to another worker.

**`accepted: false`** on succeed, fail or progress means the task was no longer yours.
Nothing was written. Drop the result.

**Fail** reports a failed attempt, with `error` as `{type, message, traceback?}`. The server
decides between retrying and failing for good from the task's `max_attempts`, and `status`
in the answer says which (`scheduled` or `pending` for a retry, `failed` otherwise). Send
`"retry": false` to fail it for good, or `"retry_in": 30` to choose the delay.

**Release** on shutdown hands back unfinished tasks without using up their attempt. Without
`task_ids`, everything the worker holds.

**ACL.** A worker account can claim and register only task names its ACL allows, on queues
its ACL allows. `tq.` names, including webhook deliveries, cannot be claimed over HTTP.

### What a remote worker does not do

Housekeeping runs inside `tq worker` processes: promoting scheduled tasks, recovering tasks
whose worker died, pruning old ones, delivering webhooks. A deployment whose only task
workers are remote must still run one ordinary `tq worker` next to the database. It needs no
executors: with `TQ_WEBHOOK_SLOTS` at 1 or more it starts, does the housekeeping and
delivers webhooks.

Remote workers do not appear in `GET /v1/workers`. Their tasks carry
`worker_id = http:<account>:<worker_id>`.

## Accounts

| Method and path | Does | Who |
|---|---|---|
| `POST /v1/auth/token` | Get a token (three grant types) | anyone with credentials |
| `GET /v1/auth/me` | What the presented token allows | any token |
| `POST /v1/auth/logout` | Revoke the account's refresh tokens | any token |
| `GET /v1/auth/limits` | The account's own limits and usage | any token |
| `GET /v1/roles` | Each role, its permissions, and whether this account may give it (`assignable`) | `users:manage` |
| `GET /v1/users` | List accounts, each with `manageable` | `users:manage` |
| `POST /v1/users` | Create an account | `users:manage` |
| `PATCH /v1/users/{username}` | Change role, ACL, limits, or disable | `users:manage` |
| `GET /v1/users/{username}/limits` | An account's limits and usage | `users:manage` |
| `POST /v1/users/{username}/password` | Set a password | `users:manage`, or the account itself |
| `DELETE /v1/users/{username}` | Delete an account | `users:manage` |

`users:manage` reaches only accounts whose role and ACL are within the caller's own; others
are a 403. In a `PATCH`, a limit set to `null` goes back to the server's default.
[auth.md](auth.md#who-may-manage-whom) explains each rule.

## Errors

| Status | Meaning |
|---|---|
| 400 | A malformed token request (OAuth error format, below). |
| 401 | No token, or one that is expired or invalid. |
| 403 | The token lacks the permission, or the queue or task name is outside its ACL, or the priority is below the account's floor. |
| 404 | No such task or schedule, or one this token may not see. |
| 409 | The task is not in a state that allows this: cancel after it finished, edit after it started, requeue before it finished. Or the schedule name is taken by one that cannot be replaced. |
| 413 | The payload is larger than `TQ_MAX_PAYLOAD_BYTES`, or a batch is larger than the account's waiting or rate limit. |
| 422 | The request is not valid: an unknown field, a bad value, or a payload that does not match the task's schema. |
| 429 | The account is at its waiting or rate [limit](auth.md#limits), or a [request rate limit](auth.md#request-rate-limits) refused the request. Both rate limits send `Retry-After`. |
| 503 | `/health/ready`: the database is not answering. Elsewhere: Redis is down and `TQ_RATE_LIMIT_FAIL_OPEN` is off. |

Errors are `{"detail": "…"}`, with a message meant to be read. Validation errors from the
framework put a list in `detail`, one entry per field.

The token endpoint uses the OAuth 2.0 format instead:

```json
{"error": "invalid_grant", "error_description": "wrong username or password"}
```
