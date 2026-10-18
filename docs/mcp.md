# MCP server

The API serves the queue as [Model Context Protocol](https://modelcontextprotocol.io) tools
at `/mcp`, so an AI assistant can enqueue tasks, follow them, read their logs and answer
"why is nothing running?".

- [What it is](#what-it-is)
- [Tools](#tools)
- [Connecting a client](#connecting-a-client)
- [Choosing what the assistant may do](#choosing-what-the-assistant-may-do)
- [Deployment notes](#deployment-notes)
- [Troubleshooting](#troubleshooting)

## What it is

The MCP server is mounted in the same FastAPI application as the REST API. Each tool calls
the same service method as the matching REST route, with the caller taken from the same
bearer token. An assistant can therefore do exactly what its token's role and ACL allow and
nothing more, and there is one set of permission checks, not two.

| | |
|---|---|
| Endpoint | `POST /mcp` on the API (`http://localhost:8010/mcp` with compose) |
| Transport | Streamable HTTP, stateless, JSON responses |
| Authentication | `Authorization: Bearer <access token>` on every request |
| Disable | `TQ_MCP_ENABLED=false` |

Stateless means no session is kept between requests. The endpoint behaves like any other
route behind a load balancer: any API replica can answer any request.

The OAuth discovery flow that some MCP clients offer (where the client opens a browser to
sign in) is not implemented. Clients send a token you give them.

## Tools

| Tool | Does | Changes anything |
|---|---|---|
| `list_task_types` | Task names, descriptions, payload schemas, defaults | no |
| `enqueue_task` | Add a task | yes |
| `get_task` | One task in full; `wait_seconds` waits for it to finish | no |
| `list_tasks` | Tasks as brief rows, filtered by status, name, queue, metadata | no |
| `get_task_logs` | What a task logged, by attempt | no |
| `cancel_task` | Cancel a task that has not finished | yes (marked destructive) |
| `requeue_task` | Run a finished task again | yes |
| `queue_stats` | Depth, oldest pending, throughput, p50/p95 over a window | no |
| `list_queues` | Queues with depth, pause state, workers and slots | no |
| `list_workers` | Worker processes, what they serve, slots in use | no |
| `pause_queue` | Stop workers claiming from a queue | yes |
| `resume_queue` | Undo | yes |
| `list_schedules` | [Periodic schedules](schedules.md): task, timing, next fire time, pause state | no |
| `set_schedule` | Create a schedule, or replace it whole | yes |
| `pause_schedule` | Stop a schedule firing | yes |
| `resume_schedule` | Undo | yes |
| `delete_schedule` | Delete a schedule. Tasks it enqueued stay. | yes (marked destructive) |

Each tool carries MCP annotations (`readOnlyHint`, `destructiveHint`), which clients use to
decide what needs confirmation.

Details that matter to an assistant:

- `list_task_types` is the place to start. It returns the JSON Schema each task's payload
  must match, published by the workers. `enqueue_task` validates against it and says which
  field is wrong.
- `enqueue_task` takes a `dedupe_key`. An assistant that retries a call it thinks failed
  will not create a second task if it passes one.
- `list_tasks` returns brief rows on purpose, so a list of fifty tasks does not put fifty
  payloads into the model's context. `get_task` returns one in full.
- `get_task` with `wait_seconds` (up to 30) holds the call until the task finishes, which is
  better than polling in a loop.
- A refused call returns a tool error whose message names the missing permission or the ACL
  rule, so the assistant can say why.

Not exposed as tools: account management, the remote-worker protocol, editing a queued
task, and batch enqueue. Use the REST API for those.

## Connecting a client

The client needs the URL and a token. Mint a token for a dedicated account:

```bash
docker compose exec api tq user create assistant --role viewer --generate
docker compose exec api tq token assistant --days 30
```

The second command prints the token. See
[auth.md](auth.md#long-lived-tokens-for-clients-that-cannot-refresh) for what a long-lived
token means; the short version is that it cannot be revoked before it expires, so give it
only what it needs.

### Claude Code

```bash
claude mcp add --transport http pgtaskqueue http://localhost:8010/mcp \
  --header "Authorization: Bearer $TOKEN"
```

Or in a project's `.mcp.json`, with the token taken from the environment:

```json
{
  "mcpServers": {
    "pgtaskqueue": {
      "type": "http",
      "url": "http://localhost:8010/mcp",
      "headers": { "Authorization": "Bearer ${TQ_MCP_TOKEN}" }
    }
  }
}
```

Then, in a session: "what task types are there?", "enqueue a `report.build` with 500 rows
and tell me when it is done", "why did the last failed task fail?".

### Other clients

Any client that speaks streamable HTTP and can send a custom header works with the same two
values. For a client that only launches local (stdio) servers, bridge with
[`mcp-remote`](https://www.npmjs.com/package/mcp-remote):

```json
{
  "mcpServers": {
    "pgtaskqueue": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://localhost:8010/mcp",
               "--header", "Authorization: Bearer ${TQ_MCP_TOKEN}"],
      "env": { "TQ_MCP_TOKEN": "eyJ…" }
    }
  }
}
```

### From Python

With version 2 of the `mcp` package (what the `api` extra installs):

```python
import asyncio
import contextlib

import httpx2
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

URL = "http://localhost:8010/mcp"
TOKEN = "eyJ…"


@contextlib.asynccontextmanager
async def transport():
    async with (
        httpx2.AsyncClient(headers={"Authorization": f"Bearer {TOKEN}"}, timeout=40) as http,
        streamable_http_client(URL, http_client=http) as streams,
    ):
        yield streams


async def main() -> None:
    async with Client(transport()) as client:
        tools = await client.list_tools()
        print([tool.name for tool in tools.tools])
        result = await client.call_tool("queue_stats", {"window_minutes": 60})
        print(result.structured_content)


asyncio.run(main())
```

The client API of the `mcp` package differs between major versions.
[tests/test_mcp.py](../tests/test_mcp.py) is a working client for the version this project
is tested with.

## Choosing what the assistant may do

The token decides. Pick the account's role for the job:

| Give it | And the assistant can |
|---|---|
| `viewer` | Look at everything: tasks, logs, statistics, workers. Change nothing. |
| `submitter` | Enqueue tasks and follow, cancel and requeue **its own**. |
| `operator` | Everything above for every task, and pause queues. |

Narrow further with:

- **An ACL** on the account: `--task-names 'report.*'` limits it to those tasks.
- **A scope** on the token: `tq token assistant --scope 'tasks:read stats:read'` limits one
  token below its account.

A `viewer` token is a good default. An assistant that can read logs and statistics can
answer most questions, and cannot enqueue ten thousand tasks by mistake.

Anything a task returns or logs can end up in the model's context when the assistant reads
it. If tasks handle data the assistant's operator should not see, scope the account away
from those task names.

## Deployment notes

**Host allow-list.** Set `TQ_MCP_ALLOWED_HOSTS` to the host names clients use, to reject
requests with any other `Host` header (protection against DNS rebinding):

```bash
TQ_MCP_ALLOWED_HOSTS=tasks.example.com,localhost:*
```

It is empty by default, which turns the check off. That is acceptable because every request
needs a bearer token, which a rebinding attack does not have, but set it in production
anyway.

**CORS.** A browser-based MCP client needs its origin in `TQ_CORS_ORIGINS`.

**TLS.** Serve `/mcp` over HTTPS like the rest of the API. The token is a bearer credential.

**Proxies.** Responses are plain JSON, not a long-lived event stream, so no special proxy
configuration is needed. `get_task` with `wait_seconds` holds a request for up to 30
seconds; make sure the proxy's read timeout is longer.

## Troubleshooting

**401 before any tool runs.** The token is missing, expired or signed with a different key.
`curl -s http://localhost:8010/v1/auth/me -H "Authorization: Bearer $TOKEN"` shows whether
the API accepts it and what it allows.

**The tool list is there, but a tool answers "this token lacks the … permission".** The
account's role does not include it. See the table above.

**404 on `/mcp`.** `TQ_MCP_ENABLED` is false, or the request went to a path with a trailing
segment. The endpoint is exactly `/mcp`.

**Every request is refused with 421 or 403.** `TQ_MCP_ALLOWED_HOSTS` is set and does not
include the host the client used. Include the port if it is not the default: `localhost:8010` or
`localhost:*`.

**A check with curl:**

```bash
curl -s http://localhost:8010/mcp \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```
