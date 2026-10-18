"""The MCP server: the queue as tools for an AI assistant, served at `/mcp`.

Each tool is a thin wrapper over the same `Service` method the REST route calls, with the
caller taken from the same bearer token. So an assistant can do exactly what its token's
role and ACL allow and nothing else — give it a `viewer` account and it can look around;
give it `operator` and it can cancel and requeue.

Transport is streamable HTTP, stateless: every request carries its own token and is
answered on its own, so the endpoint scales and restarts like any other route. The OAuth
discovery flow some MCP clients offer is not implemented; clients send a token they were
given (`Authorization: Bearer …`). docs/mcp.md has client configuration.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from pydantic import ValidationError
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from pgtaskqueue._version import __version__
from pgtaskqueue.api.deps import AppState
from pgtaskqueue.api.schemas import ScheduleIn, TaskCreate
from pgtaskqueue.api.service import ApiError
from pgtaskqueue.auth.principal import Principal
from pgtaskqueue.auth.roles import Permission
from pgtaskqueue.auth.tokens import TokenError
from pgtaskqueue.models import ScheduleRow, Task
from pgtaskqueue.schemas import ScheduleOut, TaskLogOut, TaskOut
from pgtaskqueue.settings import Settings

MCP_PATH = "/mcp"

INSTRUCTIONS = """\
A PostgreSQL-backed task queue. Tasks have a `name` (which executor runs them), a JSON
`payload`, and a status: scheduled → pending → running → succeeded | failed | canceled.

Start with `list_task_types` to see what can be run and what payload each task expects.
`enqueue_task` returns an id; `get_task` (optionally with `wait_seconds`) follows it, and
`get_task_logs` shows what it wrote while running. `queue_stats` answers "is anything
stuck": look at `oldest_pending_seconds` and the failed counts.

Periodic tasks are schedules (`list_schedules`): a cron expression or an interval that
enqueues a task each time it fires. `set_schedule` creates or replaces one.

What you may see and do depends on the token this connection uses; a refused tool call says
which permission is missing.
"""

READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)
WRITES = ToolAnnotations(readOnlyHint=False, destructiveHint=False, openWorldHint=False)
DESTRUCTIVE = ToolAnnotations(readOnlyHint=False, destructiveHint=True, openWorldHint=False)


def _bearer(header: str | None) -> str | None:
    scheme, _, token = (header or "").partition(" ")
    return token.strip() if scheme.lower() == "bearer" and token.strip() else None


def _brief(task: Task) -> dict[str, Any]:
    """A task in a list: enough to choose one, without every payload in the context."""
    return {
        "id": str(task.id),
        "queue": task.queue_name,
        "name": task.name,
        "status": task.status,
        "attempt": f"{task.attempt}/{task.max_attempts}",
        "step": task.step,
        "created_at": task.created_at.isoformat(),
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        "error": (task.error or {}).get("message"),
    }


def build_mcp(get_state: Callable[[], AppState]) -> MCPServer:
    mcp = MCPServer(
        name="pgtaskqueue",
        title="Task Queue",
        instructions=INSTRUCTIONS,
        version=__version__,
    )

    def caller(ctx: Context) -> Principal:
        # Read from this request's own headers every time. The connection has no memory
        # of who called last, so there is nothing for one caller to inherit from another.
        token = _bearer((ctx.headers or {}).get("authorization"))
        if token is None:
            raise ToolError("missing bearer token")
        try:
            return get_state().tokens.principal(token)
        except TokenError as exc:
            raise ToolError(str(exc)) from exc

    def detail(task: Task, who: Principal) -> dict[str, Any]:
        full = TaskOut.of(task, traceback=who.has(Permission.TASKS_READ_ALL))
        return full.model_dump(mode="json", exclude_none=True)

    async def run(operation: Any) -> Any:
        try:
            return await operation
        except ApiError as exc:
            raise ToolError(exc.detail) from exc

    def task_id(value: str) -> uuid.UUID:
        try:
            return uuid.UUID(value)
        except ValueError as exc:
            raise ToolError(f"{value!r} is not a task id") from exc

    @mcp.tool(title="List task types", annotations=READ_ONLY)
    async def list_task_types(ctx: Context) -> list[dict[str, Any]]:
        """The task names workers have registered, each with its description, the JSON
        Schema its payload must match, and its default attempts and timeout. Call this
        before enqueue_task to learn what a task expects."""
        types = await run(get_state().service.task_types(caller(ctx)))
        return [
            {
                "name": t.name,
                "description": t.description,
                "payload_schema": t.payload_schema,
                "max_attempts": t.max_attempts,
                "timeout_seconds": t.timeout_seconds,
            }
            for t in types
        ]

    @mcp.tool(title="Enqueue a task", annotations=WRITES)
    async def enqueue_task(
        ctx: Context,
        name: str,
        payload: dict[str, Any] | None = None,
        queue: str = "default",
        priority: int | None = None,
        delay_seconds: float | None = None,
        dedupe_key: str | None = None,
        metadata: dict[str, Any] | None = None,
        max_attempts: int | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Add a task to the queue and return it (including its `id`).

        `priority`: lower runs first (default 100). `delay_seconds`: do not start before
        then. `dedupe_key`: if a task with the same name and key is already waiting or
        running, that task is returned instead and `created` is false — use it to make a
        retry of this call harmless. `metadata`: labels to filter by later.
        """
        who = caller(ctx)
        try:
            request = TaskCreate(
                name=name,
                payload=payload or {},
                queue=queue,
                priority=priority,
                delay_seconds=delay_seconds,
                dedupe_key=dedupe_key,
                metadata=metadata or {},
                max_attempts=max_attempts,
                timeout_seconds=timeout_seconds,
            )
        except ValidationError as exc:
            raise ToolError(str(exc)) from exc
        task = await run(get_state().service.enqueue(who, request))
        return {**detail(task, who), "created": task.created}

    @mcp.tool(title="Get a task", annotations=READ_ONLY)
    async def get_task(ctx: Context, id: str, wait_seconds: float = 0) -> dict[str, Any]:
        """One task in full: status, payload, progress, output, error, timings.

        With `wait_seconds` (up to 30) the call holds on until the task finishes or the
        time is up — check `status` to see which. Prefer this to polling in a loop.
        """
        who = caller(ctx)
        task = await run(get_state().service.get_task(who, task_id(id), wait_seconds))
        return detail(task, who)

    @mcp.tool(title="List tasks", annotations=READ_ONLY)
    async def list_tasks(
        ctx: Context,
        status: list[str] | None = None,
        name: str | None = None,
        queue: str | None = None,
        metadata: dict[str, Any] | None = None,
        limit: int = 20,
        before: str | None = None,
    ) -> dict[str, Any]:
        """Tasks, newest first, as brief rows (use get_task for one in full).

        `status`: any of scheduled, pending, running, succeeded, failed, canceled.
        `metadata`: only tasks whose metadata contains these keys and values.
        `before`: the `next_before` of the previous call, for the next page.
        """
        limit = max(1, min(limit, 100))
        tasks = await run(
            get_state().service.list_tasks(
                caller(ctx),
                status=status,
                name=name,
                queue=queue,
                metadata=metadata,
                limit=limit,
                before=task_id(before) if before else None,
            )
        )
        return {
            "tasks": [_brief(t) for t in tasks],
            "next_before": str(tasks[-1].id) if len(tasks) == limit else None,
        }

    @mcp.tool(title="Get a task's log", annotations=READ_ONLY)
    async def get_task_logs(
        ctx: Context, id: str, after_id: int = 0, limit: int = 200
    ) -> dict[str, Any]:
        """What a task logged while running, oldest first: its own messages, each failed
        attempt and why, retries, cancellation. `after_id`: only lines after that log id
        (pass the returned `last_id` to read on)."""
        logs = await run(
            get_state().service.task_logs(
                caller(ctx), task_id(id), after_id=after_id, limit=max(1, min(limit, 1000))
            )
        )
        lines = [
            TaskLogOut.of(line).model_dump(mode="json", exclude_none=True, exclude={"task_id"})
            for line in logs
        ]
        return {"lines": lines, "last_id": logs[-1].id if logs else after_id}

    @mcp.tool(title="Cancel a task", annotations=DESTRUCTIVE)
    async def cancel_task(ctx: Context, id: str) -> dict[str, Any]:
        """Cancel a task that has not finished. A running task is interrupted within a few
        seconds. Fails if the task had already finished."""
        who = caller(ctx)
        return detail(await run(get_state().service.cancel(who, task_id(id))), who)

    @mcp.tool(title="Requeue a task", annotations=WRITES)
    async def requeue_task(ctx: Context, id: str, reset_progress: bool = False) -> dict[str, Any]:
        """Run a finished (failed, canceled or succeeded) task again under the same id.
        Steps that completed are skipped unless `reset_progress` is true."""
        who = caller(ctx)
        task = await run(get_state().service.requeue(who, task_id(id), reset_progress))
        return detail(task, who)

    @mcp.tool(title="Queue statistics", annotations=READ_ONLY)
    async def queue_stats(ctx: Context, window_minutes: int = 60) -> dict[str, Any]:
        """Per queue and task name: how many are scheduled, pending and running now, how
        long the oldest pending task has waited, and over the last `window_minutes` how
        many succeeded, failed and were canceled, with p50/p95 run time."""
        return await run(get_state().service.stats(caller(ctx), max(1, min(window_minutes, 10080))))

    @mcp.tool(title="List queues", annotations=READ_ONLY)
    async def list_queues(ctx: Context) -> list[dict[str, Any]]:
        """Each queue with its depth, whether it is paused, and how many workers and slots
        are serving it. A queue with pending tasks and no workers is the usual reason
        tasks are not starting."""
        return await run(get_state().service.queues(caller(ctx)))

    @mcp.tool(title="List workers", annotations=READ_ONLY)
    async def list_workers(ctx: Context, include_stopped: bool = False) -> list[dict[str, Any]]:
        """Worker processes: which queue and task names each serves, slots in use, and
        counters since it started."""
        workers = await run(get_state().service.workers(caller(ctx), not include_stopped))
        return [
            {
                **w,
                **{
                    key: w[key].isoformat() if w[key] else None
                    for key in ("started_at", "heartbeat_at", "stopped_at")
                },
            }
            for w in workers
        ]

    def schedule_out(schedule: ScheduleRow) -> dict[str, Any]:
        return ScheduleOut.of(schedule).model_dump(mode="json", exclude_none=True)

    @mcp.tool(title="List schedules", annotations=READ_ONLY)
    async def list_schedules(ctx: Context) -> list[dict[str, Any]]:
        """Periodic schedules: which task each enqueues, on what cron expression or
        interval, when it fires next, when it last did and the task that produced
        (`last_task_id`, for get_task). `source` is `code` for one declared next to its
        executor and `api` for one created with set_schedule."""
        schedules = await run(get_state().service.schedules(caller(ctx)))
        return [schedule_out(schedule) for schedule in schedules]

    @mcp.tool(title="Create or replace a schedule", annotations=WRITES)
    async def set_schedule(
        ctx: Context,
        name: str,
        task: str,
        cron: str | None = None,
        every_seconds: float | None = None,
        timezone: str = "UTC",
        payload: dict[str, Any] | None = None,
        queue: str = "default",
        priority: int | None = None,
        max_attempts: int | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Make `task` run periodically, under the schedule name `name`. Replaces a
        schedule of that name whole; `created` says whether it was new.

        Give `cron` (five fields, `0 3 * * *`, or `@hourly`, `@daily`, …; read in
        `timezone`, an IANA name) or `every_seconds`, not both. Each fire enqueues one
        task with `payload`. A fire is skipped while the previous task of the schedule is
        still waiting or running. A schedule whose `source` is `code` cannot be replaced.
        """
        who = caller(ctx)
        try:
            request = ScheduleIn(
                task=task,
                payload=payload or {},
                queue=queue,
                cron=cron,
                every_seconds=every_seconds,
                timezone=timezone,
                priority=priority,
                max_attempts=max_attempts,
                timeout_seconds=timeout_seconds,
            )
        except ValidationError as exc:
            raise ToolError(str(exc)) from exc
        schedule = await run(get_state().service.put_schedule(who, name, request))
        return {**schedule_out(schedule), "created": schedule.created}

    @mcp.tool(title="Pause a schedule", annotations=WRITES)
    async def pause_schedule(ctx: Context, name: str) -> dict[str, Any]:
        """Stop a schedule firing until resume_schedule. Tasks it already enqueued are
        not touched."""
        return schedule_out(
            await run(get_state().service.set_schedule_paused(caller(ctx), name, True))
        )

    @mcp.tool(title="Resume a schedule", annotations=WRITES)
    async def resume_schedule(ctx: Context, name: str) -> dict[str, Any]:
        """Undo pause_schedule. If a fire time passed while it was paused, it fires once
        straight away."""
        return schedule_out(
            await run(get_state().service.set_schedule_paused(caller(ctx), name, False))
        )

    @mcp.tool(title="Delete a schedule", annotations=DESTRUCTIVE)
    async def delete_schedule(ctx: Context, name: str) -> dict[str, Any]:
        """Delete a schedule; tasks it already enqueued stay. A schedule whose `source`
        is `code` comes back when a worker that declares it starts, so pause that kind."""
        await run(get_state().service.delete_schedule(caller(ctx), name))
        return {"deleted": name}

    @mcp.tool(title="Pause a queue", annotations=WRITES)
    async def pause_queue(ctx: Context, queue: str, note: str | None = None) -> dict[str, Any]:
        """Stop workers taking new tasks from a queue. Tasks can still be enqueued and
        running tasks finish. `note` records why, for whoever looks next."""
        return await run(get_state().service.set_paused(caller(ctx), queue, True, note))

    @mcp.tool(title="Resume a queue", annotations=WRITES)
    async def resume_queue(ctx: Context, queue: str) -> dict[str, Any]:
        """Undo pause_queue."""
        return await run(get_state().service.set_paused(caller(ctx), queue, False))

    return mcp


def transport_security(settings: Settings) -> TransportSecuritySettings:
    """DNS-rebinding protection for the MCP endpoint.

    The SDK's own default only admits `localhost`, which is right for a server on a
    developer's machine and wrong behind a domain name. So the allowed hosts are a setting,
    and without one the check is off — the bearer token, not the Host header, is what
    authenticates a caller here.
    """
    hosts = settings.mcp_allowed_hosts
    if not hosts:
        return TransportSecuritySettings(enable_dns_rebinding_protection=False)
    origins = [f"{scheme}://{host}" for host in hosts for scheme in ("http", "https")]
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True, allowed_hosts=hosts, allowed_origins=origins
    )


class BearerGate:
    """Refuses MCP requests without a valid token before they reach the MCP machinery.

    The tools check the token again for *who* is calling; this is what turns "no token"
    into a plain 401 that a client can act on, instead of a tool error after a handshake.
    """

    def __init__(self, app: ASGIApp, get_state: Callable[[], AppState]) -> None:
        self.app = app
        self.get_state = get_state

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Only the MCP endpoint: this app is mounted at the root, so every path that no API
        # route matched arrives here too, and those should get the 404 they deserve.
        gated = scope["type"] == "http" and scope["path"].rstrip("/").endswith(MCP_PATH)
        if gated and scope["method"] != "OPTIONS":
            headers = dict(scope["headers"])
            token = _bearer(headers.get(b"authorization", b"").decode("latin-1"))
            error = "missing bearer token"
            if token is not None:
                try:
                    self.get_state().tokens.principal(token)
                    error = ""
                except TokenError as exc:
                    error = str(exc)
            if error:
                challenge = "Bearer" if token is None else 'Bearer error="invalid_token"'
                response = JSONResponse(
                    {"detail": error}, status_code=401, headers={"WWW-Authenticate": challenge}
                )
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)
