"""The MCP endpoint, driven by the MCP SDK's own client over streamable HTTP."""

from __future__ import annotations

import contextlib

import httpx2
import pytest
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

from pgtaskqueue import Monitor
from tests.helpers import make_settings, serving

JSONRPC = {"Accept": "application/json, text/event-stream"}
LIST_TOOLS = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}


@contextlib.asynccontextmanager
async def mcp(api, headers: dict[str, str]):
    """An MCP client session over the API, in-process, authenticated by `headers`."""

    @contextlib.asynccontextmanager
    async def transport():
        async with (
            httpx2.AsyncClient(
                transport=httpx2.ASGITransport(app=api.app), headers=headers, timeout=30
            ) as http,
            streamable_http_client("http://api/mcp", http_client=http) as streams,
        ):
            yield streams

    async with Client(transport()) as client:
        yield client


@pytest.fixture
async def echo_type(db):
    await Monitor(db).register_task_types(
        [
            {
                "name": "demo.echo",
                "description": "Returns its message.",
                "payload_schema": {
                    "type": "object",
                    "properties": {"message": {"type": "string"}},
                    "required": ["message"],
                },
                "max_attempts": 3,
                "timeout_seconds": None,
            }
        ],
        registered_by="worker-1",
    )


async def test_tools_are_listed_with_their_hints(api, auth):
    async with mcp(api, auth("olga", "operator")) as client:
        assert client.server_info.name == "pgtaskqueue"
        tools = {tool.name: tool for tool in (await client.list_tools()).tools}
    assert set(tools) == {
        "list_task_types",
        "enqueue_task",
        "get_task",
        "list_tasks",
        "get_task_logs",
        "cancel_task",
        "requeue_task",
        "queue_stats",
        "list_queues",
        "list_workers",
        "pause_queue",
        "resume_queue",
        "list_schedules",
        "set_schedule",
        "pause_schedule",
        "resume_schedule",
        "delete_schedule",
    }
    assert tools["get_task"].annotations.read_only_hint is True
    assert tools["cancel_task"].annotations.destructive_hint is True
    assert tools["list_schedules"].annotations.read_only_hint is True
    assert tools["delete_schedule"].annotations.destructive_hint is True
    assert tools["set_schedule"].input_schema["required"] == ["name", "task"]
    assert tools["enqueue_task"].annotations.read_only_hint is False
    # The caller's token is not a tool argument.
    assert "ctx" not in tools["enqueue_task"].input_schema["properties"]
    assert tools["enqueue_task"].input_schema["required"] == ["name"]
    assert all(tool.description for tool in tools.values())


async def test_an_assistant_can_run_and_follow_a_task(api, auth, queue, echo_type):
    async with mcp(api, auth("olga", "operator")) as client:
        types = await client.call_tool("list_task_types", {})
        (kind,) = types.structured_content["result"]
        assert kind["name"] == "demo.echo" and kind["payload_schema"]["required"] == ["message"]

        created = await client.call_tool(
            "enqueue_task",
            {"name": "demo.echo", "payload": {"message": "hi"}, "metadata": {"via": "mcp"}},
        )
        assert not created.is_error
        task = created.structured_content
        assert (task["status"], task["created"], task["created_by"]) == ("pending", True, "olga")

        (claimed,) = await queue.claim(["demo.echo"], worker_id="w")
        await queue.append_logs([{"task_id": claimed.id, "attempt": 1, "message": "echoing"}])
        await queue.succeed(claimed.id, worker_id="w", attempt=1, output={"message": "hi"})

        got = await client.call_tool("get_task", {"id": task["id"], "wait_seconds": 5})
        assert got.structured_content["status"] == "succeeded"
        assert got.structured_content["output"] == {"message": "hi"}

        logs = await client.call_tool("get_task_logs", {"id": task["id"]})
        assert [line["message"] for line in logs.structured_content["lines"]] == ["echoing"]

        listed = await client.call_tool(
            "list_tasks", {"status": ["succeeded"], "metadata": {"via": "mcp"}}
        )
        (row,) = listed.structured_content["tasks"]
        assert (row["id"], row["attempt"]) == (task["id"], "1/3")
        assert listed.structured_content["next_before"] is None

        again = await client.call_tool("requeue_task", {"id": task["id"]})
        assert again.structured_content["status"] == "pending"
        canceled = await client.call_tool("cancel_task", {"id": task["id"]})
        assert canceled.structured_content["status"] == "canceled"

        stats = await client.call_tool("queue_stats", {"window_minutes": 10})
        assert stats.structured_content["totals"]["canceled"] == 1

        paused = await client.call_tool("pause_queue", {"queue": "default", "note": "by mcp"})
        assert paused.structured_content["paused"] is True
        queues = await client.call_tool("list_queues", {})
        assert queues.structured_content["result"][0]["note"] == "by mcp"
        resumed = await client.call_tool("resume_queue", {"queue": "default"})
        assert resumed.structured_content["paused"] is False
        assert (await client.call_tool("list_workers", {})).structured_content == {"result": []}


async def test_mistakes_come_back_as_tool_errors_the_model_can_read(api, auth, echo_type):
    async with mcp(api, auth("olga", "operator")) as client:
        for tool, arguments, complaint in (
            ("enqueue_task", {"name": "demo.echo", "payload": {"message": 5}}, "does not match"),
            ("enqueue_task", {"name": "has spaces"}, "name"),
            ("get_task", {"id": "not-a-uuid"}, "is not a task id"),
            ("get_task", {"id": "00000000-0000-7000-8000-000000000000"}, "no such task"),
        ):
            result = await client.call_tool(tool, arguments)
            assert result.is_error, (tool, arguments)
            assert complaint in result.content[0].text


async def test_tools_do_what_the_token_allows_and_no_more(api, auth, queue):
    hidden = await queue.enqueue("billing.charge", created_by="someone")
    visible = await queue.enqueue("mail.send", created_by="someone")

    async with mcp(api, auth("vic", "viewer", task_names=["mail.*"])) as client:
        listed = await client.call_tool("list_tasks", {})
        assert [row["id"] for row in listed.structured_content["tasks"]] == [str(visible.id)]
        assert (await client.call_tool("get_task", {"id": str(hidden.id)})).is_error

        for tool, arguments, permission in (
            ("enqueue_task", {"name": "mail.send"}, "tasks:enqueue"),
            ("pause_queue", {"queue": "default"}, "queues:manage"),
        ):
            result = await client.call_tool(tool, arguments)
            assert result.is_error
            assert f"lacks the '{permission}' permission" in result.content[0].text
        refused = await client.call_tool("cancel_task", {"id": str(visible.id)})
        assert refused.is_error and "may not change this task" in refused.content[0].text

    assert (await queue.get(visible.id)).status == "pending"

    # A submitter's assistant sees only what that submitter queued.
    async with mcp(api, auth("alice")) as client:
        mine = await client.call_tool("enqueue_task", {"name": "mail.send"})
        listed = await client.call_tool("list_tasks", {})
        assert [row["id"] for row in listed.structured_content["tasks"]] == [
            mine.structured_content["id"]
        ]
        assert (await client.call_tool("queue_stats", {})).is_error


async def test_an_assistant_can_manage_schedules(api, auth, echo_type):
    async with mcp(api, auth("olga", "operator")) as client:
        made = await client.call_tool(
            "set_schedule",
            {
                "name": "hello",
                "task": "demo.echo",
                "cron": "*/5 * * * *",
                "timezone": "Europe/Berlin",
                "payload": {"message": "hi"},
            },
        )
        assert not made.is_error, made.content
        schedule = made.structured_content
        assert (schedule["created"], schedule["task"], schedule["source"]) == (
            True,
            "demo.echo",
            "api",
        )
        assert (schedule["cron"], schedule["created_by"]) == ("*/5 * * * *", "olga")

        again = await client.call_tool(
            "set_schedule", {"name": "hello", "task": "demo.echo", "every_seconds": 600}
        )
        assert again.is_error and "does not match the schema" in again.content[0].text
        again = await client.call_tool(
            "set_schedule",
            {
                "name": "hello",
                "task": "demo.echo",
                "every_seconds": 600,
                "payload": {"message": "x"},
            },
        )
        assert (again.structured_content["created"], again.structured_content["every_seconds"]) == (
            False,
            600.0,
        )

        paused = await client.call_tool("pause_schedule", {"name": "hello"})
        assert paused.structured_content["paused"] is True
        (listed,) = (await client.call_tool("list_schedules", {})).structured_content["result"]
        assert (listed["name"], listed["paused"]) == ("hello", True)
        resumed = await client.call_tool("resume_schedule", {"name": "hello"})
        assert resumed.structured_content["paused"] is False

        for arguments, complaint in (
            ({"name": "x", "task": "demo.echo"}, "cron or every_seconds"),
            ({"name": "x", "task": "demo.other", "cron": "nonsense"}, "5 fields"),
        ):
            result = await client.call_tool("set_schedule", arguments)
            assert result.is_error and complaint in result.content[0].text

        deleted = await client.call_tool("delete_schedule", {"name": "hello"})
        assert deleted.structured_content == {"deleted": "hello"}
        gone = await client.call_tool("pause_schedule", {"name": "hello"})
        assert gone.is_error and "no such schedule" in gone.content[0].text

    async with mcp(api, auth("olga", "operator")) as client:
        await client.call_tool(
            "set_schedule",
            {
                "name": "hello",
                "task": "demo.echo",
                "every_seconds": 60,
                "payload": {"message": "x"},
            },
        )
    # A viewer's assistant sees schedules and changes none; one limited to other task names
    # does not see this one at all.
    async with mcp(api, auth("vic", "viewer")) as client:
        assert len((await client.call_tool("list_schedules", {})).structured_content["result"]) == 1
        for tool in ("pause_schedule", "delete_schedule"):
            refused = await client.call_tool(tool, {"name": "hello"})
            assert refused.is_error
            assert "lacks the 'schedules:manage' permission" in refused.content[0].text
    async with mcp(api, auth("mo", "operator", task_names=["mail.*"])) as client:
        assert (await client.call_tool("list_schedules", {})).structured_content == {"result": []}
        hidden = await client.call_tool("delete_schedule", {"name": "hello"})
        assert hidden.is_error and "no such schedule" in hidden.content[0].text


async def test_the_endpoint_needs_a_token(api, auth):
    missing = await api.post("/mcp", json=LIST_TOOLS, headers=JSONRPC)
    assert missing.status_code == 401
    assert missing.headers["www-authenticate"] == "Bearer"

    invalid = await api.post(
        "/mcp", json=LIST_TOOLS, headers={**JSONRPC, "Authorization": "Bearer nope"}
    )
    assert invalid.status_code == 401
    assert invalid.headers["www-authenticate"] == 'Bearer error="invalid_token"'

    ok = await api.post("/mcp", json=LIST_TOOLS, headers={**JSONRPC, **auth("vic", "viewer")})
    assert ok.status_code == 200
    assert len(ok.json()["result"]["tools"]) == 17


async def test_mcp_can_be_turned_off(db, auth):
    async with serving(db, make_settings(mcp_enabled=False)) as api:
        response = await api.post("/mcp", json=LIST_TOOLS, headers={**JSONRPC, **auth("a")})
        assert response.status_code == 404
        assert (await api.get("/health/live")).status_code == 200


async def test_host_header_allow_list(db, auth):
    guarded = make_settings(mcp_allowed_hosts=["tasks.example.com"])
    async with serving(db, guarded) as api:
        headers = {**JSONRPC, **auth("vic", "viewer")}
        refused = await api.post("/mcp", json=LIST_TOOLS, headers=headers)
        assert refused.status_code in (403, 421)
        allowed = await api.post(
            "/mcp", json=LIST_TOOLS, headers={**headers, "Host": "tasks.example.com"}
        )
        assert allowed.status_code == 200
