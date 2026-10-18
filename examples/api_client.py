"""Using the HTTP API: sign in, enqueue, wait for the result, read the log.

    docker compose up -d
    TQ_PASSWORD=<the admin password from .env> python -m examples.api_client

A service would use the client-credentials grant instead of a password:

    tq user create svc-reports --service --role submitter --generate
    curl -u svc-reports:<secret> -d grant_type=client_credentials $TQ_API_URL/v1/auth/token
"""

from __future__ import annotations

import asyncio
import os

import httpx

API = os.environ.get("TQ_API_URL", "http://localhost:8010")
USERNAME = os.environ.get("TQ_USERNAME", "admin")
PASSWORD = os.environ.get("TQ_PASSWORD") or os.environ.get("TQ_BOOTSTRAP_ADMIN_PASSWORD", "")


async def main() -> None:
    async with httpx.AsyncClient(base_url=API, timeout=40) as api:
        # OAuth2 password grant: form-encoded, like every OAuth2 token endpoint.
        response = await api.post(
            "/v1/auth/token",
            data={"grant_type": "password", "username": USERNAME, "password": PASSWORD},
        )
        response.raise_for_status()
        api.headers["Authorization"] = f"Bearer {response.json()['access_token']}"

        me = (await api.get("/v1/auth/me")).json()
        print(f"signed in as {me['subject']} ({me['role']})")

        # What can be run, and what each task expects.
        for kind in (await api.get("/v1/task-types")).json():
            summary = (kind["description"] or "").split("\n", 1)[0]
            print(f"  {kind['name']:<16} {summary}")

        # Enqueue. 201 for a new task; 200 with created=false if the dedupe_key matched one
        # that is still waiting or running.
        response = await api.post(
            "/v1/tasks",
            json={
                "name": "report.build",
                "payload": {"rows": 500, "fail_once_at": "transform"},
                "metadata": {"requested_by": "examples.api_client"},
                "dedupe_key": "example-report",
            },
        )
        response.raise_for_status()
        task = response.json()
        print("task", task["id"], task["status"], "(new)" if task["created"] else "(existing)")

        # Long-poll: the answer arrives when the task finishes, or after `wait` seconds with
        # whatever state it is in. Loop until the status is final.
        while task["status"] in ("scheduled", "pending", "running"):
            task = (await api.get(f"/v1/tasks/{task['id']}", params={"wait": 20})).json()
            print(" ...", task["status"], "attempt", task["attempt"], "step", task["step"])
        print("output:", task["output"] or task["error"])

        logs = (await api.get(f"/v1/tasks/{task['id']}/logs")).json()
        for line in logs["items"]:
            print(f"  #{line['attempt']} {line['level']:<7} {line['message']}")

        stats = (await api.get("/v1/stats", params={"window_minutes": 60})).json()
        print("last hour:", stats["totals"])


if __name__ == "__main__":
    asyncio.run(main())
