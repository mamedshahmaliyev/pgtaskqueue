"""A worker that cannot reach Postgres: it claims and settles tasks over the HTTP API.

For a machine in another network — a GPU box, a customer's server — where opening the
database is not an option. It needs an account with the `worker` role:

    tq user create gpu-1 --service --role worker --task-names 'remote.*' --generate
    TQ_API_URL=http://localhost:8010 TQ_CLIENT_ID=gpu-1 TQ_CLIENT_SECRET=<secret> \\
        python -m examples.remote_worker

    tq enqueue remote.shout '{"text": "hello"}' --wait

This file is deliberately one screen of plain httpx, with no import from `pgtaskqueue`: it is
the whole protocol, and a starting point for a worker written in any language. See
docs/api.md#remote-workers.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import traceback

import httpx

API = os.environ.get("TQ_API_URL", "http://localhost:8010")
CLIENT_ID = os.environ.get("TQ_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("TQ_CLIENT_SECRET", "")
WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"
SLOTS = 4
HEARTBEAT_SECONDS = 5


async def shout(payload: dict) -> dict:
    await asyncio.sleep(1)  # the work
    return {"text": str(payload.get("text", "")).upper()}


HANDLERS = {"remote.shout": shout}
TASK_TYPES = [
    {
        "name": "remote.shout",
        "description": "Upper-cases `text`, on a machine outside the database's network.",
        "payload_schema": {
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
        },
        "max_attempts": 3,
    }
]


class RemoteWorker:
    def __init__(self, api: httpx.AsyncClient) -> None:
        self.api = api
        self.running: dict[str, asyncio.Task] = {}  # task id → the asyncio task running it

    async def sign_in(self) -> None:
        response = await self.api.post(
            "/v1/auth/token",
            data={"grant_type": "client_credentials"},
            auth=(CLIENT_ID, CLIENT_SECRET),
        )
        response.raise_for_status()
        token = response.json()
        self.api.headers["Authorization"] = f"Bearer {token['access_token']}"
        # A long-lived worker asks again before this runs out.
        self.token_expires = asyncio.get_running_loop().time() + token["expires_in"] - 60

    async def call(self, method: str, path: str, **kwargs) -> httpx.Response:
        if asyncio.get_running_loop().time() > self.token_expires:
            await self.sign_in()
        response = await self.api.request(method, path, **kwargs)
        response.raise_for_status()
        return response

    async def run_one(self, task: dict) -> None:
        own = {"worker_id": WORKER_ID, "attempt": task["attempt"]}
        url = f"/v1/worker/tasks/{task['id']}"
        try:
            output = await HANDLERS[task["name"]](task["payload"])
        except asyncio.CancelledError:
            # The heartbeat said the task is no longer ours (canceled, or given to another
            # worker), or we are shutting down. Either way there is nothing to report.
            raise
        except Exception as exc:
            error = {
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            }
            # The server decides between "retry later" and "failed for good".
            response = await self.call("POST", f"{url}/fail", json={**own, "error": error})
        else:
            lines = [{"message": f"done by {WORKER_ID}"}]
            await self.call("POST", f"{url}/logs", json={**own, "lines": lines})
            response = await self.call("POST", f"{url}/succeed", json={**own, "output": output})
        settled = response.json()
        # accepted=false: the task stopped being ours while it ran, and nothing was written.
        print(task["id"], task["name"], "→", settled["status"] or "discarded")

    async def heartbeat(self) -> None:
        """Say "still working on these" well inside TQ_STALE_AFTER_SECONDS (60s), and stop
        working on anything the server says is no longer ours."""
        while True:
            await asyncio.sleep(HEARTBEAT_SECONDS)
            if not self.running:
                continue
            body = {"worker_id": WORKER_ID, "task_ids": list(self.running)}
            response = await self.call("POST", "/v1/worker/heartbeat", json=body)
            owned = set(response.json()["owned"])
            for task_id, running in list(self.running.items()):
                if task_id not in owned:
                    running.cancel()

    async def run(self) -> None:
        await self.sign_in()
        await self.call("PUT", f"/v1/worker/{WORKER_ID}/task-types", json=TASK_TYPES)
        heartbeat = asyncio.create_task(self.heartbeat())
        print(f"{WORKER_ID}: waiting for {', '.join(HANDLERS)}")
        try:
            while True:
                free = SLOTS - len(self.running)
                if free == 0:
                    await asyncio.wait(self.running.values(), return_when=asyncio.FIRST_COMPLETED)
                    continue
                # Long-poll: held open up to 20s when there is nothing to do, so an idle
                # worker costs one request every 20 seconds and still starts work at once.
                body = {
                    "worker_id": WORKER_ID,
                    "names": list(HANDLERS),
                    "limit": free,
                    "wait_seconds": 20,
                }
                for task in (await self.call("POST", "/v1/worker/claim", json=body)).json():
                    running = asyncio.create_task(self.run_one(task))
                    self.running[task["id"]] = running
                    running.add_done_callback(lambda _, id=task["id"]: self.running.pop(id, None))
        finally:
            heartbeat.cancel()
            for running in self.running.values():
                running.cancel()
            # Hand back what was not finished, without spending its attempt.
            await self.api.post("/v1/worker/release", json={"worker_id": WORKER_ID})


async def main() -> None:
    async with httpx.AsyncClient(base_url=API, timeout=40) as api:
        await RemoteWorker(api).run()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
