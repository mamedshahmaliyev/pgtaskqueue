"""The receiving end of a task `callback`: verify the signature, then act on the event.

    python -m examples.webhook_receiver                  # listens on :9000

    TQ_WEBHOOK_ALLOW_PRIVATE=true tq worker --executors examples.executors.basic
    curl -X POST localhost:8010/v1/tasks -H "Authorization: Bearer $TOKEN" \\
      -H 'Content-Type: application/json' -d '{
        "name": "demo.echo", "payload": {"message": "hi"},
        "callback": {"url": "http://localhost:9000/hooks/tq", "secret": "whsec-example-secret"}
      }'

Only the standard library is used, and `verify` below is the entire scheme, so it can be
ported to whatever the receiver is written in. See docs/api.md#webhooks.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

SECRET = os.environ.get("WEBHOOK_SECRET", "whsec-example-secret")
PORT = int(os.environ.get("PORT", "9000"))
seen: set[str] = set()  # a real receiver keeps this in its database


def verify(secret: str, body: bytes, header: str, tolerance: float = 300) -> bool:
    """`X-TaskQueue-Signature: t=<unix seconds>,v1=<hex>` where
    v1 = HMAC-SHA256(secret, "<t>." + raw body)."""
    try:
        parts = dict(item.split("=", 1) for item in header.split(","))
        timestamp = int(parts["t"])
    except (KeyError, ValueError):
        return False
    # An old timestamp is a replay of a request someone recorded.
    if abs(time.time() - timestamp) > tolerance:
        return False
    expected = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
    # compare_digest: constant time, so the comparison leaks nothing about the digest.
    return hmac.compare_digest(expected.hexdigest(), parts.get("v1", ""))


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        # Verify the bytes as received — not a re-serialized copy of the parsed JSON.
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if not verify(SECRET, body, self.headers.get("X-TaskQueue-Signature", "")):
            self.send_response(401)
            self.end_headers()
            return

        # Deliveries are at-least-once: the same one arrives again if this response is
        # lost. Its id is the same across retries, which is what makes ignoring it safe.
        delivery = self.headers["X-TaskQueue-Delivery"]
        if delivery not in seen:
            seen.add(delivery)
            event = json.loads(body)
            task = event["task"]
            result = task["output"] or task["error"]
            print(f"{event['event']}: {task['name']} {task['id']} → {result}")

        # Any 2xx is "received". Answer quickly and do slow work elsewhere: 5xx and 429 are
        # retried with backoff, any other 4xx is taken as final.
        self.send_response(204)
        self.end_headers()

    def log_message(self, *args: object) -> None:
        pass


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", PORT), Handler)  # noqa: S104
    print(f"listening on :{PORT}", flush=True)
    server.serve_forever()
