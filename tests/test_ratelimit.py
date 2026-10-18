"""Request rate limits, against a real Redis (the compose one, database 15).

Skipped, with the reason, when that Redis cannot be reached: `docker compose up -d redis`.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any

import pytest
from redis.asyncio import Redis

from pgtaskqueue.auth.users import UserService
from tests.helpers import TEST_REDIS_URL, bearer, make_settings, serving

# A port nothing listens on: Redis "down".
UNREACHABLE = "redis://127.0.0.1:1/0"


@pytest.fixture
async def redis() -> AsyncIterator[Redis]:
    client = Redis.from_url(TEST_REDIS_URL, socket_connect_timeout=1)
    try:
        await client.ping()
    except Exception as exc:
        await client.aclose()
        pytest.skip(f"no Redis at {TEST_REDIS_URL} ({exc}): docker compose up -d redis")
    await client.flushdb()
    yield client
    await client.aclose()


@pytest.fixture
def limited(db, redis):
    """`async with limited(rate_limit="3/minute") as api:` — the API with these limits and
    every other one off."""

    @contextlib.asynccontextmanager
    async def start(**limits: Any) -> AsyncIterator[Any]:
        values = {
            "redis_url": TEST_REDIS_URL,
            "rate_limit": "",
            "rate_limit_anonymous": "",
            "login_rate_limit": "",
            "login_failure_limit": "",
            **limits,
        }
        settings = make_settings(**values)
        async with serving(db, settings) as client:
            client.settings = settings  # type: ignore[attr-defined]
            yield client

    return start


def password_grant(username: str, password: str) -> dict[str, str]:
    return {"grant_type": "password", "username": username, "password": password}


# --------------------------------------------------------------------------- per account


async def test_an_account_is_refused_past_its_rate(limited):
    async with limited(rate_limit="3/minute") as api:
        alice = bearer(api.settings, "alice")
        remaining = []
        for _ in range(3):
            response = await api.get("/v1/auth/me", headers=alice)
            assert response.status_code == 200
            assert response.headers["ratelimit-limit"] == "3"
            remaining.append(int(response.headers["ratelimit-remaining"]))
        assert remaining == [2, 1, 0]

        refused = await api.get("/v1/auth/me", headers=alice)
        assert refused.status_code == 429
        # One place frees every 20 s.
        assert 1 <= int(refused.headers["retry-after"]) <= 20
        assert "this account" in refused.json()["detail"]

        # Counted per account: another one is untouched.
        bob = await api.get("/v1/auth/me", headers=bearer(api.settings, "bob"))
        assert bob.status_code == 200


async def test_concurrent_requests_never_pass_more_than_the_rate(limited):
    # The race the Lua script exists for: a read in one step and a write in another would
    # let many of these see the same free places.
    async with limited(rate_limit="5/minute") as api:
        alice = bearer(api.settings, "alice")
        async with asyncio.TaskGroup() as group:
            calls = [group.create_task(api.get("/v1/auth/me", headers=alice)) for _ in range(40)]
        codes = sorted(call.result().status_code for call in calls)
    assert codes.count(200) == 5
    assert codes.count(429) == 35


async def test_a_token_is_verified_before_its_subject_is_counted(limited):
    # A made-up token must not get a fresh bucket per request: it counts as anonymous.
    async with limited(rate_limit="100/minute", rate_limit_anonymous="2/minute") as api:
        codes = [
            (
                await api.get("/v1/auth/me", headers={"Authorization": f"Bearer junk-{i}"})
            ).status_code
            for i in range(3)
        ]
    assert codes == [401, 401, 429]


async def test_requests_without_a_token_are_counted_per_address(limited):
    async with limited(rate_limit_anonymous="2/minute") as api:
        codes = [(await api.get("/v1/auth/me")).status_code for _ in range(3)]
        assert codes == [401, 401, 429]
        refused = await api.get("/v1/auth/me")
        assert "this address" in refused.json()["detail"]
        # A token is counted against its account, not the address.
        ok = await api.get("/v1/auth/me", headers=bearer(api.settings, "alice"))
        assert ok.status_code == 200


async def test_health_checks_are_not_counted(limited):
    async with limited(rate_limit_anonymous="1/minute") as api:
        for _ in range(5):
            assert (await api.get("/health/live")).status_code == 200
        assert (await api.get("/v1/auth/me")).status_code == 401
        assert (await api.get("/v1/auth/me")).status_code == 429


async def test_mcp_requests_are_counted(limited):
    async with limited(rate_limit="1/minute") as api:
        alice = bearer(api.settings, "alice")
        assert (await api.get("/v1/auth/me", headers=alice)).status_code == 200
        response = await api.post(
            "/mcp",
            headers={**alice, "Accept": "application/json, text/event-stream"},
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        )
        assert response.status_code == 429


# --------------------------------------------------------------------------- sign-ins


@pytest.fixture
async def alice(db):
    await UserService(db).create("alice", "alice-password")
    await UserService(db).create("bob", "bob-password")


async def test_sign_ins_are_limited_per_address(limited, alice):
    async with limited(login_rate_limit="2/minute") as api:
        for _ in range(2):
            response = await api.post("/v1/auth/token", data=password_grant("alice", "wrong"))
            assert response.status_code == 400
        # Refused before the password is looked at, the right one included.
        refused = await api.post("/v1/auth/token", data=password_grant("alice", "alice-password"))
        assert refused.status_code == 429
        assert int(refused.headers["retry-after"]) >= 1
        assert "from this address" in refused.json()["detail"]


async def test_failed_sign_ins_are_limited_per_account_name(limited, alice):
    async with limited(login_failure_limit="2/hour") as api:
        for _ in range(2):
            response = await api.post("/v1/auth/token", data=password_grant("alice", "wrong"))
            assert response.status_code == 400
        refused = await api.post("/v1/auth/token", data=password_grant("alice", "alice-password"))
        assert refused.status_code == 429
        assert "for this account" in refused.json()["detail"]
        # Another account, from the same address, is not affected.
        bob = await api.post("/v1/auth/token", data=password_grant("bob", "bob-password"))
        assert bob.status_code == 200


async def test_successful_sign_ins_do_not_count_as_failures(limited, alice):
    async with limited(login_failure_limit="1/hour") as api:
        for _ in range(3):
            response = await api.post(
                "/v1/auth/token", data=password_grant("alice", "alice-password")
            )
            assert response.status_code == 200


async def test_client_credentials_are_limited_like_passwords(limited, db):
    await UserService(db).create("svc", "svc-secret-value", kind="service")
    async with limited(login_failure_limit="1/hour") as api:
        data = {"grant_type": "client_credentials", "client_id": "svc", "client_secret": "nope"}
        assert (await api.post("/v1/auth/token", data=data)).status_code == 401
        assert (await api.post("/v1/auth/token", data=data)).status_code == 429


async def test_the_ui_login_form_is_limited(limited, alice):
    async with limited(login_failure_limit="1/hour") as api:
        form = {"username": "alice", "password": "wrong"}
        assert (await api.post("/ui/login", data=form)).status_code == 401
        refused = await api.post("/ui/login", data=form)
        assert refused.status_code == 429
        assert int(refused.headers["retry-after"]) >= 1
        assert "Too many sign-in attempts" in refused.text
        # Still the UI's page, with its Content-Security-Policy.
        assert "content-security-policy" in refused.headers


# --------------------------------------------------------------------------- Redis down


async def test_without_redis_requests_go_through(limited):
    async with limited(redis_url=UNREACHABLE, rate_limit="1/minute") as api:
        alice = bearer(api.settings, "alice")
        for _ in range(3):
            response = await api.get("/v1/auth/me", headers=alice)
            assert response.status_code == 200
            assert "ratelimit-limit" not in response.headers


async def test_without_redis_fail_closed_refuses(limited):
    async with limited(
        redis_url=UNREACHABLE, rate_limit="1/minute", rate_limit_fail_open=False
    ) as api:
        response = await api.get("/v1/auth/me", headers=bearer(api.settings, "alice"))
        assert response.status_code == 503
        assert response.headers["retry-after"] == "5"


async def test_without_a_redis_url_nothing_is_limited(api, settings):
    assert settings.redis_url is None
    alice = bearer(settings, "alice")
    for _ in range(5):
        response = await api.get("/v1/auth/me", headers=alice)
        assert response.status_code == 200
        assert "ratelimit-limit" not in response.headers


# --------------------------------------------------------------------------- settings


@pytest.mark.parametrize("rate", ["10", "10/fortnight", "0/minute", "-1/minute", "x/second"])
def test_a_rate_that_cannot_be_read_stops_start_up(rate):
    with pytest.raises(ValueError, match="is not a rate"):
        make_settings(rate_limit=rate)


@pytest.mark.parametrize("rate", ["", "1/second", "600/minute", " 10 / hour ", "5/day"])
def test_rates_that_can_be_read(rate):
    assert make_settings(rate_limit=rate).rate_limit == rate
