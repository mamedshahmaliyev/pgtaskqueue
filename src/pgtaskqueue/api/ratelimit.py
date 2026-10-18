"""Request rate limits, counted in Redis so that every API replica sees one count.

    per account     every request with a valid token, by its subject   TQ_RATE_LIMIT
    per address     every request without one, by client address       TQ_RATE_LIMIT_ANONYMOUS
    sign-ins        password and client-credentials attempts, by address  TQ_LOGIN_RATE_LIMIT
    failed sign-ins by account name, from any address                  TQ_LOGIN_FAILURE_LIMIT

These limit how fast clients may call the API. What an account may *enqueue* is limited
separately, in PostgreSQL, in the transaction that inserts the tasks (auth/limits.py): that
check must be exact, and this one only needs to be fast.

Redis is optional. Without TQ_REDIS_URL nothing here runs. While Redis cannot be reached
the limits are skipped (TQ_RATE_LIMIT_FAIL_OPEN) rather than taking the API down with it.
docs/auth.md#request-rate-limits has the reasoning.
"""

from __future__ import annotations

import hashlib
import logging
import math
import time
from collections.abc import Callable
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import RedisError
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from pgtaskqueue.api.deps import ACCESS_COOKIE, AppState
from pgtaskqueue.auth.tokens import TokenError
from pgtaskqueue.settings import Settings, parse_rate

logger = logging.getLogger("pgtaskqueue.api.ratelimit")

_PREFIX = "tq:rl:"

# After Redis fails, requests skip it for this long instead of each waiting out a timeout.
_BACKOFF_SECONDS = 5.0

# GCRA: the key holds the "theoretical arrival time" of the next request, in milliseconds.
# A request is let in if that time is no more than one period ahead of now, which allows
# `count` at once and then one every `interval`. One script, so the read and the write are
# one step: two replicas cannot both take the last place. The clock is Redis's, so replicas
# whose clocks disagree still agree on the count. The value is formatted, not tostring()ed:
# Lua prints a large float in scientific notation and would lose the milliseconds.
_GCRA = """
local interval = tonumber(ARGV[1])
local period = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local tat = tonumber(redis.call('GET', KEYS[1]) or now)
if tat < now then tat = now end
local ahead = tat + interval - now
if ahead > period then
  return {0, math.ceil(ahead - period)}
end
if ARGV[3] == '1' then
  redis.call('SET', KEYS[1], string.format('%.3f', tat + interval), 'PX', math.ceil(ahead))
end
return {1, math.floor((period - ahead) / interval)}
"""


@dataclass(frozen=True, slots=True)
class Rate:
    count: int
    period: float

    @classmethod
    def parse(cls, value: str) -> Rate | None:
        parsed = parse_rate(value)
        return None if parsed is None else cls(*parsed)

    def __str__(self) -> str:
        unit = {1.0: "second", 60.0: "minute", 3600.0: "hour", 86400.0: "day"}[self.period]
        return f"{self.count} a {unit}"


@dataclass(frozen=True, slots=True)
class Decision:
    allowed: bool
    remaining: int
    retry_after: int  # whole seconds; 0 when allowed


class RateLimited(Exception):
    """A rate limit refused the request. The message is safe to show."""

    def __init__(self, message: str, retry_after: int) -> None:
        super().__init__(message)
        self.retry_after = max(retry_after, 1)


class LimiterUnavailable(Exception):
    """Redis cannot be reached and TQ_RATE_LIMIT_FAIL_OPEN is off."""


def _digest(value: str) -> str:
    # Keys are built from what clients send (an account name typed into a form): hashed, so
    # a key has a bounded size and Redis holds no account names.
    return hashlib.sha256(value.encode()).hexdigest()[:32]


def client_address(scope: Scope) -> str:
    """The address the request came from. Behind a proxy, uvicorn has already replaced it
    with the one in X-Forwarded-For, for proxies listed in FORWARDED_ALLOW_IPS."""
    client = scope.get("client")
    return client[0] if client else "unknown"


class RateLimiter:
    def __init__(self, redis: Redis, settings: Settings) -> None:
        self.redis = redis
        self.fail_open = settings.rate_limit_fail_open
        self.per_account = Rate.parse(settings.rate_limit)
        self.per_address = Rate.parse(settings.rate_limit_anonymous)
        self.logins = Rate.parse(settings.login_rate_limit)
        self.login_failures = Rate.parse(settings.login_failure_limit)
        self._script = redis.register_script(_GCRA)
        self._skip_until = 0.0

    @classmethod
    def from_settings(cls, settings: Settings) -> RateLimiter | None:
        if not settings.redis_url:
            return None
        redis = Redis.from_url(
            settings.redis_url,
            # Short, because every limited request waits on it. A Redis that is slower than
            # this counts as down, and requests go through without it.
            socket_connect_timeout=0.5,
            socket_timeout=0.5,
            health_check_interval=30,
        )
        return cls(redis, settings)

    async def close(self) -> None:
        await self.redis.aclose()

    async def hit(self, key: str, rate: Rate, *, take: bool = True) -> Decision | None:
        """Count one request against `key`. With `take=False`, only ask whether one more
        would be let in. None means Redis could not be asked and the limit is skipped."""
        if time.monotonic() < self._skip_until:
            return self._unavailable()
        interval = rate.period * 1000 / rate.count
        try:
            allowed, value = await self._script(
                keys=[_PREFIX + key], args=[interval, rate.period * 1000, int(take)]
            )
        except (RedisError, OSError) as exc:
            # Logged once per back-off, not once per request.
            logger.warning("rate limits skipped for %ss: Redis failed: %s", _BACKOFF_SECONDS, exc)
            self._skip_until = time.monotonic() + _BACKOFF_SECONDS
            return self._unavailable()
        if allowed:
            return Decision(True, int(value), 0)
        return Decision(False, 0, max(math.ceil(int(value) / 1000), 1))

    def _unavailable(self) -> None:
        if not self.fail_open:
            raise LimiterUnavailable
        return None

    # ----------------------------------------------------------------------- sign-ins

    async def check_login(self, address: str, name: str) -> None:
        """Before a sign-in is checked: refuse it if this address has made too many
        attempts, or this account name has failed too often. Raises RateLimited."""
        if self.logins is not None:
            decision = await self.hit(f"login:addr:{address}", self.logins)
            if decision is not None and not decision.allowed:
                raise RateLimited(
                    f"too many sign-in attempts from this address (the limit is {self.logins}). "
                    f"Try again in {decision.retry_after} s.",
                    decision.retry_after,
                )
        if self.login_failures is not None and name:
            # Only asked here; a place is taken when the attempt fails. Two wrong guesses in
            # flight together may both pass, which lets a guesser exceed the limit by its
            # concurrency, not by a factor: the next attempt after them waits.
            decision = await self.hit(
                f"login:fail:{_digest(name)}", self.login_failures, take=False
            )
            if decision is not None and not decision.allowed:
                raise RateLimited(
                    "too many failed sign-ins for this account. "
                    f"Try again in {decision.retry_after} s.",
                    decision.retry_after,
                )

    async def login_failed(self, name: str) -> None:
        if self.login_failures is not None and name:
            await self.hit(f"login:fail:{_digest(name)}", self.login_failures)


# Not counted: a load balancer's health checks, and the UI's static files, which a page
# loads several of at once and the browser then keeps.
_EXEMPT = ("/health/", "/ui/static/")


class RateLimitMiddleware:
    """Counts every request against its account, or against its address if it carries no
    valid token, and answers 429 past the limit."""

    def __init__(self, app: ASGIApp, get_state: Callable[[], AppState]) -> None:
        self.app = app
        self.get_state = get_state

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] == "OPTIONS":
            await self.app(scope, receive, send)
            return
        state = self.get_state()
        limiter = state.limiter
        path = scope["path"].removeprefix(scope.get("root_path", ""))
        if limiter is None or path.startswith(_EXEMPT):
            await self.app(scope, receive, send)
            return

        subject = self._subject(scope, state)
        if subject is not None:
            key, rate, who = f"sub:{_digest(subject)}", limiter.per_account, "this account"
        else:
            address = client_address(scope)
            key, rate, who = f"addr:{address}", limiter.per_address, "this address"
        if rate is None:
            await self.app(scope, receive, send)
            return

        try:
            decision = await limiter.hit(key, rate)
        except LimiterUnavailable:
            response = JSONResponse(
                {"detail": "the rate limiter is unavailable. Try again shortly."},
                status_code=503,
                headers={"Retry-After": str(int(_BACKOFF_SECONDS))},
            )
            await response(scope, receive, send)
            return
        if decision is None:
            await self.app(scope, receive, send)
            return
        if not decision.allowed:
            response = JSONResponse(
                {
                    "detail": f"too many requests from {who} (the limit is {rate}). "
                    f"Try again in {decision.retry_after} s."
                },
                status_code=429,
                headers={
                    "Retry-After": str(decision.retry_after),
                    "RateLimit-Limit": str(rate.count),
                    "RateLimit-Remaining": "0",
                },
            )
            await response(scope, receive, send)
            return

        extra = [
            (b"ratelimit-limit", str(rate.count).encode()),
            (b"ratelimit-remaining", str(decision.remaining).encode()),
        ]

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                message = {**message, "headers": [*message.get("headers", []), *extra]}
            await send(message)

        await self.app(scope, receive, send_with_headers)

    @staticmethod
    def _subject(scope: Scope, state: AppState) -> str | None:
        """The account a valid token names. The token is verified, not just read: an
        unverified subject would let a client pick whose limit it spends, and a fresh
        made-up token per request would never be limited at all."""
        request = Request(scope)
        header = request.headers.get("authorization", "")
        token = header[7:] if header.lower().startswith("bearer ") else None
        if not token and state.settings.ui_enabled:
            token = request.cookies.get(ACCESS_COOKIE)
        if not token:
            return None
        try:
            return state.tokens.principal(token).subject
        except TokenError:
            return None
