"""What an account may enqueue: how many tasks may wait, how many may run, how many a minute,
and how urgent they may be.

Each limit is a column of `tq_users`: NULL takes the `TQ_ACCOUNT_*` default and 0 means no
limit. The waiting, rate and priority limits are checked here, by the API, before a task
is written. The running limit is applied by the queue itself (`capped` in queue.py), so it
holds however a task arrives. docs/auth.md#limits has the reasoning.

Limits follow `created_by`, which is the token's subject. An account from an external
identity provider has no row here and gets the defaults.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from pgtaskqueue.db import Database
from pgtaskqueue.models import User
from pgtaskqueue.settings import Settings

LIMITS = ("max_waiting", "max_running", "enqueue_per_minute", "min_priority")

# The two-key form of an advisory lock, so it can never meet the single-key locks the
# workers take. The second key is the account's name hashed.
LOCK_ACCOUNT = 0x7471_0005

# How far back `usage` looks for the last minute's tasks. Past this it reports this many.
_RECENT_SCAN = 10_000

_WAITING_SQL = text(
    """
    SELECT count(*) FROM (
        SELECT 1 FROM tq_tasks
        WHERE created_by = :subject
          AND status IN ('scheduled', 'pending')
          AND left(name, 3) <> 'tq.'
        LIMIT :limit
    ) AS waiting
    """
)

# The newest task but `:offset` of the account's, by id: tasks are numbered in the order
# they were made, so if that one is under a minute old, `:offset + 1` were made in the last
# minute. A walk of `tq_tasks_created_by_idx` that stops after `:offset + 1` rows, however
# long the account's history. `created_at` is the database's clock, as is `now()`.
_RATE_SQL = text(
    """
    SELECT EXTRACT(EPOCH FROM created_at + interval '60 seconds' - now()) AS wait
    FROM tq_tasks
    WHERE created_by = :subject AND left(name, 3) <> 'tq.'
    ORDER BY id DESC
    OFFSET :offset LIMIT 1
    """
)

_USAGE_SQL = text(
    """
    SELECT count(*) FILTER (WHERE status IN ('scheduled', 'pending')) AS waiting,
           count(*) FILTER (WHERE status = 'scheduled' AND capped) AS held,
           count(*) FILTER (WHERE status = 'running') AS running,
           (SELECT count(*) FROM (
                SELECT created_at FROM tq_tasks
                WHERE created_by = :subject AND left(name, 3) <> 'tq.'
                ORDER BY id DESC LIMIT :scan
            ) AS recent
            WHERE created_at > now() - interval '60 seconds') AS last_minute
    FROM tq_tasks
    WHERE created_by = :subject
      AND status IN ('scheduled', 'pending', 'running')
      AND left(name, 3) <> 'tq.'
    """
)


@dataclass(frozen=True, slots=True)
class Limits:
    """An account's limits once its own and the defaults are combined. 0 means none."""

    max_waiting: int = 0
    max_running: int = 0
    enqueue_per_minute: int = 0
    min_priority: int = 0

    @classmethod
    def defaults(cls, settings: Settings) -> Limits:
        return cls(
            max_waiting=settings.account_max_waiting,
            max_running=settings.account_max_running,
            enqueue_per_minute=settings.account_enqueue_per_minute,
            min_priority=settings.account_min_priority,
        )

    def priority(self, asked: int | None, default: int) -> int | None:
        """The priority to enqueue with. None leaves it to the queue's default."""
        if asked is None:
            return None if default >= self.min_priority else self.min_priority
        if asked < self.min_priority:
            raise LimitExceeded(
                f"this account may not ask for a priority below {self.min_priority} "
                "(lower runs first)",
                status=403,
            )
        return asked

    def within(self, bound: Limits) -> bool:
        """Whether every limit here is at least as tight as `bound`'s. What an account that
        `bound` limits may give another: otherwise making an account would be a way round
        one's own limits. No limit (0) is looser than any number."""
        for key in ("max_waiting", "max_running", "enqueue_per_minute"):
            mine, cap = getattr(self, key), getattr(bound, key)
            if cap and not 0 < mine <= cap:
                return False
        return self.min_priority >= bound.min_priority

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


class LimitExceeded(Exception):
    """A limit refused the request. The message is safe to show. `retry_after` is when
    the same request would pass, if waiting is all it takes."""

    def __init__(self, message: str, *, status: int = 429, retry_after: float | None = None):
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after


def own_limits(user: User | None) -> dict[str, int | None]:
    """What the account itself sets: None where it takes the default."""
    return {key: getattr(user, key, None) for key in LIMITS}


class AccountLimits:
    def __init__(self, db: Database, settings: Settings) -> None:
        self.db = db
        self.settings = settings

    def combine(self, user: User | None) -> Limits:
        return self.resolve(own_limits(user))

    def resolve(self, own: Mapping[str, int | None]) -> Limits:
        """Limits as an account would have them with `own` set: the defaults where None."""
        defaults = Limits.defaults(self.settings)
        return Limits(
            **{key: getattr(defaults, key) if own.get(key) is None else own[key] for key in LIMITS}
        )

    async def of(self, subject: str) -> Limits:
        async with self.db.sessionmaker() as session:
            user = await session.scalar(select(User).where(User.username == subject))
        return self.combine(user)

    async def admit(self, tx: AsyncSession, subject: str, limits: Limits, count: int) -> None:
        """Refuse `count` new tasks from `subject` if they would break its waiting or rate
        limit. Call it in the transaction that then inserts them.

        The lock makes the check and the insert one decision: two requests from one account
        cannot both find the last free place. It is per account, so accounts do not wait
        for each other, and taken only by accounts that have one of these limits.
        """
        waiting, rate = limits.max_waiting, limits.enqueue_per_minute
        if not waiting and not rate:
            return
        if waiting and count > waiting:
            raise LimitExceeded(
                f"a batch of {count} is more than this account may have waiting ({waiting})",
                status=413,
            )
        if rate and count > rate:
            raise LimitExceeded(
                f"a batch of {count} is more than this account may enqueue in a minute ({rate})",
                status=413,
            )
        await tx.execute(
            text("SELECT pg_advisory_xact_lock(:key, hashtext(:subject))"),
            {"key": LOCK_ACCOUNT, "subject": subject},
        )
        if waiting:
            now = await tx.scalar(_WAITING_SQL, {"subject": subject, "limit": waiting})
            if now + count > waiting:
                raise LimitExceeded(
                    f"this account has {now} tasks waiting and may have at most {waiting}. "
                    "Wait for some to start, or cancel some."
                )
        if rate:
            wait = await tx.scalar(_RATE_SQL, {"subject": subject, "offset": rate - count})
            if wait is not None and wait > 0:
                seconds = max(math.ceil(wait), 1)
                raise LimitExceeded(
                    f"this account may enqueue {rate} tasks a minute. Try again in {seconds} s.",
                    retry_after=seconds,
                )

    async def usage(self, subject: str) -> dict[str, Any]:
        """What the account has now, in the terms of its limits."""
        async with self.db.sessionmaker() as session:
            row = (
                await session.execute(_USAGE_SQL, {"subject": subject, "scan": _RECENT_SCAN})
            ).one()
        return dict(row._mapping)
