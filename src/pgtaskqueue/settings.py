"""Every setting, read from the environment with a `TQ_` prefix (and from `.env`).

One class on purpose: the queue, the worker and the API share a database and a handful of
timings that have to agree — a worker that heartbeats every 30s under an API that calls a
task stale after 20s would re-queue live work. `docs/configuration.md` is the reference.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from pydantic import AfterValidator, BeforeValidator, Field, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


def _split_csv(value: object) -> object:
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return value


# A comma-separated environment variable read as a list (`TQ_WORKER_TASKS=a:2,b`), rather than
# pydantic-settings' default of expecting JSON for list fields.
CsvList = Annotated[list[str], NoDecode, BeforeValidator(_split_csv)]

_PERIODS = {"second": 1.0, "minute": 60.0, "hour": 3600.0, "day": 86400.0}


def parse_rate(value: str) -> tuple[int, float] | None:
    """`"600/minute"` → (600, 60.0). Empty is no limit (None)."""
    if not value.strip():
        return None
    count, _, period = value.strip().partition("/")
    if not count.strip().isdigit() or int(count) < 1 or period.strip() not in _PERIODS:
        raise ValueError(
            f"{value!r} is not a rate: write it as N/second, N/minute, N/hour or N/day "
            "with N at least 1, or leave it empty for no limit"
        )
    return int(count), _PERIODS[period.strip()]


def _check_rate(value: str) -> str:
    parse_rate(value)
    return value


Rate = Annotated[str, AfterValidator(_check_rate)]

_ASYMMETRIC_PREFIXES = ("RS", "ES", "PS", "Ed")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="TQ_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # --- database -------------------------------------------------------------------
    database_url: str = "postgresql+asyncpg://tq:tq@localhost:5432/task_queue"
    # Connections are held only for the duration of a queue operation, never for the length
    # of a task, so the pool is sized against queue traffic rather than against slot count.
    db_pool_size: int = 10
    db_max_overflow: int = 10
    # Test a pooled connection before each use, so one that died while idle (a database
    # restart, a NAT timeout) is replaced instead of failing the operation that drew it.
    # Costs a round trip per operation; docs/benchmarks.md measures it.
    db_pool_pre_ping: bool = True
    # `database_url` points at PgBouncer (or another pooler) in transaction mode. Turns off
    # the driver's prepared-statement caches, which do not survive a connection that changes
    # server between transactions. See docs/infrastructure.md#connection-poolers.
    db_pgbouncer: bool = False
    # A direct (unpooled) URL to the same database, used only for the one LISTEN connection a
    # worker keeps. Behind a transaction-mode pooler a session cannot LISTEN, so without this
    # a worker finds tasks by polling alone.
    listen_database_url: str | None = None
    db_echo: bool = False

    # --- queue behaviour --------------------------------------------------------------
    default_priority: int = 100
    # Used when neither the enqueue call nor the task type's registered default says.
    default_max_attempts: int = 3
    # Delay before attempt n+1: min(base * 2**(n-1), max), plus up to `jitter` of itself.
    retry_backoff_base_seconds: float = 5.0
    retry_backoff_max_seconds: float = 600.0
    retry_backoff_jitter: float = 0.2
    # A `running` task whose heartbeat is older than this is presumed lost with its worker.
    # Keep it several heartbeat intervals wide: one slow heartbeat must not requeue live work.
    stale_after_seconds: float = 60.0
    # Terminal tasks older than this are deleted by the maintenance loop. 0 keeps everything,
    # which is the default because the history is the point of keeping the queue in Postgres.
    retention_days: int = 0
    # LISTEN/NOTIFY wake-ups so an idle worker starts a new task in milliseconds rather than
    # at its next poll. Polling alone is always correct, only slower to notice.
    notify: bool = True

    # --- archive ----------------------------------------------------------------------
    # A PostgreSQL database that finished tasks are moved to, with their logs, once they
    # are `archive_after_days` old: the history is kept, off the queue's table. Unset, or
    # empty, archives nothing. May be the queue's own database; the archive's tables are
    # named `tq_archive_*`. See docs/analytics.md#the-archive.
    archive_database_url: Annotated[str | None, BeforeValidator(lambda v: v or None)] = None
    archive_after_days: float = Field(default=30.0, gt=0)
    # Tasks moved per transaction. A batch keeps its tasks locked in the queue's database
    # while it writes them to the archive, so a requeue of one of them waits that long.
    archive_batch_size: int = Field(default=1000, ge=1)
    archive_interval_seconds: float = Field(default=60.0, gt=0)

    # --- account limits ---------------------------------------------------------------
    # For an account whose own limit is unset. 0 means no limit. They apply to the
    # `created_by` of a task, so tasks enqueued without one (from Python, without an
    # account) are never limited. See docs/auth.md#limits.
    # Tasks an account may have scheduled or pending at once. Enforced by the API.
    account_max_waiting: int = Field(default=0, ge=0)
    # Tasks of an account that may be pending or running at once; the rest are held back.
    # Read by the enqueue statement and by the workers that release held tasks, so give the
    # API and the workers the same value.
    account_max_running: int = Field(default=0, ge=0)
    # Tasks an account may create in any 60 seconds. Enforced by the API.
    account_enqueue_per_minute: int = Field(default=0, ge=0)
    # The most urgent priority an account may ask for. Enforced by the API.
    account_min_priority: int = Field(default=0, ge=0, le=32767)

    # --- worker -----------------------------------------------------------------------
    worker_queue: str = "default"
    # Modules imported at start-up for the executors they register.
    worker_executors: CsvList = []
    # Slots for the names given no count of their own. See docs/workers.md.
    worker_slots: int = 10
    # `NAME`, `NAME:N`, a glob (`image.*:4`), `ALL:N`, or `NAME:0` to exclude.
    worker_tasks: CsvList = []
    poll_interval_seconds: float = 2.0
    heartbeat_interval_seconds: float = 5.0
    promote_interval_seconds: float = 1.0
    # How often a worker looks for periodic schedules that are due. A schedule can fire up
    # to this much late. See docs/schedules.md.
    schedule_interval_seconds: float = 1.0
    maintenance_interval_seconds: float = 15.0
    # Exit after this long with nothing running, so the container's restart policy gives a
    # fresh process (new code, released memory, new connections). 0 never exits.
    worker_idle_exit_seconds: float = 0.0
    # Exit after this many tasks, for the same reason. 0 never exits.
    worker_max_tasks: int = 0
    # How long a stopping worker waits for running tasks before cancelling and releasing them.
    worker_shutdown_grace_seconds: float = 30.0
    log_flush_interval_seconds: float = 1.0
    # `logging` records at or above this level, emitted while a task runs, are stored as that
    # task's log lines. Empty disables the capture; `ctx.log()` always works.
    capture_log_level: str = "INFO"
    log_message_max_chars: int = 8000

    # --- webhooks ---------------------------------------------------------------------
    # Slots every worker reserves for delivering `callback` notifications. 0 leaves delivery
    # to a dedicated worker started with `--tasks tq.webhook:N`.
    webhook_slots: int = 2
    webhook_timeout_seconds: float = 10.0
    webhook_max_attempts: int = 6
    # HMAC-SHA256 key for the `X-TaskQueue-Signature` header when the task names no secret.
    webhook_secret: str = ""
    # Refuse callback URLs that resolve to loopback, private or link-local addresses.
    webhook_allow_private: bool = False

    # --- API and auth -----------------------------------------------------------------
    jwt_algorithm: str = "HS256"
    # HS*: the shared secret. Asymmetric algorithms use the two keys below instead.
    jwt_secret: str = ""
    jwt_private_key: str = ""
    jwt_public_key: str = ""
    jwt_issuer: str = "pgtaskqueue"
    jwt_audience: str = "pgtaskqueue-api"
    access_token_minutes: int = 15
    refresh_token_days: int = 7
    # Created by `tq migrate` when the users table is empty.
    bootstrap_admin_username: str = ""
    bootstrap_admin_password: str = ""
    cors_origins: CsvList = []
    # Serve the web UI at `/ui`. It signs in with the same accounts and keeps the tokens in
    # HttpOnly cookies, which the API then accepts from the browser. See docs/ui.md.
    ui_enabled: bool = True
    mcp_enabled: bool = True
    # Host headers the MCP endpoint accepts (DNS-rebinding protection), e.g.
    # `tasks.example.com,localhost:*`. Empty disables the check.
    mcp_allowed_hosts: CsvList = []
    # Refuse to enqueue a task name that no worker has registered. Off by default, so the
    # API can be deployed before the workers that will serve a new task.
    api_require_registered_tasks: bool = False
    # Upper bound for `GET /tasks/{id}?wait=` long polling.
    long_poll_max_seconds: float = 30.0
    max_payload_bytes: int = Field(default=1_000_000, ge=1)

    # --- request rate limits ----------------------------------------------------------
    # Redis, where the API counts requests so every replica sees the same count. Only the
    # API reads it, and only for these limits: no queue state is kept there. Unset turns
    # every limit below off. See docs/auth.md#request-rate-limits.
    redis_url: Annotated[str | None, BeforeValidator(lambda v: v or None)] = None
    # Each limit is `N/second`, `N/minute`, `N/hour` or `N/day`, or empty for none. N may
    # arrive at once; after that requests are let through evenly, one every period/N.
    # Requests an account makes, counted by the token's subject.
    rate_limit: Rate = ""
    # Requests without a valid token, counted per client address.
    rate_limit_anonymous: Rate = ""
    # Sign-in attempts per client address: the password and client-credentials grants and
    # the UI's login form. Each costs an Argon2 hash, so this also caps the CPU one client
    # can make the API spend.
    login_rate_limit: Rate = "30/minute"
    # Failed sign-ins per account name, from any address: what slows a guesser who spreads
    # attempts over many addresses. A successful sign-in costs nothing.
    login_failure_limit: Rate = "10/hour"
    # While Redis cannot be reached: let requests through unlimited (true), or answer 503.
    rate_limit_fail_open: bool = True

    @model_validator(mode="after")
    def _check_timings(self) -> Settings:
        if self.stale_after_seconds < 3 * self.heartbeat_interval_seconds:
            raise ValueError(
                "TQ_STALE_AFTER_SECONDS must be at least 3x TQ_HEARTBEAT_INTERVAL_SECONDS, "
                "or one slow heartbeat re-queues a task that is still running"
            )
        if (
            self.archive_database_url
            and self.retention_days > 0
            and self.retention_days <= self.archive_after_days
        ):
            raise ValueError(
                "TQ_RETENTION_DAYS deletes finished tasks before TQ_ARCHIVE_AFTER_DAYS archives "
                "them: set TQ_RETENTION_DAYS to 0, or above TQ_ARCHIVE_AFTER_DAYS"
            )
        return self

    @property
    def jwt_is_asymmetric(self) -> bool:
        return self.jwt_algorithm.startswith(_ASYMMETRIC_PREFIXES)


@lru_cache
def get_settings() -> Settings:
    return Settings()
