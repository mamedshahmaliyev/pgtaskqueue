"""What the benchmarks share: a database of their own, worker processes, and arithmetic."""

from __future__ import annotations

import asyncio
import contextlib
import os
import platform
import signal
import statistics
import sys
import time
from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import Any

import asyncpg
from sqlalchemy import make_url, text

from pgtaskqueue import Database, Settings, __version__, migrate
from pgtaskqueue.settings import get_settings

ROOT = Path(__file__).resolve().parent.parent
TABLES = "tq_tasks, tq_task_logs, tq_task_types, tq_queues, tq_workers"


def bench_url() -> str:
    """`TQ_BENCH_DATABASE_URL`, or the configured database with `_bench` appended."""
    if url := os.environ.get("TQ_BENCH_DATABASE_URL"):
        return url
    base = make_url(get_settings().database_url)
    return base.set(database=f"{base.database}_bench").render_as_string(hide_password=False)


def settings(**overrides: Any) -> Settings:
    return Settings(database_url=bench_url(), **overrides)


async def prepare() -> Database:
    """Create and migrate the benchmark database, and return a handle to it."""
    url = make_url(bench_url())
    # Every benchmark starts by emptying the queue's tables. Never somebody's real ones.
    if not (url.database or "").endswith("_bench"):
        raise SystemExit(
            f"refusing to benchmark against {url.database!r}: the database name must end "
            "with _bench (set TQ_BENCH_DATABASE_URL)"
        )
    conn = await asyncpg.connect(
        host=url.host,
        port=url.port,
        user=url.username,
        password=url.password,
        database="postgres",
        statement_cache_size=0,  # so this also works through a transaction-mode pooler
    )
    try:
        if not await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", url.database):
            await conn.execute(f'CREATE DATABASE "{url.database}"')
    finally:
        await conn.close()
    await asyncio.to_thread(migrate.upgrade, bench_url())
    return Database(settings=settings(), pool_size=40, application_name="tq-bench")


async def reset(db: Database) -> None:
    async with db.transaction() as tx:
        await tx.execute(text(f"TRUNCATE {TABLES}"))


async def scalar(db: Database, sql: str, **params: Any) -> Any:
    async with db.engine.connect() as conn:
        return (await conn.execute(text(sql), params)).scalar()


async def environment(db: Database) -> dict[str, Any]:
    async with db.engine.connect() as conn:
        show = {
            name: (await conn.execute(text(f"SHOW {name}"))).scalar()
            for name in (
                "server_version",
                "shared_buffers",
                "max_connections",
                "synchronous_commit",
                "fsync",
            )
        }
    return {
        "pgtaskqueue": __version__,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpus": os.cpu_count(),
        "postgres": show,
    }


# --------------------------------------------------------------------------- arithmetic


def percentile(values: Sequence[float], p: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(p / 100 * (len(ordered) - 1)))]


def summary_ms(seconds: Sequence[float]) -> dict[str, float]:
    return {
        "p50_ms": round(statistics.median(seconds) * 1000, 2),
        "p95_ms": round(percentile(seconds, 95) * 1000, 2),
        "max_ms": round(max(seconds) * 1000, 2),
    }


class Timer:
    def __enter__(self) -> Timer:
        self.started = time.perf_counter()
        return self

    def __exit__(self, *exc: object) -> None:
        self.seconds = time.perf_counter() - self.started


def markdown(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    def cell(value: Any) -> str:
        if isinstance(value, float):
            return f"{value:,.2f}" if value < 100 else f"{value:,.0f}"
        if isinstance(value, int) and not isinstance(value, bool):
            return f"{value:,}"
        return str(value)

    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(cell(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)


# --------------------------------------------------------------------------- workers


@contextlib.asynccontextmanager
async def workers(
    processes: int, slots: int, **env: Any
) -> AsyncIterator[list[asyncio.subprocess.Process]]:
    """`processes` real `tq worker` processes over the benchmark executors, each with
    `slots` slots. Extra keyword arguments become TQ_ environment variables."""
    environ = {
        **os.environ,
        "TQ_DATABASE_URL": bench_url(),
        # The benchmark tasks have no callbacks; leave every slot to them.
        "TQ_WEBHOOK_SLOTS": "0",
        "TQ_WORKER_IDLE_EXIT_SECONDS": "0",
        **{f"TQ_{key.upper()}": str(value) for key, value in env.items()},
    }
    command = [sys.executable, "-m", "pgtaskqueue", "--log-level", "WARNING", "worker"]
    command += ["--executors", "benchmarks.executors", "--slots", str(slots)]
    started = [
        await asyncio.create_subprocess_exec(*command, cwd=ROOT, env=environ)
        for _ in range(processes)
    ]
    try:
        yield started
    finally:
        for process in started:
            if process.returncode is None:
                process.send_signal(signal.SIGTERM)
        await asyncio.gather(*(process.wait() for process in started))


async def wait_until_alive(db: Database, count: int, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while await scalar(db, "SELECT count(*) FROM tq_workers WHERE stopped_at IS NULL") < count:
        if time.monotonic() > deadline:
            raise SystemExit(f"{count} worker(s) did not start within {timeout:g}s")
        await asyncio.sleep(0.1)


async def wait_until_done(db: Database, total: int, timeout: float = 1800) -> None:
    deadline = time.monotonic() + timeout
    done = "SELECT count(*) FROM tq_tasks WHERE status IN ('succeeded', 'failed', 'canceled')"
    while await scalar(db, done) < total:
        if time.monotonic() > deadline:
            raise SystemExit(f"{total} tasks did not finish within {timeout:g}s")
        await asyncio.sleep(0.1)


def rss_mb(pid: int) -> float | None:
    """Resident memory of a process, from `ps` (no dependency just for this)."""
    import subprocess

    try:
        out = subprocess.run(
            ["/bin/ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True, check=True
        )
        return round(int(out.stdout.strip()) / 1024, 1)
    except (subprocess.CalledProcessError, ValueError, FileNotFoundError):
        return None
