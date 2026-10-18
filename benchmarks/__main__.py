"""`python -m benchmarks <name>` — see docs/benchmarks.md.

    enqueue        tasks/second written: one at a time, concurrently, in batches
    throughput     tasks/second end to end, by worker processes × slots
    latency        enqueue → started, on an idle worker, with and without LISTEN/NOTIFY
    concurrency    how many tasks one worker process holds running at once
    depth          claim, enqueue and stats timings as the table grows
    all            everything, at the default sizes

Runs against a database whose name ends in `_bench` (created if missing), which it empties.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

from benchmarks import common
from benchmarks.common import Timer, markdown, scalar, summary_ms
from pgtaskqueue import Database, Monitor, NewTask, TaskQueue

Result = dict[str, Any]


def say(message: str) -> None:
    print(message, flush=True)


# --------------------------------------------------------------------------- enqueue


async def bench_enqueue(db: Database, args: argparse.Namespace) -> Result:
    """How fast tasks can be written."""
    rows: list[list[Any]] = []

    async def one_by_one(label: str, total: int, concurrency: int, **overrides: Any) -> None:
        await common.reset(db)
        queue = TaskQueue(db, common.settings(**overrides))
        remaining = iter(range(total))

        async def producer() -> None:
            for i in remaining:
                await queue.enqueue("bench.noop", {"i": i})

        with Timer() as timer:
            await asyncio.gather(*(producer() for _ in range(concurrency)))
        rows.append([label, total, round(timer.seconds, 2), round(total / timer.seconds)])
        say(f"  {label}: {total / timer.seconds:,.0f}/s")

    async def batched(label: str, total: int, batch: int) -> None:
        await common.reset(db)
        queue = TaskQueue(db, common.settings())
        with Timer() as timer:
            for start in range(0, total, batch):
                await queue.enqueue_many(
                    NewTask("bench.noop", {"i": i}) for i in range(start, start + batch)
                )
        rows.append([label, total, round(timer.seconds, 2), round(total / timer.seconds)])
        say(f"  {label}: {total / timer.seconds:,.0f}/s")

    n = args.tasks
    await one_by_one("enqueue(), 1 at a time", n // 10, 1)
    await one_by_one("enqueue(), 32 concurrent", n, 32)
    await one_by_one("enqueue(), 32 concurrent, NOTIFY off", n, 32, notify=False)
    await batched("enqueue_many(), batches of 100", n * 5, 100)
    await batched("enqueue_many(), batches of 1,000", n * 10, 1000)
    await batched("enqueue_many(), batches of 5,000", n * 10, 5000)
    return {
        "title": "Enqueue",
        "headers": ["how", "tasks", "seconds", "tasks/s"],
        "rows": rows,
    }


# --------------------------------------------------------------------------- throughput


async def _drain(
    db: Database, total: int, processes: int, slots: int, **env: Any
) -> dict[str, Any]:
    """Enqueue `total` no-op tasks, then time real worker processes emptying the queue."""
    await common.reset(db)
    queue = TaskQueue(db, common.settings())
    for start in range(0, total, 5000):
        await queue.enqueue_many(NewTask("bench.noop") for _ in range(min(5000, total - start)))
    # Workers find the queue paused, so all of them are up before any of them starts.
    monitor = Monitor(db)
    await monitor.set_paused("default", True)
    async with common.workers(processes, slots, **env):
        await common.wait_until_alive(db, processes)
        await monitor.set_paused("default", False)
        await common.wait_until_done(db, total)
    async with db.engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT extract(epoch FROM max(completed_at) - min(started_at)) AS seconds, "
                    "count(*) FILTER (WHERE status = 'succeeded') AS ok, "
                    "count(*) FILTER (WHERE attempt > 1) AS repeated FROM tq_tasks"
                )
            )
        ).one()
    assert row.ok == total and row.repeated == 0, row
    return {"seconds": float(row.seconds), "rate": total / float(row.seconds)}


async def _alter(db: Database, statement: str) -> None:
    async with db.engine.connect() as conn:
        autocommit = await conn.execution_options(isolation_level="AUTOCOMMIT")
        await autocommit.execute(text(statement))


async def bench_throughput(db: Database, args: argparse.Namespace) -> Result:
    """Tasks/second through the whole path: claim → run (nothing) → record the result."""
    shapes = [(1, 10), (1, 50), (1, 200), (2, 100), (4, 100), (8, 100)]
    rows: list[list[Any]] = []
    for processes, slots in shapes:
        result = await _drain(db, args.tasks, processes, slots)
        rows.append([processes, slots, "defaults", args.tasks, round(result["rate"])])
        say(f"  {processes} × {slots}: {result['rate']:,.0f}/s")
    # What the two optional round trips cost, on the shape most deployments resemble.
    variants: list[tuple[str, dict[str, Any]]] = [
        ("pool pre-ping off", {"db_pool_pre_ping": "false"}),
        ("NOTIFY off", {"notify": "false"}),
        # 4 × 30 connections: more than the default 20 per process, and still well inside
        # the server's max_connections with the LISTEN connections and this script's own.
        ("pool 30 per process", {"db_pool_size": 30, "db_max_overflow": 0}),
    ]
    for label, env in variants:
        result = await _drain(db, args.tasks, 4, 100, **env)
        rows.append([4, 100, label, args.tasks, round(result["rate"])])
        say(f"  4 × 100, {label}: {result['rate']:,.0f}/s")
    # A Postgres setting rather than one of ours: commits stop waiting for the WAL to reach
    # disk. Shows how much of the ceiling above is the disk. See docs/benchmarks.md before
    # using it.
    database = await scalar(db, "SELECT current_database()")
    await _alter(db, f'ALTER DATABASE "{database}" SET synchronous_commit = off')
    try:
        for processes in (4, 8):
            result = await _drain(db, args.tasks, processes, 100)
            rows.append(
                [processes, 100, "synchronous_commit off", args.tasks, round(result["rate"])]
            )
            say(f"  {processes} × 100, synchronous_commit off: {result['rate']:,.0f}/s")
    finally:
        await _alter(db, f'ALTER DATABASE "{database}" RESET synchronous_commit')
    return {
        "title": "End-to-end throughput (no-op tasks)",
        "headers": ["processes", "slots each", "settings", "tasks", "tasks/s"],
        "rows": rows,
    }


# --------------------------------------------------------------------------- latency


async def bench_latency(db: Database, args: argparse.Namespace) -> Result:
    """How long a task enqueued into an idle system waits before it starts."""
    rows: list[list[Any]] = []
    for label, env in (
        ("LISTEN/NOTIFY (default)", {}),
        ("polling only, every 2s", {"notify": "false"}),
        ("polling only, every 0.25s", {"notify": "false", "poll_interval_seconds": 0.25}),
    ):
        await common.reset(db)
        queue = TaskQueue(db, common.settings())
        async with common.workers(1, 10, **env):
            await common.wait_until_alive(db, 1)
            await asyncio.sleep(1)
            for _ in range(args.samples):
                await queue.enqueue("bench.noop")
                # Spaced out, and not in step with the poll interval.
                await asyncio.sleep(0.137)
            await common.wait_until_done(db, args.samples)
        async with db.engine.connect() as conn:
            waits = [
                float(row[0])
                for row in await conn.execute(
                    text("SELECT extract(epoch FROM started_at - created_at) FROM tq_tasks")
                )
            ]
        stats = summary_ms(waits)
        rows.append([label, args.samples, stats["p50_ms"], stats["p95_ms"], stats["max_ms"]])
        say(f"  {label}: p50 {stats['p50_ms']} ms")
    return {
        "title": "Pickup latency on an idle worker (enqueued → started)",
        "headers": ["wake-up", "tasks", "p50 ms", "p95 ms", "max ms"],
        "rows": rows,
    }


# --------------------------------------------------------------------------- concurrency


async def bench_concurrency(db: Database, args: argparse.Namespace) -> Result:
    """How many tasks one worker process — one event loop — runs at the same time."""
    rows: list[list[Any]] = []
    for slots in args.slots:
        await common.reset(db)
        queue = TaskQueue(db, common.settings())
        for start in range(0, slots, 5000):
            await queue.enqueue_many(
                NewTask("bench.sleep", {"seconds": args.seconds})
                for _ in range(min(5000, slots - start))
            )
        monitor = Monitor(db)
        await monitor.set_paused("default", True)
        async with common.workers(1, slots) as (process,):
            await common.wait_until_alive(db, 1)
            idle_rss = common.rss_mb(process.pid)
            await monitor.set_paused("default", False)
            started = time.perf_counter()
            peak, ramp, peak_rss = 0, 0.0, idle_rss
            while True:
                running = await scalar(db, "SELECT count(*) FROM tq_tasks WHERE status = 'running'")
                if running > peak:
                    peak, ramp = running, time.perf_counter() - started
                    peak_rss = common.rss_mb(process.pid)
                done = await scalar(db, "SELECT count(*) FROM tq_tasks WHERE status = 'succeeded'")
                if done >= slots:
                    break
                if time.perf_counter() - started > args.seconds * 6 + 120:
                    raise SystemExit(f"{slots} tasks did not finish")
                await asyncio.sleep(0.2)
            total = time.perf_counter() - started
            repeated = await scalar(db, "SELECT count(*) FROM tq_tasks WHERE attempt > 1")
        rows.append([slots, peak, round(ramp, 1), round(total, 1), idle_rss, peak_rss, repeated])
        say(f"  {slots} slots: peak {peak} running, {total:.1f}s for {args.seconds:g}s sleeps")
    return {
        "title": f"Concurrent tasks in one worker process ({args.seconds:g}s sleeps)",
        "headers": [
            "slots",
            "peak running",
            "s to peak",
            "s total",
            "RSS idle MB",
            "RSS at peak MB",
            "re-run",
        ],
        "rows": rows,
    }


# --------------------------------------------------------------------------- depth


HISTORY_DAYS = 30


def _claims(repeats: int) -> tuple[int, int]:
    """How many single claims and how many claims of 100 `_measure` makes."""
    return repeats, max(repeats // 10, 5)


async def _seed(db: Database, count: int, status: str) -> None:
    """Insert `count` tasks with one statement per 250k rows — server-side, so seeding a
    million rows takes seconds rather than minutes."""
    version = int(await scalar(db, "SHOW server_version_num"))
    new_id = "uuidv7()" if version >= 180000 else "gen_random_uuid()"
    finished = status != "pending"
    # History is spread evenly over the last HISTORY_DAYS, as it would be on a system that
    # has been running: "the last hour" is then a small slice of it, not all of it.
    done_at = f"now() - (n / CAST(:count AS float8)) * interval '{HISTORY_DAYS} days'"
    statement = text(
        f"INSERT INTO tq_tasks (id, queue_name, name, status, payload, attempt, "
        f"started_at, completed_at, spent_seconds) "
        f"SELECT {new_id}, 'default', 'bench.depth', :status, "
        f"jsonb_build_object('n', n, 'note', 'a payload of a typical small size'), "
        f"{1 if finished else 0}, "
        f"{done_at + " - interval '0.5 seconds'" if finished else 'NULL'}, "
        f"{done_at if finished else 'NULL'}, {0.5 if finished else 0} "
        f"FROM generate_series(CAST(:first AS integer), CAST(:last AS integer)) AS n"
    )
    for start in range(0, count, 250_000):
        async with db.transaction() as tx:
            await tx.execute(
                statement,
                {
                    "status": status,
                    "count": count,
                    "first": start + 1,
                    "last": min(start + 250_000, count),
                },
            )
    async with db.engine.connect() as conn:
        autocommit = await conn.execution_options(isolation_level="AUTOCOMMIT")
        await autocommit.execute(text("VACUUM ANALYZE tq_tasks"))


async def _measure(db: Database, repeats: int) -> dict[str, Any]:
    queue, monitor = TaskQueue(db, common.settings()), Monitor(db)
    timings: dict[str, list[float]] = {"claim1": [], "claim100": [], "enqueue": []}
    singles, hundreds = _claims(repeats)
    for _ in range(singles):
        with Timer() as timer:
            claimed = await queue.claim(["bench.depth"], worker_id="bench", limit=1)
        assert len(claimed) == 1
        timings["claim1"].append(timer.seconds)
    for _ in range(hundreds):
        with Timer() as timer:
            claimed = await queue.claim(["bench.depth"], worker_id="bench", limit=100)
        assert len(claimed) == 100
        timings["claim100"].append(timer.seconds)
    for _ in range(repeats):
        with Timer() as timer:
            await queue.enqueue("bench.depth", {"n": 0})
        timings["enqueue"].append(timer.seconds)
    with Timer() as stats:
        await monitor.stats()
    with Timer() as listing:
        await queue.list(limit=50)
    return {
        "claim1": summary_ms(timings["claim1"]),
        "claim100": summary_ms(timings["claim100"]),
        "enqueue": summary_ms(timings["enqueue"]),
        "stats_ms": round(stats.seconds * 1000, 1),
        "list_ms": round(listing.seconds * 1000, 1),
        "table_mb": round(await scalar(db, "SELECT pg_table_size('tq_tasks')") / 2**20),
        "index_mb": round(await scalar(db, "SELECT pg_indexes_size('tq_tasks')") / 2**20),
    }


async def bench_depth(db: Database, args: argparse.Namespace) -> Result:
    """What a big table costs: a deep backlog, and a long history."""
    rows: list[list[Any]] = []
    singles, hundreds = _claims(args.repeats)
    # Pending tasks under the finished history: enough for every claim that is timed.
    ready = max(5000, singles + hundreds * 100)
    for depth in args.depths:
        for label, pending, finished in (
            ("pending backlog", max(depth, ready), 0),
            (f"finished history + {ready:,} pending", ready, depth),
        ):
            await common.reset(db)
            with Timer() as seeding:
                if finished:
                    await _seed(db, finished, "succeeded")
                await _seed(db, pending, "pending")
            m = await _measure(db, args.repeats)
            rows.append(
                [
                    depth,
                    label,
                    m["claim1"]["p50_ms"],
                    m["claim1"]["p95_ms"],
                    m["claim100"]["p50_ms"],
                    m["enqueue"]["p50_ms"],
                    m["stats_ms"],
                    m["list_ms"],
                    m["table_mb"],
                    m["index_mb"],
                ]
            )
            say(
                f"  {depth:,} {label}: claim p50 {m['claim1']['p50_ms']} ms "
                f"(seeded in {seeding.seconds:.0f}s)"
            )
    await common.reset(db)
    return {
        "title": "Timings by table size",
        "headers": [
            "rows",
            "shape",
            "claim 1 p50 ms",
            "claim 1 p95 ms",
            "claim 100 p50 ms",
            "enqueue p50 ms",
            "stats() ms",
            "list(50) ms",
            "table MB",
            "indexes MB",
        ],
        "rows": rows,
    }


# --------------------------------------------------------------------------- entry point

BENCHMARKS = {
    "enqueue": bench_enqueue,
    "throughput": bench_throughput,
    "latency": bench_latency,
    "concurrency": bench_concurrency,
    "depth": bench_depth,
}


def _ints(value: str) -> list[int]:
    return [int(item.replace("_", "")) for item in value.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks",
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("name", choices=[*BENCHMARKS, "all"])
    parser.add_argument("--tasks", type=int, default=20_000, help="enqueue, throughput")
    parser.add_argument("--samples", type=int, default=100, help="latency: tasks per variant")
    parser.add_argument(
        "--slots", type=_ints, default=[100, 1000, 5000, 10000], help="concurrency: slot counts"
    )
    parser.add_argument("--seconds", type=float, default=20.0, help="concurrency: sleep length")
    parser.add_argument(
        "--depths", type=_ints, default=[10_000, 100_000, 1_000_000], help="depth: row counts"
    )
    parser.add_argument("--repeats", type=int, default=200, help="depth: operations timed")
    parser.add_argument("--json", metavar="FILE", help="also write the results as JSON")
    return parser


async def main() -> None:
    args = build_parser().parse_args()
    db = await common.prepare()
    try:
        environment = await common.environment(db)
        results: list[Result] = []
        for name in BENCHMARKS if args.name == "all" else [args.name]:
            say(f"\n{name}: {BENCHMARKS[name].__doc__}")
            results.append(await BENCHMARKS[name](db, args))
        await common.reset(db)
    finally:
        await db.dispose()

    say("\n" + "=" * 78)
    say(json.dumps(environment, indent=2))
    for result in results:
        say(f"\n### {result['title']}\n")
        say(markdown(result["headers"], result["rows"]))
    if args.json:
        report = {
            "at": datetime.now(UTC).isoformat(),
            "environment": environment,
            "results": results,
        }
        await asyncio.to_thread(Path(args.json).write_text, json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
