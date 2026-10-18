"""The `tq` command.

    tq migrate                       create or upgrade the schema; create the first admin
    tq worker --executors pkg.mod    run a worker
    tq api                           run the HTTP API (needs the `api` extra)
    tq enqueue NAME '{"json": 1}'    add a task
    tq tasks | show | cancel | requeue | stats | workers | pause | resume
    tq schedule list | set | pause | resume | delete      periodic tasks
    tq archive run | tasks | show    move old finished tasks to the archive; read them there
    tq user create | list | set-password
    tq token USERNAME                mint an access token for an account
    tq seed                          demonstration accounts and tasks, for trying the web UI

Everything except `tq api` talks to Postgres directly with `TQ_DATABASE_URL`, so it is an
operator's tool: whoever can run it already has the database.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import getpass
import json
import logging
import os
import secrets
import sys
import uuid
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from pgtaskqueue._version import __version__
from pgtaskqueue.db import Database
from pgtaskqueue.enums import TERMINAL_STATUSES
from pgtaskqueue.errors import ScheduleConflict
from pgtaskqueue.monitor import Monitor
from pgtaskqueue.queue import TaskQueue
from pgtaskqueue.schedules import Schedule, Schedules
from pgtaskqueue.schemas import ScheduleOut, TaskOut
from pgtaskqueue.settings import get_settings

LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"


def _csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, default=str))


def _task_line(task: Any) -> str:
    return (
        f"{task.id}  {task.status:<9}  {task.queue_name}/{task.name}  "
        f"attempt {task.attempt}/{task.max_attempts}  "
        f"created {task.created_at.astimezone():%Y-%m-%d %H:%M:%S}"
    )


@contextlib.asynccontextmanager
async def _open():
    db = Database(application_name="tq-cli")
    try:
        yield TaskQueue(db), Monitor(db), db
    finally:
        await db.dispose()


# --------------------------------------------------------------------------- commands


async def cmd_migrate(args: argparse.Namespace) -> int:
    from pgtaskqueue import migrate

    # Alembic announces each of its plugins at INFO; the migrations themselves are the news.
    logging.getLogger("alembic.runtime.plugins").setLevel(logging.WARNING)
    # Alembic drives its own event loop, so it runs in a thread while this one waits.
    await asyncio.to_thread(migrate.upgrade, None, args.revision)
    print(f"schema is at {args.revision}")
    settings = get_settings()
    if settings.archive_database_url:
        # Always to head: --revision names a revision of the queue's history, not this one.
        await asyncio.to_thread(migrate.upgrade_archive)
        print("archive schema is at head")
    if args.no_bootstrap or not settings.bootstrap_admin_password:
        return 0
    try:
        from pgtaskqueue.auth.users import UserService
    except ImportError:
        print("skipping the admin account: the `api` extra is not installed here")
        return 0
    async with _open() as (_, _, db):
        user = await UserService(db).bootstrap_admin(
            settings.bootstrap_admin_username or "admin", settings.bootstrap_admin_password
        )
    if user is not None:
        print(f"created admin account {user.username!r}")
    return 0


async def cmd_worker(args: argparse.Namespace) -> int:
    from pgtaskqueue.worker import run_worker

    # Like `python -m`: executor modules are found relative to where the command is run,
    # so `tq worker --executors myapp.tasks` works from a project directory.
    if "" not in sys.path and os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())
    settings = get_settings()
    if args.idle_exit is not None:
        settings.worker_idle_exit_seconds = args.idle_exit
    if args.max_tasks is not None:
        settings.worker_max_tasks = args.max_tasks
    reason = await run_worker(
        executors=_csv(args.executors) if args.executors is not None else None,
        queue_name=args.queue,
        tasks=_csv(args.tasks) if args.tasks is not None else None,
        slots=args.slots,
        settings=settings,
    )
    # Always 0: stopping for a signal, for being idle or for having done enough is the
    # worker working. The container's restart policy decides whether another one starts.
    logging.getLogger("pgtaskqueue.worker").info("exit: %s", reason)
    return 0


def cmd_api(args: argparse.Namespace) -> int:
    try:
        import uvicorn
    except ImportError:
        print("`tq api` needs the api extra: pip install 'pgtaskqueue[api]'", file=sys.stderr)
        return 1
    uvicorn.run(
        "pgtaskqueue.api.app:create_app",
        factory=True,
        host=args.host,
        port=args.port,
        workers=args.workers,
        reload=args.reload,
        log_level=args.log_level.lower(),
        proxy_headers=True,
    )
    return 0


async def cmd_enqueue(args: argparse.Namespace) -> int:
    payload = json.loads(args.payload) if args.payload else {}
    if not isinstance(payload, dict):
        print("the payload must be a JSON object", file=sys.stderr)
        return 2
    async with _open() as (queue, _, _):
        task = await queue.enqueue(
            args.name,
            payload,
            queue=args.queue,
            priority=args.priority,
            delay=args.delay,
            max_attempts=args.max_attempts,
            timeout=args.timeout,
            dedupe_key=args.dedupe_key,
            created_by="cli",
        )
        print(f"{task.id}  {task.status}" + ("" if task.created else "  (already queued)"))
        if not args.wait:
            return 0
        while task.status not in TERMINAL_STATUSES:
            await asyncio.sleep(0.5)
            task = await queue.get(task.id) or task
        _print(TaskOut.of(task).model_dump(mode="json", include={"status", "output", "error"}))
        return 0 if task.status == "succeeded" else 1


async def cmd_tasks(args: argparse.Namespace) -> int:
    async with _open() as (queue, _, _):
        tasks = await queue.list(
            queue=args.queue, name=args.name, status=args.status, limit=args.limit
        )
    if args.json:
        _print([TaskOut.of(task).model_dump(mode="json") for task in tasks])
    else:
        for task in tasks:
            print(_task_line(task))
    return 0


async def cmd_show(args: argparse.Namespace) -> int:
    async with _open() as (queue, _, _):
        task = await queue.get(args.id)
        if task is None:
            print("no such task", file=sys.stderr)
            return 1
        _print(TaskOut.of(task).model_dump(mode="json"))
        if args.logs:
            for line in await queue.get_logs(task.id, limit=args.logs):
                step = f" [{line.step}]" if line.step else ""
                at = line.created_at.astimezone()
                print(f"{at:%H:%M:%S.%f} #{line.attempt} {line.level:<7}{step} {line.message}")
    return 0


async def cmd_cancel(args: argparse.Namespace) -> int:
    async with _open() as (queue, _, _):
        task = await queue.cancel(args.id)
    print(_task_line(task) if task else "not cancelled: no such task, or it had already finished")
    return 0 if task else 1


async def cmd_requeue(args: argparse.Namespace) -> int:
    async with _open() as (queue, _, _):
        task = await queue.requeue(args.id, reset_progress=args.reset_progress)
    print(_task_line(task) if task else "not requeued: no such task, or it has not finished")
    return 0 if task else 1


async def cmd_stats(args: argparse.Namespace) -> int:
    async with _open() as (_, monitor, _):
        stats = await monitor.stats(window_minutes=args.window)
    if args.json:
        _print(stats)
        return 0
    totals = stats["totals"]
    print(
        f"now: {totals['pending']} pending, {totals['running']} running, "
        f"{totals['scheduled']} scheduled   last {args.window}m: {totals['succeeded']} ok, "
        f"{totals['failed']} failed, {totals['canceled']} canceled "
        f"({totals['throughput_per_minute']}/min)"
    )
    print(
        f"{'queue/name':<40} {'sched':>7} {'pend':>7} {'run':>5} {'ok':>8} {'fail':>6} "
        f"{'p50 s':>8} {'p95 s':>8}"
    )
    for item in stats["tasks"]:
        window = item["window"]
        print(
            f"{item['queue'] + '/' + item['name']:<40} {item['scheduled']:>7} "
            f"{item['pending']:>7} {item['running']:>5} {window['succeeded']:>8} "
            f"{window['failed']:>6} {window.get('p50_seconds') or '-':>8} "
            f"{window.get('p95_seconds') or '-':>8}"
        )
    return 0


async def cmd_workers(args: argparse.Namespace) -> int:
    async with _open() as (_, monitor, _):
        workers = await monitor.list_workers(alive_only=not args.all)
    if args.json:
        _print(workers)
        return 0
    for worker in workers:
        state = "alive" if worker["alive"] else "gone "
        print(
            f"{state}  {worker['id']:<40} queue={worker['queue']} "
            f"{worker['in_flight']}/{worker['slots']} running  {worker['counters']}"
        )
    return 0


async def cmd_pause(args: argparse.Namespace) -> int:
    async with _open() as (_, monitor, _):
        queue = await monitor.set_paused(args.queue, args.command == "pause", by="cli")
    print(f"queue {queue.name!r} is {'paused' if queue.paused else 'running'}")
    return 0


def _schedule_line(schedule: Any) -> str:
    timing = (
        f"{schedule.cron} ({schedule.timezone})"
        if schedule.cron
        else f"every {schedule.every_seconds:g}s"
    )
    return (
        f"{schedule.name:<28} {schedule.queue_name}/{schedule.task_name}  {timing}  "
        f"next {schedule.next_run_at.astimezone():%Y-%m-%d %H:%M:%S}  {schedule.source}"
        + ("  (paused)" if schedule.paused else "")
    )


async def cmd_schedule(args: argparse.Namespace) -> int:
    async with _open() as (_, _, db):
        schedules = Schedules(db)
        if args.schedule_command == "list":
            rows = await schedules.list()
            if args.json:
                _print([ScheduleOut.of(row).model_dump(mode="json") for row in rows])
            else:
                for row in rows:
                    print(_schedule_line(row))
            return 0
        if args.schedule_command == "set":
            payload = json.loads(args.payload) if args.payload else {}
            if not isinstance(payload, dict):
                print("the payload must be a JSON object", file=sys.stderr)
                return 2
            try:
                row = await schedules.put(
                    args.name,
                    args.task,
                    Schedule(
                        args.cron,
                        every=args.every,
                        timezone=args.timezone,
                        payload=payload,
                        priority=args.priority,
                        max_attempts=args.max_attempts,
                        timeout=args.timeout,
                    ),
                    queue=args.queue,
                    created_by="cli",
                )
            except (ValueError, ScheduleConflict) as exc:
                print(exc, file=sys.stderr)
                return 1
            print(("created  " if row.created else "replaced  ") + _schedule_line(row))
            return 0
        if args.schedule_command == "delete":
            deleted = await schedules.delete(args.name)
            print(f"deleted {args.name!r}" if deleted else "no such schedule")
            return 0 if deleted else 1
        row = await schedules.set_paused(args.name, args.schedule_command == "pause")
        print(_schedule_line(row) if row else "no such schedule")
        return 0 if row else 1


async def cmd_archive(args: argparse.Namespace) -> int:
    from pgtaskqueue.archive import Archive, open_archive_database

    settings = get_settings()
    if not settings.archive_database_url:
        print("set TQ_ARCHIVE_DATABASE_URL to the archive database", file=sys.stderr)
        return 1
    archive_db = open_archive_database(settings, "tq-cli")
    try:
        async with _open() as (_, _, db):
            archive = Archive(db, archive_db, settings)
            if args.archive_command == "run":
                moved = await archive.move(older_than_days=args.older_than, batch=args.batch)
                print(f"archived {moved} finished task(s)")
            elif args.archive_command == "tasks":
                tasks = await archive.list(
                    queue=args.queue, name=args.name, status=args.status, limit=args.limit
                )
                if args.json:
                    _print([TaskOut.of(task).model_dump(mode="json") for task in tasks])
                else:
                    for task in tasks:
                        print(_task_line(task))
            else:
                task = await archive.get(args.id)
                if task is None:
                    print("no such task in the archive", file=sys.stderr)
                    return 1
                _print(TaskOut.of(task).model_dump(mode="json"))
                for line in await archive.get_logs(task.id, limit=args.logs) if args.logs else []:
                    step = f" [{line.step}]" if line.step else ""
                    at = line.created_at.astimezone()
                    print(f"{at:%H:%M:%S.%f} #{line.attempt} {line.level:<7}{step} {line.message}")
    finally:
        await archive_db.dispose()
    return 0


async def cmd_user(args: argparse.Namespace) -> int:
    try:
        from pgtaskqueue.auth.users import UserError, UserService
    except ImportError:
        print("`tq user` needs the api extra: pip install 'pgtaskqueue[api]'", file=sys.stderr)
        return 1
    try:
        return await _user(args, UserService)
    except UserError as exc:
        print(exc, file=sys.stderr)
        return 1


async def _user(args: argparse.Namespace, service: Any) -> int:
    async with _open() as (_, _, db):
        users = service(db)
        if args.user_command == "list":
            for user in await users.list():
                state = "" if user.is_active else "  (disabled)"
                limits = "".join(
                    f" {key}={getattr(user, key)}"
                    for key in _LIMIT_FLAGS
                    if getattr(user, key) is not None
                )
                print(
                    f"{user.username:<24} {user.kind:<8} {user.role:<10} "
                    f"queues={','.join(user.queues)} tasks={','.join(user.task_names)}"
                    f"{limits}{state}"
                )
            return 0
        if args.user_command == "set-limits":
            limits = {
                key: getattr(args, key) for key in _LIMIT_FLAGS if getattr(args, key) is not None
            }
            if not limits:
                print("give at least one limit; see `tq user set-limits --help`", file=sys.stderr)
                return 1
            user = await users.update(
                args.username,
                limits={
                    key: None if value == "default" else int(value) for key, value in limits.items()
                },
            )
            if user is None:
                print("no such user", file=sys.stderr)
                return 1
            shown = " ".join(f"{key}={getattr(user, key)}" for key in _LIMIT_FLAGS)
            print(f"limits of {args.username!r}: {shown} (None takes the default)")
            return 0

        generated = False
        password = args.password
        if password is None and args.generate:
            password, generated = secrets.token_urlsafe(24), True
        if password is None:
            password = getpass.getpass("password: ")
        if args.user_command == "create":
            user = await users.create(
                args.username,
                password,
                role=args.role,
                kind="service" if args.service else "user",
                queues=_csv(args.queues),
                task_names=_csv(args.task_names),
            )
            print(f"created {user.kind} account {user.username!r} with role {user.role}")
        else:
            user = await users.set_password(args.username, password)
            if user is None:
                print("no such user", file=sys.stderr)
                return 1
            print(f"password changed for {args.username!r}; its refresh tokens are revoked")
        if generated:
            # Shown once, here, and stored only as a hash. A service account presents it as
            # the client secret of the client-credentials grant.
            label = "client secret" if user.kind == "service" else "password"
            print(f"{label}: {password}")
    return 0


# The `tq user set-limits` flags, by the column each one sets.
_LIMIT_FLAGS = {
    "max_waiting": "tasks scheduled or pending at once",
    "max_running": "tasks pending or running at once; the rest are held",
    "enqueue_per_minute": "tasks created in any 60 seconds",
    "min_priority": "the most urgent priority the account may ask for",
}


def _limit(value: str) -> str:
    if value != "default" and not (value.isdigit() and int(value) >= 0):
        raise argparse.ArgumentTypeError("a number (0 for no limit), or 'default'")
    return value


async def cmd_token(args: argparse.Namespace) -> int:
    try:
        from pgtaskqueue.auth.tokens import AuthConfigError, TokenService, narrow_scope
        from pgtaskqueue.auth.users import UserService
    except ImportError:
        print("`tq token` needs the api extra: pip install 'pgtaskqueue[api]'", file=sys.stderr)
        return 1
    settings = get_settings()
    try:
        tokens = TokenService(settings)
    except AuthConfigError as exc:
        print(exc, file=sys.stderr)
        return 1
    if not tokens.can_issue:
        print("this configuration verifies tokens but has no key to sign them", file=sys.stderr)
        return 1
    async with _open() as (_, _, db):
        user = await UserService(db).get(args.username)
    if user is None or not user.is_active:
        print("no such active account", file=sys.stderr)
        return 1
    minutes = settings.access_token_minutes
    if args.days is not None:
        minutes = args.days * 24 * 60
    elif args.minutes is not None:
        minutes = args.minutes
    if minutes <= 0:
        print("the lifetime must be positive", file=sys.stderr)
        return 2
    scope = narrow_scope(user.role, args.scope)
    issued = tokens.issue_access(user, scope, lifetime=round(minutes * 60))
    # The token alone on stdout, so `TOKEN=$(tq token alice)` works; the rest on stderr.
    print(
        f"access token for {user.username!r} (role {user.role}"
        + (f", scope {scope!r}" if scope is not None else "")
        + f"), valid for {minutes:g} minutes. It cannot be revoked before then.",
        file=sys.stderr,
    )
    print(issued.token)
    return 0


async def cmd_seed(args: argparse.Namespace) -> int:
    from pgtaskqueue import seed

    if not args.no_users:
        try:
            import pgtaskqueue.auth.users  # noqa: F401
        except ImportError:
            print(
                "the demo accounts need the api extra: pip install 'pgtaskqueue[api]', "
                "or pass --no-users",
                file=sys.stderr,
            )
            return 1
    async with _open() as (_, _, db):
        try:
            done = await seed.seed(
                db,
                tasks=args.tasks,
                hours=args.hours,
                users=not args.no_users,
                password=args.password,
                force=args.force,
            )
        except (RuntimeError, ValueError) as exc:
            print(exc, file=sys.stderr)
            return 1
    print(
        f"seeded {done['history']} finished tasks over the last {args.hours:g} hours, "
        f"{done['pending']} pending, {done['scheduled']} scheduled, {done['running']} running, "
        f"and {done['parked']} on the paused queue {seed.PAUSED_QUEUE!r}"
    )
    if not args.no_users:
        for username, role, _ in seed.DEMO_USERS:
            state = "created" if username in done["users"] else "already there, left as it was"
            print(f"  {username:<16} {role:<10} {state}")
        if done["users"]:
            # On stderr with the other warnings: these accounts are a known way in.
            print(
                f"password for the accounts created: {args.password}\n"
                "Anyone can know it. Delete the demo accounts before this database is "
                "used for anything real.",
                file=sys.stderr,
            )
    return 0


# --------------------------------------------------------------------------- parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tq", description="PostgreSQL-backed task queue.")
    parser.add_argument("--version", action="version", version=f"tq {__version__}")
    parser.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING (default INFO)")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("migrate", help="create or upgrade the schema")
    p.add_argument("--revision", default="head")
    p.add_argument("--no-bootstrap", action="store_true", help="do not create the admin account")
    p.set_defaults(run=cmd_migrate)

    p = sub.add_parser("worker", help="run a worker")
    p.add_argument("--executors", help="modules to import, comma-separated (TQ_WORKER_EXECUTORS)")
    p.add_argument("--queue", help="queue to consume (TQ_WORKER_QUEUE, default 'default')")
    p.add_argument("--tasks", help="pools: NAME:N, glob:N, ALL:N, NAME:0 (TQ_WORKER_TASKS)")
    p.add_argument("--slots", type=int, help="slots for names given no count (TQ_WORKER_SLOTS)")
    p.add_argument("--idle-exit", type=float, help="exit after this many idle seconds")
    p.add_argument("--max-tasks", type=int, help="exit after this many tasks")
    p.set_defaults(run=cmd_worker)

    p = sub.add_parser("api", help="run the HTTP API")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--workers", type=int, default=1, help="uvicorn worker processes")
    p.add_argument("--reload", action="store_true")
    p.set_defaults(run=cmd_api)

    p = sub.add_parser("enqueue", help="add a task")
    p.add_argument("name")
    p.add_argument("payload", nargs="?", help="JSON object")
    p.add_argument("--queue", default="default")
    p.add_argument("--priority", type=int)
    p.add_argument("--delay", type=float, help="seconds from now")
    p.add_argument("--max-attempts", type=int)
    p.add_argument("--timeout", type=float)
    p.add_argument("--dedupe-key")
    p.add_argument("--wait", action="store_true", help="wait for it to finish; print the result")
    p.set_defaults(run=cmd_enqueue)

    p = sub.add_parser("tasks", help="list tasks, newest first")
    p.add_argument("--queue")
    p.add_argument("--name")
    p.add_argument("--status")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--json", action="store_true")
    p.set_defaults(run=cmd_tasks)

    p = sub.add_parser("show", help="one task, optionally with its log")
    p.add_argument("id", type=uuid.UUID)
    p.add_argument("--logs", type=int, nargs="?", const=500, default=0, metavar="N")
    p.set_defaults(run=cmd_show)

    p = sub.add_parser("cancel", help="cancel a task that has not finished")
    p.add_argument("id", type=uuid.UUID)
    p.set_defaults(run=cmd_cancel)

    p = sub.add_parser("requeue", help="run a finished task again")
    p.add_argument("id", type=uuid.UUID)
    p.add_argument("--reset-progress", action="store_true", help="forget completed steps")
    p.set_defaults(run=cmd_requeue)

    p = sub.add_parser("stats", help="queue depth and recent throughput")
    p.add_argument("--window", type=int, default=60, help="minutes (default 60)")
    p.add_argument("--json", action="store_true")
    p.set_defaults(run=cmd_stats)

    p = sub.add_parser("workers", help="list workers")
    p.add_argument("--all", action="store_true", help="include workers that have stopped")
    p.add_argument("--json", action="store_true")
    p.set_defaults(run=cmd_workers)

    for name, text in (("pause", "stop workers claiming from a queue"), ("resume", "undo pause")):
        p = sub.add_parser(name, help=text)
        p.add_argument("queue")
        p.set_defaults(run=cmd_pause)

    p = sub.add_parser("schedule", help="manage periodic tasks")
    schedule_sub = p.add_subparsers(dest="schedule_command", required=True)
    c = schedule_sub.add_parser("list")
    c.add_argument("--json", action="store_true")
    c = schedule_sub.add_parser("set", help="create a schedule, or replace it whole")
    c.add_argument("name", help="the schedule's name")
    c.add_argument("task", help="the task to enqueue")
    c.add_argument("payload", nargs="?", help="JSON object")
    timing = c.add_mutually_exclusive_group(required=True)
    timing.add_argument("--cron", help="five fields, e.g. '0 3 * * *', or @hourly, @daily, ...")
    timing.add_argument("--every", type=float, metavar="SECONDS")
    c.add_argument("--timezone", default="UTC", help="IANA zone for --cron (default UTC)")
    c.add_argument("--queue", default="default")
    c.add_argument("--priority", type=int)
    c.add_argument("--max-attempts", type=int)
    c.add_argument("--timeout", type=float)
    for name, text in (
        ("pause", "stop a schedule firing"),
        ("resume", "undo pause"),
        ("delete", "delete a schedule"),
    ):
        c = schedule_sub.add_parser(name, help=text)
        c.add_argument("name")
    p.set_defaults(run=cmd_schedule)

    p = sub.add_parser("archive", help="move old finished tasks to the archive; read them there")
    archive_sub = p.add_subparsers(dest="archive_command", required=True)
    c = archive_sub.add_parser("run", help="archive finished tasks now, until none are left")
    c.add_argument("--older-than", type=float, metavar="DAYS", help="default TQ_ARCHIVE_AFTER_DAYS")
    c.add_argument("--batch", type=int, help="tasks per transaction (TQ_ARCHIVE_BATCH_SIZE)")
    c = archive_sub.add_parser("tasks", help="list archived tasks, newest first")
    c.add_argument("--queue")
    c.add_argument("--name")
    c.add_argument("--status")
    c.add_argument("--limit", type=int, default=20)
    c.add_argument("--json", action="store_true")
    c = archive_sub.add_parser("show", help="one archived task, optionally with its log")
    c.add_argument("id", type=uuid.UUID)
    c.add_argument("--logs", type=int, nargs="?", const=500, default=0, metavar="N")
    p.set_defaults(run=cmd_archive)

    p = sub.add_parser("user", help="manage API accounts")
    user_sub = p.add_subparsers(dest="user_command", required=True)
    c = user_sub.add_parser("create")
    c.add_argument("username")
    c.add_argument(
        "--role",
        default="submitter",
        choices=["admin", "manager", "operator", "submitter", "viewer", "worker"],
    )
    c.add_argument("--service", action="store_true", help="a service account (client credentials)")
    c.add_argument("--queues", default="*", help="queue globs, comma-separated (default *)")
    c.add_argument("--task-names", default="*", help="task name globs (default *)")
    s = user_sub.add_parser("set-password")
    s.add_argument("username")
    for target in (c, s):
        target.add_argument("--password", help="omit to be prompted")
        target.add_argument("--generate", action="store_true", help="generate one and print it")
    user_sub.add_parser("list")
    c = user_sub.add_parser(
        "set-limits", help="change an account's limits; 0 is no limit, 'default' the default"
    )
    c.add_argument("username")
    for key, text in _LIMIT_FLAGS.items():
        c.add_argument(f"--{key.replace('_', '-')}", dest=key, type=_limit, help=text)
    p.set_defaults(run=cmd_user)

    p = sub.add_parser("token", help="mint an access token for an account")
    p.add_argument("username")
    life = p.add_mutually_exclusive_group()
    life.add_argument("--minutes", type=float, help="lifetime (default TQ_ACCESS_TOKEN_MINUTES)")
    life.add_argument("--days", type=float, help="lifetime in days")
    p.add_argument("--scope", help="space-separated permissions, to narrow the account's role")
    p.set_defaults(run=cmd_token)

    from pgtaskqueue.seed import DEMO_PASSWORD

    p = sub.add_parser("seed", help="demonstration accounts and tasks, for trying the web UI")
    p.add_argument("--tasks", type=int, default=400, help="finished tasks to write (default 400)")
    p.add_argument("--hours", type=float, default=24, help="how far back they reach (default 24)")
    p.add_argument("--no-users", action="store_true", help="do not create the demo accounts")
    p.add_argument("--password", default=DEMO_PASSWORD, help=f"default {DEMO_PASSWORD!r}")
    p.add_argument("--force", action="store_true", help="seed a database that has tasks already")
    p.set_defaults(run=cmd_seed)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format=LOG_FORMAT)
    run: Callable[[argparse.Namespace], int | Awaitable[int]] = args.run
    try:
        if asyncio.iscoroutinefunction(run):
            return asyncio.run(run(args))
        return run(args)  # type: ignore[return-value]
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
