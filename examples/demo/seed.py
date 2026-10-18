"""The public demo: its accounts, their limits, and the data it starts from.

Everyone who tries the demo signs in with the same few accounts, so the limits here bound
what the whole demo can ask of the server, not what one visitor can. Three things keep it
small:

* **limits** on every account, so the accounts together can run fewer tasks at once than
  the workers have slots, and none can jump the queue with a low priority;
* **an ACL** that leaves out the example executors a stranger could misuse: `demo.fetch`
  fetches any URL from the server, and `report.fanout` makes up to 10,000 tasks from inside
  the worker, where no account limit sees them;
* **repair**, every minute (`examples/demo/reset.py`): the demo accounts get their password,
  role, ACL and limits back, and accounts and schedules that visitors made past a cap go.

`reset` is the hourly part: it empties the queue, removes every account that is not an admin
or a worker, and seeds again. docs/demo.md is what visitors read.
"""

from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import Any

from sqlalchemy import bindparam, func, select, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.types import Text

from pgtaskqueue import seed as base
from pgtaskqueue.auth.passwords import verify_password
from pgtaskqueue.auth.users import UserService
from pgtaskqueue.db import Database
from pgtaskqueue.models import Task
from pgtaskqueue.settings import Settings

PASSWORD = base.DEMO_PASSWORD
# Every task name the compose workers run, but `demo.fetch` and `report.fanout`.
TASK_NAMES = (
    "demo.echo",
    "demo.sleep",
    "demo.flaky",
    "demo.wait_until",
    "demo.hash",
    "report.build",
    "report.notify",
    "image.*",
)
# min_priority is the queue's default priority, so no demo task runs ahead of another.
# The running limits add up to 11, under the general worker's 20 slots, so the seeded and
# scheduled work always finds a free one.
STAFF_LIMITS = {"max_waiting": 50, "max_running": 4, "enqueue_per_minute": 30, "min_priority": 100}
SUBMITTER_LIMITS = {
    "max_waiting": 30,
    "max_running": 3,
    "enqueue_per_minute": 20,
    "min_priority": 100,
}
# Accounts and schedules visitors may add before the oldest-first cap removes the newest.
# A manager's accounts are no looser than its own, but each brings its own running limit.
MAX_EXTRA_ACCOUNTS = 10
MAX_SCHEDULES = 10
# Made only by an admin, so not by a visitor: what the hourly reset keeps.
KEPT_ROLES = ("admin", "worker")


@dataclass(frozen=True, slots=True)
class DemoAccount:
    username: str
    role: str
    limits: dict[str, int]


ACCOUNTS = (
    DemoAccount("demo-manager", "manager", STAFF_LIMITS),
    DemoAccount("demo-operator", "operator", STAFF_LIMITS),
    DemoAccount("demo-submitter", "submitter", SUBMITTER_LIMITS),
    # A viewer enqueues nothing; the limits are there so the manager may manage it.
    DemoAccount("demo-viewer", "viewer", SUBMITTER_LIMITS),
)
_NAMES = [account.username for account in ACCOUNTS]

_WIPE = (
    # Waits for nothing long: a claim in flight holds its rows for milliseconds. Past this,
    # the reset fails and is tried again rather than stalling the workers behind its lock.
    "SET LOCAL lock_timeout = '10s'",
    # Not tq_task_types or tq_workers: the running workers wrote those and would not again
    # until they restart.
    "TRUNCATE tq_tasks, tq_task_logs, tq_queues",
    "DELETE FROM tq_schedules WHERE source = 'api'",
)
_DROP_ACCOUNTS = text("DELETE FROM tq_users WHERE role <> ALL(:kept)").bindparams(
    bindparam("kept", type_=ARRAY(Text))
)
# The newest past the cap: what a visitor made last is what goes.
_CAP_ACCOUNTS = text(
    """
    DELETE FROM tq_users WHERE id IN (
        SELECT id FROM tq_users
        WHERE role <> ALL(:kept) AND username <> ALL(:demo)
        ORDER BY created_at, id
        OFFSET :cap
    )
    """
).bindparams(bindparam("kept", type_=ARRAY(Text)), bindparam("demo", type_=ARRAY(Text)))
_CAP_SCHEDULES = text(
    """
    DELETE FROM tq_schedules WHERE name IN (
        SELECT name FROM tq_schedules WHERE source = 'api'
        ORDER BY created_at, name
        OFFSET :cap
    )
    """
)
_DROP_NAMES = text("DELETE FROM tq_tasks WHERE name = ANY(:names)").bindparams(
    bindparam("names", type_=ARRAY(Text))
)


def allowed(name: str) -> bool:
    return any(fnmatchcase(name, pattern) for pattern in TASK_NAMES)


async def repair(db: Database) -> dict[str, Any]:
    """Put the demo accounts back as they should be, and cap what visitors added.

    Writes only what differs, and sets a password only when it has changed: setting one
    ends the account's sessions, and this runs every minute.
    """
    users = UserService(db)
    fixed = []
    for account in ACCOUNTS:
        user = await users.get(account.username)
        if user is None:
            await users.create(
                account.username,
                PASSWORD,
                role=account.role,
                task_names=TASK_NAMES,
                limits=account.limits,
            )
            fixed.append(account.username)
            continue
        if not await verify_password(user.password_hash, PASSWORD):
            await users.set_password(account.username, PASSWORD)
            fixed.append(account.username)
        wanted: dict[str, Any] = {
            "role": account.role,
            "queues": ["*"],
            "task_names": list(TASK_NAMES),
            "is_active": True,
            **account.limits,
        }
        if any(getattr(user, key) != value for key, value in wanted.items()):
            await users.update(
                account.username,
                role=account.role,
                queues=["*"],
                task_names=TASK_NAMES,
                is_active=True,
                limits=account.limits,
            )
            if account.username not in fixed:
                fixed.append(account.username)
    async with db.transaction() as tx:
        accounts = await tx.execute(
            _CAP_ACCOUNTS, {"kept": list(KEPT_ROLES), "demo": _NAMES, "cap": MAX_EXTRA_ACCOUNTS}
        )
        schedules = await tx.execute(_CAP_SCHEDULES, {"cap": MAX_SCHEDULES})
    return {
        "fixed": fixed,
        "accounts_removed": accounts.rowcount,
        "schedules_removed": schedules.rowcount,
    }


async def reset(db: Database, settings: Settings | None = None) -> dict[str, Any]:
    """Empty the queue and the visitors' accounts and schedules, then seed the demo again."""
    async with db.transaction() as tx:
        for statement in _WIPE:
            await tx.execute(text(statement))
        await tx.execute(_DROP_ACCOUNTS, {"kept": list(KEPT_ROLES)})
    await repair(db)
    done = await base.seed(db, settings, users=False, force=True)
    # The base seeder writes every example name. A demo account could not see the ones its
    # ACL leaves out, and the pending ones would run for real.
    outside = sorted({kind.name for kind in base.KINDS if not allowed(kind.name)})
    async with db.transaction() as tx:
        await tx.execute(_DROP_NAMES, {"names": outside})
        tasks = await tx.scalar(select(func.count()).select_from(Task))
    return {"tasks": tasks, **{key: done[key] for key in ("running", "parked")}}
