"""Roles, and the permissions each one carries.

A role answers "what kind of thing may this account do"; the account's ACL (queue and task
name patterns) answers "to which tasks". Both must allow an action. docs/auth.md has the
table and the reasoning.
"""

from __future__ import annotations

from enum import StrEnum


class Permission(StrEnum):
    TASKS_ENQUEUE = "tasks:enqueue"
    # See tasks. Alone, only the ones the caller created.
    TASKS_READ = "tasks:read"
    TASKS_READ_ALL = "tasks:read_all"
    # Cancel, requeue and edit tasks. Alone, only the ones the caller created.
    TASKS_MANAGE = "tasks:manage"
    TASKS_MANAGE_ALL = "tasks:manage_all"
    # Claim and settle tasks over HTTP: the remote-worker protocol.
    TASKS_WORK = "tasks:work"
    QUEUES_MANAGE = "queues:manage"
    SCHEDULES_READ = "schedules:read"
    # Create, replace, pause and delete periodic schedules.
    SCHEDULES_MANAGE = "schedules:manage"
    STATS_READ = "stats:read"
    WORKERS_READ = "workers:read"
    USERS_MANAGE = "users:manage"


class Role(StrEnum):
    ADMIN = "admin"
    MANAGER = "manager"
    OPERATOR = "operator"
    SUBMITTER = "submitter"
    VIEWER = "viewer"
    WORKER = "worker"


P = Permission

_OPERATOR = frozenset(
    {
        P.TASKS_ENQUEUE,
        P.TASKS_READ,
        P.TASKS_READ_ALL,
        P.TASKS_MANAGE,
        P.TASKS_MANAGE_ALL,
        P.QUEUES_MANAGE,
        P.SCHEDULES_READ,
        P.SCHEDULES_MANAGE,
        P.STATS_READ,
        P.WORKERS_READ,
    }
)

ROLE_PERMISSIONS: dict[Role, frozenset[Permission]] = {
    # Everything, including accounts.
    Role.ADMIN: frozenset(Permission),
    # Runs the queue day to day: sees and manages every task, pauses queues, manages
    # schedules. No accounts, and does not act as a worker.
    Role.OPERATOR: _OPERATOR,
    # An operator who also manages accounts, but only accounts whose role grants nothing
    # beyond its own: not admins, not workers. See `Service` for the whole rule.
    Role.MANAGER: _OPERATOR | {P.USERS_MANAGE},
    # An application that submits work and follows its own tasks. The usual role for a
    # service account.
    Role.SUBMITTER: frozenset({P.TASKS_ENQUEUE, P.TASKS_READ, P.TASKS_MANAGE}),
    # Read-only: dashboards, on-call, an assistant that should look but not touch.
    Role.VIEWER: frozenset(
        {P.TASKS_READ, P.TASKS_READ_ALL, P.SCHEDULES_READ, P.STATS_READ, P.WORKERS_READ}
    ),
    # A remote worker: claims and settles, and nothing else.
    Role.WORKER: frozenset({P.TASKS_WORK}),
}


def permissions_for(role: str) -> frozenset[Permission]:
    try:
        return ROLE_PERMISSIONS[Role(role)]
    except ValueError:
        return frozenset()
