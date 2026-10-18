from __future__ import annotations

import fnmatch
from collections.abc import Sequence
from dataclasses import dataclass

from pgtaskqueue.auth.roles import Permission
from pgtaskqueue.models import Task


@dataclass(frozen=True, slots=True)
class Principal:
    """Who is calling, as established from a verified token. Nothing here is looked up per
    request: the token carries it, which is what keeps the API stateless."""

    subject: str
    role: str
    permissions: frozenset[str]
    # The ACL: glob patterns. `*` in either means "any".
    queues: tuple[str, ...] = ("*",)
    task_names: tuple[str, ...] = ("*",)

    def has(self, permission: Permission | str) -> bool:
        return str(permission) in self.permissions

    def can_queue(self, queue: str) -> bool:
        return _allowed(queue, self.queues)

    def can_name(self, name: str) -> bool:
        return _allowed(name, self.task_names)

    def can_touch(self, queue: str, name: str) -> bool:
        return self.can_queue(queue) and self.can_name(name)

    def can_see(self, task: Task) -> bool:
        if not self.has(Permission.TASKS_READ) or not self.can_touch(task.queue_name, task.name):
            return False
        return self.has(Permission.TASKS_READ_ALL) or task.created_by == self.subject

    def can_manage(self, task: Task) -> bool:
        if not self.has(Permission.TASKS_MANAGE) or not self.can_touch(task.queue_name, task.name):
            return False
        return self.has(Permission.TASKS_MANAGE_ALL) or task.created_by == self.subject

    @property
    def queue_patterns(self) -> Sequence[str] | None:
        """For filtering in SQL. None when nothing needs filtering."""
        return None if "*" in self.queues else self.queues

    @property
    def name_patterns(self) -> Sequence[str] | None:
        return None if "*" in self.task_names else self.task_names


def _allowed(value: str, patterns: Sequence[str]) -> bool:
    return any(fnmatch.fnmatchcase(value, pattern) for pattern in patterns)
