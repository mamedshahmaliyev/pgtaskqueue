"""What a caller may do, in one place.

The REST routes and the MCP tools are two front doors to the same operations, and every
operation here takes the `Principal` and decides for itself whether to go ahead. Neither
front door checks permissions on its own, so neither can forget to.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Sequence
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from pgtaskqueue.api.schemas import (
    ScheduleIn,
    TaskCreate,
    TaskPatch,
    TaskTypeIn,
    UserCreate,
    UserPatch,
)
from pgtaskqueue.auth.limits import LIMITS, AccountLimits, LimitExceeded, Limits, own_limits
from pgtaskqueue.auth.principal import Principal
from pgtaskqueue.auth.roles import Permission, Role, permissions_for
from pgtaskqueue.auth.users import UserError, UserService
from pgtaskqueue.db import Database
from pgtaskqueue.enums import TERMINAL_STATUSES
from pgtaskqueue.errors import DuplicateTaskError, ScheduleConflict
from pgtaskqueue.models import ScheduleRow, Task, TaskLog, TaskType, User
from pgtaskqueue.monitor import Monitor
from pgtaskqueue.queue import NewTask, TaskQueue
from pgtaskqueue.schedules import Schedule, Schedules
from pgtaskqueue.settings import Settings

P = Permission
RESERVED_PREFIX = "tq."
TASK_TYPE_CACHE_SECONDS = 5.0


class ApiError(Exception):
    """An operation was refused. `status` is the HTTP status the REST layer answers with."""

    status = 400

    def __init__(self, detail: str, headers: dict[str, str] | None = None) -> None:
        super().__init__(detail)
        self.detail = detail
        self.headers = headers or {}


class Forbidden(ApiError):
    status = 403


class NotFound(ApiError):
    status = 404


class Conflict(ApiError):
    status = 409


class TooLarge(ApiError):
    status = 413


class Invalid(ApiError):
    status = 422


class TooMany(ApiError):
    status = 429


def _refused(exc: LimitExceeded) -> ApiError:
    error = {403: Forbidden, 413: TooLarge}.get(exc.status, TooMany)
    headers = {"Retry-After": str(int(exc.retry_after))} if exc.retry_after else None
    return error(str(exc), headers)


def _within(own: Sequence[str], wanted: Sequence[str]) -> bool:
    """Whether an ACL of `wanted` patterns reaches no further than `own`. Literal
    membership, because whether one glob covers another is not something to guess at."""
    return "*" in own or set(wanted) <= set(own)


class Service:
    def __init__(self, db: Database, settings: Settings) -> None:
        self.db = db
        self.settings = settings
        self.queue = TaskQueue(db, settings)
        self.monitor = Monitor(db, settings)
        self.schedule_store = Schedules(db, settings)
        self.limits = AccountLimits(db, settings)
        self.users = UserService(db)
        self._types: dict[str, TaskType] = {}
        self._types_loaded = 0.0

    # ------------------------------------------------------------------ checks

    @staticmethod
    def need(who: Principal, permission: Permission) -> None:
        if not who.has(permission):
            raise Forbidden(f"this token lacks the {permission.value!r} permission")

    @staticmethod
    def _need_acl(who: Principal, queue: str, name: str | None = None) -> None:
        if not who.can_queue(queue):
            raise Forbidden(f"queue {queue!r} is outside this account's ACL")
        if name is not None and not who.can_name(name):
            raise Forbidden(f"task name {name!r} is outside this account's ACL")

    async def _task_types(self) -> dict[str, TaskType]:
        # Read on almost every enqueue and changed only when a worker starts, so a few
        # seconds of staleness buys one query instead of thousands.
        if time.monotonic() - self._types_loaded > TASK_TYPE_CACHE_SECONDS:
            self._types = {t.name: t for t in await self.monitor.list_task_types()}
            self._types_loaded = time.monotonic()
        return self._types

    async def _visible(self, who: Principal, task_id: uuid.UUID) -> Task:
        task = await self.queue.get(task_id)
        # The same answer for "does not exist" and "not yours": an id is not a way to
        # learn what other tenants have queued.
        if task is None or not who.can_see(task):
            raise NotFound("no such task")
        return task

    async def _manageable(self, who: Principal, task_id: uuid.UUID) -> Task:
        task = await self.queue.get(task_id)
        if task is None or not (who.can_see(task) or who.can_manage(task)):
            raise NotFound("no such task")
        if not who.can_manage(task):
            raise Forbidden("this token may not change this task")
        return task

    # ------------------------------------------------------------------ enqueue

    async def _acceptable(
        self, who: Principal, queue: str, name: str, payload: dict[str, Any]
    ) -> None:
        """Whether `who` may put a task of this name, with this payload, on this queue:
        now, or later through a schedule."""
        if name.startswith(RESERVED_PREFIX):
            raise Invalid(f"task names starting with {RESERVED_PREFIX!r} are reserved")
        self._need_acl(who, queue, name)
        size = len(json.dumps(payload, separators=(",", ":")).encode())
        if size > self.settings.max_payload_bytes:
            raise TooLarge(
                f"payload is {size} bytes; the limit is {self.settings.max_payload_bytes}. "
                "Store large inputs elsewhere and pass a reference."
            )
        task_type = (await self._task_types()).get(name)
        if task_type is None and self.settings.api_require_registered_tasks:
            raise Invalid(f"no worker has registered a task named {name!r}")
        if task_type is not None and task_type.payload_schema:
            self._validate_payload(name, payload, task_type.payload_schema)

    def _priority(self, limits: Limits, asked: int | None) -> int | None:
        try:
            return limits.priority(asked, self.settings.default_priority)
        except LimitExceeded as exc:
            raise _refused(exc) from exc

    async def _new_task(self, who: Principal, req: TaskCreate, limits: Limits) -> NewTask:
        await self._acceptable(who, req.queue, req.name, req.payload)
        return NewTask(
            req.name,
            req.payload,
            queue=req.queue,
            priority=self._priority(limits, req.priority),
            run_after=req.run_after,
            delay=req.delay_seconds,
            max_attempts=req.max_attempts,
            timeout=req.timeout_seconds,
            dedupe_key=req.dedupe_key,
            metadata=req.metadata,
            callback=req.callback.model_dump(mode="json", exclude_none=True)
            if req.callback
            else None,
            created_by=who.subject,
        )

    @staticmethod
    def _validate_payload(name: str, payload: dict[str, Any], schema: dict[str, Any]) -> None:
        try:
            errors = sorted(Draft202012Validator(schema).iter_errors(payload), key=str)
        except SchemaError:
            # A worker published a schema that is not valid JSON Schema. That is the
            # worker's bug; the executor still validates the payload when it runs.
            return
        if errors:
            details = "; ".join(
                f"{'/'.join(str(p) for p in e.absolute_path) or '(payload)'}: {e.message}"
                for e in errors[:5]
            )
            raise Invalid(f"payload does not match the schema of {name!r}: {details}")

    async def enqueue(self, who: Principal, req: TaskCreate) -> Task:
        self.need(who, P.TASKS_ENQUEUE)
        limits = await self.limits.of(who.subject)
        new = await self._new_task(who, req, limits)
        try:
            async with self.db.transaction() as tx:
                await self.limits.admit(tx, who.subject, limits, 1)
                return await self.queue.enqueue(
                    new.name,
                    new.payload,
                    queue=new.queue,
                    priority=new.priority,
                    run_after=new.run_after,
                    delay=new.delay,
                    max_attempts=new.max_attempts,
                    timeout=new.timeout,
                    dedupe_key=new.dedupe_key,
                    metadata=new.metadata,
                    callback=new.callback,
                    created_by=new.created_by,
                    session=tx,
                )
        except DuplicateTaskError as exc:
            raise Conflict(str(exc)) from exc
        except LimitExceeded as exc:
            raise _refused(exc) from exc

    async def enqueue_batch(self, who: Principal, reqs: Sequence[TaskCreate]) -> list[Task]:
        self.need(who, P.TASKS_ENQUEUE)
        limits = await self.limits.of(who.subject)
        # Every task is checked before any is written: a batch is accepted or refused whole.
        new = [await self._new_task(who, req, limits) for req in reqs]
        try:
            async with self.db.transaction() as tx:
                await self.limits.admit(tx, who.subject, limits, len(new))
                return await self.queue.enqueue_many(new, session=tx)
        except LimitExceeded as exc:
            raise _refused(exc) from exc

    # ------------------------------------------------------------------ read

    async def get_task(self, who: Principal, task_id: uuid.UUID, wait: float = 0) -> Task:
        """One task. With `wait`, holds on until it finishes or the time is up."""
        task = await self._visible(who, task_id)
        deadline = time.monotonic() + min(max(wait, 0), self.settings.long_poll_max_seconds)
        delay = 0.2
        while task.status not in TERMINAL_STATUSES:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            await asyncio.sleep(min(delay, remaining))
            delay = min(delay * 1.5, 1.0)
            current = await self.queue.get(task_id)
            if current is None:
                raise NotFound("no such task")
            task = current
        return task

    async def list_tasks(
        self,
        who: Principal,
        *,
        queue: str | None = None,
        name: str | None = None,
        status: Sequence[str] | None = None,
        created_by: str | None = None,
        worker_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        before: uuid.UUID | None = None,
        limit: int = 50,
    ) -> list[Task]:
        self.need(who, P.TASKS_READ)
        if not who.has(P.TASKS_READ_ALL):
            if created_by not in (None, who.subject):
                return []
            created_by = who.subject
        return await self.queue.list(
            queue=queue,
            name=name,
            status=list(status) if status else None,
            created_by=created_by,
            worker_id=worker_id,
            metadata=metadata,
            queue_patterns=who.queue_patterns,
            name_patterns=who.name_patterns,
            before=before,
            limit=limit,
        )

    async def task_logs(
        self, who: Principal, task_id: uuid.UUID, *, after_id: int = 0, limit: int = 500
    ) -> list[TaskLog]:
        await self._visible(who, task_id)
        return await self.queue.get_logs(task_id, after_id=after_id, limit=limit)

    # ------------------------------------------------------------------ control

    async def cancel(self, who: Principal, task_id: uuid.UUID) -> Task:
        await self._manageable(who, task_id)
        task = await self.queue.cancel(task_id)
        if task is None:
            raise Conflict("the task had already finished")
        return task

    async def requeue(self, who: Principal, task_id: uuid.UUID, reset_progress: bool) -> Task:
        await self._manageable(who, task_id)
        try:
            task = await self.queue.requeue(task_id, reset_progress=reset_progress)
        except DuplicateTaskError as exc:
            raise Conflict(str(exc)) from exc
        if task is None:
            raise Conflict("only a finished task can be requeued; cancel it first")
        return task

    async def update(self, who: Principal, task_id: uuid.UUID, patch: TaskPatch) -> Task:
        current = await self._manageable(who, task_id)
        fields = patch.model_dump(exclude_unset=True)
        if fields.get("priority") is not None:
            self._priority(await self.limits.of(who.subject), fields["priority"])
        if "payload" in fields and fields["payload"] is not None:
            task_type = (await self._task_types()).get(current.name)
            if task_type is not None and task_type.payload_schema:
                self._validate_payload(current.name, fields["payload"], task_type.payload_schema)
        task = await self.queue.update_waiting(task_id, **fields)
        if task is None:
            raise Conflict("the task has started or finished; it can no longer be changed")
        return task

    # ------------------------------------------------------------------ schedules
    #
    # A schedule is covered by an ACL through what it enqueues: its queue and its task name.

    async def _schedule(self, who: Principal, name: str) -> ScheduleRow:
        schedule = await self.schedule_store.get(name)
        # As for tasks: one answer for "does not exist" and "outside your ACL".
        if schedule is None or not who.can_touch(schedule.queue_name, schedule.task_name):
            raise NotFound("no such schedule")
        return schedule

    async def schedules(self, who: Principal) -> list[ScheduleRow]:
        self.need(who, P.SCHEDULES_READ)
        return await self.schedule_store.list(
            queue_patterns=who.queue_patterns, name_patterns=who.name_patterns
        )

    async def get_schedule(self, who: Principal, name: str) -> ScheduleRow:
        self.need(who, P.SCHEDULES_READ)
        return await self._schedule(who, name)

    async def put_schedule(self, who: Principal, name: str, req: ScheduleIn) -> ScheduleRow:
        """Create the schedule or replace it whole."""
        self.need(who, P.SCHEDULES_MANAGE)
        await self._acceptable(who, req.queue, req.task, req.payload)
        # Replacing is also a change to what was there: a schedule outside the caller's
        # ACL is not theirs to take over by pointing it at a task that is inside it.
        current = await self.schedule_store.get(name)
        if current is not None and not who.can_touch(current.queue_name, current.task_name):
            raise Conflict("a schedule with this name already exists")
        # The tasks it fires are the caller's: they count towards its waiting and running
        # limits, and are held to its priority limit here, once, rather than refused later.
        priority = self._priority(await self.limits.of(who.subject), req.priority)
        schedule = Schedule(
            req.cron,
            every=req.every_seconds,
            timezone=req.timezone,
            payload=req.payload,
            priority=priority,
            max_attempts=req.max_attempts,
            timeout=req.timeout_seconds,
        )
        try:
            return await self.schedule_store.put(
                name, req.task, schedule, queue=req.queue, created_by=who.subject
            )
        except ValueError as exc:
            raise Invalid(str(exc)) from exc
        except ScheduleConflict as exc:
            raise Conflict(str(exc)) from exc

    async def set_schedule_paused(self, who: Principal, name: str, paused: bool) -> ScheduleRow:
        self.need(who, P.SCHEDULES_MANAGE)
        await self._schedule(who, name)
        schedule = await self.schedule_store.set_paused(name, paused)
        if schedule is None:
            raise NotFound("no such schedule")
        return schedule

    async def delete_schedule(self, who: Principal, name: str) -> None:
        self.need(who, P.SCHEDULES_MANAGE)
        await self._schedule(who, name)
        await self.schedule_store.delete(name)

    # ------------------------------------------------------------------ accounts
    #
    # `users:manage` lets an account manage others, but never past itself. It may give a
    # role only if it holds every permission that role grants, and ACL patterns only out of
    # its own; it may change, reset or remove only an account it could have created. An
    # admin holds every permission and has `*`, so none of this limits it. A manager holds
    # neither `tasks:work` nor admin's, so it can make neither a worker nor an admin, and
    # cannot touch one. And no account loosens its own role, ACL or limits: that takes
    # someone else.

    @staticmethod
    def _grantable(who: Principal, role: str, queues: Sequence[str], names: Sequence[str]) -> bool:
        return (
            permissions_for(role) <= who.permissions
            and _within(who.queues, queues)
            and _within(who.task_names, names)
        )

    def _may_manage(self, who: Principal, user: User) -> bool:
        return self._grantable(who, user.role, user.queues, user.task_names)

    async def _account(self, who: Principal, username: str) -> User:
        self.need(who, P.USERS_MANAGE)
        user = await self.users.get(username)
        if user is None:
            raise NotFound("no such user")
        if not self._may_manage(who, user):
            raise Forbidden(
                f"{username!r} has a role or ACL that reaches beyond this account's; ask an admin"
            )
        return user

    async def accounts(self, who: Principal) -> list[tuple[User, bool]]:
        """Every account, and whether `who` may change it."""
        self.need(who, P.USERS_MANAGE)
        return [(user, self._may_manage(who, user)) for user in await self.users.list()]

    def roles(self, who: Principal) -> list[dict[str, Any]]:
        self.need(who, P.USERS_MANAGE)
        return [
            {
                "name": role.value,
                "permissions": sorted(permissions_for(role)),
                "assignable": permissions_for(role) <= who.permissions,
            }
            for role in Role
        ]

    async def create_account(self, who: Principal, req: UserCreate) -> User:
        self.need(who, P.USERS_MANAGE)
        if not self._grantable(who, req.role, req.queues, req.task_names):
            raise Forbidden("this account may not give a role or ACL that reaches beyond its own")
        try:
            return await self.users.create(
                req.username,
                req.password,
                role=req.role,
                kind=req.kind,
                queues=req.queues,
                task_names=req.task_names,
                limits={key: getattr(req, key) for key in LIMITS},
            )
        except UserError as exc:
            raise Invalid(str(exc)) from exc

    async def update_account(self, who: Principal, username: str, req: UserPatch) -> User:
        current = await self._account(who, username)
        fields = req.model_dump(exclude_unset=True)
        limits = {key: fields.pop(key) for key in LIMITS if key in fields}
        if username == who.subject:
            # Not a security rule, a seatbelt: the last admin locking itself out is a bad day.
            if fields.get("is_active") is False or fields.get("role") not in (None, who.role):
                raise Conflict("change your own role from another account")
            # This one is a rule: a limit an account could lift itself would limit nothing.
            loosening = limits or fields.get("queues") or fields.get("task_names")
            if loosening and not who.permissions >= set(Permission):
                raise Forbidden("ask an admin to change your own ACL or limits")
        role = fields.get("role") or current.role
        queues = fields.get("queues") or current.queues
        names = fields.get("task_names") or current.task_names
        if not self._grantable(who, role, queues, names):
            raise Forbidden("this account may not give a role or ACL that reaches beyond its own")
        try:
            user = await self.users.update(username, limits=limits, **fields)
        except UserError as exc:
            raise Invalid(str(exc)) from exc
        if user is None:
            raise NotFound("no such user")
        return user

    async def set_account_password(self, who: Principal, username: str, password: str) -> None:
        """Anyone may set their own; an account manager, the password of anyone it manages."""
        if username != who.subject:
            await self._account(who, username)
        try:
            user = await self.users.set_password(username, password)
        except UserError as exc:
            raise Invalid(str(exc)) from exc
        if user is None:
            raise NotFound("no such user")

    async def delete_account(self, who: Principal, username: str) -> None:
        if username == who.subject:
            self.need(who, P.USERS_MANAGE)
            raise Conflict("delete your own account from another account")
        await self._account(who, username)
        if not await self.users.delete(username):
            raise NotFound("no such user")

    async def account_limits(self, who: Principal, username: str | None = None) -> dict[str, Any]:
        """An account's limits and what it has now. Without `username`, the caller's own,
        which any account may read."""
        if username is None or username == who.subject:
            username, user = who.subject, await self.users.get(who.subject)
        else:
            user = await self._account(who, username)
        return {
            "username": username,
            "effective": self.limits.combine(user).as_dict(),
            "account": own_limits(user),
            "defaults": Limits.defaults(self.settings).as_dict(),
            "usage": await self.limits.usage(username),
        }

    # ------------------------------------------------------------------ observability

    async def task_types(self, who: Principal) -> list[TaskType]:
        if not (who.has(P.TASKS_ENQUEUE) or who.has(P.TASKS_READ)):
            raise Forbidden("this token may not list task types")
        return await self.monitor.list_task_types(who.name_patterns)

    async def queues(self, who: Principal) -> list[dict[str, Any]]:
        self.need(who, P.STATS_READ)
        return await self.monitor.list_queues(who.queue_patterns)

    async def set_paused(
        self, who: Principal, queue: str, paused: bool, note: str | None = None
    ) -> dict[str, Any]:
        self.need(who, P.QUEUES_MANAGE)
        self._need_acl(who, queue)
        await self.monitor.set_paused(queue, paused, by=who.subject, note=note)
        return next(q for q in await self.monitor.list_queues() if q["name"] == queue)

    async def workers(self, who: Principal, alive_only: bool = True) -> list[dict[str, Any]]:
        self.need(who, P.WORKERS_READ)
        return await self.monitor.list_workers(
            alive_only=alive_only, queue_patterns=who.queue_patterns
        )

    async def stats(self, who: Principal, window_minutes: int = 60) -> dict[str, Any]:
        self.need(who, P.STATS_READ)
        return await self.monitor.stats(
            window_minutes=window_minutes,
            queue_patterns=who.queue_patterns,
            name_patterns=who.name_patterns,
        )

    async def timeline(
        self, who: Principal, window_minutes: int = 60, buckets: int = 30
    ) -> dict[str, Any]:
        self.need(who, P.STATS_READ)
        return await self.monitor.timeline(
            window_minutes=window_minutes,
            buckets=buckets,
            queue_patterns=who.queue_patterns,
            name_patterns=who.name_patterns,
        )

    # ------------------------------------------------------------------ worker protocol
    #
    # For workers that cannot reach Postgres: a GPU box in another network, a customer's
    # machine. They get the same guarantees through HTTP, because each call below is the
    # same conditional statement an in-network worker would have run itself.

    @staticmethod
    def worker_id(who: Principal, client_id: str) -> str:
        # Namespaced by account, so one account's worker cannot settle a task claimed by
        # another's by guessing its id.
        return f"http:{who.subject}:{client_id}"

    async def register_task_types(
        self, who: Principal, worker_id: str, types: Sequence[TaskTypeIn]
    ) -> None:
        self.need(who, P.TASKS_WORK)
        for task_type in types:
            if task_type.name.startswith(RESERVED_PREFIX) or not who.can_name(task_type.name):
                raise Forbidden(f"task name {task_type.name!r} is outside this account's ACL")
        await self.monitor.register_task_types(
            [t.model_dump() for t in types], registered_by=self.worker_id(who, worker_id)
        )
        self._types_loaded = 0.0

    async def claim(
        self,
        who: Principal,
        worker_id: str,
        names: Sequence[str],
        *,
        queue: str,
        limit: int,
        wait: float = 0,
    ) -> list[Task]:
        self.need(who, P.TASKS_WORK)
        self._need_acl(who, queue)
        for name in names:
            if name.startswith(RESERVED_PREFIX) or not who.can_name(name):
                raise Forbidden(f"task name {name!r} is outside this account's ACL")
        owner = self.worker_id(who, worker_id)
        deadline = time.monotonic() + min(max(wait, 0), self.settings.long_poll_max_seconds)
        while True:
            tasks = await self.queue.claim(names, worker_id=owner, queue=queue, limit=limit)
            remaining = deadline - time.monotonic()
            if tasks or remaining <= 0:
                return tasks
            await asyncio.sleep(min(1.0, remaining))

    async def heartbeat(
        self, who: Principal, worker_id: str, task_ids: Sequence[uuid.UUID]
    ) -> set[uuid.UUID]:
        self.need(who, P.TASKS_WORK)
        return await self.queue.heartbeat(self.worker_id(who, worker_id), list(task_ids))

    async def succeed(
        self, who: Principal, task_id: uuid.UUID, worker_id: str, attempt: int, output: Any
    ) -> Task | None:
        self.need(who, P.TASKS_WORK)
        return await self.queue.succeed(
            task_id, worker_id=self.worker_id(who, worker_id), attempt=attempt, output=output
        )

    async def fail(
        self,
        who: Principal,
        task_id: uuid.UUID,
        worker_id: str,
        attempt: int,
        error: dict[str, Any],
        *,
        retry: bool,
        retry_in: float | None,
    ) -> Task | None:
        self.need(who, P.TASKS_WORK)
        owner = self.worker_id(who, worker_id)
        delay: float | None = None
        if retry:
            task = await self.queue.get(task_id)
            if task is not None and attempt < task.max_attempts:
                delay = self.queue.backoff(attempt) if retry_in is None else retry_in
        error = {**error, "attempt": attempt, "worker": owner}
        return await self.queue.fail(
            task_id, worker_id=owner, attempt=attempt, error=error, retry_in=delay
        )

    async def progress(
        self,
        who: Principal,
        task_id: uuid.UUID,
        worker_id: str,
        attempt: int,
        patch: dict[str, Any],
        step: str | None,
    ) -> bool:
        self.need(who, P.TASKS_WORK)
        return await self.queue.set_progress(
            task_id, patch, worker_id=self.worker_id(who, worker_id), attempt=attempt, step=step
        )

    async def append_logs(
        self,
        who: Principal,
        task_id: uuid.UUID,
        worker_id: str,
        attempt: int,
        lines: Sequence[dict[str, Any]],
    ) -> None:
        self.need(who, P.TASKS_WORK)
        task = await self.queue.get(task_id)
        # Only the worker that last held the task writes to its log — which still lets it
        # flush the tail of its lines after settling, until someone else claims the task.
        if task is None or task.worker_id != self.worker_id(who, worker_id):
            raise NotFound("no such task held by this worker")
        limit = self.settings.log_message_max_chars
        await self.queue.append_logs(
            [
                {
                    **line,
                    "task_id": task_id,
                    "attempt": attempt,
                    "level": line.get("level")
                    if line.get("level") in ("debug", "info", "warning", "error")
                    else "info",
                    "message": str(line["message"]).replace("\x00", "")[:limit],
                }
                for line in lines
            ]
        )

    async def release(
        self, who: Principal, worker_id: str, task_ids: Sequence[uuid.UUID] | None
    ) -> int:
        self.need(who, P.TASKS_WORK)
        ids = list(task_ids) if task_ids is not None else None
        return await self.queue.release(self.worker_id(who, worker_id), ids)
