"""Accounts: people who sign in with a password, and services that use client credentials."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from pgtaskqueue.auth.limits import LIMITS
from pgtaskqueue.auth.passwords import hash_password, needs_rehash, verify_password
from pgtaskqueue.auth.roles import Role
from pgtaskqueue.db import Database
from pgtaskqueue.models import User

USERNAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@-]{0,62}$")
MIN_PASSWORD_LENGTH = 8


class UserError(ValueError):
    """The request cannot be carried out as given. The message is safe to show."""


def _check(username: str | None = None, password: str | None = None, role: str | None = None):
    if username is not None and not USERNAME_RE.match(username):
        raise UserError("username: 1-63 characters from letters, digits, . _ @ -")
    if password is not None and len(password) < MIN_PASSWORD_LENGTH:
        raise UserError(f"password must be at least {MIN_PASSWORD_LENGTH} characters")
    if role is not None and role not in {r.value for r in Role}:
        raise UserError(f"role must be one of {', '.join(r.value for r in Role)}")


def _limits(value: Mapping[str, int | None] | None) -> dict[str, int | None]:
    """The limits to store. A key that is present is set, to NULL when its value is None
    (back to the default); a key that is absent is left as it is."""
    limits = dict(value or {})
    if unknown := set(limits) - set(LIMITS):
        raise UserError(f"unknown limits: {', '.join(sorted(unknown))}")
    for key, limit in limits.items():
        if limit is not None and limit < 0:
            raise UserError(f"{key} must be 0 (no limit) or more")
    if (limits.get("min_priority") or 0) > 32767:
        raise UserError("min_priority must be at most 32767")
    return limits


def _patterns(value: Sequence[str] | None) -> list[str]:
    patterns = [p.strip() for p in (value or ["*"]) if p.strip()]
    if not patterns:
        raise UserError("an ACL needs at least one pattern (use * for everything)")
    return patterns


class UserService:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def create(
        self,
        username: str,
        password: str,
        *,
        role: str = Role.SUBMITTER,
        kind: str = "user",
        queues: Sequence[str] | None = None,
        task_names: Sequence[str] | None = None,
        limits: Mapping[str, int | None] | None = None,
    ) -> User:
        _check(username, password, str(role))
        if kind not in ("user", "service"):
            raise UserError("kind must be 'user' or 'service'")
        user = User(
            username=username,
            kind=kind,
            password_hash=await hash_password(password),
            role=str(role),
            queues=_patterns(queues),
            task_names=_patterns(task_names),
            **_limits(limits),
        )
        try:
            async with self.db.transaction() as tx:
                tx.add(user)
                await tx.flush()
                await tx.refresh(user)
        except IntegrityError as exc:
            raise UserError(f"username {username!r} is taken") from exc
        return user

    async def get(self, username: str) -> User | None:
        async with self.db.sessionmaker() as session:
            return await session.scalar(select(User).where(User.username == username))

    async def list(self) -> list[User]:
        async with self.db.sessionmaker() as session:
            return list(await session.scalars(select(User).order_by(User.username)))

    async def _update(self, username: str, values: dict[str, Any]) -> User | None:
        async with self.db.transaction() as tx:
            return await tx.scalar(
                update(User).where(User.username == username).values(**values).returning(User)
            )

    async def update(
        self,
        username: str,
        *,
        role: str | None = None,
        queues: Sequence[str] | None = None,
        task_names: Sequence[str] | None = None,
        is_active: bool | None = None,
        limits: Mapping[str, int | None] | None = None,
    ) -> User | None:
        """Change an account. A new role or ACL applies to tokens issued from now on —
        within one access-token lifetime at the latest. New limits apply to the next
        enqueue: they are read from this table, not carried in the token. Disabling also
        revokes the account's refresh tokens."""
        _check(role=role)
        values: dict[str, Any] = _limits(limits)
        if role is not None:
            values["role"] = role
        if queues is not None:
            values["queues"] = _patterns(queues)
        if task_names is not None:
            values["task_names"] = _patterns(task_names)
        if is_active is not None:
            values["is_active"] = is_active
            if not is_active:
                values["token_version"] = User.token_version + 1
        if not values:
            return await self.get(username)
        return await self._update(username, values)

    async def set_password(self, username: str, password: str) -> User | None:
        """Change a password (or client secret) and revoke the account's refresh tokens."""
        _check(password=password)
        return await self._update(
            username,
            {
                "password_hash": await hash_password(password),
                "token_version": User.token_version + 1,
            },
        )

    async def revoke_tokens(self, username: str) -> User | None:
        """Invalidate every refresh token the account holds. Access tokens already issued
        stay valid until they expire (TQ_ACCESS_TOKEN_MINUTES)."""
        return await self._update(username, {"token_version": User.token_version + 1})

    async def delete(self, username: str) -> bool:
        async with self.db.transaction() as tx:
            user = await tx.scalar(select(User).where(User.username == username))
            if user is None:
                return False
            await tx.delete(user)
            return True

    async def authenticate(self, username: str, password: str, *, kind: str) -> User | None:
        """The account, if the credentials are right and it may sign in this way."""
        user = await self.get(username)
        usable = user is not None and user.is_active and user.kind == kind
        if not await verify_password(user.password_hash if usable else None, password):
            return None
        assert user is not None
        values: dict[str, Any] = {"last_login_at": func.now()}
        if needs_rehash(user.password_hash):
            values["password_hash"] = await hash_password(password)
        return await self._update(username, values)

    async def bootstrap_admin(self, username: str, password: str) -> User | None:
        """Create the first admin. Does nothing (returns None) once any account exists, so
        it is safe to run on every deploy."""
        async with self.db.sessionmaker() as session:
            if await session.scalar(select(func.count()).select_from(User)):
                return None
        try:
            return await self.create(username, password, role=Role.ADMIN)
        except UserError as exc:
            if "is taken" in str(exc):
                return None  # another deploy got there first
            raise
