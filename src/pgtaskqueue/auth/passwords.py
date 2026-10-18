"""Password and client-secret hashing: Argon2id, with the library's current parameters."""

from __future__ import annotations

import asyncio

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError

_hasher = PasswordHasher()

# Verified against when the account does not exist, so that "no such user" takes as long as
# "wrong password" and the difference cannot be used to enumerate usernames.
_DUMMY_HASH = _hasher.hash("pgtaskqueue-dummy-password")


def _verify(hashed: str, password: str) -> bool:
    try:
        return _hasher.verify(hashed, password)
    except (VerificationError, InvalidHashError):
        return False


# Argon2 is deliberately slow (tens of milliseconds of CPU), so it runs in a thread: a login
# must not stall every other request on the event loop.


async def hash_password(password: str) -> str:
    return await asyncio.to_thread(_hasher.hash, password)


async def verify_password(hashed: str | None, password: str) -> bool:
    if hashed is None:
        await asyncio.to_thread(_verify, _DUMMY_HASH, password)
        return False
    return await asyncio.to_thread(_verify, hashed, password)


def needs_rehash(hashed: str) -> bool:
    """True when `hashed` was made with weaker parameters than today's defaults."""
    try:
        return _hasher.check_needs_rehash(hashed)
    except InvalidHashError:
        return True
