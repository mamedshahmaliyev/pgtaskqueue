"""Issuing and verifying JWTs.

Access tokens are short-lived and self-contained: role, ACL and (optionally) a narrowed
scope are claims, so verifying a request costs a signature check and no query. Refresh
tokens are longer-lived, carry the account's `token_version`, and are checked against the
database when used — that check is where a disabled account or a changed password takes
effect.

Signing is HS256 with a shared secret by default. Set `TQ_JWT_ALGORITHM` to an asymmetric
algorithm (RS256, ES256, EdDSA…) with `TQ_JWT_PRIVATE_KEY` / `TQ_JWT_PUBLIC_KEY` to let other
services verify tokens without being able to mint them — or set only the public key to
accept tokens issued by your own identity provider. docs/auth.md covers each setup.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jwt

from pgtaskqueue.auth.principal import Principal
from pgtaskqueue.auth.roles import Permission, permissions_for
from pgtaskqueue.models import User
from pgtaskqueue.settings import Settings

ACCESS = "access"
REFRESH = "refresh"
# RFC 9068: the media type that marks a JWT as an OAuth access token, so it cannot be
# mistaken for (or replayed as) an ID token or a refresh token.
ACCESS_TYP = "at+jwt"
LEEWAY_SECONDS = 10
MIN_SECRET_BYTES = 32


class TokenError(Exception):
    """The token is missing something, expired, or not ours. The message is safe to show."""


class AuthConfigError(RuntimeError):
    """The signing configuration cannot work. Raised at start-up, not per request."""


@dataclass(frozen=True, slots=True)
class IssuedToken:
    token: str
    expires_in: int
    scope: str | None = None


def _load_key(value: str) -> str:
    """A PEM key given inline (newlines may be written as `\\n`) or as a path to a file."""
    if "-----BEGIN" in value:
        return value.replace("\\n", "\n")
    return Path(value).expanduser().read_text()


def narrow_scope(role: str, requested: str | None) -> str | None:
    """The `scope` claim for a token request: the requested permissions that the role
    actually has. None (no claim) means "everything the role allows"."""
    if not requested or not requested.strip():
        return None
    allowed = permissions_for(role)
    granted = [p for p in dict.fromkeys(requested.split()) if p in allowed]
    return " ".join(granted)


class TokenService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.algorithm = settings.jwt_algorithm
        if self.algorithm.lower() == "none":
            raise AuthConfigError("TQ_JWT_ALGORITHM=none is not accepted")
        self._signing_key: str | None
        if settings.jwt_is_asymmetric:
            if not settings.jwt_public_key and not settings.jwt_private_key:
                raise AuthConfigError(
                    f"{self.algorithm} needs TQ_JWT_PUBLIC_KEY (to verify) and "
                    "TQ_JWT_PRIVATE_KEY (to issue tokens here)"
                )
            self._signing_key = (
                _load_key(settings.jwt_private_key) if settings.jwt_private_key else None
            )
            self._verifying_key = (
                _load_key(settings.jwt_public_key)
                if settings.jwt_public_key
                else self._public_from_private()
            )
        else:
            if len(settings.jwt_secret.encode()) < MIN_SECRET_BYTES:
                raise AuthConfigError(
                    f"TQ_JWT_SECRET must be at least {MIN_SECRET_BYTES} bytes "
                    "(generate one with: openssl rand -hex 32)"
                )
            self._signing_key = self._verifying_key = settings.jwt_secret

    def _public_from_private(self) -> Any:
        from cryptography.hazmat.primitives.serialization import load_pem_private_key

        assert self._signing_key is not None
        return load_pem_private_key(self._signing_key.encode(), password=None).public_key()

    @property
    def can_issue(self) -> bool:
        """False when only a public key is configured: tokens come from somewhere else."""
        return self._signing_key is not None

    # ------------------------------------------------------------------ issuing

    def _encode(self, claims: dict[str, Any], lifetime: int, typ: str | None = None) -> str:
        if self._signing_key is None:
            raise AuthConfigError("this server verifies tokens but is not configured to issue them")
        now = int(time.time())
        payload = {
            "iss": self.settings.jwt_issuer,
            "aud": self.settings.jwt_audience,
            "iat": now,
            "nbf": now,
            "exp": now + lifetime,
            "jti": uuid.uuid4().hex,
            **claims,
        }
        headers = {"typ": typ} if typ else None
        return jwt.encode(payload, self._signing_key, algorithm=self.algorithm, headers=headers)

    def issue_access(
        self, user: User, scope: str | None = None, *, lifetime: int | None = None
    ) -> IssuedToken:
        """An access token for `user`. `lifetime` (seconds) overrides
        TQ_ACCESS_TOKEN_MINUTES — for `tq token`, which mints tokens for clients that
        cannot refresh one."""
        if lifetime is None:
            lifetime = self.settings.access_token_minutes * 60
        claims: dict[str, Any] = {
            "sub": user.username,
            "token_use": ACCESS,
            "role": user.role,
            "queues": list(user.queues),
            "task_names": list(user.task_names),
            "client_id": user.username,
        }
        if scope is not None:
            claims["scope"] = scope
        return IssuedToken(self._encode(claims, lifetime, ACCESS_TYP), lifetime, scope)

    def issue_refresh(self, user: User, scope: str | None = None) -> IssuedToken:
        lifetime = self.settings.refresh_token_days * 86400
        claims: dict[str, Any] = {
            "sub": user.username,
            "token_use": REFRESH,
            "ver": user.token_version,
        }
        if scope is not None:
            claims["scope"] = scope
        return IssuedToken(self._encode(claims, lifetime), lifetime, scope)

    # ------------------------------------------------------------------ verifying

    def decode(self, token: str, token_use: str = ACCESS) -> dict[str, Any]:
        """The claims of a valid token of the expected kind. Raises `TokenError` otherwise."""
        try:
            claims = jwt.decode(
                token,
                self._verifying_key,
                # Pinned to the one configured algorithm: the token's own `alg` header is
                # never what decides how it is verified.
                algorithms=[self.algorithm],
                audience=self.settings.jwt_audience,
                issuer=self.settings.jwt_issuer,
                leeway=LEEWAY_SECONDS,
                options={"require": ["exp", "iat", "sub", "iss", "aud"]},
            )
        except jwt.ExpiredSignatureError as exc:
            raise TokenError("token has expired") from exc
        except jwt.InvalidTokenError as exc:
            raise TokenError(f"invalid token: {exc}") from exc
        # A token from an external issuer may not say what it is for; ours always do, and a
        # refresh token must never be accepted where an access token is expected.
        found = claims.get("token_use", ACCESS if not self.can_issue else None)
        if found != token_use:
            raise TokenError(f"not {'an' if token_use == ACCESS else 'a'} {token_use} token")
        return claims

    def principal(self, token: str) -> Principal:
        """Who an access token says is calling."""
        return principal_from_claims(self.decode(token, ACCESS))


def principal_from_claims(claims: dict[str, Any]) -> Principal:
    role = str(claims.get("role", ""))
    permissions: frozenset[str] = frozenset(str(p) for p in permissions_for(role))
    if not permissions:
        raise TokenError(f"token carries no known role (role={role!r})")
    if "scope" in claims:
        # A scope can only take permissions away from the role, never add to it.
        permissions &= frozenset(str(claims["scope"]).split())
    return Principal(
        subject=str(claims["sub"]),
        role=role,
        permissions=permissions,
        queues=_patterns(claims.get("queues")),
        task_names=_patterns(claims.get("task_names")),
    )


def _patterns(value: Any) -> tuple[str, ...]:
    if value is None:
        return ("*",)
    if isinstance(value, str):
        value = value.split(",")
    return tuple(str(item).strip() for item in value if str(item).strip())


__all__ = [
    "ACCESS",
    "REFRESH",
    "AuthConfigError",
    "IssuedToken",
    "Permission",
    "TokenError",
    "TokenService",
    "narrow_scope",
    "principal_from_claims",
]
