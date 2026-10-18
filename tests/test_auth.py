"""Tokens, passwords, accounts, and the OAuth2 token endpoint."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from pgtaskqueue.auth import AuthConfigError, TokenError, TokenService, UserError, UserService
from pgtaskqueue.auth.passwords import hash_password, verify_password
from pgtaskqueue.auth.roles import ROLE_PERMISSIONS, Permission, Role, permissions_for
from pgtaskqueue.auth.tokens import REFRESH, narrow_scope, principal_from_claims
from pgtaskqueue.models import User
from tests.helpers import JWT_SECRET, make_settings, serving


def account(**overrides) -> User:
    values = {
        "username": "alice",
        "kind": "user",
        "role": "submitter",
        "queues": ["*"],
        "task_names": ["mail.*"],
        "password_hash": "",
        "token_version": 3,
    }
    return User(**{**values, **overrides})


def ec_keys() -> tuple[str, str]:
    key = ec.generate_private_key(ec.SECP256R1())
    private = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    public = (
        key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    return private, public


def b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def claims_for(settings, **overrides) -> dict:
    now = int(time.time())
    return {
        "iss": settings.jwt_issuer,
        "aud": settings.jwt_audience,
        "iat": now,
        "exp": now + 60,
        "sub": "alice",
        "token_use": "access",
        "role": "submitter",
        **overrides,
    }


# --------------------------------------------------------------------------- roles


def test_roles():
    assert ROLE_PERMISSIONS[Role.ADMIN] == frozenset(Permission)
    assert Permission.USERS_MANAGE not in permissions_for("operator")
    assert permissions_for("worker") == {Permission.TASKS_WORK}
    assert Permission.TASKS_ENQUEUE not in permissions_for("viewer")
    assert permissions_for("root") == frozenset()


def test_scope_can_only_narrow():
    assert narrow_scope("viewer", None) is None
    assert narrow_scope("viewer", "  ") is None
    assert narrow_scope("viewer", "tasks:read users:manage tasks:read") == "tasks:read"
    assert narrow_scope("viewer", "users:manage") == ""


# --------------------------------------------------------------------------- tokens


def test_access_token_carries_role_and_acl(settings):
    tokens = TokenService(settings)
    issued = tokens.issue_access(account())
    assert issued.expires_in == settings.access_token_minutes * 60
    assert jwt.get_unverified_header(issued.token) == {"alg": "HS256", "typ": "at+jwt"}

    who = tokens.principal(issued.token)
    assert (who.subject, who.role) == ("alice", "submitter")
    assert who.permissions == permissions_for("submitter")
    assert who.can_name("mail.send") and not who.can_name("billing.charge")
    assert who.name_patterns == ("mail.*",) and who.queue_patterns is None


def test_scope_claim_removes_permissions(settings):
    tokens = TokenService(settings)
    who = tokens.principal(tokens.issue_access(account(role="admin"), "tasks:read").token)
    assert who.permissions == {"tasks:read"}
    # A scope naming something the role lacks grants nothing.
    forged = principal_from_claims({"sub": "x", "role": "viewer", "scope": "users:manage"})
    assert forged.permissions == frozenset()


def test_refresh_and_access_tokens_are_not_interchangeable(settings):
    tokens = TokenService(settings)
    access = tokens.issue_access(account()).token
    refresh = tokens.issue_refresh(account()).token
    assert tokens.decode(refresh, REFRESH)["ver"] == 3
    with pytest.raises(TokenError, match="not an access token"):
        tokens.principal(refresh)
    with pytest.raises(TokenError, match="not a refresh token"):
        tokens.decode(access, REFRESH)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"exp": int(time.time()) - 3600}, "expired"),
        ({"aud": "another-api"}, "invalid token"),
        ({"iss": "someone-else"}, "invalid token"),
        ({"role": "superuser"}, "no known role"),
        ({"role": None}, "no known role"),
    ],
)
def test_bad_claims_are_rejected(settings, overrides, message):
    token = jwt.encode(claims_for(settings, **overrides), JWT_SECRET, algorithm="HS256")
    with pytest.raises(TokenError, match=message):
        TokenService(settings).principal(token)


def test_bad_signatures_are_rejected(settings):
    tokens = TokenService(settings)
    claims = claims_for(settings, role="admin")
    with pytest.raises(TokenError):
        tokens.principal(jwt.encode(claims, "y" * 48, algorithm="HS256"))
    # A different algorithm of the same family is still not the configured one.
    with pytest.raises(TokenError):
        tokens.principal(jwt.encode(claims, JWT_SECRET + "y" * 30, algorithm="HS512"))
    # Unsigned.
    header = b64(json.dumps({"alg": "none", "typ": "JWT"}).encode())
    unsigned = f"{header}.{b64(json.dumps(claims).encode())}."
    with pytest.raises(TokenError):
        tokens.principal(unsigned)
    for garbage in ("", "abc", "a.b.c"):
        with pytest.raises(TokenError):
            tokens.principal(garbage)
    # Missing a required claim.
    del claims["exp"]
    with pytest.raises(TokenError):
        tokens.principal(jwt.encode(claims, JWT_SECRET, algorithm="HS256"))


def test_signing_configuration_is_checked_at_start_up():
    with pytest.raises(AuthConfigError, match="at least 32 bytes"):
        TokenService(make_settings(jwt_secret="short"))
    with pytest.raises(AuthConfigError, match="at least 32 bytes"):
        TokenService(make_settings(jwt_secret=""))
    with pytest.raises(AuthConfigError, match="not accepted"):
        TokenService(make_settings(jwt_algorithm="none"))
    with pytest.raises(AuthConfigError, match="TQ_JWT_PUBLIC_KEY"):
        TokenService(make_settings(jwt_algorithm="ES256"))


def test_asymmetric_keys():
    private, public = ec_keys()
    issuer = TokenService(make_settings(jwt_algorithm="ES256", jwt_private_key=private))
    token = issuer.issue_access(account(role="operator")).token

    # A service holding only the public key verifies, and cannot mint.
    verifier = TokenService(make_settings(jwt_algorithm="ES256", jwt_public_key=public))
    assert not verifier.can_issue
    assert verifier.principal(token).role == "operator"
    with pytest.raises(AuthConfigError, match="not configured to issue"):
        verifier.issue_access(account())

    # A key given with literal "\n", as an environment variable often is.
    escaped = TokenService(
        make_settings(jwt_algorithm="ES256", jwt_public_key=public.replace("\n", "\\n"))
    )
    assert escaped.principal(token).subject == "alice"

    _, other_public = ec_keys()
    stranger = TokenService(make_settings(jwt_algorithm="ES256", jwt_public_key=other_public))
    with pytest.raises(TokenError):
        stranger.principal(token)


def test_public_key_cannot_be_used_as_an_hmac_secret():
    """The classic algorithm-confusion forgery: sign with HS256, using the server's
    *public* key as the secret, and hope the server verifies with whatever `alg` says."""
    _, public = ec_keys()
    settings = make_settings(jwt_algorithm="ES256", jwt_public_key=public)
    header = b64(json.dumps({"alg": "HS256", "typ": "at+jwt"}).encode())
    body = b64(json.dumps(claims_for(settings, role="admin")).encode())
    signature = hmac.new(public.encode(), f"{header}.{body}".encode(), hashlib.sha256).digest()
    with pytest.raises(TokenError):
        TokenService(settings).principal(f"{header}.{body}.{b64(signature)}")


def test_tokens_from_an_external_issuer():
    """Verify-only mode: an identity provider signs, and the claims are mapped."""
    private, public = ec_keys()
    settings = make_settings(
        jwt_algorithm="ES256",
        jwt_public_key=public,
        jwt_issuer="https://idp.example.com/",
        jwt_audience="pgtaskqueue",
    )
    claims = claims_for(settings, role="operator", queues="media-*, default")
    del claims["token_use"]  # an IdP does not know about ours
    who = TokenService(settings).principal(jwt.encode(claims, private, algorithm="ES256"))
    assert (who.role, who.queues, who.task_names) == ("operator", ("media-*", "default"), ("*",))


# --------------------------------------------------------------------------- passwords


async def test_passwords():
    hashed = await hash_password("correct horse battery staple")
    assert hashed.startswith("$argon2id$")
    assert await verify_password(hashed, "correct horse battery staple")
    assert not await verify_password(hashed, "wrong")
    assert not await verify_password(None, "anything")
    assert not await verify_password("not-a-hash", "anything")


# --------------------------------------------------------------------------- accounts


async def test_user_service(db):
    users = UserService(db)
    alice = await users.create("alice", "alice-password", task_names=["mail.*"])
    assert (alice.role, alice.kind, alice.queues, alice.task_names) == (
        "submitter",
        "user",
        ["*"],
        ["mail.*"],
    )
    assert alice.password_hash != "alice-password"

    for bad in (
        {"username": "alice", "password": "another-password"},  # taken
        {"username": "has space", "password": "long-enough"},
        {"username": "bob", "password": "short"},
        {"username": "bob", "password": "long-enough", "role": "root"},
        {"username": "bob", "password": "long-enough", "kind": "robot"},
        {"username": "bob", "password": "long-enough", "queues": [" "]},
    ):
        with pytest.raises(UserError):
            await users.create(**bad)

    assert (await users.authenticate("alice", "alice-password", kind="user")).last_login_at
    assert await users.authenticate("alice", "wrong-password", kind="user") is None
    assert await users.authenticate("nobody", "alice-password", kind="user") is None
    # A person's password is not a client secret, and the other way round.
    assert await users.authenticate("alice", "alice-password", kind="service") is None

    version = alice.token_version
    assert (await users.set_password("alice", "a-new-password")).token_version == version + 1
    assert await users.authenticate("alice", "alice-password", kind="user") is None
    assert await users.authenticate("alice", "a-new-password", kind="user")

    disabled = await users.update("alice", is_active=False, role="viewer")
    assert (disabled.is_active, disabled.role, disabled.token_version) == (
        False,
        "viewer",
        version + 2,
    )
    assert await users.authenticate("alice", "a-new-password", kind="user") is None

    assert await users.update("nobody", role="viewer") is None
    assert await users.delete("alice") and not await users.delete("alice")


async def test_bootstrap_admin_only_into_an_empty_table(db):
    users = UserService(db)
    admin = await users.bootstrap_admin("root", "root-password")
    assert admin.role == "admin"
    assert await users.bootstrap_admin("second", "second-password") is None
    assert [user.username for user in await users.list()] == ["root"]


# --------------------------------------------------------------------------- the token endpoint


@pytest.fixture
async def people(db):
    users = UserService(db)
    await users.create("admin", "admin-password", role="admin")
    await users.create("alice", "alice-password", role="operator")
    await users.create("svc-reports", "svc-secret-value", role="submitter", kind="service")
    return users


async def login(api, username: str, password: str, **extra) -> dict:
    response = await api.post(
        "/v1/auth/token",
        data={"grant_type": "password", "username": username, "password": password, **extra},
    )
    assert response.status_code == 200, response.text
    return response.json()


def as_bearer(tokens: dict) -> dict[str, str]:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


async def test_password_grant(api, people, settings):
    response = await api.post(
        "/v1/auth/token",
        data={"grant_type": "password", "username": "alice", "password": "alice-password"},
    )
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    tokens = response.json()
    assert tokens["token_type"] == "bearer"
    assert tokens["expires_in"] == settings.access_token_minutes * 60
    assert "scope" not in tokens

    me = (await api.get("/v1/auth/me", headers=as_bearer(tokens))).json()
    assert (me["subject"], me["role"], me["queues"]) == ("alice", "operator", ["*"])
    assert "tasks:manage_all" in me["permissions"] and "users:manage" not in me["permissions"]


async def test_wrong_credentials(api, people):
    for data in (
        {"grant_type": "password", "username": "alice", "password": "wrong-password"},
        {"grant_type": "password", "username": "nobody", "password": "alice-password"},
        # A service account does not sign in with the password grant.
        {"grant_type": "password", "username": "svc-reports", "password": "svc-secret-value"},
    ):
        response = await api.post("/v1/auth/token", data=data)
        assert response.status_code == 400
        # The same answer whichever half was wrong.
        assert response.json() == {
            "error": "invalid_grant",
            "error_description": "wrong username or password",
        }

    response = await api.post("/v1/auth/token", data={"grant_type": "password"})
    assert response.json()["error"] == "invalid_request"
    response = await api.post("/v1/auth/token", data={"grant_type": "implicit"})
    assert response.json()["error"] == "unsupported_grant_type"


async def test_client_credentials_grant(api, people):
    basic = await api.post(
        "/v1/auth/token",
        data={"grant_type": "client_credentials"},
        auth=("svc-reports", "svc-secret-value"),
    )
    form = await api.post(
        "/v1/auth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "svc-reports",
            "client_secret": "svc-secret-value",
        },
    )
    for response in (basic, form):
        assert response.status_code == 200
        # A service asks again instead of refreshing.
        assert "refresh_token" not in response.json()
    me = (await api.get("/v1/auth/me", headers=as_bearer(basic.json()))).json()
    assert (me["subject"], me["role"]) == ("svc-reports", "submitter")

    for auth in (("svc-reports", "wrong-secret-value"), ("alice", "alice-password")):
        response = await api.post(
            "/v1/auth/token", data={"grant_type": "client_credentials"}, auth=auth
        )
        assert response.status_code == 401
        assert response.json()["error"] == "invalid_client"
        assert response.headers["www-authenticate"].startswith("Basic")
    response = await api.post("/v1/auth/token", data={"grant_type": "client_credentials"})
    assert response.status_code == 401


async def test_requests_need_a_valid_access_token(api, people):
    tokens = await login(api, "alice", "alice-password")
    assert (await api.get("/v1/tasks", headers=as_bearer(tokens))).status_code == 200

    missing = await api.get("/v1/tasks")
    assert missing.status_code == 401 and missing.headers["www-authenticate"] == "Bearer"
    for header in (
        "Bearer not-a-token",
        f"Bearer {tokens['refresh_token']}",  # the wrong kind of token
        f"Basic {tokens['access_token']}",
    ):
        response = await api.get("/v1/tasks", headers={"Authorization": header})
        assert response.status_code == 401, header


async def test_refresh_rotates_and_can_be_revoked(api, people):
    first = await login(api, "alice", "alice-password")

    async def refresh(token: str, **extra):
        return await api.post(
            "/v1/auth/token",
            data={"grant_type": "refresh_token", "refresh_token": token, **extra},
        )

    second = await refresh(first["refresh_token"])
    assert second.status_code == 200
    assert second.json()["access_token"] != first["access_token"]
    assert (await api.get("/v1/auth/me", headers=as_bearer(second.json()))).status_code == 200

    # An access token is not a refresh token.
    assert (await refresh(first["access_token"])).json()["error"] == "invalid_grant"

    # "Log out everywhere".
    assert (await api.post("/v1/auth/logout", headers=as_bearer(first))).status_code == 204
    revoked = await refresh(second.json()["refresh_token"])
    assert revoked.status_code == 400
    assert revoked.json() == {
        "error": "invalid_grant",
        "error_description": "the refresh token has been revoked",
    }

    # A password change does the same...
    third = await login(api, "alice", "alice-password")
    await people.set_password("alice", "a-new-password")
    assert (await refresh(third["refresh_token"])).status_code == 400
    # ...and so does disabling the account.
    fourth = await login(api, "alice", "a-new-password")
    await people.update("alice", is_active=False)
    assert (await refresh(fourth["refresh_token"])).status_code == 400


async def test_scope_narrows_a_token_and_refresh_cannot_widen_it(api, people):
    tokens = await login(api, "admin", "admin-password", scope="tasks:read stats:read bogus")
    assert tokens["scope"] == "tasks:read stats:read"
    headers = as_bearer(tokens)
    assert (await api.get("/v1/stats", headers=headers)).status_code == 200
    assert (await api.post("/v1/tasks", json={"name": "t"}, headers=headers)).status_code == 403
    assert (await api.get("/v1/users", headers=headers)).status_code == 403

    async def refresh(**extra):
        response = await api.post(
            "/v1/auth/token",
            data={"grant_type": "refresh_token", "refresh_token": tokens["refresh_token"], **extra},
        )
        return response.json()

    assert (await refresh())["scope"] == "tasks:read stats:read"
    assert (await refresh(scope="tasks:read"))["scope"] == "tasks:read"
    # Asking for more than was first granted gets only the overlap.
    assert (await refresh(scope="tasks:read users:manage"))["scope"] == "tasks:read"


async def test_account_management_is_for_admins(api, people):
    admin = as_bearer(await login(api, "admin", "admin-password"))
    alice = as_bearer(await login(api, "alice", "alice-password"))

    assert (await api.get("/v1/users", headers=alice)).status_code == 403
    listed = (await api.get("/v1/users", headers=admin)).json()
    assert [user["username"] for user in listed] == ["admin", "alice", "svc-reports"]
    assert "password_hash" not in listed[0]

    body = {"username": "bob", "password": "bob-password", "role": "viewer", "queues": ["media"]}
    assert (await api.post("/v1/users", json=body, headers=alice)).status_code == 403
    created = await api.post("/v1/users", json=body, headers=admin)
    assert created.status_code == 201
    assert (created.json()["role"], created.json()["queues"]) == ("viewer", ["media"])
    assert (await api.post("/v1/users", json=body, headers=admin)).status_code == 422
    weak = {**body, "username": "carol", "password": "short"}
    assert (await api.post("/v1/users", json=weak, headers=admin)).status_code == 422

    patched = await api.patch("/v1/users/bob", json={"role": "operator"}, headers=admin)
    assert patched.json()["role"] == "operator"
    assert (await api.patch("/v1/users/nobody", json={}, headers=admin)).status_code == 404
    # An admin cannot demote, disable or delete itself.
    for change in ({"role": "viewer"}, {"is_active": False}):
        response = await api.patch("/v1/users/admin", json=change, headers=admin)
        assert response.status_code == 409
    assert (await api.delete("/v1/users/admin", headers=admin)).status_code == 409

    # Anyone may change their own password; only an admin may change another's.
    own = await api.post(
        "/v1/users/alice/password", json={"password": "alice-new-password"}, headers=alice
    )
    assert own.status_code == 204
    await login(api, "alice", "alice-new-password")
    other = await api.post(
        "/v1/users/bob/password", json={"password": "set-by-alice"}, headers=alice
    )
    assert other.status_code == 403

    assert (await api.delete("/v1/users/bob", headers=admin)).status_code == 204
    assert (await api.delete("/v1/users/bob", headers=admin)).status_code == 404


async def test_verify_only_server_issues_nothing(db):
    private, public = ec_keys()
    settings = make_settings(jwt_algorithm="ES256", jwt_public_key=public)
    async with serving(db, settings) as api:
        response = await api.post(
            "/v1/auth/token",
            data={"grant_type": "password", "username": "a", "password": "b"},
        )
        assert response.status_code == 400 and response.json()["error"] == "invalid_request"

        # But a token the identity provider signed is accepted.
        token = jwt.encode(claims_for(settings, role="viewer"), private, algorithm="ES256")
        me = await api.get("/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert me.status_code == 200 and me.json()["role"] == "viewer"
