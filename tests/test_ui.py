"""The web UI: the pages, and the cookie session that lets a browser use the API."""

from __future__ import annotations

import pytest

from pgtaskqueue.api import assets
from pgtaskqueue.auth import UserService
from tests.helpers import make_settings, serving

PASSWORD = "a-good-password"


@pytest.fixture
async def olga(db):
    """An operator with a password, since the UI signs in the way a person does."""
    return await UserService(db).create("olga", PASSWORD, role="operator")


async def sign_in(api, username="olga", password=PASSWORD, **form):
    return await api.post("/ui/login", data={"username": username, "password": password, **form})


async def test_pages_send_a_visitor_to_the_login_page(api):
    for path in ("/ui", "/ui/tasks?status=failed", "/ui/users", "/ui/stats"):
        response = await api.get(path)
        assert response.status_code == 303, path
    # The page asked for is remembered, filters and all.
    assert response.headers["location"] == "/ui/login?next=%2Fui%2Fstats"
    response = await api.get("/ui/tasks?status=failed")
    assert response.headers["location"] == "/ui/login?next=%2Fui%2Ftasks%3Fstatus%3Dfailed"
    assert (await api.get("/")).headers["location"] == "/ui"

    login = await api.get("/ui/login")
    assert login.status_code == 200 and 'name="password"' in login.text
    # The stylesheet and scripts are public; they hold nothing.
    assert (await api.get("/ui/static/app.css")).status_code == 200
    assert (await api.get("/ui/static/core.js")).status_code == 200


def test_static_version_matches_the_files():
    # A browser keeps a version's files for a year, so a change shipped under the old
    # version would not reach it; and two modules importing core.js by different URLs would
    # each get their own copy of it.
    assert assets.problems() == [], "Run: uv run python -m pgtaskqueue.api.assets bump"


async def test_pages_load_static_files_with_the_current_version(api):
    current = f"?v={assets.version()}"
    login = await api.get("/ui/login")
    assert f'href="/ui/static/app.css{current}"' in login.text

    # Kept for a year: the version in the URL changes when the file does.
    for name in ("app.css", "tasks.js", "core.js"):
        response = await api.get(f"/ui/static/{name}{current}")
        assert response.status_code == 200, name
        assert response.headers["cache-control"] == "public, max-age=31536000, immutable"

    # Another version, as a page from an older API asks for during a deploy, or none: served,
    # but checked again every time.
    for path in ("/ui/static/core.js?v=1", "/ui/static/core.js"):
        response = await api.get(path)
        assert response.status_code == 200, path
        assert response.headers["cache-control"] == "no-cache", path

    assert (await api.get(f"/ui/static/missing.js{current}")).status_code == 404


async def test_sign_in_sets_cookies_a_script_cannot_read(api, olga):
    wrong = await sign_in(api, password="not-the-password")
    assert wrong.status_code == 401 and "Wrong username or password" in wrong.text
    assert "set-cookie" not in wrong.headers

    response = await sign_in(api, next="/ui/tasks?status=failed")
    assert response.status_code == 303
    assert response.headers["location"] == "/ui/tasks?status=failed"
    cookies = response.headers.get_list("set-cookie")
    assert len(cookies) == 2
    for cookie in cookies:
        assert "HttpOnly" in cookie and "SameSite=lax" in cookie
    access, refresh = sorted(cookies)
    assert access.startswith("tq_access=") and "Path=/;" in access
    # The refresh token goes only to the UI's own routes, never to /v1.
    assert refresh.startswith("tq_refresh=") and "Path=/ui;" in refresh

    page = await api.get("/ui")
    assert page.status_code == 200
    assert '"subject": "olga"' in page.text and "<tq-dashboard>" in page.text
    assert "script-src" not in page.headers["content-security-policy"]
    assert "default-src 'self'" in page.headers["content-security-policy"]
    assert "frame-ancestors 'none'" in page.headers["content-security-policy"]
    task = await api.get("/ui/tasks/0192e3a1-7c1e-7d3a-9b52-0f6d1c2e4a77")
    assert 'task-id="0192e3a1-7c1e-7d3a-9b52-0f6d1c2e4a77"' in task.text
    assert (await api.get("/ui/tasks/not-an-id")).status_code == 422
    # Someone signed in has no use for the login page.
    assert (await api.get("/ui/login")).headers["location"] == "/ui"


@pytest.mark.parametrize(
    "target", ["https://evil.example/ui", "//evil.example", "/uix", "/docs", "/ui\\evil"]
)
async def test_sign_in_only_returns_to_a_page_of_the_ui(api, olga, target):
    response = await sign_in(api, next=target)
    assert response.headers["location"] == "/ui"


async def test_the_accounts_page_is_linked_for_account_managers_only(api, olga, db):
    await sign_in(api)
    assert "/ui/users" not in (await api.get("/ui")).text
    # The page itself loads for anyone signed in, and says what the API refuses: the UI
    # decides no permission.
    page = await api.get("/ui/users")
    assert page.status_code == 200 and "<tq-users>" in page.text
    assert (await api.get("/v1/users", headers={"X-TQ-UI": "1"})).status_code == 403

    await api.post("/ui/logout")
    await UserService(db).create("mia", PASSWORD, role="manager")
    await sign_in(api, "mia")
    assert 'href="/ui/users"' in (await api.get("/ui")).text
    assert (await api.get("/v1/users", headers={"X-TQ-UI": "1"})).status_code == 200


async def test_a_service_account_does_not_sign_in_to_the_ui(api, db):
    await UserService(db).create("svc", "a-client-secret", role="operator", kind="service")
    assert (await sign_in(api, "svc", "a-client-secret")).status_code == 401


async def test_the_cookie_authenticates_the_api_but_writes_need_the_header(api, olga, auth):
    await sign_in(api)
    me = await api.get("/v1/auth/me")
    assert me.status_code == 200 and me.json()["subject"] == "olga"

    # What another site's form or fetch could send: the cookie, without the header.
    refused = await api.post("/v1/tasks", json={"name": "mail.send"})
    assert refused.status_code == 403 and "X-TQ-UI" in refused.json()["detail"]
    assert (await api.get("/v1/tasks")).json()["items"] == []

    created = await api.post("/v1/tasks", json={"name": "mail.send"}, headers={"X-TQ-UI": "1"})
    assert created.status_code == 201 and created.json()["created_by"] == "olga"
    task_id = created.json()["id"]
    assert (await api.post(f"/v1/tasks/{task_id}/cancel")).status_code == 403
    canceled = await api.post(f"/v1/tasks/{task_id}/cancel", headers={"X-TQ-UI": "1"})
    assert canceled.json()["status"] == "canceled"

    # A bearer token wins over the cookie, and needs no header: it is not sent by a browser
    # on its own.
    as_bob = await api.post("/v1/tasks", json={"name": "mail.send"}, headers=auth("bob"))
    assert as_bob.status_code == 201 and as_bob.json()["created_by"] == "bob"


async def test_an_expired_access_token_is_renewed_from_the_refresh_token(api, olga):
    await sign_in(api)
    api.cookies.delete("tq_access")
    assert (await api.get("/v1/auth/me")).status_code == 401

    # What the page's JavaScript does when the API answers 401.
    assert (await api.post("/ui/session/refresh")).status_code == 401
    renewed = await api.post("/ui/session/refresh", headers={"X-TQ-UI": "1"})
    assert renewed.status_code == 204
    assert (await api.get("/v1/auth/me")).status_code == 200

    # And what opening a page does: the visitor is not sent to sign in again.
    api.cookies.delete("tq_access")
    page = await api.get("/ui/stats")
    assert page.status_code == 200
    assert any(c.startswith("tq_access=") for c in page.headers.get_list("set-cookie"))


async def test_sign_out_ends_the_session_everywhere(api, olga, db):
    await sign_in(api)
    refresh_token = api.cookies.get("tq_refresh")
    out = await api.post("/ui/logout")
    assert out.status_code == 303 and out.headers["location"] == "/ui/login"
    assert api.cookies.get("tq_access") is None and api.cookies.get("tq_refresh") is None
    assert (await api.get("/ui")).status_code == 303

    # A copy of the refresh cookie taken before signing out is worth nothing now.
    api.cookies.set("tq_refresh", refresh_token, domain="api", path="/ui")
    assert (await api.get("/ui")).status_code == 303
    stale = await api.post("/ui/session/refresh", headers={"X-TQ-UI": "1"})
    assert stale.status_code == 401


async def test_a_disabled_account_loses_its_session_at_the_next_refresh(api, olga, db):
    await sign_in(api)
    await UserService(db).update("olga", is_active=False)
    api.cookies.delete("tq_access")
    assert (await api.get("/ui")).status_code == 303
    assert (await sign_in(api)).status_code == 401


async def test_the_ui_can_be_turned_off(db, olga):
    async with serving(db, make_settings(ui_enabled=False)) as api:
        assert (await api.get("/ui")).status_code == 404
        assert (await api.get("/ui/static/app.css")).status_code == 404
        assert (await sign_in(api)).status_code == 404
    # With the UI off, the API takes a token from the Authorization header and nowhere else.
    async with serving(db, make_settings()) as api:
        await sign_in(api)
        token = api.cookies.get("tq_access")
    async with serving(db, make_settings(ui_enabled=False)) as api:
        api.cookies.set("tq_access", token, domain="api", path="/")
        assert (await api.get("/v1/auth/me")).status_code == 401
        bearer = {"Authorization": f"Bearer {token}"}
        assert (await api.get("/v1/auth/me", headers=bearer)).status_code == 200
