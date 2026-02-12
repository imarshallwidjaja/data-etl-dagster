# =============================================================================
# Login / Logout Flow Tests
# =============================================================================
# TDD tests for GET /login, POST /login, POST /logout endpoints covering
# CSRF, credential validation, session issuance/rotation, safe redirect,
# and audit logging.
# =============================================================================

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.config import get_settings
from app.main import app


@pytest.fixture(autouse=True)
def _mock_activity_service():
    """Prevent real MongoDB connections in unit tests."""
    mock_svc = MagicMock()
    with patch("app.services.activity_service.get_activity_service") as factory:
        factory.return_value = mock_svc
        yield mock_svc


# ---------------------------------------------------------------------------
# GET /login
# ---------------------------------------------------------------------------


class TestGetLogin:
    """GET /login returns the login form with CSRF token."""

    def test_returns_200(self):
        """GET /login → 200."""
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/login")
        assert resp.status_code == 200

    def test_html_contains_csrf_hidden_field(self):
        """The login form must include a hidden csrf_token field."""
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/login")
        assert resp.status_code == 200
        body = resp.text
        assert 'name="csrf_token"' in body
        assert 'type="hidden"' in body


# ---------------------------------------------------------------------------
# POST /login — CSRF enforcement
# ---------------------------------------------------------------------------


class TestPostLoginCsrf:
    """POST /login without a valid CSRF token → 403."""

    def test_missing_csrf_returns_403(self):
        """POST /login without csrf_token → 403."""
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.post(
            "/login",
            data={"username": "admin", "password": "admin"},
            follow_redirects=False,
        )
        assert resp.status_code == 403

    def test_wrong_csrf_returns_403(self):
        """POST /login with incorrect csrf_token → 403."""
        with TestClient(app, raise_server_exceptions=False) as client:
            # Fetch the page to get a session, then submit with a bad token
            client.get("/login")
            resp = client.post(
                "/login",
                data={
                    "username": "admin",
                    "password": "admin",
                    "csrf_token": "bad-token",
                },
                follow_redirects=False,
            )
            assert resp.status_code == 403


# ---------------------------------------------------------------------------
# POST /login — wrong credentials
# ---------------------------------------------------------------------------


class TestPostLoginWrongCredentials:
    """POST /login with wrong credentials returns 200 with a generic error."""

    def test_wrong_password_shows_error_not_redirect(self):
        """Wrong creds → 200 (re-rendered form) with generic error message."""
        with TestClient(app, raise_server_exceptions=False) as client:
            # GET to obtain csrf token
            get_resp = client.get("/login")
            csrf = _extract_csrf(get_resp.text)

            resp = client.post(
                "/login",
                data={
                    "username": "admin",
                    "password": "WRONG",
                    "csrf_token": csrf,
                },
                follow_redirects=False,
            )
            assert resp.status_code == 200
            # Generic error — must NOT reveal which field was wrong
            assert "Invalid username or password" in resp.text


# ---------------------------------------------------------------------------
# POST /login — correct credentials
# ---------------------------------------------------------------------------


class TestPostLoginSuccess:
    """POST /login with correct credentials + CSRF → 303, sets cookie."""

    def test_correct_creds_redirect_to_root(self):
        """Correct creds → 303 redirect to /."""
        with TestClient(app, raise_server_exceptions=False) as client:
            csrf = _extract_csrf(client.get("/login").text)
            resp = client.post(
                "/login",
                data={
                    "username": "admin",
                    "password": "admin",
                    "csrf_token": csrf,
                },
                follow_redirects=False,
            )
            assert resp.status_code == 303
            assert resp.headers["location"] == "/"

    def test_correct_creds_sets_session_cookie(self):
        """After login, the session cookie is set."""
        settings = get_settings()
        cookie_name = settings.webapp_session_cookie_name
        with TestClient(app, raise_server_exceptions=False) as client:
            csrf = _extract_csrf(client.get("/login").text)
            resp = client.post(
                "/login",
                data={
                    "username": "admin",
                    "password": "admin",
                    "csrf_token": csrf,
                },
                follow_redirects=False,
            )
            assert cookie_name in resp.cookies or cookie_name in client.cookies

    def test_redirect_to_safe_next(self):
        """POST /login with valid next → 303 redirect to that path."""
        with TestClient(app, raise_server_exceptions=False) as client:
            csrf = _extract_csrf(client.get("/login?next=%2Flanding%2F").text)
            resp = client.post(
                "/login?next=%2Flanding%2F",
                data={
                    "username": "admin",
                    "password": "admin",
                    "csrf_token": csrf,
                },
                follow_redirects=False,
            )
            assert resp.status_code == 303
            assert resp.headers["location"] == "/landing/"


# ---------------------------------------------------------------------------
# Open redirect prevention
# ---------------------------------------------------------------------------


class TestOpenRedirectBlocked:
    """Open redirect via next parameter must be blocked."""

    @pytest.mark.parametrize(
        "evil_next",
        [
            "https://evil.com",
            "http://evil.com",
            "//evil.com",
            "https://evil.com/path",
        ],
    )
    def test_external_url_blocked(self, evil_next: str):
        """Absolute external URLs in next → redirect to / instead."""
        with TestClient(app, raise_server_exceptions=False) as client:
            csrf = _extract_csrf(client.get(f"/login?next={evil_next}").text)
            resp = client.post(
                f"/login?next={evil_next}",
                data={
                    "username": "admin",
                    "password": "admin",
                    "csrf_token": csrf,
                },
                follow_redirects=False,
            )
            assert resp.status_code == 303
            location = resp.headers["location"]
            assert not location.startswith("http")
            assert not location.startswith("//")


# ---------------------------------------------------------------------------
# Session fixation protection
# ---------------------------------------------------------------------------


class TestSessionFixation:
    """Session cookie value must change between anon and logged-in session."""

    def test_session_rotates_on_login(self):
        """The session ID should differ before and after login."""
        settings = get_settings()
        cookie_name = settings.webapp_session_cookie_name
        with TestClient(app, raise_server_exceptions=False) as client:
            # GET /login creates an anonymous session
            get_resp = client.get("/login")
            anon_cookie = client.cookies.get(cookie_name, "")

            csrf = _extract_csrf(get_resp.text)
            client.post(
                "/login",
                data={
                    "username": "admin",
                    "password": "admin",
                    "csrf_token": csrf,
                },
                follow_redirects=False,
            )
            logged_in_cookie = client.cookies.get(cookie_name, "")

            # Cookie value must have changed (session rotated)
            assert anon_cookie != logged_in_cookie
            assert logged_in_cookie != ""


# ---------------------------------------------------------------------------
# POST /logout
# ---------------------------------------------------------------------------


class TestPostLogout:
    """POST /logout clears the session; subsequent requests redirect."""

    def test_logout_clears_session_and_redirects(self):
        """POST /logout → 303 to /login, session cleared."""
        with TestClient(app, raise_server_exceptions=False) as client:
            # Login first
            csrf = _extract_csrf(client.get("/login").text)
            client.post(
                "/login",
                data={
                    "username": "admin",
                    "password": "admin",
                    "csrf_token": csrf,
                },
                follow_redirects=False,
            )

            # Now logout
            resp = client.post("/logout", follow_redirects=False)
            assert resp.status_code == 303
            assert resp.headers["location"] == "/login"

            # Subsequent GET / should redirect to login (no session)
            resp = client.get("/", follow_redirects=False)
            assert resp.status_code == 303
            assert "/login" in resp.headers["location"]


# ---------------------------------------------------------------------------
# Audit logging
# ---------------------------------------------------------------------------


class TestAuditLogging:
    """Auth lifecycle events are logged to activity_logs."""

    def test_login_success_logged(self, _mock_activity_service: MagicMock):
        """Successful login logs 'login_success' event."""
        with TestClient(app, raise_server_exceptions=False) as client:
            csrf = _extract_csrf(client.get("/login").text)
            client.post(
                "/login",
                data={
                    "username": "admin",
                    "password": "admin",
                    "csrf_token": csrf,
                },
                follow_redirects=False,
            )

        _assert_activity_logged(_mock_activity_service, "login_success")

    def test_login_failure_logged(self, _mock_activity_service: MagicMock):
        """Failed login logs 'login_failure' event."""
        with TestClient(app, raise_server_exceptions=False) as client:
            csrf = _extract_csrf(client.get("/login").text)
            client.post(
                "/login",
                data={
                    "username": "admin",
                    "password": "WRONG",
                    "csrf_token": csrf,
                },
                follow_redirects=False,
            )

        _assert_activity_logged(_mock_activity_service, "login_failure")

    def test_logout_logged(self, _mock_activity_service: MagicMock):
        """Logout logs 'logout' event."""
        with TestClient(app, raise_server_exceptions=False) as client:
            csrf = _extract_csrf(client.get("/login").text)
            client.post(
                "/login",
                data={
                    "username": "admin",
                    "password": "admin",
                    "csrf_token": csrf,
                },
                follow_redirects=False,
            )
            client.post("/logout", follow_redirects=False)

        _assert_activity_logged(_mock_activity_service, "logout")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_csrf(html: str) -> str:
    """Extract the csrf_token value from a hidden input in the HTML."""
    import re

    match = re.search(r'name="csrf_token"\s+value="([^"]+)"', html)
    if not match:
        # Try alternate ordering
        match = re.search(r'value="([^"]+)"\s+name="csrf_token"', html)
    assert match, f"csrf_token hidden field not found in HTML:\n{html[:500]}"
    return match.group(1)


def _assert_activity_logged(mock_svc: MagicMock, action: str) -> None:
    """Assert that log_activity was called with the given action."""
    calls = mock_svc.log_activity.call_args_list
    actions = [c.kwargs.get("action") or c.args[1] for c in calls]
    assert action in actions, f"Expected action '{action}' in {actions}"
