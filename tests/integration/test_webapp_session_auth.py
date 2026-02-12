# =============================================================================
# Webapp Session Auth Integration Tests
# =============================================================================
# End-to-end session flow tests against a running webapp container.
# Uses requests.Session() to exercise real cookie/session behavior.
#
# Requires:
#   - Webapp container running (via worktree_stack.py)
#   - WEBAPP_AUTH_MODE=hybrid in environment
#   - WEBAPP_SESSION_SECRET set
# =============================================================================

from __future__ import annotations

import re

import pytest
import requests


# Default credentials matching env.example / .env.ci
_USERNAME = "admin"
_PASSWORD = "admin"


def _extract_csrf_token(html: str) -> str:
    """Extract the CSRF token from a hidden form field in the HTML body."""
    match = re.search(r'name="csrf_token"\s+value="([^"]+)"', html)
    if not match:
        raise ValueError("csrf_token hidden field not found in HTML")
    return match.group(1)


def _login(
    session: requests.Session,
    base_url: str,
    *,
    username: str = _USERNAME,
    password: str = _PASSWORD,
    next_url: str = "",
) -> requests.Response:
    """Perform a full GET /login → POST /login cycle, returning the POST response.

    The caller's ``session`` will hold the resulting cookie state.
    """
    login_url = f"{base_url}/login"
    if next_url:
        login_url = f"{login_url}?next={next_url}"

    # GET the login form to seed the CSRF token in the session cookie.
    get_resp = session.get(login_url, timeout=10, allow_redirects=False)
    assert get_resp.status_code == 200, (
        f"Expected 200 from GET /login, got {get_resp.status_code}"
    )

    csrf = _extract_csrf_token(get_resp.text)

    # POST credentials + CSRF token.
    post_url = f"{base_url}/login"
    if next_url:
        post_url = f"{post_url}?next={next_url}"

    return session.post(
        post_url,
        data={"username": username, "password": password, "csrf_token": csrf},
        timeout=10,
        allow_redirects=False,
    )


@pytest.mark.integration
class TestLoginPage:
    """GET /login renders a login form with a CSRF token."""

    def test_login_page_renders(self, webapp_url: str) -> None:
        """GET /login should return 200 with a form containing a CSRF token."""
        resp = requests.get(f"{webapp_url}/login", timeout=10)

        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
        assert 'name="csrf_token"' in resp.text
        assert 'name="username"' in resp.text
        assert 'name="password"' in resp.text


@pytest.mark.integration
class TestLoginSuccess:
    """POST /login with valid credentials issues a session and redirects."""

    def test_login_redirects_to_root(self, webapp_url: str) -> None:
        """Successful login without next= should redirect to /."""
        s = requests.Session()
        resp = _login(s, webapp_url)

        assert resp.status_code == 303
        assert resp.headers.get("location") in ("/", f"{webapp_url}/")

    def test_login_redirects_to_safe_next(self, webapp_url: str) -> None:
        """Successful login with safe next= should redirect there."""
        s = requests.Session()
        resp = _login(s, webapp_url, next_url="/landing/")

        assert resp.status_code == 303
        location = resp.headers.get("location", "")
        assert location.endswith("/landing/")

    def test_login_ignores_unsafe_next(self, webapp_url: str) -> None:
        """Successful login with absolute URL next= should redirect to /."""
        s = requests.Session()
        resp = _login(s, webapp_url, next_url="https://evil.com/pwned")

        assert resp.status_code == 303
        assert resp.headers.get("location") in ("/", f"{webapp_url}/")

    def test_session_cookie_set(self, webapp_url: str) -> None:
        """After login the session cookie should be present in the jar."""
        s = requests.Session()
        _login(s, webapp_url)

        cookie_names = [c.name for c in s.cookies]
        assert "webapp_session" in cookie_names


@pytest.mark.integration
class TestLoginFailure:
    """POST /login with invalid credentials re-renders the form."""

    def test_bad_password(self, webapp_url: str) -> None:
        """Wrong password should re-render the login page with an error."""
        s = requests.Session()
        resp = _login(s, webapp_url, password="wrong")

        # Re-rendered form, not a redirect
        assert resp.status_code == 200
        assert "Invalid username or password" in resp.text

    def test_bad_username(self, webapp_url: str) -> None:
        """Non-existent user should re-render the login page with an error."""
        s = requests.Session()
        resp = _login(s, webapp_url, username="nobody", password="wrong")

        assert resp.status_code == 200
        assert "Invalid username or password" in resp.text


@pytest.mark.integration
class TestSessionProtectedAccess:
    """Authenticated session grants access to protected endpoints."""

    def test_whoami_via_session(self, webapp_url: str) -> None:
        """/whoami should return the session user as JSON."""
        s = requests.Session()
        _login(s, webapp_url)

        resp = s.get(f"{webapp_url}/whoami", timeout=10)
        assert resp.status_code == 200

        data = resp.json()
        assert data["username"] == _USERNAME

    def test_index_via_session(self, webapp_url: str) -> None:
        """GET / with a valid session should return the index page."""
        s = requests.Session()
        _login(s, webapp_url)

        resp = s.get(f"{webapp_url}/", timeout=10)
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    def test_landing_zone_via_session_json(self, webapp_url: str) -> None:
        """GET /landing/?format=json with a valid session should return 200 JSON."""
        s = requests.Session()
        _login(s, webapp_url)

        resp = s.get(
            f"{webapp_url}/landing/",
            params={"format": "json"},
            timeout=10,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "files" in data


@pytest.mark.integration
class TestUnauthenticatedBehavior:
    """Unauthenticated requests are rejected or redirected appropriately."""

    def test_html_redirect_to_login(self, webapp_url: str) -> None:
        """Unauthenticated GET / should redirect to /login."""
        resp = requests.get(
            f"{webapp_url}/",
            timeout=10,
            allow_redirects=False,
        )

        assert resp.status_code == 303
        location = resp.headers.get("location", "")
        assert "/login" in location

    def test_json_returns_401(self, webapp_url: str) -> None:
        """Unauthenticated GET /whoami should return 401 JSON."""
        resp = requests.get(f"{webapp_url}/whoami", timeout=10)

        assert resp.status_code == 401
        data = resp.json()
        assert data["detail"] == "Not authenticated"

    def test_json_format_returns_401(self, webapp_url: str) -> None:
        """Unauthenticated GET /landing/?format=json should return 401 JSON."""
        resp = requests.get(
            f"{webapp_url}/landing/",
            params={"format": "json"},
            timeout=10,
        )

        assert resp.status_code == 401


@pytest.mark.integration
class TestLogout:
    """POST /logout clears the session and redirects."""

    def test_logout_clears_session(self, webapp_url: str) -> None:
        """After logout, /whoami should return 401."""
        s = requests.Session()
        _login(s, webapp_url)

        # Confirm authenticated
        assert s.get(f"{webapp_url}/whoami", timeout=10).status_code == 200

        # Get CSRF token from an authenticated page for the logout POST
        index_resp = s.get(f"{webapp_url}/", timeout=10)
        csrf = _extract_csrf_from_meta_or_form(index_resp.text)

        # Logout
        logout_resp = s.post(
            f"{webapp_url}/logout",
            data={"csrf_token": csrf},
            timeout=10,
            allow_redirects=False,
        )
        assert logout_resp.status_code == 303
        assert "/login" in logout_resp.headers.get("location", "")

        # After logout, session should be gone
        whoami_resp = s.get(f"{webapp_url}/whoami", timeout=10)
        assert whoami_resp.status_code == 401

    def test_logout_without_csrf_fails(self, webapp_url: str) -> None:
        """POST /logout without CSRF token should be rejected."""
        s = requests.Session()
        _login(s, webapp_url)

        resp = s.post(f"{webapp_url}/logout", timeout=10)
        assert resp.status_code == 403


@pytest.mark.integration
class TestCSRFEnforcement:
    """CSRF tokens are required for state-changing session requests."""

    def test_post_without_csrf_rejected(self, webapp_url: str) -> None:
        """POST to a protected endpoint without CSRF should return 403."""
        s = requests.Session()
        _login(s, webapp_url)

        # Try a POST without CSRF token
        resp = s.post(f"{webapp_url}/logout", timeout=10)
        assert resp.status_code == 403

    def test_post_with_header_csrf_accepted(self, webapp_url: str) -> None:
        """POST with X-CSRF-Token header should be accepted."""
        s = requests.Session()
        _login(s, webapp_url)

        # Extract CSRF from session — get it from meta tag or an authenticated page
        index_resp = s.get(f"{webapp_url}/", timeout=10)
        csrf = _extract_csrf_from_meta_or_form(index_resp.text)

        # POST /logout with header CSRF
        resp = s.post(
            f"{webapp_url}/logout",
            headers={"X-CSRF-Token": csrf},
            timeout=10,
            allow_redirects=False,
        )
        assert resp.status_code == 303


@pytest.mark.integration
class TestBasicAuthBehavior:
    """Basic auth behavior depends on the auth mode configured on the server.

    The default test stack runs in ``session`` mode where Basic auth is
    ignored.  These tests verify that raw Basic auth credentials alone do
    NOT grant access in the default deployment — the session cookie is the
    only accepted transport.
    """

    def test_basic_auth_alone_rejected_in_session_mode(self, webapp_url: str) -> None:
        """In session mode, /whoami with only Basic auth should return 401."""
        resp = requests.get(
            f"{webapp_url}/whoami",
            auth=(_USERNAME, _PASSWORD),
            timeout=10,
        )

        # session mode: Basic auth is not a valid transport, expect 401.
        assert resp.status_code == 401

    def test_basic_auth_bad_password(self, webapp_url: str) -> None:
        """/whoami with wrong Basic auth should always return 401."""
        resp = requests.get(
            f"{webapp_url}/whoami",
            auth=(_USERNAME, "wrong"),
            timeout=10,
        )

        assert resp.status_code == 401


@pytest.mark.integration
class TestSessionFixation:
    """Session identifier rotates on login (fixation protection)."""

    def test_sid_rotates_on_login(self, webapp_url: str) -> None:
        """The session cookie value should change after successful login."""
        s = requests.Session()

        # Get the anonymous session cookie from GET /login
        s.get(f"{webapp_url}/login", timeout=10)
        pre_login_cookie = _get_session_cookie_value(s)

        # Login
        _login(s, webapp_url)
        post_login_cookie = _get_session_cookie_value(s)

        # The cookie value must have changed (session was rotated)
        assert pre_login_cookie != post_login_cookie


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_csrf_from_meta_or_form(html: str) -> str:
    """Extract CSRF token from either meta tag or hidden form field."""
    # Try meta tag first (base.html injects <meta name="csrf-token" content="...">)
    meta_match = re.search(r'<meta\s+name="csrf-token"\s+content="([^"]+)"', html)
    if meta_match:
        return meta_match.group(1)

    # Fall back to hidden form field (e.g. logout form in nav)
    field_match = re.search(r'name="csrf_token"\s+value="([^"]+)"', html)
    if field_match:
        return field_match.group(1)

    raise ValueError("Could not find CSRF token in HTML (meta or form field)")


def _get_session_cookie_value(session: requests.Session) -> str | None:
    """Return the value of the webapp_session cookie, or None."""
    for cookie in session.cookies:
        if cookie.name == "webapp_session":
            return cookie.value
    return None
