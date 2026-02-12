# =============================================================================
# Transport-Agnostic get_current_user Tests
# =============================================================================
# TDD tests for session-first auth with hybrid Basic fallback,
# redirect vs 401 JSON for unauthenticated requests.
# =============================================================================

import base64
from unittest.mock import MagicMock, patch

import pytest
from fastapi import Request
from fastapi.testclient import TestClient
from starlette.datastructures import QueryParams, Headers
from starlette.responses import JSONResponse

from app.config import Settings, get_settings
from app.main import app

client = TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def _mock_activity_service():
    """Prevent real MongoDB connections in unit tests."""
    with patch("app.services.activity_service.get_activity_service") as mock:
        mock.return_value = MagicMock()
        yield mock


class TestUnauthenticatedRedirect:
    """Unauthenticated HTML requests should 303 redirect to /login."""

    def test_unauth_get_root_redirects_to_login(self):
        """GET / without session → 303 with Location: /login?next=%2F."""
        response = client.get("/", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/login?next=%2F"

    def test_unauth_get_landing_redirects_to_login(self):
        """GET /landing/ without session → 303 redirect to /login?next=..."""
        response = client.get("/landing/", follow_redirects=False)
        assert response.status_code == 303
        assert "/login?next=" in response.headers["location"]


class TestUnauthenticatedJson401:
    """Unauthenticated JSON/API requests should get 401 JSON, not redirect."""

    def test_unauth_get_with_format_json_returns_401(self):
        """GET /manifests/?format=json without session → 401 JSON."""
        response = client.get("/manifests/?format=json", follow_redirects=False)
        assert response.status_code == 401
        body = response.json()
        assert "detail" in body
        # Must NOT include WWW-Authenticate: Basic header
        assert "www-authenticate" not in response.headers

    def test_unauth_get_whoami_returns_401(self):
        """GET /whoami without session → 401 JSON."""
        response = client.get("/whoami", follow_redirects=False)
        assert response.status_code == 401
        body = response.json()
        assert "detail" in body
        assert "www-authenticate" not in response.headers


class TestHybridBasicFallback:
    """In hybrid mode, Basic auth header should still work for API endpoints."""

    def test_whoami_with_basic_auth_returns_200(self):
        """GET /whoami with valid Basic header → 200 in hybrid mode."""
        # Override settings to hybrid mode for this test
        _real_settings = get_settings()
        hybrid_settings = Settings(
            webapp_auth_mode="hybrid",
            webapp_username=_real_settings.webapp_username,
            webapp_password=_real_settings.webapp_password,
            webapp_session_secret=_real_settings.webapp_session_secret,
        )
        app.dependency_overrides[get_settings] = lambda: hybrid_settings
        try:
            credentials = base64.b64encode(b"admin:admin").decode()
            response = client.get(
                "/whoami",
                headers={"Authorization": f"Basic {credentials}"},
            )
            assert response.status_code == 200
            body = response.json()
            assert body["username"] == "admin"
        finally:
            app.dependency_overrides.pop(get_settings, None)

    def test_basic_auth_ignored_in_session_mode(self):
        """GET /whoami with Basic header in session mode → 401 (Basic not used)."""
        credentials = base64.b64encode(b"admin:admin").decode()
        response = client.get(
            "/whoami",
            headers={"Authorization": f"Basic {credentials}"},
        )
        # Default mode is "session", so Basic auth should be ignored
        assert response.status_code == 401


class TestSessionAuthenticatedAccess:
    """Session-authenticated requests should pass through normally."""

    def test_session_user_can_access_whoami(self):
        """GET /whoami with valid session → 200."""
        from app.auth.session import set_session_user

        # Register a temporary test route to establish a session
        @app.get("/_test_set_session")
        async def _set_session(request: Request):
            set_session_user(request.session, "admin")
            return JSONResponse({"ok": True})

        try:
            with TestClient(app, raise_server_exceptions=False) as c:
                # First establish a session
                resp = c.get("/_test_set_session")
                assert resp.status_code == 200

                # Now access whoami with the session cookie
                resp = c.get("/whoami")
                assert resp.status_code == 200
                body = resp.json()
                assert body["username"] == "admin"
        finally:
            # Clean up the test route
            app.routes[:] = [
                r
                for r in app.routes
                if getattr(r, "path", None) != "/_test_set_session"
            ]

    def test_session_user_can_access_root(self):
        """GET / with valid session → 200 (no redirect)."""
        from app.auth.session import set_session_user

        @app.get("/_test_set_session2")
        async def _set_session2(request: Request):
            set_session_user(request.session, "admin")
            return JSONResponse({"ok": True})

        try:
            with TestClient(app, raise_server_exceptions=False) as c:
                resp = c.get("/_test_set_session2")
                assert resp.status_code == 200

                resp = c.get("/", follow_redirects=False)
                assert resp.status_code == 200
        finally:
            app.routes[:] = [
                r
                for r in app.routes
                if getattr(r, "path", None) != "/_test_set_session2"
            ]


class TestWantsJsonDetection:
    """Tests for the wants_json() helper function."""

    def test_format_json_query_param(self):
        """wants_json returns True when format=json query param is present."""
        from app.auth.dependencies import wants_json

        request = MagicMock()
        request.query_params = QueryParams("format=json")
        request.url.path = "/manifests/"
        request.headers = Headers(raw=[])

        assert wants_json(request) is True

    def test_whoami_path_is_json(self):
        """wants_json returns True for /whoami path."""
        from app.auth.dependencies import wants_json

        request = MagicMock()
        request.query_params = QueryParams("")
        request.url.path = "/whoami"
        request.headers = Headers(raw=[])

        assert wants_json(request) is True

    def test_accept_json_header(self):
        """wants_json returns True when Accept contains application/json."""
        from app.auth.dependencies import wants_json

        request = MagicMock()
        request.query_params = QueryParams("")
        request.url.path = "/landing/"
        request.headers = Headers(raw=[(b"accept", b"application/json")])

        assert wants_json(request) is True

    def test_html_request_is_not_json(self):
        """wants_json returns False for regular HTML page requests."""
        from app.auth.dependencies import wants_json

        request = MagicMock()
        request.query_params = QueryParams("")
        request.url.path = "/landing/"
        request.headers = Headers(raw=[(b"accept", b"text/html")])

        assert wants_json(request) is False
