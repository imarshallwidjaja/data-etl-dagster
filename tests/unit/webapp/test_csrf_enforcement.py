# =============================================================================
# CSRF Enforcement Tests
# =============================================================================
# TDD tests for the `require_csrf` dependency.
# Validates that all unsafe endpoints return 403 without a valid CSRF token
# and succeed with a valid token (from session).
# =============================================================================

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.config import get_settings
from app.main import app


@pytest.fixture(autouse=True)
def _mock_services():
    """Prevent real MongoDB / MinIO connections in unit tests."""
    mock_activity = MagicMock()
    mock_mongo = MagicMock()
    mock_minio = MagicMock()

    # list_assets returns empty lists by default
    mock_mongo.list_assets.return_value = []
    mock_mongo.get_manifest.return_value = None
    mock_minio.upload_to_landing.return_value = None
    mock_minio.delete_from_landing.return_value = None
    mock_minio.create_folder.return_value = "test/"
    mock_minio.delete_folder.return_value = None

    with (
        patch(
            "app.services.activity_service.get_activity_service",
            return_value=mock_activity,
        ),
        patch(
            "app.routers.landing.get_mongodb_service",
            return_value=mock_mongo,
        )
        if _can_patch("app.routers.landing.get_mongodb_service")
        else _noop(),
        patch(
            "app.routers.landing.get_minio_service",
            return_value=mock_minio,
        ),
        patch(
            "app.routers.manifests.get_minio_service",
            return_value=mock_minio,
        ),
        patch(
            "app.routers.manifests.get_mongodb_service",
            return_value=mock_mongo,
        ),
        patch(
            "app.routers.workflows.get_mongodb_service",
            return_value=mock_mongo,
        )
        if _can_patch("app.routers.workflows.get_mongodb_service")
        else _noop(),
    ):
        yield {
            "activity": mock_activity,
            "mongo": mock_mongo,
            "minio": mock_minio,
        }


def _can_patch(target: str) -> bool:
    """Check if a target can be patched (attribute exists)."""
    parts = target.rsplit(".", 1)
    try:
        mod = __import__(parts[0], fromlist=[parts[1]])
        return hasattr(mod, parts[1])
    except Exception:
        return False


from contextlib import contextmanager


@contextmanager
def _noop():
    yield


# ---------------------------------------------------------------------------
# Helper: create an authenticated session and extract CSRF token
# ---------------------------------------------------------------------------


def _get_csrf_from_page(client: TestClient) -> str:
    """Extract CSRF token from the csrf meta tag in any authenticated page."""
    resp = client.get("/")
    assert resp.status_code == 200, f"Could not load /: {resp.status_code}"
    import re

    match = re.search(r'<meta\s+name="csrf-token"\s+content="([^"]+)"', resp.text)
    assert match, "csrf-token meta tag not found in page"
    return match.group(1)


def _login(client: TestClient) -> str:
    """Log in and return the CSRF token from the authenticated session."""
    settings = get_settings()

    # Seed CSRF
    resp = client.get("/login")
    assert resp.status_code == 200
    import re

    match = re.search(r'name="csrf_token"\s+value="([^"]+)"', resp.text)
    assert match, "csrf_token hidden field not found"
    csrf_token = match.group(1)

    # Log in
    resp = client.post(
        "/login",
        data={
            "username": settings.webapp_username,
            "password": settings.webapp_password,
            "csrf_token": csrf_token,
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303

    # Get the post-login CSRF from the page meta tag
    return _get_csrf_from_page(client)


# ===================================================================
# require_csrf dependency — unit tests
# ===================================================================


class TestRequireCsrfDependency:
    """Core ``require_csrf`` dependency behavior."""

    def test_no_session_skips_csrf(self):
        """Unauthenticated requests (no session) should NOT get 403 for CSRF.

        They'll get the normal 401/redirect from get_current_user instead.
        """
        client = TestClient(app, raise_server_exceptions=False)
        # POST /logout without session → should redirect to login (302/303)
        # or 401, NOT 403 for CSRF.
        resp = client.post("/logout", follow_redirects=False)
        # Without a session the user gets redirected to /login
        assert resp.status_code in (303, 401), (
            f"Expected redirect or 401, got {resp.status_code}"
        )

    def test_valid_header_token_passes(self):
        """A valid X-CSRF-Token header should pass CSRF enforcement."""
        client = TestClient(app, raise_server_exceptions=False)
        csrf = _login(client)
        resp = client.post(
            "/logout",
            headers={"X-CSRF-Token": csrf},
            follow_redirects=False,
        )
        # Should succeed (303 redirect to /login after logout)
        assert resp.status_code == 303

    def test_valid_form_field_passes(self):
        """A valid csrf_token form field should pass CSRF enforcement."""
        client = TestClient(app, raise_server_exceptions=False)
        csrf = _login(client)
        resp = client.post(
            "/logout",
            data={"csrf_token": csrf},
            follow_redirects=False,
        )
        assert resp.status_code == 303

    def test_missing_token_returns_403(self):
        """An authenticated request without CSRF token → 403."""
        client = TestClient(app, raise_server_exceptions=False)
        _login(client)  # Establish session
        resp = client.post(
            "/logout",
            follow_redirects=False,
        )
        assert resp.status_code == 403

    def test_wrong_token_returns_403(self):
        """An authenticated request with wrong CSRF token → 403."""
        client = TestClient(app, raise_server_exceptions=False)
        _login(client)
        resp = client.post(
            "/logout",
            headers={"X-CSRF-Token": "totally-wrong-token"},
            follow_redirects=False,
        )
        assert resp.status_code == 403


# ===================================================================
# Endpoint-specific CSRF tests: POST /logout
# ===================================================================


class TestCsrfOnLogout:
    """POST /logout requires valid CSRF when session is active."""

    def test_logout_without_csrf_returns_403(self):
        client = TestClient(app, raise_server_exceptions=False)
        _login(client)
        resp = client.post("/logout", follow_redirects=False)
        assert resp.status_code == 403

    def test_logout_with_csrf_succeeds(self):
        client = TestClient(app, raise_server_exceptions=False)
        csrf = _login(client)
        resp = client.post(
            "/logout",
            headers={"X-CSRF-Token": csrf},
            follow_redirects=False,
        )
        assert resp.status_code == 303


# ===================================================================
# Endpoint-specific CSRF tests: POST /landing/* endpoints
# ===================================================================


class TestCsrfOnLandingUpload:
    """POST /landing/upload requires CSRF when session is active."""

    def test_upload_without_csrf_returns_403(self):
        client = TestClient(app, raise_server_exceptions=False)
        _login(client)
        resp = client.post(
            "/landing/upload",
            files={"file": ("test.txt", b"hello", "text/plain")},
            follow_redirects=False,
        )
        assert resp.status_code == 403

    def test_upload_with_csrf_header_succeeds(self):
        client = TestClient(app, raise_server_exceptions=False)
        csrf = _login(client)
        resp = client.post(
            "/landing/upload",
            files={"file": ("test.txt", b"hello", "text/plain")},
            headers={"X-CSRF-Token": csrf},
            follow_redirects=False,
        )
        assert resp.status_code == 200


class TestCsrfOnLandingDelete:
    """POST /landing/delete/{path} requires CSRF."""

    def test_delete_without_csrf_returns_403(self):
        client = TestClient(app, raise_server_exceptions=False)
        _login(client)
        resp = client.post("/landing/delete/test.txt", follow_redirects=False)
        assert resp.status_code == 403

    def test_delete_with_csrf_succeeds(self):
        client = TestClient(app, raise_server_exceptions=False)
        csrf = _login(client)
        resp = client.post(
            "/landing/delete/test.txt",
            headers={"X-CSRF-Token": csrf},
            follow_redirects=False,
        )
        assert resp.status_code == 200


class TestCsrfOnLandingFolder:
    """POST /landing/folder requires CSRF."""

    def test_folder_without_csrf_returns_403(self):
        client = TestClient(app, raise_server_exceptions=False)
        _login(client)
        resp = client.post(
            "/landing/folder",
            json={"name": "testfolder"},
            follow_redirects=False,
        )
        assert resp.status_code == 403

    def test_folder_with_csrf_succeeds(self):
        client = TestClient(app, raise_server_exceptions=False)
        csrf = _login(client)
        resp = client.post(
            "/landing/folder",
            json={"name": "testfolder"},
            headers={"X-CSRF-Token": csrf},
            follow_redirects=False,
        )
        assert resp.status_code == 200


class TestCsrfOnLandingDeleteFolder:
    """POST /landing/delete-folder/{path} requires CSRF."""

    def test_delete_folder_without_csrf_returns_403(self):
        client = TestClient(app, raise_server_exceptions=False)
        _login(client)
        resp = client.post(
            "/landing/delete-folder/testfolder",
            follow_redirects=False,
        )
        assert resp.status_code == 403

    def test_delete_folder_with_csrf_succeeds(self):
        client = TestClient(app, raise_server_exceptions=False)
        csrf = _login(client)
        resp = client.post(
            "/landing/delete-folder/testfolder",
            headers={"X-CSRF-Token": csrf},
            follow_redirects=False,
        )
        assert resp.status_code == 200


# ===================================================================
# Endpoint-specific CSRF tests: POST /manifests/* endpoints
# ===================================================================


class TestCsrfOnManifestCreate:
    """POST /manifests/new/{asset_type} requires CSRF."""

    def test_create_manifest_without_csrf_returns_403(self):
        client = TestClient(app, raise_server_exceptions=False)
        _login(client)
        resp = client.post(
            "/manifests/new/spatial",
            json={"intent": "test"},
            follow_redirects=False,
        )
        assert resp.status_code == 403

    def test_create_manifest_with_csrf_does_not_return_403(self):
        """With valid CSRF, the request should NOT be rejected for CSRF.

        It may fail for other reasons (bad payload), but not 403 CSRF.
        """
        client = TestClient(app, raise_server_exceptions=False)
        csrf = _login(client)
        resp = client.post(
            "/manifests/new/spatial",
            json={"intent": "test"},
            headers={"X-CSRF-Token": csrf},
            follow_redirects=False,
        )
        # Not 403 — CSRF passed; may be 400/422 for bad payload
        assert resp.status_code != 403


class TestCsrfOnManifestDelete:
    """POST /manifests/{batch_id}/delete requires CSRF."""

    def test_delete_manifest_without_csrf_returns_403(self):
        client = TestClient(app, raise_server_exceptions=False)
        _login(client)
        resp = client.post(
            "/manifests/test-batch/delete",
            follow_redirects=False,
        )
        assert resp.status_code == 403

    def test_delete_manifest_with_csrf_does_not_return_403(self):
        client = TestClient(app, raise_server_exceptions=False)
        csrf = _login(client)
        resp = client.post(
            "/manifests/test-batch/delete",
            headers={"X-CSRF-Token": csrf},
            follow_redirects=False,
        )
        # Will be 404 (manifest not found) but NOT 403
        assert resp.status_code != 403


class TestCsrfOnManifestRerun:
    """POST /manifests/{batch_id}/rerun requires CSRF."""

    def test_rerun_manifest_without_csrf_returns_403(self):
        client = TestClient(app, raise_server_exceptions=False)
        _login(client)
        resp = client.post(
            "/manifests/test-batch/rerun",
            follow_redirects=False,
        )
        assert resp.status_code == 403

    def test_rerun_manifest_with_csrf_does_not_return_403(self):
        client = TestClient(app, raise_server_exceptions=False)
        csrf = _login(client)
        resp = client.post(
            "/manifests/test-batch/rerun",
            headers={"X-CSRF-Token": csrf},
            follow_redirects=False,
        )
        assert resp.status_code != 403


# ===================================================================
# Endpoint-specific CSRF tests: POST /workflows/{id}/step/{idx}
# ===================================================================


class TestCsrfOnWorkflowStep:
    """POST /workflows/{id}/step/{idx} requires CSRF."""

    def test_workflow_step_without_csrf_returns_403(self):
        client = TestClient(app, raise_server_exceptions=False)
        _login(client)
        resp = client.post(
            "/workflows/joined/step/0",
            data={"_nav": "next"},
            follow_redirects=False,
        )
        assert resp.status_code == 403

    def test_workflow_step_with_csrf_does_not_return_403(self):
        client = TestClient(app, raise_server_exceptions=False)
        csrf = _login(client)
        resp = client.post(
            "/workflows/joined/step/0",
            data={"_nav": "next", "csrf_token": csrf},
            headers={"X-CSRF-Token": csrf},
            follow_redirects=False,
        )
        # May be 404 or 200, but not 403 CSRF
        assert resp.status_code != 403


# ===================================================================
# CSRF meta tag in base template
# ===================================================================


class TestCsrfMetaTag:
    """Authenticated pages must include a csrf-token meta tag."""

    def test_authenticated_page_has_csrf_meta(self):
        client = TestClient(app, raise_server_exceptions=False)
        _login(client)
        resp = client.get("/")
        assert resp.status_code == 200
        import re

        assert re.search(r'<meta\s+name="csrf-token"\s+content="[^"]+"', resp.text), (
            "csrf-token meta tag not found in authenticated page"
        )
