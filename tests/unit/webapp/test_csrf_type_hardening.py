"""CSRF hardening tests.

These tests cover edge cases where the submitted csrf_token is not a string.
In particular, multipart form submissions can contain UploadFile objects.

Expected behavior: CSRF validation should fail with 403 (not 500).
"""

from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.config import get_settings
from app.main import app


@pytest.fixture(autouse=True)
def _mock_services():
    """Prevent real service connections in unit tests."""
    mock_activity = MagicMock()
    mock_mongo = MagicMock()
    mock_minio = MagicMock()

    mock_mongo.list_assets.return_value = []
    mock_mongo.get_manifest.return_value = None

    def _maybe_patch(target: str, **kwargs):
        """Patch only if the attribute exists."""
        module_path, attr = target.rsplit(".", 1)
        mod = __import__(module_path, fromlist=[attr])
        if not hasattr(mod, attr):
            return _noop()
        return patch(target, **kwargs)

    from contextlib import contextmanager

    @contextmanager
    def _noop():
        yield

    with (
        patch(
            "app.services.activity_service.get_activity_service",
            return_value=mock_activity,
        ),
        patch("app.routers.landing.get_minio_service", return_value=mock_minio),
        _maybe_patch(
            "app.routers.landing.get_mongodb_service", return_value=mock_mongo
        ),
        patch("app.routers.manifests.get_minio_service", return_value=mock_minio),
        patch("app.routers.manifests.get_mongodb_service", return_value=mock_mongo),
        _maybe_patch(
            "app.routers.workflows.get_mongodb_service", return_value=mock_mongo
        ),
    ):
        yield


def _login(client: TestClient) -> str:
    settings = get_settings()
    # Seed CSRF
    resp = client.get("/login")
    assert resp.status_code == 200
    import re

    match = re.search(r'name="csrf_token"\s+value="([^"]+)"', resp.text)
    assert match
    csrf = match.group(1)

    resp = client.post(
        "/login",
        data={
            "username": settings.webapp_username,
            "password": settings.webapp_password,
            "csrf_token": csrf,
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    # Return post-login CSRF from meta tag
    home = client.get("/")
    assert home.status_code == 200
    match = re.search(r'<meta\s+name="csrf-token"\s+content="([^"]+)"', home.text)
    assert match
    return match.group(1)


def test_csrf_token_uploadfile_returns_403_not_500():
    """Multipart csrf_token as file should be treated as invalid (403)."""
    client = TestClient(app, raise_server_exceptions=False)
    good_csrf = _login(client)

    # Send csrf_token as a file field (UploadFile-like) instead of a string.
    resp = client.post(
        "/logout",
        files={"csrf_token": ("csrf.txt", BytesIO(b"not-a-token"), "text/plain")},
        follow_redirects=False,
    )

    assert resp.status_code == 403

    # Control: correct token in header still works.
    resp = client.post(
        "/logout",
        headers={"X-CSRF-Token": good_csrf},
        follow_redirects=False,
    )
    assert resp.status_code == 303
