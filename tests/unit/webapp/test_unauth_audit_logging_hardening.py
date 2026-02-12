"""Hardening tests for unauthorized access audit logging.

This file covers two concerns:
1) The unauthenticated path should not block the event loop by performing
   synchronous Mongo writes inline.
2) The logged details should use a true path (no scheme/host/query string).
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture(autouse=True)
def _mock_activity_service():
    """Prevent real MongoDB connections in unit tests."""
    mock_svc = MagicMock()
    with patch("app.services.activity_service.get_activity_service") as factory:
        factory.return_value = mock_svc
        yield mock_svc


def test_unauth_redirect_logs_path_only(_mock_activity_service: MagicMock):
    """Unauthenticated browser request should log only url.path, not full URL."""
    client = TestClient(app, raise_server_exceptions=False)

    resp = client.get(
        "/landing/?token=SECRET&format=html",
        headers={"Host": "example.com"},
        follow_redirects=False,
    )
    assert resp.status_code == 303

    # Verify logged details
    assert _mock_activity_service.log_activity.called
    kwargs = _mock_activity_service.log_activity.call_args.kwargs
    details = kwargs.get("details")
    assert isinstance(details, dict)
    assert details.get("path") == "/landing/"
    assert "SECRET" not in str(details)


def test_unauth_logging_is_offloaded_to_thread(_mock_activity_service: MagicMock):
    """get_current_user should offload sync log_activity via asyncio.to_thread."""
    import asyncio
    import inspect

    from app.auth import dependencies

    source = inspect.getsource(dependencies.get_current_user)
    assert "to_thread" in source, (
        "get_current_user should use asyncio.to_thread() for audit logging "
        "to avoid blocking the event loop"
    )
