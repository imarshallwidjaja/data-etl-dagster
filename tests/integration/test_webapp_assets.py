# =============================================================================
# Webapp Assets Integration Tests
# =============================================================================
# Tests asset browsing endpoints against running container with MongoDB.
# =============================================================================

import pytest
import requests

WEBAPP_URL = "http://localhost:8080"
AUTH = ("admin", "admin")


@pytest.mark.integration
class TestWebappAssets:
    """Integration tests for asset endpoints."""

    def test_list_assets_html(self):
        """Assets list should return HTML by default."""
        response = requests.get(
            f"{WEBAPP_URL}/assets/",
            auth=AUTH,
            timeout=10,
        )

        assert response.status_code == 200
        assert "text/html" in response.headers.get("content-type", "")
        assert "Assets" in response.text

    def test_list_assets_json(self):
        """Assets list should return JSON with format param."""
        response = requests.get(
            f"{WEBAPP_URL}/assets/",
            auth=AUTH,
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 200
        data = response.json()
        assert "assets" in data
        assert "count" in data
        assert isinstance(data["assets"], list)

    def test_filter_assets_by_kind(self):
        """Assets list should filter by kind."""
        response = requests.get(
            f"{WEBAPP_URL}/assets/",
            auth=AUTH,
            params={"format": "json", "kind": "spatial"},
            timeout=10,
        )

        assert response.status_code == 200
        data = response.json()
        # All returned assets should be spatial
        for asset in data["assets"]:
            assert asset["kind"] == "spatial"

    def test_assets_requires_auth(self):
        """Assets endpoint should require authentication."""
        response = requests.get(
            f"{WEBAPP_URL}/assets/",
            timeout=10,
        )

        assert response.status_code == 401
