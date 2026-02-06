# =============================================================================
# Webapp Health Integration Tests
# =============================================================================
# Tests webapp health endpoints against running container.
# =============================================================================

import pytest
import requests


@pytest.mark.integration
class TestWebappHealth:
    """Integration tests for webapp health endpoints."""

    def test_health_endpoint(self, webapp_url):
        """Health endpoint should return healthy status."""
        response = requests.get(f"{webapp_url}/health", timeout=10)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data

    def test_ready_endpoint(self, webapp_url):
        """Ready endpoint should check all dependencies."""
        response = requests.get(f"{webapp_url}/ready", timeout=10)

        assert response.status_code == 200
        data = response.json()
        assert "services" in data
        assert "minio" in data["services"]
        assert "mongodb" in data["services"]

    def test_whoami_requires_auth(self, webapp_url):
        """Whoami endpoint should require authentication."""
        response = requests.get(f"{webapp_url}/whoami", timeout=10)

        assert response.status_code == 401

    def test_whoami_with_valid_auth(self, webapp_url):
        """Whoami endpoint should return user info with valid auth."""
        response = requests.get(
            f"{webapp_url}/whoami",
            auth=("admin", "admin"),
            timeout=10,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "admin"
