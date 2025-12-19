# =============================================================================
# Webapp Health Integration Tests
# =============================================================================
# Tests webapp health endpoints against running container.
# =============================================================================

import pytest
import requests

WEBAPP_URL = "http://localhost:8080"


@pytest.mark.integration
class TestWebappHealth:
    """Integration tests for webapp health endpoints."""

    def test_health_endpoint(self):
        """Health endpoint should return healthy status."""
        response = requests.get(f"{WEBAPP_URL}/health", timeout=10)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data

    def test_ready_endpoint(self):
        """Ready endpoint should check all dependencies."""
        response = requests.get(f"{WEBAPP_URL}/ready", timeout=10)

        assert response.status_code == 200
        data = response.json()
        assert "services" in data
        assert "minio" in data["services"]
        assert "mongodb" in data["services"]

    def test_whoami_requires_auth(self):
        """Whoami endpoint should require authentication."""
        response = requests.get(f"{WEBAPP_URL}/whoami", timeout=10)

        assert response.status_code == 401

    def test_whoami_with_valid_auth(self):
        """Whoami endpoint should return user info with valid auth."""
        response = requests.get(
            f"{WEBAPP_URL}/whoami",
            auth=("admin", "admin"),
            timeout=10,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "admin"
