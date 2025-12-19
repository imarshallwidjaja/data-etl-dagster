# =============================================================================
# Webapp Landing Zone Integration Tests
# =============================================================================
# Tests landing zone endpoints against running container with MinIO.
# =============================================================================

import io
import pytest
import requests

WEBAPP_URL = "http://localhost:8080"
AUTH = ("admin", "admin")


@pytest.mark.integration
class TestWebappLanding:
    """Integration tests for landing zone endpoints."""

    def test_list_landing_zone_html(self):
        """Landing zone list should return HTML by default."""
        response = requests.get(
            f"{WEBAPP_URL}/landing/",
            auth=AUTH,
            timeout=10,
        )

        assert response.status_code == 200
        assert "text/html" in response.headers.get("content-type", "")
        assert "Landing Zone" in response.text

    def test_list_landing_zone_json(self):
        """Landing zone list should return JSON with format param."""
        response = requests.get(
            f"{WEBAPP_URL}/landing/",
            auth=AUTH,
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 200
        data = response.json()
        assert "files" in data
        assert "count" in data
        assert isinstance(data["files"], list)

    def test_upload_and_delete_file(self):
        """Should upload and delete a file from landing zone."""
        # Upload
        test_content = b"test file content"
        files = {"file": ("test_upload.txt", io.BytesIO(test_content), "text/plain")}

        response = requests.post(
            f"{WEBAPP_URL}/landing/upload",
            auth=AUTH,
            files=files,
            params={"prefix": "test"},
            timeout=10,
        )

        assert response.status_code == 200
        data = response.json()
        assert "test/test_upload.txt" in data["key"]

        # Verify file exists
        list_response = requests.get(
            f"{WEBAPP_URL}/landing/",
            auth=AUTH,
            params={"format": "json", "prefix": "test/"},
            timeout=10,
        )
        data = list_response.json()
        keys = [f["key"] for f in data["files"]]
        assert any("test_upload.txt" in k for k in keys)

        # Delete
        delete_response = requests.post(
            f"{WEBAPP_URL}/landing/delete/test/test_upload.txt",
            auth=AUTH,
            timeout=10,
        )

        assert delete_response.status_code == 200

    def test_list_requires_auth(self):
        """Landing zone should require authentication."""
        response = requests.get(
            f"{WEBAPP_URL}/landing/",
            timeout=10,
        )

        assert response.status_code == 401
