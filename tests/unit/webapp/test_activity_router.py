import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from datetime import datetime
from app.main import app
from app.services.activity_service import ActivityLogEntry

from app.auth.dependencies import get_current_user

client = TestClient(app)


# Mock authenticated user
@pytest.fixture
def mock_auth():
    mock_user = MagicMock(username="test_user", display_name="Test User")
    app.dependency_overrides[get_current_user] = lambda: mock_user
    yield mock_user
    app.dependency_overrides = {}


@pytest.fixture
def mock_activity_service():
    with patch("app.routers.activity.get_activity_service") as mock:
        service = MagicMock()
        mock.return_value = service
        yield service


def test_list_activity_html(mock_auth, mock_activity_service):
    """Test listing activity in HTML format."""
    mock_activity_service.get_recent_activity.return_value = ([], 0)

    response = client.get("/activity/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Activity Log" in response.text


def test_list_activity_json(mock_auth, mock_activity_service):
    """Test listing activity in JSON format."""
    mock_entry = ActivityLogEntry(
        id="60d5ecb8b5c9c62b3c7c3b5a",
        user="test_user",
        action="upload",
        resource_type="file",
        resource_id="file_123",
        details={"filename": "test.csv"},
        timestamp=datetime(2023, 1, 1, 12, 0, 0),
        ip_address="127.0.0.1",
    )
    mock_activity_service.get_recent_activity.return_value = ([mock_entry], 1)

    response = client.get("/activity/?format=json")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    data = response.json()
    assert data["count_returned"] == 1
    assert len(data["items"]) == 1
    assert data["items"][0]["user"] == "test_user"
    assert data["items"][0]["action"] == "upload"


def test_list_activity_filters(mock_auth, mock_activity_service):
    """Test listing activity with filters."""
    mock_activity_service.get_recent_activity.return_value = ([], 0)

    response = client.get("/activity/?user=ivan&action=delete&format=json")

    assert response.status_code == 200
    mock_activity_service.get_recent_activity.assert_called_with(
        user="ivan",
        action="delete",
        resource_type=None,
        resource_id=None,
        offset=0,
        limit=50,
    )

    data = response.json()
    assert data["filters"]["user"] == "ivan"
    assert data["filters"]["action"] == "delete"


def test_list_activity_pagination(mock_auth, mock_activity_service):
    """Test listing activity with pagination."""
    mock_activity_service.get_recent_activity.return_value = ([], 0)

    response = client.get("/activity/?offset=10&limit=20&format=json")

    assert response.status_code == 200
    mock_activity_service.get_recent_activity.assert_called_with(
        user=None,
        action=None,
        resource_type=None,
        resource_id=None,
        offset=10,
        limit=20,
    )

    data = response.json()
    assert data["offset"] == 10
    assert data["limit"] == 20
