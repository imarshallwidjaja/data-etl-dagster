"""
Unit tests for ActivityLog model.

Tests validation logic, type checking, and model behavior.
"""

import pytest
from datetime import datetime, timezone
from pydantic import ValidationError

from libs.models import ActivityLog


class TestActivityLogValidation:
    """Test ActivityLog model validation."""

    def test_valid_activity_log_minimal(self):
        """Test creating ActivityLog with minimal required fields."""
        log = ActivityLog(
            user="user_123",
            action="create_manifest",
            resource_type="manifest",
            resource_id="batch_abc123",
        )
        assert log.user == "user_123"
        assert log.action == "create_manifest"
        assert log.resource_type == "manifest"
        assert log.resource_id == "batch_abc123"
        assert log.details == {}
        assert log.timestamp is not None

    def test_valid_activity_log_with_details(self):
        """Test creating ActivityLog with details."""
        log = ActivityLog(
            user="admin",
            action="run_success",
            resource_type="run",
            resource_id="run_xyz789",
            details={"duration_ms": 1234, "asset_count": 3},
        )
        assert log.details == {"duration_ms": 1234, "asset_count": 3}

    def test_valid_activity_log_with_timestamp(self):
        """Test creating ActivityLog with explicit timestamp."""
        ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        log = ActivityLog(
            timestamp=ts,
            user="system",
            action="run_started",
            resource_type="run",
            resource_id="run_001",
        )
        assert log.timestamp == ts

    def test_default_timestamp_is_utc(self):
        """Test that default timestamp is in UTC."""
        log = ActivityLog(
            user="user_1",
            action="upload_file",
            resource_type="file",
            resource_id="file_001",
        )
        assert log.timestamp.tzinfo is not None

    def test_all_valid_actions(self):
        """Test all valid action values."""
        valid_actions = [
            "create_manifest",
            "rerun_manifest",
            "delete_manifest",
            "upload_file",
            "delete_file",
            "download_asset",
            "run_started",
            "run_success",
            "run_failure",
            "run_canceled",
        ]
        for action in valid_actions:
            log = ActivityLog(
                user="test",
                action=action,
                resource_type="manifest",
                resource_id="test_id",
            )
            assert log.action == action

    def test_all_valid_resource_types(self):
        """Test all valid resource_type values."""
        valid_types = ["manifest", "file", "asset", "run"]
        for rtype in valid_types:
            log = ActivityLog(
                user="test",
                action="create_manifest",
                resource_type=rtype,
                resource_id="test_id",
            )
            assert log.resource_type == rtype

    def test_invalid_action_rejected(self):
        """Test that invalid action values are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            ActivityLog(
                user="user_1",
                action="invalid_action",
                resource_type="manifest",
                resource_id="test_id",
            )
        assert "action" in str(exc_info.value)

    def test_invalid_resource_type_rejected(self):
        """Test that invalid resource_type values are rejected."""
        with pytest.raises(ValidationError) as exc_info:
            ActivityLog(
                user="user_1",
                action="create_manifest",
                resource_type="invalid_type",
                resource_id="test_id",
            )
        assert "resource_type" in str(exc_info.value)

    def test_missing_user_rejected(self):
        """Test that missing user field is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            ActivityLog(
                action="create_manifest",
                resource_type="manifest",
                resource_id="test_id",
            )
        assert "user" in str(exc_info.value)

    def test_missing_action_rejected(self):
        """Test that missing action field is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            ActivityLog(
                user="user_1",
                resource_type="manifest",
                resource_id="test_id",
            )
        assert "action" in str(exc_info.value)

    def test_missing_resource_type_rejected(self):
        """Test that missing resource_type field is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            ActivityLog(
                user="user_1",
                action="create_manifest",
                resource_id="test_id",
            )
        assert "resource_type" in str(exc_info.value)

    def test_missing_resource_id_rejected(self):
        """Test that missing resource_id field is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            ActivityLog(
                user="user_1",
                action="create_manifest",
                resource_type="manifest",
            )
        assert "resource_id" in str(exc_info.value)

    def test_details_accepts_nested_dict(self):
        """Test that details accepts nested dictionaries."""
        log = ActivityLog(
            user="admin",
            action="run_success",
            resource_type="run",
            resource_id="run_001",
            details={
                "metadata": {"key": "value", "count": 42},
                "tags": ["a", "b", "c"],
            },
        )
        assert log.details["metadata"]["key"] == "value"
        assert log.details["tags"] == ["a", "b", "c"]

    def test_model_dump(self):
        """Test that model_dump works correctly."""
        log = ActivityLog(
            user="user_1",
            action="create_manifest",
            resource_type="manifest",
            resource_id="batch_001",
        )
        data = log.model_dump()
        assert "timestamp" in data
        assert data["user"] == "user_1"
        assert data["action"] == "create_manifest"
        assert data["resource_type"] == "manifest"
        assert data["resource_id"] == "batch_001"
        assert data["details"] == {}
