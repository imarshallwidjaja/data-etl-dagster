# =============================================================================
# Activity Service Unit Tests
# =============================================================================

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from bson import ObjectId

from app.services.activity_service import (
    ActivityService,
    ActivityLogEntry,
    get_activity_service,
)


class TestLogActivity:
    """Tests for activity logging."""

    @patch("app.services.activity_service.get_settings")
    def test_log_activity_basic(self, mock_settings):
        """Should insert activity log with required fields."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl?authSource=admin"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()
            mock_collection.insert_one.return_value = Mock(
                inserted_id=ObjectId("507f1f77bcf86cd799439011")
            )
            service._db.__getitem__ = Mock(return_value=mock_collection)

            result = service.log_activity(
                user="admin",
                action="upload",
                resource_type="file",
                resource_id="batch_001/data.csv",
                details={"size": 1024},
            )

            assert result == "507f1f77bcf86cd799439011"
            mock_collection.insert_one.assert_called_once()
            call_args = mock_collection.insert_one.call_args[0][0]
            assert call_args["user"] == "admin"
            assert call_args["action"] == "upload"
            assert call_args["resource_type"] == "file"
            assert call_args["resource_id"] == "batch_001/data.csv"
            assert call_args["details"] == {"size": 1024}
            assert "timestamp" in call_args
            assert "ip_address" not in call_args

    @patch("app.services.activity_service.get_settings")
    def test_log_activity_with_ip_address(self, mock_settings):
        """Should include ip_address when provided."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()
            mock_collection.insert_one.return_value = Mock(
                inserted_id=ObjectId("507f1f77bcf86cd799439012")
            )
            service._db.__getitem__ = Mock(return_value=mock_collection)

            result = service.log_activity(
                user="admin",
                action="download",
                resource_type="asset",
                resource_id="dataset_abc123",
                details={},
                ip_address="192.168.1.100",
            )

            assert result == "507f1f77bcf86cd799439012"
            call_args = mock_collection.insert_one.call_args[0][0]
            assert call_args["ip_address"] == "192.168.1.100"

    @patch("app.services.activity_service.get_settings")
    def test_log_activity_empty_details(self, mock_settings):
        """Should use empty dict for details when None provided."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()
            mock_collection.insert_one.return_value = Mock(
                inserted_id=ObjectId("507f1f77bcf86cd799439013")
            )
            service._db.__getitem__ = Mock(return_value=mock_collection)

            service.log_activity(
                user="admin",
                action="delete",
                resource_type="manifest",
                resource_id="batch_002",
                details=None,
            )

            call_args = mock_collection.insert_one.call_args[0][0]
            assert call_args["details"] == {}


class TestGetRecentActivity:
    """Tests for querying recent activity."""

    @patch("app.services.activity_service.get_settings")
    def test_get_recent_activity_no_filters(self, mock_settings):
        """Should return all activity logs without filters."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()

            # Mock cursor chain
            mock_cursor = MagicMock()
            mock_cursor.sort.return_value = mock_cursor
            mock_cursor.skip.return_value = mock_cursor
            mock_cursor.limit.return_value = mock_cursor

            # Return two mock documents
            timestamp = datetime(2026, 1, 13, 10, 0, 0)
            mock_cursor.__iter__ = Mock(
                return_value=iter(
                    [
                        {
                            "_id": ObjectId("507f1f77bcf86cd799439011"),
                            "user": "admin",
                            "action": "upload",
                            "resource_type": "file",
                            "resource_id": "batch_001/data.csv",
                            "details": {"size": 1024},
                            "timestamp": timestamp,
                        },
                        {
                            "_id": ObjectId("507f1f77bcf86cd799439012"),
                            "user": "user1",
                            "action": "download",
                            "resource_type": "asset",
                            "resource_id": "dataset_abc",
                            "details": {},
                            "timestamp": timestamp,
                            "ip_address": "10.0.0.1",
                        },
                    ]
                )
            )
            mock_collection.find.return_value = mock_cursor
            service._db.__getitem__ = Mock(return_value=mock_collection)

            results, count = service.get_recent_activity()

            assert count == 2
            assert len(results) == 2
            assert isinstance(results[0], ActivityLogEntry)
            assert results[0].user == "admin"
            assert results[0].action == "upload"
            assert results[1].ip_address == "10.0.0.1"
            mock_collection.find.assert_called_once_with({})

    @patch("app.services.activity_service.get_settings")
    def test_get_recent_activity_filter_by_user(self, mock_settings):
        """Should filter by user when provided."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()

            mock_cursor = MagicMock()
            mock_cursor.sort.return_value = mock_cursor
            mock_cursor.skip.return_value = mock_cursor
            mock_cursor.limit.return_value = mock_cursor
            mock_cursor.__iter__ = Mock(return_value=iter([]))
            mock_collection.find.return_value = mock_cursor
            service._db.__getitem__ = Mock(return_value=mock_collection)

            results, count = service.get_recent_activity(user="admin")

            assert count == 0
            mock_collection.find.assert_called_once_with({"user": "admin"})

    @patch("app.services.activity_service.get_settings")
    def test_get_recent_activity_filter_by_action(self, mock_settings):
        """Should filter by action when provided."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()

            mock_cursor = MagicMock()
            mock_cursor.sort.return_value = mock_cursor
            mock_cursor.skip.return_value = mock_cursor
            mock_cursor.limit.return_value = mock_cursor
            mock_cursor.__iter__ = Mock(return_value=iter([]))
            mock_collection.find.return_value = mock_cursor
            service._db.__getitem__ = Mock(return_value=mock_collection)

            results, count = service.get_recent_activity(action="upload")

            mock_collection.find.assert_called_once_with({"action": "upload"})

    @patch("app.services.activity_service.get_settings")
    def test_get_recent_activity_filter_by_resource_type(self, mock_settings):
        """Should filter by resource_type when provided."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()

            mock_cursor = MagicMock()
            mock_cursor.sort.return_value = mock_cursor
            mock_cursor.skip.return_value = mock_cursor
            mock_cursor.limit.return_value = mock_cursor
            mock_cursor.__iter__ = Mock(return_value=iter([]))
            mock_collection.find.return_value = mock_cursor
            service._db.__getitem__ = Mock(return_value=mock_collection)

            results, count = service.get_recent_activity(resource_type="manifest")

            mock_collection.find.assert_called_once_with({"resource_type": "manifest"})

    @patch("app.services.activity_service.get_settings")
    def test_get_recent_activity_filter_by_resource_id(self, mock_settings):
        """Should filter by resource_id when provided."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()

            mock_cursor = MagicMock()
            mock_cursor.sort.return_value = mock_cursor
            mock_cursor.skip.return_value = mock_cursor
            mock_cursor.limit.return_value = mock_cursor
            mock_cursor.__iter__ = Mock(return_value=iter([]))
            mock_collection.find.return_value = mock_cursor
            service._db.__getitem__ = Mock(return_value=mock_collection)

            results, count = service.get_recent_activity(resource_id="batch_001")

            mock_collection.find.assert_called_once_with({"resource_id": "batch_001"})

    @patch("app.services.activity_service.get_settings")
    def test_get_recent_activity_combined_filters(self, mock_settings):
        """Should combine multiple filters."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()

            mock_cursor = MagicMock()
            mock_cursor.sort.return_value = mock_cursor
            mock_cursor.skip.return_value = mock_cursor
            mock_cursor.limit.return_value = mock_cursor
            mock_cursor.__iter__ = Mock(return_value=iter([]))
            mock_collection.find.return_value = mock_cursor
            service._db.__getitem__ = Mock(return_value=mock_collection)

            results, count = service.get_recent_activity(
                user="admin", action="upload", resource_type="file"
            )

            mock_collection.find.assert_called_once_with(
                {"user": "admin", "action": "upload", "resource_type": "file"}
            )

    @patch("app.services.activity_service.get_settings")
    def test_get_recent_activity_empty_results(self, mock_settings):
        """Should return empty list and zero count when no results."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()

            mock_cursor = MagicMock()
            mock_cursor.sort.return_value = mock_cursor
            mock_cursor.skip.return_value = mock_cursor
            mock_cursor.limit.return_value = mock_cursor
            mock_cursor.__iter__ = Mock(return_value=iter([]))
            mock_collection.find.return_value = mock_cursor
            service._db.__getitem__ = Mock(return_value=mock_collection)

            results, count = service.get_recent_activity()

            assert results == []
            assert count == 0


class TestPagination:
    """Tests for pagination behavior."""

    @patch("app.services.activity_service.get_settings")
    def test_pagination_offset_zero_limit_default(self, mock_settings):
        """Should use offset=0 and limit=50 by default."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()

            mock_cursor = MagicMock()
            mock_cursor.sort.return_value = mock_cursor
            mock_cursor.skip.return_value = mock_cursor
            mock_cursor.limit.return_value = mock_cursor
            mock_cursor.__iter__ = Mock(return_value=iter([]))
            mock_collection.find.return_value = mock_cursor
            service._db.__getitem__ = Mock(return_value=mock_collection)

            service.get_recent_activity()

            mock_cursor.skip.assert_called_once_with(0)
            mock_cursor.limit.assert_called_once_with(50)

    @patch("app.services.activity_service.get_settings")
    def test_pagination_custom_offset_limit(self, mock_settings):
        """Should use provided offset and limit values."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()

            mock_cursor = MagicMock()
            mock_cursor.sort.return_value = mock_cursor
            mock_cursor.skip.return_value = mock_cursor
            mock_cursor.limit.return_value = mock_cursor
            mock_cursor.__iter__ = Mock(return_value=iter([]))
            mock_collection.find.return_value = mock_cursor
            service._db.__getitem__ = Mock(return_value=mock_collection)

            service.get_recent_activity(offset=20, limit=10)

            mock_cursor.skip.assert_called_once_with(20)
            mock_cursor.limit.assert_called_once_with(10)

    @patch("app.services.activity_service.get_settings")
    def test_pagination_limit_boundary_one(self, mock_settings):
        """Should handle limit=1 (boundary case)."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()

            mock_cursor = MagicMock()
            mock_cursor.sort.return_value = mock_cursor
            mock_cursor.skip.return_value = mock_cursor
            mock_cursor.limit.return_value = mock_cursor

            timestamp = datetime(2026, 1, 13, 10, 0, 0)
            mock_cursor.__iter__ = Mock(
                return_value=iter(
                    [
                        {
                            "_id": ObjectId("507f1f77bcf86cd799439011"),
                            "user": "admin",
                            "action": "upload",
                            "resource_type": "file",
                            "resource_id": "batch_001",
                            "details": {},
                            "timestamp": timestamp,
                        }
                    ]
                )
            )
            mock_collection.find.return_value = mock_cursor
            service._db.__getitem__ = Mock(return_value=mock_collection)

            results, count = service.get_recent_activity(limit=1)

            assert count == 1
            assert len(results) == 1
            mock_cursor.limit.assert_called_once_with(1)

    @patch("app.services.activity_service.get_settings")
    def test_pagination_large_offset_returns_empty(self, mock_settings):
        """Should return empty when offset exceeds data."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()

            mock_cursor = MagicMock()
            mock_cursor.sort.return_value = mock_cursor
            mock_cursor.skip.return_value = mock_cursor
            mock_cursor.limit.return_value = mock_cursor
            mock_cursor.__iter__ = Mock(return_value=iter([]))
            mock_collection.find.return_value = mock_cursor
            service._db.__getitem__ = Mock(return_value=mock_collection)

            results, count = service.get_recent_activity(offset=1000)

            assert results == []
            assert count == 0
            mock_cursor.skip.assert_called_once_with(1000)


class TestSortOrder:
    """Tests for result ordering."""

    @patch("app.services.activity_service.get_settings")
    def test_results_sorted_by_timestamp_desc(self, mock_settings):
        """Should sort results by timestamp descending."""
        mock_settings.return_value = Mock(
            mongo_connection_string="mongodb://user:pass@localhost:27017/spatial_etl"
        )

        with patch.object(ActivityService, "__init__", lambda x: None):
            service = ActivityService()
            service._db = Mock()
            mock_collection = Mock()

            mock_cursor = MagicMock()
            mock_cursor.sort.return_value = mock_cursor
            mock_cursor.skip.return_value = mock_cursor
            mock_cursor.limit.return_value = mock_cursor
            mock_cursor.__iter__ = Mock(return_value=iter([]))
            mock_collection.find.return_value = mock_cursor
            service._db.__getitem__ = Mock(return_value=mock_collection)

            from pymongo import DESCENDING

            service.get_recent_activity()

            mock_cursor.sort.assert_called_once_with("timestamp", DESCENDING)


class TestDbNameExtraction:
    """Tests for database name extraction."""

    def test_extract_db_name_with_auth(self):
        """Should extract db name from connection string with auth."""
        conn_str = "mongodb://user:pass@localhost:27017/my_database?authSource=admin"
        result = ActivityService._extract_db_name(conn_str)
        assert result == "my_database"

    def test_extract_db_name_without_options(self):
        """Should extract db name when no query options."""
        conn_str = "mongodb://user:pass@localhost:27017/test_db"
        result = ActivityService._extract_db_name(conn_str)
        assert result == "test_db"

    def test_extract_db_name_fallback(self):
        """Should return default when extraction fails."""
        conn_str = "mongodb://localhost:27017/"
        result = ActivityService._extract_db_name(conn_str)
        assert result == "spatial_etl"


class TestSingleton:
    """Tests for singleton pattern."""

    @patch("app.services.activity_service._activity_service", None)
    @patch("app.services.activity_service.ActivityService")
    def test_get_activity_service_creates_singleton(self, mock_class):
        """Should create new instance when none exists."""
        mock_instance = Mock()
        mock_class.return_value = mock_instance

        result = get_activity_service()

        assert result == mock_instance
        mock_class.assert_called_once()

    @patch("app.services.activity_service._activity_service", None)
    @patch("app.services.activity_service.ActivityService")
    def test_get_activity_service_returns_existing(self, mock_class):
        """Should return existing instance on subsequent calls."""
        mock_instance = Mock()
        mock_class.return_value = mock_instance

        result1 = get_activity_service()
        result2 = get_activity_service()

        assert result1 is result2
        mock_class.assert_called_once()
