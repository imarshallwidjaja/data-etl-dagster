# =============================================================================
# MinIO Service Unit Tests - Folder Operations
# =============================================================================

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from io import BytesIO

from app.services.minio_service import (
    MinIOService,
    LandingZoneObject,
    PROTECTED_PREFIXES,
)


class TestCreateFolder:
    """Tests for folder creation."""

    @patch("app.services.minio_service.get_settings")
    def test_create_folder_success(self, mock_settings):
        """Should create folder marker object."""
        mock_settings.return_value = Mock(
            minio_endpoint="localhost:9000",
            minio_root_user="user",
            minio_root_password="pass",
            minio_use_ssl=False,
            minio_landing_bucket="landing-zone",
            minio_lake_bucket="data-lake",
        )

        with patch.object(MinIOService, "__init__", lambda x: None):
            service = MinIOService()
            service._client = Mock()
            service._landing_bucket = "landing-zone"

            result = service.create_folder("test_folder")

            assert result == "test_folder/"
            service._client.put_object.assert_called_once()
            call_args = service._client.put_object.call_args
            assert call_args[0][0] == "landing-zone"
            assert call_args[0][1] == "test_folder/"
            assert call_args[1]["length"] == 0

    @patch("app.services.minio_service.get_settings")
    def test_create_folder_adds_trailing_slash(self, mock_settings):
        """Should add trailing slash if not present."""
        mock_settings.return_value = Mock(
            minio_endpoint="localhost:9000",
            minio_root_user="user",
            minio_root_password="pass",
            minio_use_ssl=False,
            minio_landing_bucket="landing-zone",
            minio_lake_bucket="data-lake",
        )

        with patch.object(MinIOService, "__init__", lambda x: None):
            service = MinIOService()
            service._client = Mock()
            service._landing_bucket = "landing-zone"

            result = service.create_folder("my_folder")

            assert result == "my_folder/"

    @patch("app.services.minio_service.get_settings")
    def test_create_folder_keeps_trailing_slash(self, mock_settings):
        """Should keep trailing slash if already present."""
        mock_settings.return_value = Mock(
            minio_endpoint="localhost:9000",
            minio_root_user="user",
            minio_root_password="pass",
            minio_use_ssl=False,
            minio_landing_bucket="landing-zone",
            minio_lake_bucket="data-lake",
        )

        with patch.object(MinIOService, "__init__", lambda x: None):
            service = MinIOService()
            service._client = Mock()
            service._landing_bucket = "landing-zone"

            result = service.create_folder("my_folder/")

            assert result == "my_folder/"


class TestFolderIsEmpty:
    """Tests for folder empty check."""

    @patch("app.services.minio_service.get_settings")
    def test_folder_is_empty_no_items(self, mock_settings):
        """Empty folder with no items returns True."""
        mock_settings.return_value = Mock(
            minio_endpoint="localhost:9000",
            minio_root_user="user",
            minio_root_password="pass",
            minio_use_ssl=False,
            minio_landing_bucket="landing-zone",
            minio_lake_bucket="data-lake",
        )

        with patch.object(MinIOService, "__init__", lambda x: None):
            service = MinIOService()
            service._client = Mock()
            service._landing_bucket = "landing-zone"
            service._client.list_objects.return_value = iter([])

            assert service.folder_is_empty("empty_folder/") is True

    @patch("app.services.minio_service.get_settings")
    def test_folder_is_empty_only_marker(self, mock_settings):
        """Folder with only folder marker returns True."""
        mock_settings.return_value = Mock(
            minio_endpoint="localhost:9000",
            minio_root_user="user",
            minio_root_password="pass",
            minio_use_ssl=False,
            minio_landing_bucket="landing-zone",
            minio_lake_bucket="data-lake",
        )

        with patch.object(MinIOService, "__init__", lambda x: None):
            service = MinIOService()
            service._client = Mock()
            service._landing_bucket = "landing-zone"

            # Mock folder marker object
            marker = Mock()
            marker.object_name = "my_folder/"
            service._client.list_objects.return_value = iter([marker])

            assert service.folder_is_empty("my_folder/") is True

    @patch("app.services.minio_service.get_settings")
    def test_folder_is_empty_has_content(self, mock_settings):
        """Folder with content returns False."""
        mock_settings.return_value = Mock(
            minio_endpoint="localhost:9000",
            minio_root_user="user",
            minio_root_password="pass",
            minio_use_ssl=False,
            minio_landing_bucket="landing-zone",
            minio_lake_bucket="data-lake",
        )

        with patch.object(MinIOService, "__init__", lambda x: None):
            service = MinIOService()
            service._client = Mock()
            service._landing_bucket = "landing-zone"

            # Mock folder with file inside
            folder_marker = Mock()
            folder_marker.object_name = "my_folder/"
            file_obj = Mock()
            file_obj.object_name = "my_folder/file.txt"
            service._client.list_objects.return_value = iter([folder_marker, file_obj])

            assert service.folder_is_empty("my_folder/") is False


class TestDeleteFolder:
    """Tests for folder deletion."""

    @patch("app.services.minio_service.get_settings")
    def test_delete_folder_success(self, mock_settings):
        """Should delete empty folder."""
        mock_settings.return_value = Mock(
            minio_endpoint="localhost:9000",
            minio_root_user="user",
            minio_root_password="pass",
            minio_use_ssl=False,
            minio_landing_bucket="landing-zone",
            minio_lake_bucket="data-lake",
        )

        with patch.object(MinIOService, "__init__", lambda x: None):
            service = MinIOService()
            service._client = Mock()
            service._landing_bucket = "landing-zone"
            service._client.list_objects.return_value = iter([])

            service.delete_folder("empty_folder/")

            service._client.remove_object.assert_called_once_with(
                "landing-zone", "empty_folder/"
            )

    @patch("app.services.minio_service.get_settings")
    def test_delete_folder_not_empty_raises_error(self, mock_settings):
        """Should raise ValueError for non-empty folder."""
        mock_settings.return_value = Mock(
            minio_endpoint="localhost:9000",
            minio_root_user="user",
            minio_root_password="pass",
            minio_use_ssl=False,
            minio_landing_bucket="landing-zone",
            minio_lake_bucket="data-lake",
        )

        with patch.object(MinIOService, "__init__", lambda x: None):
            service = MinIOService()
            service._client = Mock()
            service._landing_bucket = "landing-zone"

            # Mock folder with content
            folder_marker = Mock()
            folder_marker.object_name = "my_folder/"
            file_obj = Mock()
            file_obj.object_name = "my_folder/file.txt"
            service._client.list_objects.return_value = iter([folder_marker, file_obj])

            with pytest.raises(ValueError, match="folder is not empty"):
                service.delete_folder("my_folder/")

    @patch("app.services.minio_service.get_settings")
    def test_delete_folder_archive_protected(self, mock_settings):
        """Should raise PermissionError for archive folder."""
        mock_settings.return_value = Mock(
            minio_endpoint="localhost:9000",
            minio_root_user="user",
            minio_root_password="pass",
            minio_use_ssl=False,
            minio_landing_bucket="landing-zone",
            minio_lake_bucket="data-lake",
        )

        with patch.object(MinIOService, "__init__", lambda x: None):
            service = MinIOService()
            service._client = Mock()
            service._landing_bucket = "landing-zone"

            with pytest.raises(PermissionError, match="Cannot delete protected folder"):
                service.delete_folder("archive/")

    @patch("app.services.minio_service.get_settings")
    def test_delete_folder_manifests_protected(self, mock_settings):
        """Should raise PermissionError for manifests folder."""
        mock_settings.return_value = Mock(
            minio_endpoint="localhost:9000",
            minio_root_user="user",
            minio_root_password="pass",
            minio_use_ssl=False,
            minio_landing_bucket="landing-zone",
            minio_lake_bucket="data-lake",
        )

        with patch.object(MinIOService, "__init__", lambda x: None):
            service = MinIOService()
            service._client = Mock()
            service._landing_bucket = "landing-zone"

            with pytest.raises(PermissionError, match="Cannot delete protected folder"):
                service.delete_folder("manifests/")

    @patch("app.services.minio_service.get_settings")
    def test_delete_subfolder_of_protected_also_protected(self, mock_settings):
        """Should raise PermissionError for subfolders of protected prefixes."""
        mock_settings.return_value = Mock(
            minio_endpoint="localhost:9000",
            minio_root_user="user",
            minio_root_password="pass",
            minio_use_ssl=False,
            minio_landing_bucket="landing-zone",
            minio_lake_bucket="data-lake",
        )

        with patch.object(MinIOService, "__init__", lambda x: None):
            service = MinIOService()
            service._client = Mock()
            service._landing_bucket = "landing-zone"

            with pytest.raises(PermissionError, match="Cannot delete protected folder"):
                service.delete_folder("archive/sub_folder/")


class TestProtectedPrefixes:
    """Tests for protected prefixes constant."""

    def test_protected_prefixes_includes_archive(self):
        """archive/ should be protected."""
        assert "archive/" in PROTECTED_PREFIXES

    def test_protected_prefixes_includes_manifests(self):
        """manifests/ should be protected."""
        assert "manifests/" in PROTECTED_PREFIXES
