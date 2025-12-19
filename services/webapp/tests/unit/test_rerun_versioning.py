# =============================================================================
# Rerun Versioning Unit Tests
# =============================================================================

import pytest
from unittest.mock import patch, MagicMock

from app.services.manifest_builder import create_rerun_batch_id


class TestRerunVersioning:
    """Tests for batch ID versioning during reruns."""

    @patch("app.services.manifest_builder.get_mongodb_service")
    def test_first_rerun_creates_v2(self, mock_get_mongodb):
        """First rerun of a batch should create _v2 suffix."""
        mock_mongodb = MagicMock()
        mock_mongodb.get_next_rerun_version.return_value = 2
        mock_get_mongodb.return_value = mock_mongodb

        new_batch_id = create_rerun_batch_id("batch_20241219_abc123")

        assert new_batch_id == "batch_20241219_abc123_v2"
        mock_mongodb.get_next_rerun_version.assert_called_once_with(
            "batch_20241219_abc123"
        )

    @patch("app.services.manifest_builder.get_mongodb_service")
    def test_subsequent_rerun_increments_version(self, mock_get_mongodb):
        """Subsequent reruns should increment version."""
        mock_mongodb = MagicMock()
        mock_mongodb.get_next_rerun_version.return_value = 5
        mock_get_mongodb.return_value = mock_mongodb

        new_batch_id = create_rerun_batch_id("batch_20241219_abc123")

        assert new_batch_id == "batch_20241219_abc123_v5"

    @patch("app.services.manifest_builder.get_mongodb_service")
    def test_rerun_of_rerun_strips_existing_version(self, mock_get_mongodb):
        """Rerunning a _v2 batch should create _v3, not _v2_v3."""
        mock_mongodb = MagicMock()
        mock_mongodb.get_next_rerun_version.return_value = 3
        mock_get_mongodb.return_value = mock_mongodb

        new_batch_id = create_rerun_batch_id("batch_20241219_abc123_v2")

        # Should strip the existing _v2 and add _v3
        assert new_batch_id == "batch_20241219_abc123_v3"

    @patch("app.services.manifest_builder.get_mongodb_service")
    def test_rerun_preserves_base_id(self, mock_get_mongodb):
        """Base batch ID should be preserved."""
        mock_mongodb = MagicMock()
        mock_mongodb.get_next_rerun_version.return_value = 2
        mock_get_mongodb.return_value = mock_mongodb

        original = "my_custom_batch_id"
        new_batch_id = create_rerun_batch_id(original)

        assert new_batch_id.startswith("my_custom_batch_id")
        assert new_batch_id == "my_custom_batch_id_v2"

    @patch("app.services.manifest_builder.get_mongodb_service")
    def test_rerun_handles_complex_base_id(self, mock_get_mongodb):
        """Complex batch IDs with underscores should work."""
        mock_mongodb = MagicMock()
        mock_mongodb.get_next_rerun_version.return_value = 2
        mock_get_mongodb.return_value = mock_mongodb

        original = "sa1_spatial_census_2021_v10"  # Has _v10 at end
        new_batch_id = create_rerun_batch_id(original)

        # Should strip _v10 and add _v2
        assert new_batch_id == "sa1_spatial_census_2021_v2"
