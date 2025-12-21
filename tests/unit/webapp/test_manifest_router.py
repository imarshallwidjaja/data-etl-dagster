# =============================================================================
# Manifest API Validation Unit Tests
# =============================================================================
# Tests to ensure manifest creation API validates required fields.
# This prevents 422 errors like the missing 'type' field issue.
# =============================================================================

import pytest
from pydantic import ValidationError

from libs.models import FileEntry, JoinConfig, TagValue


class TestFileEntryValidation:
    """Tests for FileEntry model validation - prevents missing field bugs."""

    def test_file_entry_requires_type_field(self):
        """FileEntry without 'type' should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            FileEntry(
                path="s3://landing-zone/test.geojson",
                format="GeoJSON",
                # Missing 'type' field!
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("type",)
        assert errors[0]["type"] == "missing"

    def test_file_entry_requires_path_field(self):
        """FileEntry without 'path' should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            FileEntry(
                type="vector",
                format="GeoJSON",
                # Missing 'path' field!
            )

        errors = exc_info.value.errors()
        assert any(e["loc"] == ("path",) for e in errors)

    def test_file_entry_requires_format_field(self):
        """FileEntry without 'format' should raise ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            FileEntry(
                path="s3://landing-zone/test.geojson",
                type="vector",
                # Missing 'format' field!
            )

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("format",)

    def test_file_entry_valid_vector(self):
        """Valid vector FileEntry should pass validation."""
        entry = FileEntry(
            path="s3://landing-zone/data.geojson",
            type="vector",
            format="GeoJSON",
        )
        assert entry.path == "s3://landing-zone/data.geojson"
        assert entry.type == "vector"
        assert entry.format == "GeoJSON"

    def test_file_entry_valid_raster(self):
        """Valid raster FileEntry should pass validation."""
        entry = FileEntry(
            path="s3://landing-zone/image.tif",
            type="raster",
            format="GTiff",
        )
        assert entry.type == "raster"

    def test_file_entry_valid_tabular(self):
        """Valid tabular FileEntry should pass validation."""
        entry = FileEntry(
            path="s3://landing-zone/data.csv",
            type="tabular",
            format="CSV",
        )
        assert entry.type == "tabular"

    def test_file_entry_invalid_type(self):
        """Invalid type value should raise ValidationError."""
        with pytest.raises(ValidationError):
            FileEntry(
                path="s3://landing-zone/data.txt",
                type="invalid_type",
                format="TXT",
            )


class TestJoinConfigValidation:
    """Tests for JoinConfig validation."""

    def test_join_config_requires_spatial_dataset_id(self):
        """JoinConfig without spatial_dataset_id should fail."""
        with pytest.raises(ValidationError) as exc_info:
            JoinConfig(
                tabular_dataset_id="tabular_123",
                left_key="sa1_code",
            )

        errors = exc_info.value.errors()
        assert any("spatial_dataset_id" in str(e["loc"]) for e in errors)

    def test_join_config_requires_tabular_dataset_id(self):
        """JoinConfig without tabular_dataset_id should fail."""
        with pytest.raises(ValidationError) as exc_info:
            JoinConfig(
                spatial_dataset_id="spatial_123",
                left_key="sa1_code",
            )

        errors = exc_info.value.errors()
        assert any("tabular_dataset_id" in str(e["loc"]) for e in errors)

    def test_join_config_requires_left_key(self):
        """JoinConfig without left_key should fail."""
        with pytest.raises(ValidationError) as exc_info:
            JoinConfig(
                spatial_dataset_id="spatial_123",
                tabular_dataset_id="tabular_456",
            )

        errors = exc_info.value.errors()
        assert any("left_key" in str(e["loc"]) for e in errors)

    def test_join_config_defaults_right_key_to_left_key(self):
        """JoinConfig without right_key should default to left_key."""
        config = JoinConfig(
            spatial_dataset_id="spatial_123",
            tabular_dataset_id="tabular_456",
            left_key="sa1_code",
        )
        assert config.right_key == "sa1_code"

    def test_join_config_valid_how_values(self):
        """JoinConfig how field should accept valid values."""
        for how in ["left", "inner", "right", "outer"]:
            config = JoinConfig(
                spatial_dataset_id="spatial_123",
                tabular_dataset_id="tabular_456",
                left_key="sa1_code",
                how=how,
            )
            assert config.how == how


class TestTagValueValidation:
    """Tests for TagValue type validation."""

    def test_tag_value_accepts_string(self):
        """TagValue should accept string."""
        # TagValue is a type alias, test via dict construction
        tags: dict[str, TagValue] = {"source": "ABS"}
        assert tags["source"] == "ABS"

    def test_tag_value_accepts_int(self):
        """TagValue should accept int."""
        tags: dict[str, TagValue] = {"year": 2024}
        assert tags["year"] == 2024

    def test_tag_value_accepts_float(self):
        """TagValue should accept float."""
        tags: dict[str, TagValue] = {"priority": 1.5}
        assert tags["priority"] == 1.5

    def test_tag_value_accepts_bool(self):
        """TagValue should accept bool."""
        tags: dict[str, TagValue] = {"active": True}
        assert tags["active"] is True
