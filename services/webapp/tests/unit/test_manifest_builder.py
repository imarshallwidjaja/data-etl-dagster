# =============================================================================
# Manifest Builder Unit Tests
# =============================================================================

import pytest

from app.services.manifest_builder import (
    generate_batch_id,
    generate_dataset_id,
    build_manifest,
)


class TestGenerateBatchId:
    """Tests for batch ID generation."""

    def test_generate_batch_id_format(self):
        """Batch ID should follow expected format."""
        batch_id = generate_batch_id()

        assert batch_id is not None
        assert isinstance(batch_id, str)
        assert len(batch_id) > 0
        # Should start with 'batch_'
        assert batch_id.startswith("batch_")

    def test_generate_batch_id_uniqueness(self):
        """Each call should generate a unique ID."""
        ids = [generate_batch_id() for _ in range(10)]

        assert len(set(ids)) == 10  # All unique


class TestGenerateDatasetId:
    """Tests for dataset ID generation."""

    def test_generate_dataset_id_format(self):
        """Dataset ID should follow expected format."""
        dataset_id = generate_dataset_id()

        assert dataset_id is not None
        assert isinstance(dataset_id, str)
        assert dataset_id.startswith("dataset_")

    def test_generate_dataset_id_uniqueness(self):
        """Each call should generate a unique ID."""
        ids = [generate_dataset_id() for _ in range(10)]

        assert len(set(ids)) == 10


class TestBuildManifest:
    """Tests for manifest building from form data."""

    def test_build_spatial_manifest(self):
        """Spatial manifest should have correct structure."""
        form_data = {
            "project": "TEST_PROJECT",
            "intent": "ingest_vector",
            "files": [
                {
                    "path": "s3://bucket/test.geojson",
                    "type": "vector",
                    "format": "GeoJSON",
                }
            ],
        }

        manifest = build_manifest(
            asset_type="spatial",
            form_data=form_data,
            uploader="testuser",
        )

        assert manifest.batch_id is not None
        assert manifest.intent == "ingest_vector"
        assert manifest.uploader == "testuser"
        assert len(manifest.files) == 1
        assert manifest.files[0].path == "s3://bucket/test.geojson"
        assert manifest.metadata.project == "TEST_PROJECT"

    def test_build_tabular_manifest(self):
        """Tabular manifest should set correct intent."""
        form_data = {
            "project": "CENSUS_DATA",
            "files": [
                {"path": "s3://bucket/data.csv", "type": "tabular", "format": "CSV"}
            ],
        }

        manifest = build_manifest(
            asset_type="tabular",
            form_data=form_data,
            uploader="testuser",
        )

        assert manifest.intent == "ingest_tabular"
        assert manifest.files[0].type == "tabular"

    def test_build_joined_manifest(self):
        """Joined manifest should have join config."""
        form_data = {
            "project": "JOIN_TEST",
            "join_config": {
                "spatial_dataset_id": "sa1_spatial",
                "tabular_dataset_id": "census_table",
                "left_key": "SA1_CODE",
                "right_key": "sa1",
                "how": "left",
            },
        }

        manifest = build_manifest(
            asset_type="joined",
            form_data=form_data,
            uploader="testuser",
        )

        assert manifest.intent == "join_datasets"
        assert manifest.metadata.join_config is not None
        assert manifest.metadata.join_config.spatial_dataset_id == "sa1_spatial"
        assert manifest.metadata.join_config.left_key == "SA1_CODE"

    def test_build_manifest_with_custom_batch_id(self):
        """Custom batch ID should be used if provided."""
        form_data = {
            "batch_id": "custom_batch_123",
            "project": "TEST",
            "intent": "ingest_vector",
            "files": [
                {
                    "path": "s3://bucket/test.geojson",
                    "type": "vector",
                    "format": "GeoJSON",
                }
            ],
        }

        manifest = build_manifest(
            asset_type="spatial",
            form_data=form_data,
            uploader="testuser",
        )

        assert manifest.batch_id == "custom_batch_123"

    def test_build_manifest_with_tags(self):
        """Tags should be included in metadata."""
        form_data = {
            "project": "TEST",
            "intent": "ingest_vector",
            "files": [
                {
                    "path": "s3://bucket/test.geojson",
                    "type": "vector",
                    "format": "GeoJSON",
                }
            ],
            "tags": {"source": "ABS", "year": "2024"},
        }

        manifest = build_manifest(
            asset_type="spatial",
            form_data=form_data,
            uploader="testuser",
        )

        # Tags should include custom tags (plus auto-generated dataset_id)
        assert manifest.metadata.tags["source"] == "ABS"
        assert manifest.metadata.tags["year"] == "2024"
        assert "dataset_id" in manifest.metadata.tags

    def test_build_manifest_invalid_type(self):
        """Invalid asset type should raise ValueError."""
        form_data = {"project": "TEST"}

        with pytest.raises(ValueError, match="Unknown asset type"):
            build_manifest(
                asset_type="invalid",
                form_data=form_data,
                uploader="testuser",
            )
