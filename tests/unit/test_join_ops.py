"""
Unit tests for join operations.

Tests the core join operation functions in ops/join_ops.py.
"""

import pytest
from unittest.mock import Mock, patch

from libs.models import Manifest, JoinConfig, AssetKind


class TestResolveJoinAssets:
    """Test _resolve_join_assets function."""

    def test_resolve_join_assets_missing_join_config(self, valid_manifest_dict):
        """Test that missing join_config raises ValueError."""
        from services.dagster.etl_pipelines.ops.join_ops import _resolve_join_assets

        # Remove join_config
        manifest_dict = valid_manifest_dict.copy()
        manifest_dict["intent"] = "join_datasets"
        manifest_dict["metadata"] = {"project": "ALPHA"}

        with pytest.raises(ValueError, match="metadata.join_config is missing"):
            _resolve_join_assets(Mock(), manifest_dict, Mock())

    def test_resolve_join_assets_none_target(self, valid_manifest_dict):
        """Test that None target_asset_id raises ValueError."""
        from services.dagster.etl_pipelines.ops.join_ops import _resolve_join_assets

        # Set target_asset_id to None
        manifest_dict = valid_manifest_dict.copy()
        manifest_dict["intent"] = "join_datasets"
        manifest_dict["metadata"] = {
            "project": "ALPHA",
            "join_config": {
                "target_asset_id": None,
                "left_key": "id",
                "how": "left"
            }
        }

        with pytest.raises(ValueError, match="requires join_config.target_asset_id"):
            _resolve_join_assets(Mock(), manifest_dict, Mock())

    def test_resolve_join_assets_asset_not_found(self, valid_manifest_dict):
        """Test that non-existent asset raises ValueError."""
        from services.dagster.etl_pipelines.ops.join_ops import _resolve_join_assets

        # Mock MongoDB resource
        mock_mongodb = Mock()
        mock_mongodb.get_asset_by_id.return_value = None

        manifest_dict = valid_manifest_dict.copy()
        manifest_dict["intent"] = "join_datasets"
        manifest_dict["metadata"] = {
            "project": "ALPHA",
            "join_config": {
                "target_asset_id": "nonexistent_id",
                "left_key": "id",
                "how": "left"
            }
        }

        with pytest.raises(ValueError, match="Secondary asset not found"):
            _resolve_join_assets(mock_mongodb, manifest_dict, Mock())

    def test_resolve_join_assets_wrong_kind(self, valid_manifest_dict, valid_asset):
        """Test that non-spatial secondary asset raises ValueError."""
        from services.dagster.etl_pipelines.ops.join_ops import _resolve_join_assets

        # Mock MongoDB resource and tabular asset
        mock_mongodb = Mock()
        tabular_asset = valid_asset.model_copy()
        tabular_asset.kind = AssetKind.TABULAR
        mock_mongodb.get_asset_by_id.return_value = tabular_asset

        manifest_dict = valid_manifest_dict.copy()
        manifest_dict["intent"] = "join_datasets"
        manifest_dict["metadata"] = {
            "project": "ALPHA",
            "join_config": {
                "target_asset_id": "tabular_asset_id",
                "left_key": "id",
                "how": "left"
            }
        }

        with pytest.raises(ValueError, match="Secondary asset must be spatial"):
            _resolve_join_assets(mock_mongodb, manifest_dict, Mock())

    def test_resolve_join_assets_success(self, valid_manifest_dict, valid_asset):
        """Test successful resolution."""
        from services.dagster.etl_pipelines.ops.join_ops import _resolve_join_assets

        # Mock MongoDB resource and spatial asset
        mock_mongodb = Mock()
        spatial_asset = valid_asset.model_copy()
        spatial_asset.kind = AssetKind.SPATIAL
        spatial_asset.dataset_id = "spatial_dataset"
        spatial_asset.version = 1
        mock_mongodb.get_asset_by_id.return_value = spatial_asset

        manifest_dict = valid_manifest_dict.copy()
        manifest_dict["intent"] = "join_datasets"
        manifest_dict["metadata"] = {
            "project": "ALPHA",
            "join_config": {
                "target_asset_id": "spatial_asset_id",
                "left_key": "parcel_id",
                "right_key": "parcel_id",
                "how": "left"
            }
        }

        mock_logger = Mock()
        result = _resolve_join_assets(mock_mongodb, manifest_dict, mock_logger)

        assert result["manifest"] == manifest_dict
        assert result["primary_type"] == "tabular"
        assert result["secondary_asset"] == spatial_asset.model_dump(mode="json")
        assert result["join_config"]["left_key"] == "parcel_id"
        assert result["join_config"]["right_key"] == "parcel_id"
        assert result["join_config"]["how"] == "left"

        mock_logger.info.assert_called()


class TestExecuteSpatialJoin:
    """Test _execute_spatial_join function."""

    def test_execute_spatial_join_left_join(self):
        """Test LEFT JOIN SQL generation."""
        from services.dagster.etl_pipelines.ops.join_ops import _execute_spatial_join

        # Mock PostGIS resource
        mock_postgis = Mock()
        mock_postgis.execute_sql.return_value = None
        mock_postgis.get_table_bounds.return_value = Mock(minx=-180, miny=-90, maxx=180, maxy=90)

        tabular_info = {"schema": "run_123", "table": "tabular", "columns": ["id", "name"]}
        spatial_info = {"schema": "run_123", "table": "spatial", "geom_column": "geom"}
        join_config = {"left_key": "id", "right_key": "spatial_id", "how": "left"}
        output_table = "joined"

        result = _execute_spatial_join(
            mock_postgis, tabular_info, spatial_info, join_config, output_table, Mock()
        )

        # Verify SQL was executed
        mock_postgis.execute_sql.assert_called_once()
        sql_call = mock_postgis.execute_sql.call_args[0][0]

        # Verify SQL contains expected elements
        assert "CREATE TABLE" in sql_call
        assert "LEFT JOIN" in sql_call
        assert '"id"' in sql_call
        assert '"spatial_id"' in sql_call
        assert "geom" in sql_call

        # Verify bounds calculation
        mock_postgis.get_table_bounds.assert_called_once_with("run_123", "joined", geom_column="geom")

        # Verify result structure
        assert result["schema"] == "run_123"
        assert result["table"] == "joined"
        assert result["geom_column"] == "geom"
        assert result["bounds"]["minx"] == -180

    def test_execute_spatial_join_inner_join(self):
        """Test INNER JOIN SQL generation."""
        from services.dagster.etl_pipelines.ops.join_ops import _execute_spatial_join

        # Mock PostGIS resource
        mock_postgis = Mock()
        mock_postgis.execute_sql.return_value = None
        mock_postgis.get_table_bounds.return_value = None  # No bounds

        join_config = {"left_key": "id", "how": "inner"}
        tabular_info = {"schema": "run_123", "table": "tabular"}
        spatial_info = {"schema": "run_123", "table": "spatial"}

        result = _execute_spatial_join(
            mock_postgis, tabular_info, spatial_info, join_config, "joined", Mock()
        )

        # Verify SQL contains INNER JOIN
        sql_call = mock_postgis.execute_sql.call_args[0][0]
        assert "INNER JOIN" in sql_call

        # Verify bounds is None when no bounds returned
        assert result["bounds"] is None
