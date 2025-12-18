"""
Unit tests: Join Ops (Phase 4).

Focused tests for join helper functions in `services.dagster.etl_pipelines.ops.join_ops`.
These tests must not require Docker.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from libs.models import (
    Asset,
    AssetKind,
    AssetMetadata,
    Bounds,
    CRS,
    Manifest,
    OutputFormat,
)
from services.dagster.etl_pipelines.ops.join_ops import (
    _choose_dataset_id,
    _execute_spatial_join,
    _require_identifier,
    _resolve_join_assets,
)


SAMPLE_JOIN_MANIFEST: dict = {
    "batch_id": "batch_join_001",
    "uploader": "test_user",
    "intent": "join_datasets",
    "files": [],
    "metadata": {
        "project": "TEST_PROJECT",
        "description": "Test join",
        "tags": {"dataset_id": "joined_dataset_001"},
        "join_config": {
            "spatial_asset_id": "507f1f77bcf86cd799439011",
            "tabular_asset_id": "507f1f77bcf86cd799439012",
            "left_key": "parcel_id",
            "right_key": "parcel_id",
            "how": "left",
        },
    },
}


def make_spatial_asset() -> Asset:
    return Asset(
        s3_key="data-lake/spatial_001/v1/data.parquet",
        dataset_id="spatial_001",
        version=1,
        content_hash="sha256:" + "a" * 64,
        dagster_run_id="run_123",
        kind=AssetKind.SPATIAL,
        format=OutputFormat.GEOPARQUET,
        crs=CRS("EPSG:4326"),
        bounds=Bounds(minx=0.0, miny=0.0, maxx=1.0, maxy=1.0),
        metadata=AssetMetadata(title="Test Spatial Dataset"),
        created_at=datetime.now(timezone.utc),
        updated_at=None,
    )


def make_tabular_asset() -> Asset:
    return Asset(
        s3_key="data-lake/tabular_001/v1/data.parquet",
        dataset_id="tabular_001",
        version=1,
        content_hash="sha256:" + "b" * 64,
        dagster_run_id="run_456",
        kind=AssetKind.TABULAR,
        format=OutputFormat.PARQUET,
        crs=None,
        bounds=None,
        metadata=AssetMetadata(title="Test Tabular Dataset"),
        created_at=datetime.now(timezone.utc),
        updated_at=None,
    )


class TestRequireIdentifier:
    def test_valid_identifier(self):
        assert _require_identifier("parcel_id", label="column") == "parcel_id"
        assert _require_identifier("_private", label="column") == "_private"
        assert _require_identifier("Column123", label="column") == "Column123"

    def test_invalid_identifier_spaces(self):
        with pytest.raises(ValueError, match=r"Invalid column"):
            _require_identifier("parcel id", label="column")

    def test_invalid_identifier_special_chars(self):
        with pytest.raises(ValueError, match=r"Invalid column"):
            _require_identifier("parcel-id", label="column")
        with pytest.raises(ValueError, match=r"Invalid column"):
            _require_identifier("parcel.id", label="column")

    def test_invalid_identifier_starts_with_number(self):
        with pytest.raises(ValueError, match=r"Invalid column"):
            _require_identifier("123column", label="column")


class TestResolveJoinAssets:
    def test_resolve_success(self):
        mock_mongodb = Mock()
        mock_log = Mock()
        spatial_asset = make_spatial_asset()
        tabular_asset = make_tabular_asset()
        mock_mongodb.get_latest_asset.side_effect = [spatial_asset, tabular_asset]

        result = _resolve_join_assets(
            mongodb=mock_mongodb,
            manifest=SAMPLE_JOIN_MANIFEST,
            log=mock_log,
        )

        assert result["spatial_asset"].dataset_id == "spatial_001"
        assert result["tabular_asset"].dataset_id == "tabular_001"
        assert result["join_config"].left_key == "parcel_id"
        assert mock_mongodb.get_latest_asset.call_args_list == [
            (("507f1f77bcf86cd799439011",),),
            (("507f1f77bcf86cd799439012",),),
        ]

    def test_missing_join_config(self):
        mock_mongodb = Mock()
        mock_log = Mock()

        manifest_no_config = {**SAMPLE_JOIN_MANIFEST, "metadata": {"project": "TEST"}}

        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            _resolve_join_assets(
                mongodb=mock_mongodb,
                manifest=manifest_no_config,
                log=mock_log,
            )

    def test_missing_spatial_or_tabular_asset_id(self):
        from pydantic import ValidationError

        mock_mongodb = Mock()
        mock_log = Mock()

        manifest_no_ids = {
            **SAMPLE_JOIN_MANIFEST,
            "metadata": {
                **SAMPLE_JOIN_MANIFEST["metadata"],
                "join_config": {"left_key": "parcel_id", "how": "left"},
            },
        }

        with pytest.raises(ValidationError):
            _resolve_join_assets(
                mongodb=mock_mongodb,
                manifest=manifest_no_ids,
                log=mock_log,
            )

    def test_asset_not_found(self):
        mock_mongodb = Mock()
        mock_mongodb.get_latest_asset.return_value = None
        mock_log = Mock()

        with pytest.raises(ValueError, match="Spatial asset not found"):
            _resolve_join_assets(
                mongodb=mock_mongodb,
                manifest=SAMPLE_JOIN_MANIFEST,
                log=mock_log,
            )

    def test_wrong_asset_kind(self):
        mock_mongodb = Mock()
        mock_mongodb.get_latest_asset.side_effect = [
            make_tabular_asset(),
            make_tabular_asset(),
        ]
        mock_log = Mock()

        with pytest.raises(
            ValueError, match="spatial_asset_id must reference a spatial asset"
        ):
            _resolve_join_assets(
                mongodb=mock_mongodb,
                manifest=SAMPLE_JOIN_MANIFEST,
                log=mock_log,
            )


class TestExecuteSpatialJoin:
    def test_left_join_sql(self):
        mock_postgis = Mock()
        mock_postgis.get_table_bounds.return_value = Bounds(
            minx=0.0, miny=0.0, maxx=1.0, maxy=1.0
        )
        mock_log = Mock()

        result = _execute_spatial_join(
            postgis=mock_postgis,
            schema="proc_abc123",
            tabular_table="tabular_parent",
            spatial_table="spatial_parent",
            left_key="parcel_id",
            right_key="parcel_id",
            how="left",
            output_table="joined",
            log=mock_log,
        )

        assert result["schema"] == "proc_abc123"
        assert result["table"] == "joined"
        assert result["bounds"] is not None

        mock_postgis.execute_sql.assert_called_once()
        sql_call = mock_postgis.execute_sql.call_args[0][0]
        assert "LEFT JOIN" in sql_call
        assert 't."parcel_id"::TEXT = s."parcel_id"::TEXT' in sql_call

    def test_inner_join_sql(self):
        mock_postgis = Mock()
        mock_postgis.get_table_bounds.return_value = None
        mock_log = Mock()

        result = _execute_spatial_join(
            postgis=mock_postgis,
            schema="proc_abc123",
            tabular_table="tabular_parent",
            spatial_table="spatial_parent",
            left_key="id",
            right_key="id",
            how="inner",
            output_table="joined",
            log=mock_log,
        )

        sql_call = mock_postgis.execute_sql.call_args[0][0]
        assert "INNER JOIN" in sql_call
        assert result["bounds"] is None

    def test_right_join_sql(self):
        mock_postgis = Mock()
        mock_postgis.get_table_bounds.return_value = None
        mock_log = Mock()

        _execute_spatial_join(
            postgis=mock_postgis,
            schema="test_schema",
            tabular_table="tabular_parent",
            spatial_table="spatial_parent",
            left_key="id",
            right_key="id",
            how="right",
            output_table="joined",
            log=mock_log,
        )

        sql_call = mock_postgis.execute_sql.call_args[0][0]
        assert "RIGHT JOIN" in sql_call

    def test_outer_join_sql(self):
        mock_postgis = Mock()
        mock_postgis.get_table_bounds.return_value = None
        mock_log = Mock()

        _execute_spatial_join(
            postgis=mock_postgis,
            schema="test_schema",
            tabular_table="tabular_parent",
            spatial_table="spatial_parent",
            left_key="id",
            right_key="id",
            how="outer",
            output_table="joined",
            log=mock_log,
        )

        sql_call = mock_postgis.execute_sql.call_args[0][0]
        assert "FULL OUTER JOIN" in sql_call


class TestChooseDatasetId:
    def test_uses_tag_when_present(self):
        m = Manifest(**SAMPLE_JOIN_MANIFEST)
        m.metadata.tags["dataset_id"] = "my_custom_id"
        assert _choose_dataset_id(m) == "my_custom_id"

    def test_generates_uuid_fallback(self):
        m = Manifest(**SAMPLE_JOIN_MANIFEST)
        m.metadata.tags.pop("dataset_id", None)
        result = _choose_dataset_id(m)
        assert result.startswith("dataset_")
        assert len(result) == len("dataset_") + 12

    def test_strips_whitespace(self):
        m = Manifest(**SAMPLE_JOIN_MANIFEST)
        m.metadata.tags["dataset_id"] = "  my_id  "
        assert _choose_dataset_id(m) == "my_id"
