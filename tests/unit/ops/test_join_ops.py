"""
Unit tests: Join Ops (Phase 4).

Focused tests for join helper functions in `services.dagster.etl_pipelines.ops.join_ops`.
These tests must not require Docker.
"""

from __future__ import annotations

import builtins
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

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
        "title": "Test Join Dataset",
        "description": "Test join",
        "keywords": ["test", "join"],
        "source": "Unit Test",
        "license": "MIT",
        "attribution": "Test Team",
        "project": "TEST_PROJECT",
        "tags": {"dataset_id": "joined_dataset_001"},
        "join_config": {
            "spatial_dataset_id": "spatial_001",
            "tabular_dataset_id": "tabular_001",
            "left_key": "parcel_id",
            "right_key": "parcel_id",
            "how": "left",
        },
    },
}


def _require_duckdb():
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        pytest.fail(
            "DuckDB dependency is required for join helper tests.",
            pytrace=False,
        )
    return duckdb


def _require_duckdb_join_helper():
    import services.dagster.etl_pipelines.ops.join_ops as join_ops

    helper = getattr(join_ops, "_execute_duckdb_join", None)
    if helper is None:
        pytest.fail("Missing join helper: _execute_duckdb_join", pytrace=False)
    return helper


def _block_pandas_import(monkeypatch) -> None:
    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name == "pandas" or name.startswith("pandas."):
            raise AssertionError("pandas import not allowed in join helper")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)


def _write_tabular_parquet(path, rows, *, key_name: str = "parcel_id") -> None:
    table = pa.table(
        {
            key_name: [row[0] for row in rows],
            "value": [row[1] for row in rows],
        }
    )
    pq.write_table(table, path)


def _write_spatial_geoparquet(path, rows, *, key_name: str = "parcel_id") -> None:
    duckdb = _require_duckdb()
    con = duckdb.connect()
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    table = pa.table(
        {
            key_name: [row[0] for row in rows],
            "wkt": [row[1] for row in rows],
        }
    )
    con.register("input_rows", table)
    con.execute(
        f"CREATE TABLE spatial AS "
        f"SELECT {key_name}, ST_GeomFromText(wkt) AS geom FROM input_rows"
    )
    con.execute(f"COPY spatial TO '{path}' (FORMAT PARQUET)")
    con.close()


def make_spatial_asset() -> Asset:
    from libs.models import ColumnInfo

    return Asset(
        s3_key="data-lake/spatial_001/v1/data.parquet",
        dataset_id="spatial_001",
        version=1,
        content_hash="sha256:" + "a" * 64,
        run_id="507f1f77bcf86cd799439011",
        kind=AssetKind.SPATIAL,
        format=OutputFormat.GEOPARQUET,
        crs="EPSG:4326",
        bounds=Bounds(minx=0.0, miny=0.0, maxx=1.0, maxy=1.0),
        metadata=AssetMetadata(
            title="Test Spatial Dataset",
            description="Test spatial dataset for joins",
            keywords=["test", "spatial"],
            source="Unit Test",
            license="MIT",
            attribution="Test Team",
            geometry_type="POLYGON",
            column_schema={
                "geom": ColumnInfo(
                    title="geom", type_name="GEOMETRY", logical_type="geometry"
                ),
                "parcel_id": ColumnInfo(
                    title="parcel_id", type_name="STRING", logical_type="string"
                ),
            },
        ),
        created_at=datetime.now(timezone.utc),
        updated_at=None,
    )


def make_tabular_asset() -> Asset:
    from libs.models import ColumnInfo

    return Asset(
        s3_key="data-lake/tabular_001/v1/data.parquet",
        dataset_id="tabular_001",
        version=1,
        content_hash="sha256:" + "b" * 64,
        run_id="507f1f77bcf86cd799439012",
        kind=AssetKind.TABULAR,
        format=OutputFormat.PARQUET,
        crs=None,
        bounds=None,
        metadata=AssetMetadata(
            title="Test Tabular Dataset",
            description="Test tabular dataset for joins",
            keywords=["test", "tabular"],
            source="Unit Test",
            license="MIT",
            attribution="Test Team",
            column_schema={
                "id": ColumnInfo(title="ID", type_name="STRING", logical_type="string")
            },
        ),
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
        # New: mock get_latest_asset instead of get_asset_by_id
        mock_mongodb.get_latest_asset.side_effect = [spatial_asset, tabular_asset]
        # Mock ObjectID lookup for lineage
        mock_mongodb.get_asset_object_id_for_version.side_effect = [
            "507f1f77bcf86cd799439011",
            "507f1f77bcf86cd799439012",
        ]

        result = _resolve_join_assets(
            mongodb=mock_mongodb,
            manifest=SAMPLE_JOIN_MANIFEST,
            log=mock_log,
        )

        assert result["spatial_asset"].dataset_id == "spatial_001"
        assert result["tabular_asset"].dataset_id == "tabular_001"
        assert result["join_config"].left_key == "parcel_id"
        assert result["spatial_object_id"] == "507f1f77bcf86cd799439011"
        assert result["tabular_object_id"] == "507f1f77bcf86cd799439012"
        # Verify get_latest_asset was called with dataset_ids
        assert mock_mongodb.get_latest_asset.call_args_list == [
            (("spatial_001",),),
            (("tabular_001",),),
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

        with pytest.raises(ValueError, match="Spatial dataset not found"):
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
            ValueError, match="spatial_dataset_id must reference a spatial asset"
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


class TestDuckdbJoinHelper:
    def test_left_join_outputs_metadata(self, tmp_path, monkeypatch):
        helper = _require_duckdb_join_helper()

        tabular_path = tmp_path / "tabular.parquet"
        spatial_path = tmp_path / "spatial.parquet"
        output_path = tmp_path / "joined.parquet"

        _write_tabular_parquet(tabular_path, [("A", 1), ("B", 2)])
        _write_spatial_geoparquet(
            spatial_path,
            [
                ("A", "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"),
                ("C", "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))"),
            ],
        )

        _block_pandas_import(monkeypatch)

        result = helper(
            tabular_path=str(tabular_path),
            spatial_path=str(spatial_path),
            left_key="parcel_id",
            right_key="parcel_id",
            how="left",
            output_path=str(output_path),
            temp_dir=str(tmp_path),
        )

        assert result["row_count"] == 2
        assert result["geometry_type"] == "POLYGON"
        assert result["bounds"]["minx"] == pytest.approx(0.0)
        assert result["bounds"]["miny"] == pytest.approx(0.0)
        assert result["bounds"]["maxx"] == pytest.approx(2.0)
        assert result["bounds"]["maxy"] == pytest.approx(2.0)

        duckdb = _require_duckdb()
        con = duckdb.connect()
        try:
            rows = con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)",
                [str(output_path)],
            ).fetchall()
        finally:
            con.close()

        column_names = [row[0] for row in rows]
        assert column_names == ["parcel_id", "value", "geom"]

    @pytest.mark.parametrize(
        "how, expected_row_count",
        [("inner", 1), ("right", 2), ("outer", 3)],
    )
    def test_join_types_row_counts(self, tmp_path, monkeypatch, how, expected_row_count):
        helper = _require_duckdb_join_helper()

        tabular_path = tmp_path / f"tabular_{how}.parquet"
        spatial_path = tmp_path / f"spatial_{how}.parquet"
        output_path = tmp_path / f"joined_{how}.parquet"

        _write_tabular_parquet(tabular_path, [("A", 1), ("B", 2)])
        _write_spatial_geoparquet(
            spatial_path,
            [
                ("A", "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"),
                ("C", "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))"),
            ],
        )

        _block_pandas_import(monkeypatch)

        result = helper(
            tabular_path=str(tabular_path),
            spatial_path=str(spatial_path),
            left_key="parcel_id",
            right_key="parcel_id",
            how=how,
            output_path=str(output_path),
            temp_dir=str(tmp_path),
        )

        assert result["row_count"] == expected_row_count

    def test_join_key_casts_to_varchar(self, tmp_path, monkeypatch):
        helper = _require_duckdb_join_helper()

        tabular_path = tmp_path / "tabular_cast.parquet"
        spatial_path = tmp_path / "spatial_cast.parquet"
        output_path = tmp_path / "joined_cast.parquet"

        _write_tabular_parquet(tabular_path, [("01", 1)])
        _write_spatial_geoparquet(
            spatial_path,
            [(1, "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")],
        )

        _block_pandas_import(monkeypatch)

        result = helper(
            tabular_path=str(tabular_path),
            spatial_path=str(spatial_path),
            left_key="parcel_id",
            right_key="parcel_id",
            how="inner",
            output_path=str(output_path),
            temp_dir=str(tmp_path),
        )

        assert result["row_count"] == 0
