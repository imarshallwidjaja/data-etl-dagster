# =============================================================================
# Unit Tests: Load Op
# =============================================================================

import pytest
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from dagster import build_op_context

from services.dagster.etl_pipelines.ops.load_op import (
    load_to_postgis,
    _load_files_to_postgis,
)
from services.dagster.etl_pipelines.resources.gdal_resource import GDALResult


# =============================================================================
# Test Data
# =============================================================================

SAMPLE_MANIFEST = {
    "batch_id": "batch_001",
    "uploader": "test_user",
    "intent": "ingest_vector",
    "files": [
        {
            "path": "s3://landing-zone/batch_001/data.geojson",
            "type": "vector",
            "format": "GeoJSON",
        }
    ],
    "metadata": {
        "title": "Test Dataset",
        "description": "Test dataset",
        "keywords": ["test"],
        "source": "Unit Test",
        "license": "MIT",
        "attribution": "Test Team",
        "project": "TEST_PROJECT",
        "tags": {},
        "join_config": None,
    },
}

MULTI_FILE_MANIFEST = {
    "batch_id": "batch_002",
    "uploader": "test_user",
    "intent": "ingest_vector",
    "files": [
        {
            "path": "s3://landing-zone/batch_002/file1.geojson",
            "type": "vector",
            "format": "GeoJSON",
        },
        {
            "path": "s3://landing-zone/batch_002/file2.geojson",
            "type": "vector",
            "format": "GeoJSON",
        },
    ],
    "metadata": {
        "title": "Multi-file Test Dataset",
        "description": "Multi-file test dataset",
        "keywords": ["test", "multi-file"],
        "source": "Unit Test",
        "license": "MIT",
        "attribution": "Test Team",
        "project": "TEST_PROJECT",
        "tags": {},
        "join_config": None,
    },
}


# =============================================================================
# Test: Core Logic (_load_files_to_postgis)
# =============================================================================


def test_load_files_to_postgis_success():
    """Test successful load of single file."""
    # Create mock resources
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=True,
        command=["ogr2ogr", "-f", "PostgreSQL", "PG:...", "/vsis3/..."],
        stdout="Success",
        stderr="",
        return_code=0,
        output_path=None,
    )

    mock_postgis = Mock()
    mock_postgis.host = "postgis"
    mock_postgis.database = "spatial_compute"
    mock_postgis.user = "test_user"
    mock_postgis.password = "test_password"

    # Mock execute_sql for schema creation
    mock_postgis.execute_sql = Mock()
    mock_postgis.get_engine = Mock()
    mock_engine = Mock()
    mock_postgis.get_engine.return_value = mock_engine
    mock_conn = Mock()
    mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = Mock(return_value=None)

    mock_log = Mock()

    # Call core function
    result = _load_files_to_postgis(
        gdal=mock_gdal,
        postgis=mock_postgis,
        manifest=SAMPLE_MANIFEST,
        run_id="abc12345-def6-7890-abcd-ef1234567890",
        log=mock_log,
    )

    # Verify result
    assert result["schema"] == "proc_abc12345_def6_7890_abcd_ef1234567890"
    assert result["manifest"] == SAMPLE_MANIFEST
    assert result["tables"] == ["raw_data"]
    assert result["run_id"] == "abc12345-def6-7890-abcd-ef1234567890"
    assert result["geom_column"] == "geom"

    # Verify ogr2ogr was called
    mock_gdal.ogr2ogr.assert_called_once()
    call_args = mock_gdal.ogr2ogr.call_args

    # Verify ogr2ogr arguments
    assert (
        call_args.kwargs["input_path"] == "/vsis3/landing-zone/batch_001/data.geojson"
    )
    assert "PG:host=postgis" in call_args.kwargs["output_path"]
    assert call_args.kwargs["output_format"] == "PostgreSQL"
    assert (
        call_args.kwargs["layer_name"]
        == "proc_abc12345_def6_7890_abcd_ef1234567890.raw_data"
    )
    assert call_args.kwargs["target_crs"] is None
    assert call_args.kwargs["options"] == {
        "-overwrite": "",
        "-lco": "GEOMETRY_NAME=geom",
    }


def test_load_files_to_postgis_multiple_files():
    """Test successful load of multiple files (all into same table)."""
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=True,
        command=["ogr2ogr", "-f", "PostgreSQL", "PG:...", "/vsis3/..."],
        stdout="Success",
        stderr="",
        return_code=0,
        output_path=None,
    )

    # Mock ogrinfo for schema compatibility check (JSON output)
    mock_gdal.ogrinfo.return_value = GDALResult(
        success=True,
        command=["ogrinfo", "-json", "/vsis3/..."],
        stdout='{"layers": [{"name": "data", "fields": [{"name": "id", "type": "Integer"}, {"name": "name", "type": "String"}], "geometryFields": [{"type": "Point"}]}]}',
        stderr="",
        return_code=0,
        output_path=None,
    )

    mock_postgis = Mock()
    mock_postgis.host = "postgis"
    mock_postgis.database = "spatial_compute"
    mock_postgis.user = "test_user"
    mock_postgis.password = "test_password"
    mock_postgis.execute_sql = Mock()
    mock_postgis.get_engine = Mock()
    mock_engine = Mock()
    mock_postgis.get_engine.return_value = mock_engine
    mock_conn = Mock()
    mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = Mock(return_value=None)

    mock_log = Mock()

    # Call core function
    result = _load_files_to_postgis(
        gdal=mock_gdal,
        postgis=mock_postgis,
        manifest=MULTI_FILE_MANIFEST,
        run_id="abc12345-def6-7890-abcd-ef1234567890",
        log=mock_log,
    )

    # Verify ogrinfo was called for schema validation (twice for 2 files)
    assert mock_gdal.ogrinfo.call_count == 2
    # Verify ogrinfo was called with as_json=True
    for call in mock_gdal.ogrinfo.call_args_list:
        assert call.kwargs.get("as_json") is True

    # Verify ogr2ogr was called twice (once per file)
    assert mock_gdal.ogr2ogr.call_count == 2

    # Verify both calls use the same layer name
    calls = mock_gdal.ogr2ogr.call_args_list
    layer_name = "proc_abc12345_def6_7890_abcd_ef1234567890.raw_data"
    assert calls[0].kwargs["layer_name"] == layer_name
    assert calls[1].kwargs["layer_name"] == layer_name

    # Verify first call uses -overwrite and geometry column option
    assert calls[0].kwargs["options"] == {
        "-overwrite": "",
        "-lco": "GEOMETRY_NAME=geom",
    }

    # Verify second call uses -append, -update and geometry column option
    assert calls[1].kwargs["options"] == {
        "-append": "",
        "-update": "",
        "-lco": "GEOMETRY_NAME=geom",
    }


def test_load_files_to_postgis_s3_path_conversion():
    """Test S3 path conversion (s3:// -> /vsis3/)."""
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=True,
        command=["ogr2ogr", "-f", "PostgreSQL", "PG:...", "/vsis3/..."],
        stdout="Success",
        stderr="",
        return_code=0,
        output_path=None,
    )

    mock_postgis = Mock()
    mock_postgis.host = "postgis"
    mock_postgis.database = "spatial_compute"
    mock_postgis.user = "test_user"
    mock_postgis.password = "test_password"
    mock_postgis.execute_sql = Mock()
    mock_postgis.get_engine = Mock()
    mock_engine = Mock()
    mock_postgis.get_engine.return_value = mock_engine
    mock_conn = Mock()
    mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = Mock(return_value=None)

    mock_log = Mock()

    # Call with s3:// path
    _load_files_to_postgis(
        gdal=mock_gdal,
        postgis=mock_postgis,
        manifest=SAMPLE_MANIFEST,
        run_id="abc12345-def6-7890-abcd-ef1234567890",
        log=mock_log,
        geom_column_name="geom",
    )

    # Verify path conversion
    call_args = mock_gdal.ogr2ogr.call_args
    assert (
        call_args.kwargs["input_path"] == "/vsis3/landing-zone/batch_001/data.geojson"
    )
    assert not call_args.kwargs["input_path"].startswith("s3://")


def test_load_files_to_postgis_ogr2ogr_failure():
    """Test error handling when ogr2ogr fails."""
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=False,
        command=["ogr2ogr", "-f", "PostgreSQL", "PG:...", "/vsis3/..."],
        stdout="",
        stderr="ERROR: Failed to load data",
        return_code=1,
        output_path=None,
    )

    mock_postgis = Mock()
    mock_postgis.host = "postgis"
    mock_postgis.database = "spatial_compute"
    mock_postgis.user = "test_user"
    mock_postgis.password = "test_password"
    mock_postgis.execute_sql = Mock()
    mock_postgis.get_engine = Mock()
    mock_engine = Mock()
    mock_postgis.get_engine.return_value = mock_engine
    mock_conn = Mock()
    mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = Mock(return_value=None)

    mock_log = Mock()

    # Call should raise RuntimeError
    with pytest.raises(RuntimeError) as exc_info:
        _load_files_to_postgis(
            gdal=mock_gdal,
            postgis=mock_postgis,
            manifest=SAMPLE_MANIFEST,
            run_id="abc12345-def6-7890-abcd-ef1234567890",
            log=mock_log,
            geom_column_name="geom",
        )

    assert "ogr2ogr failed" in str(exc_info.value)
    assert "data.geojson" in str(exc_info.value)
    assert "ERROR: Failed to load data" in str(exc_info.value)


def test_load_files_to_postgis_postgres_connection_string():
    """Test PostgreSQL connection string building."""
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=True,
        command=["ogr2ogr", "-f", "PostgreSQL", "PG:...", "/vsis3/..."],
        stdout="Success",
        stderr="",
        return_code=0,
        output_path=None,
    )

    mock_postgis = Mock()
    mock_postgis.host = "remote-postgis"
    mock_postgis.database = "my_database"
    mock_postgis.user = "my_user"
    mock_postgis.password = "my_password"
    mock_postgis.execute_sql = Mock()
    mock_postgis.get_engine = Mock()
    mock_engine = Mock()
    mock_postgis.get_engine.return_value = mock_engine
    mock_conn = Mock()
    mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = Mock(return_value=None)

    mock_log = Mock()

    # Call core function
    _load_files_to_postgis(
        gdal=mock_gdal,
        postgis=mock_postgis,
        manifest=SAMPLE_MANIFEST,
        run_id="abc12345-def6-7890-abcd-ef1234567890",
        log=mock_log,
        geom_column_name="geom",
    )

    # Verify connection string
    call_args = mock_gdal.ogr2ogr.call_args
    conn_str = call_args.kwargs["output_path"]
    assert "host=remote-postgis" in conn_str
    assert "dbname=my_database" in conn_str
    assert "user=my_user" in conn_str
    assert "password=my_password" in conn_str


# =============================================================================
# Test: Dagster Op (load_to_postgis)
# =============================================================================


def test_load_to_postgis_op():
    """Test the Dagster op wrapper."""
    # Create mock resources
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=True,
        command=["ogr2ogr", "-f", "PostgreSQL", "PG:...", "/vsis3/..."],
        stdout="Success",
        stderr="",
        return_code=0,
        output_path=None,
    )

    mock_postgis = Mock()
    mock_postgis.host = "postgis"
    mock_postgis.database = "spatial_compute"
    mock_postgis.user = "test_user"
    mock_postgis.password = "test_password"
    mock_postgis.execute_sql = Mock()
    mock_postgis.get_engine = Mock()
    mock_engine = Mock()
    mock_postgis.get_engine.return_value = mock_engine
    mock_conn = Mock()
    mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_conn)
    mock_engine.connect.return_value.__exit__ = Mock(return_value=None)

    mock_minio = Mock()

    # Create mock context
    context = build_op_context(
        resources={
            "gdal": mock_gdal,
            "postgis": mock_postgis,
            "minio": mock_minio,
        },
    )
    # Mock run_id property
    type(context).run_id = PropertyMock(
        return_value="abc12345-def6-7890-abcd-ef1234567890"
    )

    # Call op
    result = load_to_postgis(context, manifest=SAMPLE_MANIFEST)

    # Verify result
    assert result["schema"] == "proc_abc12345_def6_7890_abcd_ef1234567890"
    assert result["manifest"] == SAMPLE_MANIFEST
    assert result["tables"] == ["raw_data"]
    assert result["run_id"] == "abc12345-def6-7890-abcd-ef1234567890"
    assert result["geom_column"] == "geom"
