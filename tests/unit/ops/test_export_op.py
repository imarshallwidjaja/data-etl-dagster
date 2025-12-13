# =============================================================================
# Unit Tests: Export Op
# =============================================================================

import hashlib
from unittest.mock import Mock, patch, mock_open, PropertyMock
import pytest
from dagster import build_op_context

from services.dagster.etl_pipelines.ops.export_op import export_to_datalake, _export_to_datalake
from services.dagster.etl_pipelines.resources.gdal_resource import GDALResult
from libs.models import Asset, OutputFormat, CRS


# =============================================================================
# Test Data
# =============================================================================

SAMPLE_TRANSFORM_RESULT = {
    "schema": "proc_abc12345_def6_7890_abcd_ef1234567890",
    "table": "processed",
    "manifest": {
        "batch_id": "batch_001",
        "uploader": "test_user",
        "intent": "ingest_vector",
        "files": [
            {
                "path": "s3://landing-zone/batch_001/data.geojson",
                "type": "vector",
                "format": "GeoJSON",
                "crs": "EPSG:3857"
            }
        ],
        "metadata": {
            "project": "TEST_PROJECT",
            "description": "Test dataset"
        }
    },
    "bounds": {
        "minx": -180.0,
        "miny": -90.0,
        "maxx": 180.0,
        "maxy": 90.0,
    },
    "crs": "EPSG:4326",
    "run_id": "abc12345-def6-7890-abcd-ef1234567890",
}


# =============================================================================
# Test: Core Logic (_export_to_datalake)
# =============================================================================

@patch('services.dagster.etl_pipelines.ops.export_op.tempfile.NamedTemporaryFile')
@patch('services.dagster.etl_pipelines.ops.export_op.Path')
def test_export_to_datalake_success(mock_path, mock_tempfile):
    """Test successful export and registration."""
    # Setup mocks
    mock_temp_file = Mock()
    mock_temp_file.name = "/tmp/test_export.parquet"
    mock_temp_file.close = Mock()
    mock_tempfile.return_value = mock_temp_file
    
    mock_path_instance = Mock()
    mock_path.return_value = mock_path_instance
    mock_path_instance.unlink = Mock()
    
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=True,
        command=["ogr2ogr", "-f", "Parquet", "PG:...", "/tmp/test_export.parquet"],
        stdout="Success",
        stderr="",
        return_code=0,
        output_path="/tmp/test_export.parquet",
    )
    
    mock_postgis = Mock()
    mock_postgis.host = "postgis"
    mock_postgis.database = "spatial_compute"
    mock_postgis.user = "test_user"
    mock_postgis.password = "test_password"
    
    mock_minio = Mock()
    mock_minio.upload_to_lake = Mock()
    
    mock_mongodb = Mock()
    mock_mongodb.get_next_version.return_value = 1
    
    # Mock Asset with id
    mock_asset = Mock()
    mock_asset.id = "507f1f77bcf86cd799439011"
    mock_mongodb.insert_asset.return_value = mock_asset
    
    mock_log = Mock()
    
    # Mock file read for hash calculation
    test_content = b"test parquet content"
    with patch('builtins.open', mock_open(read_data=test_content)):
        # Call core function
        result = _export_to_datalake(
            gdal=mock_gdal,
            postgis=mock_postgis,
            minio=mock_minio,
            mongodb=mock_mongodb,
            transform_result=SAMPLE_TRANSFORM_RESULT,
            run_id="abc12345-def6-7890-abcd-ef1234567890",
            log=mock_log,
        )
    
    # Verify result
    assert result["asset_id"] == "507f1f77bcf86cd799439011"
    assert result["s3_key"].startswith("dataset_")
    assert result["s3_key"].endswith("/v1/data.parquet")
    assert result["dataset_id"].startswith("dataset_")
    assert result["version"] == 1
    assert result["content_hash"].startswith("sha256:")
    assert len(result["content_hash"]) == 71  # "sha256:" + 64 hex chars
    assert result["run_id"] == "abc12345-def6-7890-abcd-ef1234567890"
    
    # Verify ogr2ogr was called
    mock_gdal.ogr2ogr.assert_called_once()
    call_args = mock_gdal.ogr2ogr.call_args
    
    # Verify ogr2ogr arguments
    assert "PG:host=postgis" in call_args.kwargs["input_path"]
    assert "schemas=proc_abc12345_def6_7890_abcd_ef1234567890" in call_args.kwargs["input_path"]
    assert "tables=processed" in call_args.kwargs["input_path"]
    assert call_args.kwargs["output_path"] == "/tmp/test_export.parquet"
    assert call_args.kwargs["output_format"] == "Parquet"
    assert call_args.kwargs["target_crs"] == "EPSG:4326"
    
    # Verify MinIO upload
    mock_minio.upload_to_lake.assert_called_once()
    upload_args = mock_minio.upload_to_lake.call_args
    assert upload_args[0][0] == "/tmp/test_export.parquet"
    assert upload_args[0][1].endswith("/v1/data.parquet")
    
    # Verify MongoDB operations
    mock_mongodb.get_next_version.assert_called_once()
    mock_mongodb.insert_asset.assert_called_once()
    
    # Verify temp file cleanup
    mock_path_instance.unlink.assert_called_once_with(missing_ok=True)


@patch('services.dagster.etl_pipelines.ops.export_op.tempfile.NamedTemporaryFile')
@patch('services.dagster.etl_pipelines.ops.export_op.Path')
def test_export_to_datalake_dataset_id_generation(mock_path, mock_tempfile):
    """Test dataset_id generation (UUID format)."""
    mock_temp_file = Mock()
    mock_temp_file.name = "/tmp/test_export.parquet"
    mock_temp_file.close = Mock()
    mock_tempfile.return_value = mock_temp_file
    
    mock_path_instance = Mock()
    mock_path.return_value = mock_path_instance
    mock_path_instance.unlink = Mock()
    
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=True,
        command=["ogr2ogr", "-f", "Parquet", "PG:...", "/tmp/test_export.parquet"],
        stdout="Success",
        stderr="",
        return_code=0,
        output_path="/tmp/test_export.parquet",
    )
    
    mock_postgis = Mock()
    mock_postgis.host = "postgis"
    mock_postgis.database = "spatial_compute"
    mock_postgis.user = "test_user"
    mock_postgis.password = "test_password"
    
    mock_minio = Mock()
    mock_minio.upload_to_lake = Mock()
    
    mock_mongodb = Mock()
    mock_mongodb.get_next_version.return_value = 1
    mock_asset = Mock()
    mock_asset.id = "507f1f77bcf86cd799439011"
    mock_mongodb.insert_asset.return_value = mock_asset
    
    mock_log = Mock()
    
    test_content = b"test content"
    with patch('builtins.open', mock_open(read_data=test_content)):
        result = _export_to_datalake(
            gdal=mock_gdal,
            postgis=mock_postgis,
            minio=mock_minio,
            mongodb=mock_mongodb,
            transform_result=SAMPLE_TRANSFORM_RESULT,
            run_id="abc12345-def6-7890-abcd-ef1234567890",
            log=mock_log,
        )
    
    # Verify dataset_id format
    assert result["dataset_id"].startswith("dataset_")
    dataset_id_suffix = result["dataset_id"][8:]  # Remove "dataset_" prefix
    assert len(dataset_id_suffix) == 12  # 12 hex characters
    # Verify it's hex
    try:
        int(dataset_id_suffix, 16)
    except ValueError:
        pytest.fail("dataset_id suffix is not valid hex")


@patch('services.dagster.etl_pipelines.ops.export_op.tempfile.NamedTemporaryFile')
@patch('services.dagster.etl_pipelines.ops.export_op.Path')
def test_export_to_datalake_version_numbering(mock_path, mock_tempfile):
    """Test version number retrieval (new vs existing dataset)."""
    mock_temp_file = Mock()
    mock_temp_file.name = "/tmp/test_export.parquet"
    mock_temp_file.close = Mock()
    mock_tempfile.return_value = mock_temp_file
    
    mock_path_instance = Mock()
    mock_path.return_value = mock_path_instance
    mock_path_instance.unlink = Mock()
    
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=True,
        command=["ogr2ogr", "-f", "Parquet", "PG:...", "/tmp/test_export.parquet"],
        stdout="Success",
        stderr="",
        return_code=0,
        output_path="/tmp/test_export.parquet",
    )
    
    mock_postgis = Mock()
    mock_postgis.host = "postgis"
    mock_postgis.database = "spatial_compute"
    mock_postgis.user = "test_user"
    mock_postgis.password = "test_password"
    
    mock_minio = Mock()
    mock_minio.upload_to_lake = Mock()
    
    mock_mongodb = Mock()
    mock_mongodb.get_next_version.return_value = 3  # Existing dataset, version 3
    mock_asset = Mock()
    mock_asset.id = "507f1f77bcf86cd799439011"
    mock_mongodb.insert_asset.return_value = mock_asset
    
    mock_log = Mock()
    
    test_content = b"test content"
    with patch('builtins.open', mock_open(read_data=test_content)):
        result = _export_to_datalake(
            gdal=mock_gdal,
            postgis=mock_postgis,
            minio=mock_minio,
            mongodb=mock_mongodb,
            transform_result=SAMPLE_TRANSFORM_RESULT,
            run_id="abc12345-def6-7890-abcd-ef1234567890",
            log=mock_log,
        )
    
    # Verify version
    assert result["version"] == 3
    assert result["s3_key"].endswith("/v3/data.parquet")


@patch('services.dagster.etl_pipelines.ops.export_op.tempfile.NamedTemporaryFile')
@patch('services.dagster.etl_pipelines.ops.export_op.Path')
def test_export_to_datalake_s3_key_generation(mock_path, mock_tempfile):
    """Test S3 key generation pattern."""
    mock_temp_file = Mock()
    mock_temp_file.name = "/tmp/test_export.parquet"
    mock_temp_file.close = Mock()
    mock_tempfile.return_value = mock_temp_file
    
    mock_path_instance = Mock()
    mock_path.return_value = mock_path_instance
    mock_path_instance.unlink = Mock()
    
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=True,
        command=["ogr2ogr", "-f", "Parquet", "PG:...", "/tmp/test_export.parquet"],
        stdout="Success",
        stderr="",
        return_code=0,
        output_path="/tmp/test_export.parquet",
    )
    
    mock_postgis = Mock()
    mock_postgis.host = "postgis"
    mock_postgis.database = "spatial_compute"
    mock_postgis.user = "test_user"
    mock_postgis.password = "test_password"
    
    mock_minio = Mock()
    mock_minio.upload_to_lake = Mock()
    
    mock_mongodb = Mock()
    mock_mongodb.get_next_version.return_value = 1
    mock_asset = Mock()
    mock_asset.id = "507f1f77bcf86cd799439011"
    mock_mongodb.insert_asset.return_value = mock_asset
    
    mock_log = Mock()
    
    test_content = b"test content"
    with patch('builtins.open', mock_open(read_data=test_content)):
        result = _export_to_datalake(
            gdal=mock_gdal,
            postgis=mock_postgis,
            minio=mock_minio,
            mongodb=mock_mongodb,
            transform_result=SAMPLE_TRANSFORM_RESULT,
            run_id="abc12345-def6-7890-abcd-ef1234567890",
            log=mock_log,
        )
    
    # Verify S3 key pattern: {dataset_id}/v{version}/data.parquet
    s3_key = result["s3_key"]
    assert s3_key.startswith(result["dataset_id"])
    assert f"/v{result['version']}/data.parquet" in s3_key


@patch('services.dagster.etl_pipelines.ops.export_op.tempfile.NamedTemporaryFile')
@patch('services.dagster.etl_pipelines.ops.export_op.Path')
def test_export_to_datalake_sha256_hash_calculation(mock_path, mock_tempfile):
    """Test SHA256 hash calculation."""
    mock_temp_file = Mock()
    mock_temp_file.name = "/tmp/test_export.parquet"
    mock_temp_file.close = Mock()
    mock_tempfile.return_value = mock_temp_file
    
    mock_path_instance = Mock()
    mock_path.return_value = mock_path_instance
    mock_path_instance.unlink = Mock()
    
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=True,
        command=["ogr2ogr", "-f", "Parquet", "PG:...", "/tmp/test_export.parquet"],
        stdout="Success",
        stderr="",
        return_code=0,
        output_path="/tmp/test_export.parquet",
    )
    
    mock_postgis = Mock()
    mock_postgis.host = "postgis"
    mock_postgis.database = "spatial_compute"
    mock_postgis.user = "test_user"
    mock_postgis.password = "test_password"
    
    mock_minio = Mock()
    mock_minio.upload_to_lake = Mock()
    
    mock_mongodb = Mock()
    mock_mongodb.get_next_version.return_value = 1
    mock_asset = Mock()
    mock_asset.id = "507f1f77bcf86cd799439011"
    mock_mongodb.insert_asset.return_value = mock_asset
    
    mock_log = Mock()
    
    # Use real hashlib to calculate expected hash
    test_content = b"test parquet file content for hashing"
    expected_hash = hashlib.sha256(test_content).hexdigest()
    expected_content_hash = f"sha256:{expected_hash}"
    
    with patch('builtins.open', mock_open(read_data=test_content)):
        result = _export_to_datalake(
            gdal=mock_gdal,
            postgis=mock_postgis,
            minio=mock_minio,
            mongodb=mock_mongodb,
            transform_result=SAMPLE_TRANSFORM_RESULT,
            run_id="abc12345-def6-7890-abcd-ef1234567890",
            log=mock_log,
        )
    
    # Verify hash format and value
    assert result["content_hash"] == expected_content_hash
    assert result["content_hash"].startswith("sha256:")
    assert len(result["content_hash"]) == 71  # "sha256:" + 64 hex chars


@patch('services.dagster.etl_pipelines.ops.export_op.tempfile.NamedTemporaryFile')
@patch('services.dagster.etl_pipelines.ops.export_op.Path')
def test_export_to_datalake_asset_model_creation(mock_path, mock_tempfile):
    """Test Asset model creation with correct fields."""
    mock_temp_file = Mock()
    mock_temp_file.name = "/tmp/test_export.parquet"
    mock_temp_file.close = Mock()
    mock_tempfile.return_value = mock_temp_file
    
    mock_path_instance = Mock()
    mock_path.return_value = mock_path_instance
    mock_path_instance.unlink = Mock()
    
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=True,
        command=["ogr2ogr", "-f", "Parquet", "PG:...", "/tmp/test_export.parquet"],
        stdout="Success",
        stderr="",
        return_code=0,
        output_path="/tmp/test_export.parquet",
    )
    
    mock_postgis = Mock()
    mock_postgis.host = "postgis"
    mock_postgis.database = "spatial_compute"
    mock_postgis.user = "test_user"
    mock_postgis.password = "test_password"
    
    mock_minio = Mock()
    mock_minio.upload_to_lake = Mock()
    
    mock_mongodb = Mock()
    mock_mongodb.get_next_version.return_value = 1
    mock_asset = Mock()
    mock_asset.id = "507f1f77bcf86cd799439011"
    mock_mongodb.insert_asset.return_value = mock_asset
    
    mock_log = Mock()
    
    test_content = b"test content"
    with patch('builtins.open', mock_open(read_data=test_content)):
        _export_to_datalake(
            gdal=mock_gdal,
            postgis=mock_postgis,
            minio=mock_minio,
            mongodb=mock_mongodb,
            transform_result=SAMPLE_TRANSFORM_RESULT,
            run_id="abc12345-def6-7890-abcd-ef1234567890",
            log=mock_log,
        )
    
    # Verify insert_asset was called with Asset model
    mock_mongodb.insert_asset.assert_called_once()
    asset_arg = mock_mongodb.insert_asset.call_args[0][0]
    
    # Verify Asset model fields
    assert isinstance(asset_arg, Asset)
    assert asset_arg.s3_key.endswith("/v1/data.parquet")
    assert asset_arg.dataset_id.startswith("dataset_")
    assert asset_arg.version == 1
    assert asset_arg.content_hash.startswith("sha256:")
    assert asset_arg.dagster_run_id == "abc12345-def6-7890-abcd-ef1234567890"
    assert asset_arg.format == OutputFormat.GEOPARQUET
    assert asset_arg.crs == CRS("EPSG:4326")
    assert asset_arg.bounds.minx == -180.0
    assert asset_arg.bounds.miny == -90.0
    assert asset_arg.bounds.maxx == 180.0
    assert asset_arg.bounds.maxy == 90.0
    assert asset_arg.metadata.title == "TEST_PROJECT"
    assert asset_arg.metadata.description == "Test dataset"
    assert asset_arg.created_at is not None


@patch('services.dagster.etl_pipelines.ops.export_op.tempfile.NamedTemporaryFile')
@patch('services.dagster.etl_pipelines.ops.export_op.Path')
def test_export_to_datalake_ogr2ogr_failure(mock_path, mock_tempfile):
    """Test error handling when GDAL export fails."""
    mock_temp_file = Mock()
    mock_temp_file.name = "/tmp/test_export.parquet"
    mock_temp_file.close = Mock()
    mock_tempfile.return_value = mock_temp_file
    
    mock_path_instance = Mock()
    mock_path.return_value = mock_path_instance
    mock_path_instance.unlink = Mock()
    
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=False,
        command=["ogr2ogr", "-f", "Parquet", "PG:...", "/tmp/test_export.parquet"],
        stdout="",
        stderr="ERROR: Export failed",
        return_code=1,
        output_path="/tmp/test_export.parquet",
    )
    
    mock_postgis = Mock()
    mock_postgis.host = "postgis"
    mock_postgis.database = "spatial_compute"
    mock_postgis.user = "test_user"
    mock_postgis.password = "test_password"
    
    mock_minio = Mock()
    mock_mongodb = Mock()
    mock_log = Mock()
    
    # Call should raise RuntimeError
    with pytest.raises(RuntimeError) as exc_info:
        _export_to_datalake(
            gdal=mock_gdal,
            postgis=mock_postgis,
            minio=mock_minio,
            mongodb=mock_mongodb,
            transform_result=SAMPLE_TRANSFORM_RESULT,
            run_id="abc12345-def6-7890-abcd-ef1234567890",
            log=mock_log,
        )
    
    assert "ogr2ogr export failed" in str(exc_info.value)
    assert "ERROR: Export failed" in str(exc_info.value)
    
    # Verify temp file cleanup even on error
    mock_path_instance.unlink.assert_called_once_with(missing_ok=True)


@patch('services.dagster.etl_pipelines.ops.export_op.tempfile.NamedTemporaryFile')
@patch('services.dagster.etl_pipelines.ops.export_op.Path')
def test_export_to_datalake_temp_file_cleanup_on_error(mock_path, mock_tempfile):
    """Test temp file cleanup in error cases."""
    mock_temp_file = Mock()
    mock_temp_file.name = "/tmp/test_export.parquet"
    mock_temp_file.close = Mock()
    mock_tempfile.return_value = mock_temp_file
    
    mock_path_instance = Mock()
    mock_path.return_value = mock_path_instance
    mock_path_instance.unlink = Mock()
    
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=True,
        command=["ogr2ogr", "-f", "Parquet", "PG:...", "/tmp/test_export.parquet"],
        stdout="Success",
        stderr="",
        return_code=0,
        output_path="/tmp/test_export.parquet",
    )
    
    mock_postgis = Mock()
    mock_postgis.host = "postgis"
    mock_postgis.database = "spatial_compute"
    mock_postgis.user = "test_user"
    mock_postgis.password = "test_password"
    
    mock_minio = Mock()
    mock_minio.upload_to_lake.side_effect = Exception("Upload failed")
    
    mock_mongodb = Mock()
    mock_mongodb.get_next_version.return_value = 1
    
    mock_log = Mock()
    
    test_content = b"test content"
    with patch('builtins.open', mock_open(read_data=test_content)):
        # Call should raise exception
        with pytest.raises(Exception) as exc_info:
            _export_to_datalake(
                gdal=mock_gdal,
                postgis=mock_postgis,
                minio=mock_minio,
                mongodb=mock_mongodb,
                transform_result=SAMPLE_TRANSFORM_RESULT,
                run_id="abc12345-def6-7890-abcd-ef1234567890",
                log=mock_log,
            )
    
    assert "Upload failed" in str(exc_info.value)
    
    # Verify temp file cleanup even on error
    mock_path_instance.unlink.assert_called_once_with(missing_ok=True)


# =============================================================================
# Test: Dagster Op (export_to_datalake)
# =============================================================================

@patch('services.dagster.etl_pipelines.ops.export_op.tempfile.NamedTemporaryFile')
@patch('services.dagster.etl_pipelines.ops.export_op.Path')
def test_export_to_datalake_op(mock_path, mock_tempfile):
    """Test the Dagster op wrapper."""
    mock_temp_file = Mock()
    mock_temp_file.name = "/tmp/test_export.parquet"
    mock_temp_file.close = Mock()
    mock_tempfile.return_value = mock_temp_file
    
    mock_path_instance = Mock()
    mock_path.return_value = mock_path_instance
    mock_path_instance.unlink = Mock()
    
    mock_gdal = Mock()
    mock_gdal.ogr2ogr.return_value = GDALResult(
        success=True,
        command=["ogr2ogr", "-f", "Parquet", "PG:...", "/tmp/test_export.parquet"],
        stdout="Success",
        stderr="",
        return_code=0,
        output_path="/tmp/test_export.parquet",
    )
    
    mock_postgis = Mock()
    mock_postgis.host = "postgis"
    mock_postgis.database = "spatial_compute"
    mock_postgis.user = "test_user"
    mock_postgis.password = "test_password"
    
    mock_minio = Mock()
    mock_minio.upload_to_lake = Mock()
    
    mock_mongodb = Mock()
    mock_mongodb.get_next_version.return_value = 1
    mock_asset = Mock()
    mock_asset.id = "507f1f77bcf86cd799439011"
    mock_mongodb.insert_asset.return_value = mock_asset
    
    # Create mock context
    context = build_op_context(
        resources={
            "gdal": mock_gdal,
            "postgis": mock_postgis,
            "minio": mock_minio,
            "mongodb": mock_mongodb,
        },
    )
    # Mock run_id property
    type(context).run_id = PropertyMock(return_value="abc12345-def6-7890-abcd-ef1234567890")
    
    test_content = b"test content"
    with patch('builtins.open', mock_open(read_data=test_content)):
        # Call op
        result = export_to_datalake(context, transform_result=SAMPLE_TRANSFORM_RESULT)
    
    # Verify result
    assert result["asset_id"] == "507f1f77bcf86cd799439011"
    assert result["dataset_id"].startswith("dataset_")
    assert result["version"] == 1

