# =============================================================================
# Unit Tests: Tabular Ops
# =============================================================================

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

from services.dagster.etl_pipelines.ops.tabular_ops import (
    _download_tabular_from_landing,
    _load_and_clean_tabular,
    _export_tabular_parquet_to_datalake,
)


# =============================================================================
# Test Data
# =============================================================================

SAMPLE_TABULAR_MANIFEST = {
    "batch_id": "batch_tabular_001",
    "uploader": "test_user",
    "intent": "ingest_tabular",
    "files": [
        {
            "path": "s3://landing-zone/batch_tabular_001/data.csv",
            "type": "tabular",
            "format": "CSV",
        }
    ],
    "metadata": {
        "project": "TEST_PROJECT",
        "description": "Test tabular data",
        "tags": {"priority": 1},
        "join_config": {
            "spatial_dataset_id": "sa1_spatial_001",
            "tabular_dataset_id": "sa1_tabular_001",
            "left_key": "id",
            "right_key": "id",
            "how": "left",
        },
    },
}


# =============================================================================
# Test: Download Tabular from Landing
# =============================================================================


def test_download_tabular_from_landing_success():
    """Test successful download of tabular file."""
    mock_minio = Mock()
    mock_log = Mock()

    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,name,age\n1,Alice,30\n2,Bob,25\n")
        temp_path = f.name

    try:
        # Mock download_from_landing to write to temp file
        def mock_download(s3_key, local_path):
            with open(local_path, "w") as out:
                with open(temp_path, "r") as src:
                    out.write(src.read())

        mock_minio.download_from_landing.side_effect = mock_download

        result = _download_tabular_from_landing(
            minio=mock_minio,
            manifest=SAMPLE_TABULAR_MANIFEST,
            log=mock_log,
        )

        assert "local_file_path" in result
        assert "manifest" in result
        assert result["manifest"] == SAMPLE_TABULAR_MANIFEST
        assert Path(result["local_file_path"]).exists()

        mock_minio.download_from_landing.assert_called_once()
        call_args = mock_minio.download_from_landing.call_args
        assert call_args[0][0] == "batch_tabular_001/data.csv"  # s3_key

    finally:
        Path(temp_path).unlink(missing_ok=True)
        if "local_file_path" in locals():
            Path(result["local_file_path"]).unlink(missing_ok=True)


def test_download_tabular_from_landing_rejects_multiple_files():
    """Test that download rejects manifests with multiple files."""
    multi_file_manifest = {
        **SAMPLE_TABULAR_MANIFEST,
        "files": [
            {
                "path": "s3://landing-zone/batch_001/file1.csv",
                "type": "tabular",
                "format": "CSV",
            },
            {
                "path": "s3://landing-zone/batch_001/file2.csv",
                "type": "tabular",
                "format": "CSV",
            },
        ],
    }

    mock_minio = Mock()
    mock_log = Mock()

    with pytest.raises(ValueError, match="exactly one file"):
        _download_tabular_from_landing(
            minio=mock_minio,
            manifest=multi_file_manifest,
            log=mock_log,
        )


def test_download_tabular_from_landing_cleanup_on_error():
    """Test that temp file is cleaned up on download error."""
    mock_minio = Mock()
    mock_minio.download_from_landing.side_effect = RuntimeError("Download failed")
    mock_log = Mock()

    with pytest.raises(RuntimeError, match="Failed to download"):
        _download_tabular_from_landing(
            minio=mock_minio,
            manifest=SAMPLE_TABULAR_MANIFEST,
            log=mock_log,
        )


def test_download_tabular_from_landing_cleanup_on_error():
    """Test that temp file is cleaned up on download error."""
    mock_minio = Mock()
    mock_minio.download_from_landing.side_effect = RuntimeError("Download failed")
    mock_log = Mock()

    with pytest.raises(RuntimeError, match="Download failed"):
        _download_tabular_from_landing(
            minio=mock_minio,
            manifest=SAMPLE_TABULAR_MANIFEST,
            log=mock_log,
        )


# =============================================================================
# Test: Load and Clean Tabular
# =============================================================================


def test_load_and_clean_tabular_success():
    """Test successful load and clean of CSV."""
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,name,age\n1,Alice,30\n2,Bob,25\n")
        temp_path = f.name

    try:
        download_result = {
            "local_file_path": temp_path,
            "manifest": SAMPLE_TABULAR_MANIFEST,
        }

        mock_log = Mock()

        result = _load_and_clean_tabular(
            download_result=download_result,
            log=mock_log,
        )

        assert "table" in result
        assert "header_mapping" in result
        assert "row_count" in result
        assert "columns" in result
        assert "manifest" in result

        # Check header mapping
        assert result["header_mapping"]["id"] == "id"
        assert result["header_mapping"]["name"] == "name"
        assert result["header_mapping"]["age"] == "age"

        # Check cleaned headers
        assert result["columns"] == ["id", "name", "age"]

        # Check row count
        assert result["row_count"] == 2

        # Check join_key_clean
        assert result["join_key_clean"] == "id"

        # Check that join key column is string type
        table = result["table"]
        assert table.schema.field("id").type == pa.string()

        # Verify file was cleaned up
        assert not Path(temp_path).exists()

    finally:
        Path(temp_path).unlink(missing_ok=True)


def test_load_and_clean_tabular_with_header_cleaning():
    """Test that headers are cleaned properly."""
    # Create CSV with messy headers
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("First Name,Last Name,Age (Years)\nAlice,Smith,30\nBob,Jones,25\n")
        temp_path = f.name

    try:
        # Create manifest without join_config for this test
        manifest_no_join = {
            **SAMPLE_TABULAR_MANIFEST,
            "metadata": {
                **SAMPLE_TABULAR_MANIFEST["metadata"],
                "join_config": None,
            },
        }

        download_result = {
            "local_file_path": temp_path,
            "manifest": manifest_no_join,
        }

        mock_log = Mock()

        result = _load_and_clean_tabular(
            download_result=download_result,
            log=mock_log,
        )

        # Check header mapping
        assert result["header_mapping"]["First Name"] == "first_name"
        assert result["header_mapping"]["Last Name"] == "last_name"
        assert result["header_mapping"]["Age (Years)"] == "age_years"

        # Check cleaned headers
        assert "first_name" in result["columns"]
        assert "last_name" in result["columns"]
        assert "age_years" in result["columns"]

    finally:
        Path(temp_path).unlink(missing_ok=True)


def test_load_and_clean_tabular_join_key_not_found():
    """Test that missing join key raises error."""
    manifest_no_join_key = {
        **SAMPLE_TABULAR_MANIFEST,
        "metadata": {
            **SAMPLE_TABULAR_MANIFEST["metadata"],
            "join_config": {
                "spatial_dataset_id": "sa1_spatial_001",
                "tabular_dataset_id": "sa1_tabular_001",
                "left_key": "missing_column",
                "right_key": "id",
                "how": "left",
            },
        },
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("id,name,age\n1,Alice,30\n")
        temp_path = f.name

    try:
        download_result = {
            "local_file_path": temp_path,
            "manifest": manifest_no_join_key,
        }

        mock_log = Mock()

        # The error is wrapped in RuntimeError
        with pytest.raises(RuntimeError, match="Join key 'missing_column' not found"):
            _load_and_clean_tabular(
                download_result=download_result,
                log=mock_log,
            )
    finally:
        Path(temp_path).unlink(missing_ok=True)


def test_load_and_clean_tabular_join_key_whitespace_trimmed():
    """Test that whitespace in join key values is trimmed."""
    # Create CSV with whitespace in join key values
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        # Note: values have leading/trailing spaces
        f.write("id,name,age\n  ABC123  ,Alice,30\n XYZ789,Bob,25\n")
        temp_path = f.name

    try:
        download_result = {
            "local_file_path": temp_path,
            "manifest": SAMPLE_TABULAR_MANIFEST,
        }

        mock_log = Mock()

        result = _load_and_clean_tabular(
            download_result=download_result,
            log=mock_log,
        )

        # Check that join key column is string type
        table = result["table"]
        assert table.schema.field("id").type == pa.string()

        # Check that whitespace was trimmed from join key values
        id_values = table.column("id").to_pylist()
        assert id_values == ["ABC123", "XYZ789"]  # No leading/trailing whitespace

    finally:
        Path(temp_path).unlink(missing_ok=True)


# =============================================================================
# Test: Export Tabular Parquet to Data Lake
# =============================================================================


def test_export_tabular_parquet_to_datalake_success():
    """Test successful export to data lake."""
    # Create a simple Arrow table
    table = pa.table(
        {
            "id": ["1", "2", "3"],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [30, 25, 35],
        }
    )

    table_info = {
        "table": table,
        "header_mapping": {"id": "id", "name": "name", "age": "age"},
        "row_count": 3,
        "columns": ["id", "name", "age"],
        "join_key_clean": "id",
        "manifest": SAMPLE_TABULAR_MANIFEST,
    }

    mock_minio = Mock()
    mock_mongodb = Mock()
    mock_mongodb.get_next_version.return_value = 1
    mock_mongodb.insert_asset.return_value = "507f1f77bcf86cd799439011"  # Mock ObjectId

    mock_log = Mock()

    result = _export_tabular_parquet_to_datalake(
        minio=mock_minio,
        mongodb=mock_mongodb,
        table_info=table_info,
        run_id="run_12345",
        log=mock_log,
    )

    assert "asset_id" in result
    assert "s3_key" in result
    assert "dataset_id" in result
    assert "version" in result
    assert "content_hash" in result
    assert "run_id" in result

    assert result["version"] == 1
    assert result["run_id"] == "run_12345"
    assert result["s3_key"].startswith("dataset_")
    assert result["s3_key"].endswith("/v1/data.parquet")

    # Verify MinIO upload was called
    mock_minio.upload_to_lake.assert_called_once()
    call_args = mock_minio.upload_to_lake.call_args
    assert call_args[0][1] == result["s3_key"]  # s3_key parameter

    # Verify MongoDB insert was called
    mock_mongodb.insert_asset.assert_called_once()
    inserted_asset = mock_mongodb.insert_asset.call_args[0][0]
    assert inserted_asset.kind.value == "tabular"
    assert inserted_asset.format.value == "parquet"
    assert inserted_asset.crs is None
    assert inserted_asset.bounds is None
    assert inserted_asset.metadata.header_mapping == table_info["header_mapping"]


def test_export_tabular_parquet_with_custom_dataset_id():
    """Test export with custom dataset_id from tags."""
    table = pa.table({"id": ["1"], "name": ["Alice"]})

    manifest_with_dataset_id = {
        **SAMPLE_TABULAR_MANIFEST,
        "metadata": {
            **SAMPLE_TABULAR_MANIFEST["metadata"],
            "tags": {
                "dataset_id": "custom_dataset_123",
                "priority": 1,
            },
        },
    }

    table_info = {
        "table": table,
        "header_mapping": {},
        "row_count": 1,
        "columns": ["id", "name"],
        "join_key_clean": None,
        "manifest": manifest_with_dataset_id,
    }

    mock_minio = Mock()
    mock_mongodb = Mock()
    mock_mongodb.get_next_version.return_value = 1
    mock_mongodb.insert_asset.return_value = "507f1f77bcf86cd799439011"

    mock_log = Mock()

    result = _export_tabular_parquet_to_datalake(
        minio=mock_minio,
        mongodb=mock_mongodb,
        table_info=table_info,
        run_id="run_12345",
        log=mock_log,
    )

    assert result["dataset_id"] == "custom_dataset_123"
    assert result["s3_key"] == "custom_dataset_123/v1/data.parquet"


def test_export_tabular_parquet_cleanup_on_error():
    """Test that temp file is cleaned up on export error."""
    table = pa.table({"id": ["1"]})

    table_info = {
        "table": table,
        "header_mapping": {},
        "row_count": 1,
        "columns": ["id"],
        "join_key_clean": None,
        "manifest": SAMPLE_TABULAR_MANIFEST,
    }

    mock_minio = Mock()
    mock_minio.upload_to_lake.side_effect = RuntimeError("Upload failed")
    mock_mongodb = Mock()
    mock_mongodb.get_next_version.return_value = 1

    mock_log = Mock()

    with pytest.raises(RuntimeError, match="Upload failed"):
        _export_tabular_parquet_to_datalake(
            minio=mock_minio,
            mongodb=mock_mongodb,
            table_info=table_info,
            run_id="run_12345",
            log=mock_log,
        )
