"""
Unit tests for MinIOResource.

Tests all methods with mocked minio.Minio client to avoid network calls.
"""

import json
from unittest.mock import Mock, patch
import pytest

from services.dagster.etl_pipelines.resources import MinIOResource
from minio.error import S3Error


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def minio_resource():
    """Create a MinIOResource instance with test configuration."""
    return MinIOResource(
        endpoint="localhost:9000",
        access_key="test_access",
        secret_key="test_secret",
        use_ssl=False,
        landing_bucket="test-landing",
        lake_bucket="test-lake",
    )


@pytest.fixture
def mock_minio_client():
    """Create a mock Minio client."""
    return Mock()


# =============================================================================
# Test: get_client
# =============================================================================


def test_get_client(minio_resource):
    """Test that get_client creates a properly configured Minio client."""
    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        minio_resource.get_client()

        # Verify Minio was instantiated with correct parameters
        mock_minio.assert_called_once_with(
            "localhost:9000",
            access_key="test_access",
            secret_key="test_secret",
            secure=False,
        )


# =============================================================================
# Test: list_manifests
# =============================================================================


def test_list_manifests_returns_json_files_only(minio_resource):
    """Test that list_manifests filters to JSON files only."""
    # Mock objects returned from MinIO
    mock_obj1 = Mock()
    mock_obj1.object_name = "manifests/batch_123.json"

    mock_obj2 = Mock()
    mock_obj2.object_name = "manifests/batch_456.json"

    mock_obj3 = Mock()
    mock_obj3.object_name = "manifests/readme.txt"  # Not JSON

    mock_obj4 = Mock()
    mock_obj4.object_name = "manifests/data.csv"  # Not JSON

    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()
        mock_client.list_objects.return_value = [
            mock_obj1,
            mock_obj2,
            mock_obj3,
            mock_obj4,
        ]
        mock_minio.return_value = mock_client

        result = minio_resource.list_manifests()

        # Verify only JSON files returned
        assert result == ["manifests/batch_123.json", "manifests/batch_456.json"]

        # Verify list_objects called with correct parameters
        mock_client.list_objects.assert_called_once_with(
            "test-landing",
            prefix="manifests/",
            recursive=True,
        )


def test_list_manifests_returns_empty_list_when_no_manifests(minio_resource):
    """Test that list_manifests returns empty list when no manifests found."""
    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()
        mock_client.list_objects.return_value = []  # No objects
        mock_minio.return_value = mock_client

        result = minio_resource.list_manifests()

        assert result == []


def test_list_manifests_raises_on_missing_bucket(minio_resource):
    """Test that list_manifests raises clear error when bucket missing."""
    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()

        # Simulate NoSuchBucket error
        error = S3Error(
            "NoSuchBucket",
            "The specified bucket does not exist",
            resource="test-landing",
            request_id="test",
            host_id="test",
            response=Mock(status=404),
        )
        mock_client.list_objects.side_effect = error
        mock_minio.return_value = mock_client

        with pytest.raises(
            RuntimeError, match="Landing bucket 'test-landing' does not exist"
        ):
            minio_resource.list_manifests()


# =============================================================================
# Test: get_manifest
# =============================================================================


def test_get_manifest_downloads_and_parses_json(minio_resource):
    """Test that get_manifest downloads object and parses JSON."""
    manifest_data = {
        "batch_id": "test_batch",
        "uploader": "test_user",
        "files": [],
    }

    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()

        # Mock response object
        mock_response = Mock()
        mock_response.read.return_value = json.dumps(manifest_data).encode()
        mock_client.get_object.return_value = mock_response

        mock_minio.return_value = mock_client

        result = minio_resource.get_manifest("manifests/batch_123.json")

        # Verify result
        assert result == manifest_data

        # Verify client calls
        mock_client.get_object.assert_called_once_with(
            "test-landing",
            "manifests/batch_123.json",
        )
        mock_response.close.assert_called_once()
        mock_response.release_conn.assert_called_once()


def test_get_manifest_raises_on_missing_object(minio_resource):
    """Test that get_manifest raises clear error when object missing."""
    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()

        # Simulate NoSuchKey error
        error = S3Error(
            "NoSuchKey",
            "The specified key does not exist",
            resource="manifests/missing.json",
            request_id="test",
            host_id="test",
            response=Mock(status=404),
        )
        mock_client.get_object.side_effect = error
        mock_minio.return_value = mock_client

        with pytest.raises(
            RuntimeError, match="Manifest 'manifests/missing.json' not found"
        ):
            minio_resource.get_manifest("manifests/missing.json")


def test_get_manifest_raises_on_invalid_json(minio_resource):
    """Test that get_manifest raises clear error on invalid JSON."""
    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()

        # Mock response with invalid JSON
        mock_response = Mock()
        mock_response.read.return_value = b"not valid json {{"
        mock_client.get_object.return_value = mock_response

        mock_minio.return_value = mock_client

        with pytest.raises(RuntimeError, match="contains invalid JSON"):
            minio_resource.get_manifest("manifests/bad.json")


# =============================================================================
# Test: move_to_archive
# =============================================================================


def test_move_to_archive_copies_and_deletes(minio_resource):
    """Test that move_to_archive copies to archive/ then deletes original."""
    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()
        mock_minio.return_value = mock_client

        minio_resource.move_to_archive("manifests/batch_123.json")

        # Verify copy operation - CopySource object is used per minio API
        mock_client.copy_object.assert_called_once()
        call_args = mock_client.copy_object.call_args
        assert call_args[0][0] == "test-landing"  # destination bucket
        assert call_args[0][1] == "archive/manifests/batch_123.json"  # destination key
        # Third arg is CopySource object
        copy_source = call_args[0][2]
        assert copy_source.bucket_name == "test-landing"
        assert copy_source.object_name == "manifests/batch_123.json"

        # Verify delete operation
        mock_client.remove_object.assert_called_once_with(
            "test-landing",
            "manifests/batch_123.json",
        )


def test_move_to_archive_tolerates_missing_original_on_delete(minio_resource):
    """Test that move_to_archive doesn't raise if original already deleted."""
    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()

        # Simulate NoSuchKey on delete (already moved)
        error = S3Error(
            "NoSuchKey",
            "The specified key does not exist",
            resource="manifests/batch_123.json",
            request_id="test",
            host_id="test",
            response=Mock(status=404),
        )
        mock_client.remove_object.side_effect = error

        mock_minio.return_value = mock_client

        # Should not raise
        minio_resource.move_to_archive("manifests/batch_123.json")

        # Verify copy still happened
        mock_client.copy_object.assert_called_once()


def test_move_to_archive_raises_on_other_delete_errors(minio_resource):
    """Test that move_to_archive raises on non-NotFound delete errors."""
    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()

        # Simulate access denied error
        error = S3Error(
            "AccessDenied",
            "Access Denied",
            resource="manifests/batch_123.json",
            request_id="test",
            host_id="test",
            response=Mock(status=403),
        )
        mock_client.remove_object.side_effect = error

        mock_minio.return_value = mock_client

        with pytest.raises(S3Error):
            minio_resource.move_to_archive("manifests/batch_123.json")


# =============================================================================
# Test: upload_to_lake
# =============================================================================


def test_upload_to_lake_uploads_file_with_inferred_content_type(
    minio_resource, tmp_path
):
    """Test that upload_to_lake uploads file with inferred content type."""
    # Create temporary test file
    test_file = tmp_path / "test.parquet"
    test_file.write_bytes(b"test data")

    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()
        mock_minio.return_value = mock_client

        minio_resource.upload_to_lake(str(test_file), "datasets/test.parquet")

        # Verify put_object called correctly
        call_args = mock_client.put_object.call_args
        assert call_args[0][0] == "test-lake"  # bucket
        assert call_args[0][1] == "datasets/test.parquet"  # key
        assert call_args[1]["length"] == 9  # file size
        assert call_args[1]["content_type"] == "application/vnd.apache.parquet"


def test_upload_to_lake_uploads_file_with_explicit_content_type(
    minio_resource, tmp_path
):
    """Test that upload_to_lake respects explicit content_type parameter."""
    # Create temporary test file
    test_file = tmp_path / "test.dat"
    test_file.write_bytes(b"test data")

    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()
        mock_minio.return_value = mock_client

        minio_resource.upload_to_lake(
            str(test_file),
            "datasets/test.dat",
            content_type="application/custom",
        )

        # Verify content_type used
        call_args = mock_client.put_object.call_args
        assert call_args[1]["content_type"] == "application/custom"


def test_upload_to_lake_raises_on_missing_file(minio_resource):
    """Test that upload_to_lake raises FileNotFoundError if file missing."""
    with pytest.raises(FileNotFoundError, match="Local file not found"):
        minio_resource.upload_to_lake(
            "/nonexistent/file.parquet", "datasets/test.parquet"
        )


def test_upload_to_lake_infers_correct_content_types(minio_resource, tmp_path):
    """Test that content type inference works for various extensions."""
    test_cases = [
        (".parquet", "application/vnd.apache.parquet"),
        (".tif", "image/tiff"),
        (".tiff", "image/tiff"),
        (".json", "application/json"),
        (".geojson", "application/geo+json"),
        (".gpkg", "application/geopackage+sqlite3"),
        (".unknown", "application/octet-stream"),
    ]

    for extension, expected_type in test_cases:
        test_file = tmp_path / f"test{extension}"
        test_file.write_bytes(b"data")

        with patch(
            "services.dagster.etl_pipelines.resources.minio_resource.Minio"
        ) as mock_minio:
            mock_client = Mock()
            mock_minio.return_value = mock_client

            minio_resource.upload_to_lake(str(test_file), f"test{extension}")

            call_args = mock_client.put_object.call_args
            assert call_args[1]["content_type"] == expected_type


# =============================================================================
# Test: get_presigned_url
# =============================================================================


def test_get_presigned_url_generates_url(minio_resource):
    """Test that get_presigned_url generates a presigned URL."""
    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()
        mock_client.presigned_get_object.return_value = (
            "https://example.com/presigned-url"
        )
        mock_minio.return_value = mock_client

        url = minio_resource.get_presigned_url("test-lake", "datasets/test.parquet")

        assert url == "https://example.com/presigned-url"

        # Verify client call
        mock_client.presigned_get_object.assert_called_once_with(
            "test-lake",
            "datasets/test.parquet",
            expires=3600,  # default
        )


def test_get_presigned_url_respects_custom_expiry(minio_resource):
    """Test that get_presigned_url respects custom expiry_seconds."""
    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()
        mock_client.presigned_get_object.return_value = (
            "https://example.com/presigned-url"
        )
        mock_minio.return_value = mock_client

        minio_resource.get_presigned_url(
            "test-landing",
            "batch/file.tif",
            expiry_seconds=7200,
        )

        # Verify custom expiry used
        call_args = mock_client.presigned_get_object.call_args
        assert call_args[1]["expires"] == 7200


# =============================================================================
# Test: object_exists_in_landing
# =============================================================================


def test_object_exists_in_landing_returns_true_when_object_exists(minio_resource):
    """Test that object_exists_in_landing returns True when object exists."""
    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()
        mock_client.stat_object.return_value = Mock()  # stat succeeds
        mock_minio.return_value = mock_client

        result = minio_resource.object_exists_in_landing("batch_001/data.json")

        assert result is True
        mock_client.stat_object.assert_called_once_with(
            "test-landing", "batch_001/data.json"
        )


def test_object_exists_in_landing_returns_false_when_object_missing(minio_resource):
    """Test that object_exists_in_landing returns False when object does not exist."""
    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()
        error = S3Error(
            "NoSuchKey",
            "The specified key does not exist",
            resource="batch_001/data.json",
            request_id="test",
            host_id="test",
            response=Mock(status=404),
        )
        mock_client.stat_object.side_effect = error
        mock_minio.return_value = mock_client

        result = minio_resource.object_exists_in_landing("batch_001/data.json")

        assert result is False


def test_object_exists_in_landing_raises_on_other_s3_errors(minio_resource):
    """Test that object_exists_in_landing re-raises non-NoSuchKey S3 errors."""
    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()
        error = S3Error(
            "AccessDenied",
            "Access Denied",
            resource="batch_001/data.json",
            request_id="test",
            host_id="test",
            response=Mock(status=403),
        )
        mock_client.stat_object.side_effect = error
        mock_minio.return_value = mock_client

        with pytest.raises(S3Error):
            minio_resource.object_exists_in_landing("batch_001/data.json")


# =============================================================================
# Test: upload_json_to_landing
# =============================================================================


def test_upload_json_to_landing_uploads_json_payload(minio_resource):
    """Test that upload_json_to_landing serializes and uploads JSON to landing bucket.

    Default if_not_exists=False means no existence check — just overwrite.
    """
    payload = {"sheet": "Sheet1", "rows": [1, 2, 3]}

    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()
        mock_minio.return_value = mock_client

        minio_resource.upload_json_to_landing("batch_001/config.json", payload)

        # Should NOT have checked existence (default is overwrite)
        mock_client.stat_object.assert_not_called()

        # Verify put_object called with correct args
        mock_client.put_object.assert_called_once()
        call_args = mock_client.put_object.call_args
        assert call_args[0][0] == "test-landing"  # bucket
        assert call_args[0][1] == "batch_001/config.json"  # key

        # Read the data stream that was passed
        data_stream = call_args[0][2]
        uploaded_bytes = data_stream.read()
        assert json.loads(uploaded_bytes) == payload

        assert call_args[1]["length"] == len(json.dumps(payload).encode("utf-8"))
        assert call_args[1]["content_type"] == "application/json"


def test_upload_json_to_landing_raises_file_exists_when_if_not_exists_true(
    minio_resource,
):
    """Test that upload_json_to_landing raises FileExistsError when object exists and if_not_exists=True."""
    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()
        mock_client.stat_object.return_value = Mock()  # object exists
        mock_minio.return_value = mock_client

        with pytest.raises(
            FileExistsError,
            match="Object 'batch_001/config.json' already exists in bucket 'test-landing'",
        ):
            minio_resource.upload_json_to_landing(
                "batch_001/config.json",
                {"data": "value"},
                if_not_exists=True,
            )

        # put_object should NOT have been called
        mock_client.put_object.assert_not_called()


def test_upload_json_to_landing_overwrites_when_if_not_exists_false(minio_resource):
    """Test that upload_json_to_landing overwrites existing object when if_not_exists=False."""
    payload = {"overwrite": True}

    with patch(
        "services.dagster.etl_pipelines.resources.minio_resource.Minio"
    ) as mock_minio:
        mock_client = Mock()
        mock_minio.return_value = mock_client

        minio_resource.upload_json_to_landing(
            "batch_001/config.json",
            payload,
            if_not_exists=False,
        )

        # Should NOT have checked existence
        mock_client.stat_object.assert_not_called()

        # Should have uploaded
        mock_client.put_object.assert_called_once()
        call_args = mock_client.put_object.call_args
        data_stream = call_args[0][2]
        uploaded_bytes = data_stream.read()
        assert json.loads(uploaded_bytes) == payload
