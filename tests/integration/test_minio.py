"""
Integration tests for MinIO (S3-compatible object storage).

Tests basic connectivity, bucket operations, and read/write functionality.
"""

import pytest
from io import BytesIO
from datetime import datetime

from libs.models import MinIOSettings
from minio import Minio
from minio.error import S3Error


pytestmark = pytest.mark.integration


@pytest.fixture
def minio_settings():
    """Load MinIO settings from environment."""
    return MinIOSettings()


@pytest.fixture
def minio_client(minio_settings):
    """Create MinIO client."""
    return Minio(
        minio_settings.endpoint,
        access_key=minio_settings.access_key,
        secret_key=minio_settings.secret_key,
        secure=minio_settings.use_ssl,
    )


class TestMinIOConnectivity:
    """Test MinIO connectivity and basic operations."""
    
    def test_minio_connection(self, minio_client):
        """Test that MinIO is accessible."""
        # List buckets to verify connection
        buckets = minio_client.list_buckets()
        assert isinstance(buckets, list)
    
    def test_required_buckets_exist(self, minio_client, minio_settings):
        """Test that required buckets (landing-zone, data-lake) exist."""
        buckets = [b.name for b in minio_client.list_buckets()]
        
        assert minio_settings.landing_bucket in buckets, \
            f"Landing bucket '{minio_settings.landing_bucket}' not found"
        assert minio_settings.lake_bucket in buckets, \
            f"Lake bucket '{minio_settings.lake_bucket}' not found"
    
    def test_minio_read_write(self, minio_client, minio_settings):
        """Test basic read/write operations."""
        # Generate unique test object key
        test_key = f"integration-test/{datetime.now().isoformat()}/test.txt"
        test_content = b"Hello, MinIO! This is a test."
        
        # Write object
        data = BytesIO(test_content)
        minio_client.put_object(
            minio_settings.landing_bucket,
            test_key,
            data,
            length=len(test_content),
            content_type="text/plain",
        )
        
        # Read object back
        response = minio_client.get_object(minio_settings.landing_bucket, test_key)
        read_content = response.read()
        response.close()
        response.release_conn()
        
        assert read_content == test_content, "Read content does not match written content"
        
        # Cleanup: remove test object
        try:
            minio_client.remove_object(minio_settings.landing_bucket, test_key)
        except S3Error:
            pass  # Ignore cleanup errors

