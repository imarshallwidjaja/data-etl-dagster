# =============================================================================
# Unit Tests: S3 Utils
# =============================================================================

import pytest

from libs.s3_utils import parse_s3_path, extract_s3_key, s3_to_vsis3


# =============================================================================
# Test: parse_s3_path
# =============================================================================

class TestParseS3Path:
    """Tests for parse_s3_path function."""
    
    def test_valid_s3_path_landing_zone(self):
        """Parse valid landing zone path."""
        bucket, key = parse_s3_path("s3://landing-zone/batch_001/data.csv")
        assert bucket == "landing-zone"
        assert key == "batch_001/data.csv"
    
    def test_valid_s3_path_data_lake(self):
        """Parse valid data lake path."""
        bucket, key = parse_s3_path("s3://data-lake/dataset_abc/v1/output.parquet")
        assert bucket == "data-lake"
        assert key == "dataset_abc/v1/output.parquet"
    
    def test_valid_s3_path_simple_key(self):
        """Parse path with simple key (no nested folders)."""
        bucket, key = parse_s3_path("s3://mybucket/file.txt")
        assert bucket == "mybucket"
        assert key == "file.txt"
    
    def test_valid_s3_path_deeply_nested(self):
        """Parse path with deeply nested key."""
        bucket, key = parse_s3_path("s3://bucket/a/b/c/d/e/file.json")
        assert bucket == "bucket"
        assert key == "a/b/c/d/e/file.json"
    
    def test_invalid_missing_prefix(self):
        """Reject path without s3:// prefix."""
        with pytest.raises(ValueError, match="Must start with 's3://'"):
            parse_s3_path("landing-zone/batch_001/data.csv")
    
    def test_invalid_http_prefix(self):
        """Reject path with http prefix."""
        with pytest.raises(ValueError, match="Must start with 's3://'"):
            parse_s3_path("http://landing-zone/batch_001/data.csv")
    
    def test_invalid_bucket_only(self):
        """Reject path with bucket only (no key)."""
        with pytest.raises(ValueError, match="Expected 's3://bucket/key'"):
            parse_s3_path("s3://landing-zone")
    
    def test_invalid_bucket_with_trailing_slash(self):
        """Reject path with bucket and trailing slash but no key."""
        with pytest.raises(ValueError, match="Expected 's3://bucket/key'"):
            parse_s3_path("s3://landing-zone/")
    
    def test_invalid_empty_bucket(self):
        """Reject path with empty bucket."""
        with pytest.raises(ValueError, match="Expected 's3://bucket/key'"):
            parse_s3_path("s3:///path/to/file.csv")


# =============================================================================
# Test: extract_s3_key
# =============================================================================

class TestExtractS3Key:
    """Tests for extract_s3_key function."""
    
    def test_s3_path_returns_key(self):
        """Extract key from s3:// path."""
        assert extract_s3_key("s3://landing-zone/batch_001/data.csv") == "batch_001/data.csv"
    
    def test_already_key_returns_unchanged(self):
        """Return key unchanged when already in key format."""
        assert extract_s3_key("batch_001/data.csv") == "batch_001/data.csv"
    
    def test_nested_path(self):
        """Extract key from nested s3 path."""
        assert extract_s3_key("s3://bucket/path/to/file.csv") == "path/to/file.csv"
    
    def test_simple_path(self):
        """Return simple path unchanged."""
        assert extract_s3_key("file.csv") == "file.csv"


# =============================================================================
# Test: s3_to_vsis3
# =============================================================================

class TestS3ToVsis3:
    """Tests for s3_to_vsis3 function."""
    
    def test_s3_path_converted(self):
        """Convert s3:// path to /vsis3/."""
        assert s3_to_vsis3("s3://landing-zone/batch_001/data.geojson") == "/vsis3/landing-zone/batch_001/data.geojson"
    
    def test_already_vsis3_unchanged(self):
        """Return vsis3 path unchanged."""
        assert s3_to_vsis3("/vsis3/landing-zone/batch_001/data.geojson") == "/vsis3/landing-zone/batch_001/data.geojson"
    
    def test_other_format_unchanged(self):
        """Return non-s3 paths unchanged."""
        assert s3_to_vsis3("/path/to/local/file.geojson") == "/path/to/local/file.geojson"
    
    def test_nested_s3_path(self):
        """Convert nested s3 path."""
        assert s3_to_vsis3("s3://data-lake/dataset/v1/data.parquet") == "/vsis3/data-lake/dataset/v1/data.parquet"

