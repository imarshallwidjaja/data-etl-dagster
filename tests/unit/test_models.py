"""
Unit tests for data models.

Tests validation logic, type checking, and model behavior for all Pydantic models.
"""

import pytest
from datetime import datetime
from pydantic import ValidationError

from libs.models import (
    CRS,
    Bounds,
    FileEntry,
    Manifest,
    ManifestStatus,
    ManifestRecord,
    Asset,
    AssetMetadata,
    ContentHash,
    S3Path,
    S3Key,
    FileType,
    OutputFormat,
    MinIOSettings,
    MongoSettings,
    PostGISSettings,
    DagsterPostgresSettings,
)
from libs.models.spatial import validate_crs
from libs.models.asset import validate_content_hash, validate_s3_key
from libs.models.manifest import validate_s3_path
from pydantic import ValidationError, validate_call


# =============================================================================
# CRS Validation Tests
# =============================================================================

class TestCRSValidation:
    """Test CRS validation logic."""
    
    def test_valid_epsg_codes(self):
        """Test that valid EPSG codes pass validation."""
        # Standard EPSG codes - test via FileEntry model
        entry = FileEntry(
            path="s3://bucket/file.tif",
            type=FileType.RASTER,
            format="GTiff",
            crs="EPSG:4326"
        )
        assert entry.crs == "EPSG:4326"
        
        entry2 = FileEntry(
            path="s3://bucket/file.tif",
            type=FileType.RASTER,
            format="GTiff",
            crs="EPSG:28354"
        )
        assert entry2.crs == "EPSG:28354"
    
    def test_case_insensitive_epsg(self):
        """Test that EPSG codes are case-insensitive and normalized to uppercase."""
        # Test via validator function
        crs_lower = validate_crs("epsg:4326")
        assert crs_lower == "EPSG:4326"
        
        crs_mixed = validate_crs("Epsg:4326")
        assert crs_mixed == "EPSG:4326"
        
        # Test via model
        entry = FileEntry(
            path="s3://bucket/file.tif",
            type=FileType.RASTER,
            format="GTiff",
            crs="epsg:4326"
        )
        assert entry.crs == "EPSG:4326"
    
    def test_valid_wkt_strings(self, sample_wkt_crs):
        """Test that valid WKT strings pass validation."""
        # Test via validator
        crs = validate_crs(sample_wkt_crs)
        assert crs == sample_wkt_crs
        
        # Test GEOGCS
        geogcs = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]]]'
        crs2 = validate_crs(geogcs)
        assert crs2 == geogcs
    
    def test_valid_proj_strings(self, sample_proj_crs):
        """Test that valid PROJ strings pass validation."""
        crs = validate_crs(sample_proj_crs)
        assert crs == sample_proj_crs
        
        # Simple PROJ string
        simple_proj = "+proj=longlat +datum=WGS84 +no_defs"
        crs2 = validate_crs(simple_proj)
        assert crs2 == simple_proj
    
    def test_invalid_format_rejection(self):
        """Test that invalid CRS formats are rejected."""
        # Invalid prefix
        with pytest.raises(ValueError, match="Invalid CRS format"):
            validate_crs("INVALID:123")
        
        # Missing prefix
        with pytest.raises(ValueError, match="Invalid CRS format"):
            validate_crs("4326")
        
        # Empty string
        with pytest.raises(ValueError, match="CRS must be a non-empty string"):
            validate_crs("")
        
        # Whitespace only
        with pytest.raises(ValueError, match="CRS cannot be empty or whitespace only"):
            validate_crs("   ")
        
        # Wrong type
        with pytest.raises(TypeError, match="CRS must be a string"):
            validate_crs(4326)


# =============================================================================
# Bounds Validation Tests
# =============================================================================

class TestBoundsValidation:
    """Test Bounds validation logic."""
    
    def test_valid_bounds_pass(self, valid_bounds_dict):
        """Test that valid bounds pass validation."""
        bounds = Bounds(**valid_bounds_dict)
        assert bounds.minx == valid_bounds_dict["minx"]
        assert bounds.miny == valid_bounds_dict["miny"]
        assert bounds.maxx == valid_bounds_dict["maxx"]
        assert bounds.maxy == valid_bounds_dict["maxy"]
    
    def test_minx_greater_than_maxx_raises_error(self):
        """Test that minx > maxx raises ValueError."""
        with pytest.raises(ValueError, match="minx.*must be less than or equal to maxx"):
            Bounds(minx=180.0, miny=-90.0, maxx=-180.0, maxy=90.0)
    
    def test_miny_greater_than_maxy_raises_error(self):
        """Test that miny > maxy raises ValueError."""
        with pytest.raises(ValueError, match="miny.*must be less than or equal to maxy"):
            Bounds(minx=-180.0, miny=90.0, maxx=180.0, maxy=-90.0)
    
    def test_equal_min_max_allowed(self):
        """Test that equal min/max values (point bounds) are allowed."""
        # Equal minx and maxx (point bounds)
        point_bounds_x = Bounds(minx=0.0, miny=-90.0, maxx=0.0, maxy=90.0)
        assert point_bounds_x.minx == point_bounds_x.maxx
        assert point_bounds_x.width == 0.0
        
        # Equal miny and maxy (point bounds)
        point_bounds_y = Bounds(minx=-180.0, miny=0.0, maxx=180.0, maxy=0.0)
        assert point_bounds_y.miny == point_bounds_y.maxy
        assert point_bounds_y.height == 0.0
        
        # True point (both equal)
        true_point = Bounds(minx=0.0, miny=0.0, maxx=0.0, maxy=0.0)
        assert true_point.width == 0.0
        assert true_point.height == 0.0
        assert true_point.area == 0.0
    
    def test_bounds_properties(self, valid_bounds):
        """Test Bounds computed properties."""
        assert valid_bounds.width == 360.0  # 180 - (-180)
        assert valid_bounds.height == 180.0  # 90 - (-90)
        assert valid_bounds.area == 360.0 * 180.0


# =============================================================================
# Manifest Validation Tests
# =============================================================================

class TestManifestValidation:
    """Test Manifest validation logic."""
    
    def test_valid_manifest_passes(self, valid_manifest):
        """Test that valid manifest passes validation."""
        assert valid_manifest.batch_id == "batch_001"
        assert len(valid_manifest.files) == 1
        assert valid_manifest.metadata.project == "ALPHA"
    
    def test_missing_required_fields_raise_errors(self, valid_manifest_dict):
        """Test that missing required fields raise errors."""
        # Missing batch_id
        data = valid_manifest_dict.copy()
        del data["batch_id"]
        with pytest.raises(ValidationError):
            Manifest(**data)
        
        # Missing uploader
        data = valid_manifest_dict.copy()
        del data["uploader"]
        with pytest.raises(ValidationError):
            Manifest(**data)
        
        # Missing files
        data = valid_manifest_dict.copy()
        del data["files"]
        with pytest.raises(ValidationError):
            Manifest(**data)
        
        # Missing metadata
        data = valid_manifest_dict.copy()
        del data["metadata"]
        with pytest.raises(ValidationError):
            Manifest(**data)
    
    def test_empty_files_array_rejected(self, valid_manifest_dict):
        """Test that empty files array is rejected."""
        data = valid_manifest_dict.copy()
        data["files"] = []
        with pytest.raises(ValidationError, match="at least 1 item"):
            Manifest(**data)
    
    def test_duplicate_paths_rejected(self, valid_file_entry_dict):
        """Test that duplicate file paths are rejected."""
        data = {
            "batch_id": "batch_001",
            "uploader": "user_123",
            "intent": "ingest_raster",
            "files": [
                valid_file_entry_dict,
                {**valid_file_entry_dict}  # Same path
            ],
            "metadata": {
                "project": "ALPHA",
                "description": "Test"
            }
        }
        with pytest.raises(ValueError, match="Duplicate file paths"):
            Manifest(**data)
    
    def test_invalid_crs_in_file_entry_bubbles_up(self, valid_manifest_dict):
        """Test that invalid CRS in FileEntry bubbles up."""
        data = valid_manifest_dict.copy()
        data["files"][0]["crs"] = "INVALID:123"
        with pytest.raises(ValidationError):
            Manifest(**data)


# =============================================================================
# ManifestRecord Tests
# =============================================================================

class TestManifestRecord:
    """Test ManifestRecord model."""
    
    def test_can_be_created_from_manifest(self, valid_manifest):
        """Test that ManifestRecord can be created from Manifest."""
        record = ManifestRecord.from_manifest(valid_manifest)
        assert record.batch_id == valid_manifest.batch_id
        assert record.status == ManifestStatus.PENDING
        assert isinstance(record.ingested_at, datetime)
    
    def test_status_enum_validation(self, valid_manifest_record_dict):
        """Test that status enum validation works."""
        # Valid statuses
        for status in ["pending", "processing", "completed", "failed"]:
            data = valid_manifest_record_dict.copy()
            data["status"] = status
            record = ManifestRecord(**data)
            assert record.status == ManifestStatus(status)
        
        # Invalid status
        data = valid_manifest_record_dict.copy()
        data["status"] = "invalid_status"
        with pytest.raises(ValidationError):
            ManifestRecord(**data)
    
    def test_from_manifest_with_custom_status(self, valid_manifest):
        """Test from_manifest with custom status."""
        record = ManifestRecord.from_manifest(
            valid_manifest,
            status=ManifestStatus.PROCESSING,
            ingested_at=datetime(2024, 1, 1, 10, 0, 0)
        )
        assert record.status == ManifestStatus.PROCESSING
        assert record.ingested_at == datetime(2024, 1, 1, 10, 0, 0)


# =============================================================================
# Asset Validation Tests
# =============================================================================

class TestAssetValidation:
    """Test Asset validation logic."""
    
    def test_valid_asset_passes(self, valid_asset):
        """Test that valid asset passes validation."""
        assert valid_asset.dataset_id == "dataset_001"
        assert valid_asset.version == 1
        assert valid_asset.format == OutputFormat.GEOPARQUET
    
    def test_invalid_content_hash_format_rejected(self, valid_asset_dict):
        """Test that invalid content_hash format is rejected."""
        # Wrong prefix
        data = valid_asset_dict.copy()
        data["content_hash"] = "md5:" + "a" * 32
        with pytest.raises(ValidationError):
            Asset(**data)
        
        # Wrong length
        data = valid_asset_dict.copy()
        data["content_hash"] = "sha256:" + "a" * 32  # Too short
        with pytest.raises(ValidationError):
            Asset(**data)
        
        # Invalid characters
        data = valid_asset_dict.copy()
        data["content_hash"] = "sha256:" + "g" * 64  # 'g' is not hex
        with pytest.raises(ValidationError):
            Asset(**data)
    
    def test_version_less_than_one_rejected(self, valid_asset_dict):
        """Test that version < 1 is rejected."""
        data = valid_asset_dict.copy()
        data["version"] = 0
        with pytest.raises(ValidationError, match="greater than or equal to 1"):
            Asset(**data)
        
        data["version"] = -1
        with pytest.raises(ValidationError, match="greater than or equal to 1"):
            Asset(**data)
    
    def test_missing_optional_metadata_fields_allowed(self, valid_asset_dict):
        """Test that missing optional metadata fields are allowed."""
        data = valid_asset_dict.copy()
        # Remove optional fields
        data["metadata"] = {
            "title": "Test Dataset"
            # description, source, license are optional
        }
        asset = Asset(**data)
        assert asset.metadata.description is None
        assert asset.metadata.source is None
        assert asset.metadata.license is None
    
    def test_asset_methods(self, valid_asset):
        """Test Asset helper methods."""
        assert valid_asset.get_full_s3_path("data-lake") == "s3://data-lake/data-lake/dataset_001/v1/data.parquet"
        assert valid_asset.get_s3_key_pattern() == "dataset_001/v1/"


# =============================================================================
# ContentHash Validation Tests
# =============================================================================

class TestContentHashValidation:
    """Test ContentHash validation logic."""
    
    def test_valid_hash_passes(self):
        """Test that valid hash passes."""
        hash_str = "sha256:" + "a" * 64
        hash_val = validate_content_hash(hash_str)
        assert hash_val == hash_str.lower()  # Normalized to lowercase
        
        # Test via Asset model
        asset = Asset(
            s3_key="data-lake/file.parquet",
            dataset_id="test",
            version=1,
            content_hash=hash_str,
            dagster_run_id="run_123",
            format=OutputFormat.GEOPARQUET,
            crs="EPSG:4326",
            bounds=Bounds(minx=-180, miny=-90, maxx=180, maxy=90),
            metadata=AssetMetadata(title="Test"),
            created_at=datetime.now()
        )
        assert asset.content_hash == hash_str.lower()
    
    def test_wrong_prefix_rejected(self):
        """Test that wrong prefix is rejected."""
        with pytest.raises(ValueError, match="Invalid content hash format"):
            validate_content_hash("md5:" + "a" * 32)
        
        with pytest.raises(ValueError, match="Invalid content hash format"):
            validate_content_hash("sha1:" + "a" * 40)
    
    def test_wrong_length_rejected(self):
        """Test that wrong length is rejected."""
        # Too short
        with pytest.raises(ValueError, match="Invalid content hash format"):
            validate_content_hash("sha256:" + "a" * 32)
        
        # Too long
        with pytest.raises(ValueError, match="Invalid content hash format"):
            validate_content_hash("sha256:" + "a" * 128)
    
    def test_invalid_characters_rejected(self):
        """Test that invalid characters are rejected."""
        # Non-hex characters
        with pytest.raises(ValueError, match="Invalid content hash format"):
            validate_content_hash("sha256:" + "g" * 64)
        
        # Uppercase (should be normalized)
        hash_upper = "sha256:" + "A" * 64
        hash_val = validate_content_hash(hash_upper)
        assert hash_val == hash_upper.lower()


# =============================================================================
# Config Settings Tests
# =============================================================================

class TestConfigSettings:
    """Test configuration settings models."""
    
    def test_mongo_connection_string_property(self, monkeypatch):
        """Test that MongoSettings connection_string property builds correctly."""
        monkeypatch.setenv("MONGO_HOST", "custom-host")
        monkeypatch.setenv("MONGO_PORT", "27018")
        monkeypatch.setenv("MONGO_INITDB_ROOT_USERNAME", "admin")
        monkeypatch.setenv("MONGO_INITDB_ROOT_PASSWORD", "password")
        monkeypatch.setenv("MONGO_DATABASE", "test_db")
        monkeypatch.setenv("MONGO_AUTH_SOURCE", "admin")
        
        settings = MongoSettings()
        conn_str = settings.connection_string
        assert "mongodb://admin:password@custom-host:27018/test_db?authSource=admin" == conn_str
    
    def test_postgis_connection_string_property(self, monkeypatch):
        """Test that PostGISSettings connection_string property builds correctly."""
        monkeypatch.setenv("POSTGRES_HOST", "custom-postgis")
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        monkeypatch.setenv("POSTGRES_USER", "postgres")
        monkeypatch.setenv("POSTGRES_PASSWORD", "password")
        monkeypatch.setenv("POSTGRES_DB", "test_db")
        
        settings = PostGISSettings()
        conn_str = settings.connection_string
        assert "postgresql://postgres:password@custom-postgis:5433/test_db" == conn_str
    
    def test_dagster_postgres_connection_string_property(self, monkeypatch):
        """Test that DagsterPostgresSettings connection_string property builds correctly."""
        monkeypatch.setenv("DAGSTER_POSTGRES_HOST", "custom-postgres")
        monkeypatch.setenv("DAGSTER_POSTGRES_PORT", "5434")
        monkeypatch.setenv("DAGSTER_POSTGRES_USER", "dagster")
        monkeypatch.setenv("DAGSTER_POSTGRES_PASSWORD", "password")
        monkeypatch.setenv("DAGSTER_POSTGRES_DB", "dagster_db")
        
        settings = DagsterPostgresSettings()
        conn_str = settings.connection_string
        assert "postgresql://dagster:password@custom-postgres:5434/dagster_db" == conn_str


# =============================================================================
# S3Path Validation Tests
# =============================================================================

class TestS3PathValidation:
    """Test S3Path validation logic."""
    
    def test_valid_s3_paths(self):
        """Test that valid S3 paths pass validation."""
        # Test via validator
        path1 = validate_s3_path("s3://bucket-name/path/to/file.ext")
        assert path1 == "s3://bucket-name/path/to/file.ext"
        
        # Without s3:// prefix (should be normalized)
        path2 = validate_s3_path("bucket-name/path/to/file.ext")
        assert path2 == "s3://bucket-name/path/to/file.ext"
        
        # Test via FileEntry model
        entry = FileEntry(
            path="bucket-name/path/to/file.ext",
            type=FileType.RASTER,
            format="GTiff",
            crs="EPSG:4326"
        )
        assert entry.path == "s3://bucket-name/path/to/file.ext"
    
    def test_invalid_s3_paths_rejected(self):
        """Test that invalid S3 paths are rejected."""
        # Empty string
        with pytest.raises(ValueError, match="S3 path cannot be empty"):
            validate_s3_path("")
        
        # Missing bucket
        with pytest.raises(ValueError, match="bucket name and object key"):
            validate_s3_path("s3://")
        
        # Missing object key
        with pytest.raises(ValueError, match="bucket name and object key"):
            validate_s3_path("s3://bucket-name")
        
        # Invalid bucket name (uppercase)
        with pytest.raises(ValueError, match="Invalid S3 bucket name"):
            validate_s3_path("s3://BUCKET-NAME/file.ext")
        
        # Invalid bucket name (starts with dot)
        with pytest.raises(ValueError, match="Invalid S3 bucket name"):
            validate_s3_path("s3://.bucket/file.ext")


# =============================================================================
# S3Key Validation Tests
# =============================================================================

class TestS3KeyValidation:
    """Test S3Key validation logic."""
    
    def test_valid_s3_keys(self):
        """Test that valid S3 keys pass validation."""
        # Test via validator
        key1 = validate_s3_key("data-lake/dataset_001/v1/data.parquet")
        assert key1 == "data-lake/dataset_001/v1/data.parquet"
        
        # Leading/trailing spaces should be trimmed
        key2 = validate_s3_key("  data-lake/file.ext  ")
        assert key2 == "data-lake/file.ext"
        
        # Test via Asset model
        asset = Asset(
            s3_key="data-lake/file.parquet",
            dataset_id="test",
            version=1,
            content_hash="sha256:" + "a" * 64,
            dagster_run_id="run_123",
            format=OutputFormat.GEOPARQUET,
            crs="EPSG:4326",
            bounds=Bounds(minx=-180, miny=-90, maxx=180, maxy=90),
            metadata=AssetMetadata(title="Test"),
            created_at=datetime.now()
        )
        assert asset.s3_key == "data-lake/file.parquet"
    
    def test_invalid_s3_keys_rejected(self):
        """Test that invalid S3 keys are rejected."""
        # Empty string
        with pytest.raises(ValueError, match="S3 key cannot be empty"):
            validate_s3_key("")
        
        # Leading slash
        with pytest.raises(ValueError, match="cannot start with"):
            validate_s3_key("/data-lake/file.ext")
        
        # Trailing slash
        with pytest.raises(ValueError, match="cannot end with"):
            validate_s3_key("data-lake/file.ext/")

