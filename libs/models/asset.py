# =============================================================================
# Asset Models Module
# =============================================================================
# Defines models for asset registry:
# - ContentHash: Validated SHA256 hash type
# - AssetMetadata: Asset metadata (title, description, source, license)
# - Asset: Core asset registry model
# =============================================================================

import re
from datetime import datetime
from typing import Annotated
from pydantic import BaseModel, Field, BeforeValidator, ConfigDict

from .spatial import CRS, Bounds, OutputFormat

__all__ = [
    "S3Key",
    "ContentHash",
    "AssetMetadata",
    "Asset",
]


# =============================================================================
# S3 Key Validation
# =============================================================================

def validate_s3_key(value: str) -> str:
    """
    Validate S3 object key format.
    
    S3 keys should:
    - Not be empty
    - Not start or end with '/'
    - Be a valid object key path
    
    Args:
        value: S3 key string to validate
        
    Returns:
        Normalized S3 key (trimmed)
        
    Raises:
        TypeError: If the value is not a string
        ValueError: If the key format is invalid
    """
    if not isinstance(value, str):
        raise TypeError(f"S3 key must be a string, got {type(value).__name__}")
    
    if not value:
        raise ValueError("S3 key cannot be empty")
    
    value = value.strip()
    
    if not value:
        raise ValueError("S3 key cannot be empty or whitespace only")
    
    # S3 keys should not start or end with '/'
    if value.startswith('/'):
        raise ValueError("S3 key cannot start with '/'")
    
    if value.endswith('/'):
        raise ValueError("S3 key cannot end with '/'")
    
    return value


S3Key = Annotated[
    str,
    Field(..., description="S3 object key (e.g., 'data-lake/dataset_001/v1/data.parquet')"),
    BeforeValidator(validate_s3_key)
]
"""S3 object key type with validation. Ensures non-empty and no leading/trailing slashes."""


# =============================================================================
# Content Hash Validation
# =============================================================================

def validate_content_hash(value: str) -> str:
    """
    Validate SHA256 content hash format.
    
    Expected format: sha256:<64 hex characters>
    Example: sha256:abc123def456...
    
    Args:
        value: Content hash string to validate
        
    Returns:
        Normalized content hash string (lowercase)
        
    Raises:
        TypeError: If the value is not a string
        ValueError: If the hash format is invalid
    """
    if not isinstance(value, str):
        raise TypeError(f"Content hash must be a string, got {type(value).__name__}")
    
    if not value:
        raise ValueError("Content hash cannot be empty")
    
    value = value.strip().lower()
    
    # Validate format: sha256:<64 hex characters>
    pattern = r'^sha256:[a-f0-9]{64}$'
    if not re.match(pattern, value):
        raise ValueError(
            f"Invalid content hash format. Expected 'sha256:<64 hex characters>', "
            f"got: {value[:50]}{'...' if len(value) > 50 else ''}"
        )
    
    return value


ContentHash = Annotated[
    str,
    Field(..., description="SHA256 content hash (format: sha256:<64 hex chars>)"),
    BeforeValidator(validate_content_hash)
]
"""
Content hash type with validation.

Validates SHA256 hash format: sha256:<64 hexadecimal characters>
Automatically normalizes to lowercase.

Examples:
    >>> hash: ContentHash = "sha256:abc123..."  # Valid
    >>> hash: ContentHash = "SHA256:ABC123..."  # Valid, normalized to lowercase
    >>> hash: ContentHash = "md5:abc123"  # Invalid (wrong prefix)
    >>> hash: ContentHash = "sha256:abc"  # Invalid (wrong length)
"""


# =============================================================================
# Asset Metadata Model
# =============================================================================

class AssetMetadata(BaseModel):
    """
    Metadata for an asset.
    
    Provides descriptive information about the asset, including title,
    description, source attribution, and license information.
    
    Attributes:
        title: Asset title (required)
        description: Optional description of the asset
        source: Optional source attribution
        license: Optional license information
    """
    
    title: str = Field(..., description="Asset title")
    description: str | None = Field(None, description="Optional description of the asset")
    source: str | None = Field(None, description="Optional source attribution")
    license: str | None = Field(None, description="Optional license information")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "title": "Urban Land Use Dataset",
                "description": "High-resolution land use classification for metropolitan area",
                "source": "City Planning Department",
                "license": "CC-BY-4.0"
            }
        }
    )


# =============================================================================
# Asset Model
# =============================================================================

class Asset(BaseModel):
    """
    Asset registry model - represents a processed spatial dataset.
    
    This is the core model for tracking processed assets in the data lake.
    Each asset represents a versioned dataset stored in MinIO with metadata
    tracked in MongoDB.
    
    Attributes:
        s3_key: S3 object key (e.g., "data-lake/dataset_001/v1/data.parquet")
        dataset_id: Unique dataset identifier
        version: Asset version number (>= 1)
        content_hash: SHA256 hash of the asset content
        dagster_run_id: Dagster run ID that created this asset
        format: Output format (geoparquet, cog, or geojson)
        crs: Coordinate Reference System
        bounds: Geographic bounding box
        metadata: Asset metadata (title, description, source, license)
        created_at: Creation timestamp
        updated_at: Last update timestamp (optional)
    """
    
    s3_key: S3Key = Field(..., description="S3 object key (e.g., 'data-lake/dataset_001/v1/data.parquet')")
    dataset_id: str = Field(..., description="Unique dataset identifier")
    version: int = Field(..., ge=1, description="Asset version number (>= 1)")
    content_hash: ContentHash = Field(..., description="SHA256 content hash")
    dagster_run_id: str = Field(..., description="Dagster run ID that created this asset")
    format: OutputFormat = Field(..., description="Output format")
    crs: CRS = Field(..., description="Coordinate Reference System")
    bounds: Bounds | None = Field(None, description="Geographic bounding box (optional; None means empty/unknown)")
    metadata: AssetMetadata = Field(..., description="Asset metadata")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime | None = Field(None, description="Last update timestamp")
    
    def get_full_s3_path(self, bucket: str) -> str:
        """
        Get the full S3 path for this asset.
        
        Args:
            bucket: S3 bucket name
            
        Returns:
            Full S3 path: s3://{bucket}/{s3_key}
        """
        return f"s3://{bucket}/{self.s3_key}"
    
    def get_s3_key_pattern(self) -> str:
        """
        Get the expected S3 key pattern for this asset.
        
        Returns:
            Pattern string: {dataset_id}/v{version}/...
        """
        return f"{self.dataset_id}/v{self.version}/"
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "s3_key": "data-lake/dataset_001/v1/data.parquet",
                "dataset_id": "dataset_001",
                "version": 1,
                "content_hash": "sha256:abc123def4567890abcdef1234567890abcdef1234567890abcdef1234567890",
                "dagster_run_id": "run_12345",
                "format": "geoparquet",
                "crs": "EPSG:4326",
                "bounds": {
                    "minx": -180.0,
                    "miny": -90.0,
                    "maxx": 180.0,
                    "maxy": 90.0
                },
                "metadata": {
                    "title": "Sample Dataset",
                    "description": "Test dataset",
                    "source": "Test Source",
                    "license": "CC-BY-4.0"
                },
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": None
            }
        }
    )

