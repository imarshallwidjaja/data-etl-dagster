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
from enum import Enum
from typing import Annotated, TYPE_CHECKING
from pydantic import BaseModel, Field, BeforeValidator, ConfigDict, model_validator

if TYPE_CHECKING:
    from .manifest import ManifestMetadata

from .spatial import CRS, Bounds, OutputFormat
from .manifest import TagValue
from .base import HumanMetadataMixin

__all__ = [
    "S3Key",
    "ContentHash",
    "AssetKind",
    "ColumnInfo",
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
    if value.startswith("/"):
        raise ValueError("S3 key cannot start with '/'")

    if value.endswith("/"):
        raise ValueError("S3 key cannot end with '/'")

    return value


S3Key = Annotated[
    str,
    Field(
        ..., description="S3 object key (e.g., 'data-lake/dataset_001/v1/data.parquet')"
    ),
    BeforeValidator(validate_s3_key),
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
    pattern = r"^sha256:[a-f0-9]{64}$"
    if not re.match(pattern, value):
        raise ValueError(
            f"Invalid content hash format. Expected 'sha256:<64 hex characters>', "
            f"got: {value[:50]}{'...' if len(value) > 50 else ''}"
        )

    return value


ContentHash = Annotated[
    str,
    Field(..., description="SHA256 content hash (format: sha256:<64 hex chars>)"),
    BeforeValidator(validate_content_hash),
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
# Asset Kind Enum
# =============================================================================


class AssetKind(str, Enum):
    """Asset kind classification: spatial, tabular, or joined."""

    SPATIAL = "spatial"
    TABULAR = "tabular"
    JOINED = "joined"


# =============================================================================
# Column Info Model (for Columnar Assets)
# =============================================================================


class ColumnInfo(BaseModel):
    """
    Metadata for a single column in a columnar asset (tabular or joined).

    Captures both type information (for schema introspection) and human
    metadata (for cataloging and documentation).

    Attributes:
        title: Human-readable column title (defaults to column name)
        description: Optional detailed description of the column
        type_name: Canonical type category (STRING, INTEGER, FLOAT, etc.)
        logical_type: Detailed type from PyArrow (int64, float32, timestamp[ns])
        nullable: Whether the column can contain null values
    """

    title: str = Field(..., description="Human-readable column title")
    description: str = Field(default="", description="Optional column description")
    type_name: str = Field(
        ...,
        description="Canonical type category (STRING, INTEGER, FLOAT, BOOLEAN, etc.)",
    )
    logical_type: str = Field(
        ..., description="Detailed type (int64, float32, timestamp[ns])"
    )
    nullable: bool = Field(
        default=True, description="Whether the column can contain null values"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "title": "Population Count",
                "description": "Total resident population as of census date",
                "type_name": "INTEGER",
                "logical_type": "int64",
                "nullable": False,
            }
        }
    )


# =============================================================================
# Asset Metadata Model
# =============================================================================


class AssetMetadata(HumanMetadataMixin):
    """
    Metadata for an asset.

    Inherits human semantic fields from HumanMetadataMixin:
    - title, description, keywords, source, license, attribution

    Provides descriptive information about the asset, including queryable tags,
    columnar schema information (for tabular/joined), and spatial type
    information (for spatial/joined).

    Attributes:
        tags: Queryable tags for matching (primitive scalars only)
        header_mapping: Optional header mapping for tabular assets (original → cleaned)
        column_schema: Optional column schema for tabular/joined assets
        geometry_type: Optional OGC geometry type for spatial/joined assets
    """

    # Inherited from HumanMetadataMixin:
    # title, description, keywords, source, license, attribution

    # Asset-specific fields
    tags: dict[str, TagValue] = Field(
        default_factory=dict,
        description="Queryable tags for matching (str/int/float/bool values only)",
    )

    # Columnar asset fields (tabular and joined)
    header_mapping: dict[str, str] | None = Field(
        None,
        description="Header mapping for tabular assets (original → cleaned column names)",
    )
    column_schema: dict[str, ColumnInfo] | None = Field(
        None,
        description="Column schema for tabular/joined assets (column_name → ColumnInfo)",
    )

    # Spatial asset fields (spatial and joined)
    geometry_type: str | None = Field(
        None,
        description="OGC geometry type (POINT, POLYGON, MULTIPOLYGON, etc.) for spatial/joined assets",
    )

    @classmethod
    def from_manifest_metadata(
        cls, manifest_meta: "ManifestMetadata", **additional_fields
    ) -> "AssetMetadata":
        """
        Create AssetMetadata from ManifestMetadata with safe field propagation.

        This factory method ensures consistent propagation of human semantic
        metadata from manifests to assets, with defensive copies for mutable fields.

        Args:
            manifest_meta: Source manifest metadata
            **additional_fields: System-derived fields (geometry_type, column_schema, etc.)

        Returns:
            AssetMetadata instance with propagated human metadata + system fields

        Example:
            >>> asset_meta = AssetMetadata.from_manifest_metadata(
            ...     manifest.metadata,
            ...     geometry_type="POINT",
            ...     column_schema={"col1": ColumnInfo(...)}
            ... )
        """
        return cls(
            # Propagate human metadata from mixin
            title=manifest_meta.title,
            description=manifest_meta.description,
            keywords=manifest_meta.keywords.copy(),  # Defensive copy
            source=manifest_meta.source,
            license=manifest_meta.license,
            attribution=manifest_meta.attribution,
            # Propagate asset tags (defensive copy)
            tags=manifest_meta.tags.copy(),
            # Add system-derived fields
            **additional_fields,
        )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "title": "Urban Land Use Dataset",
                "description": "High-resolution land use classification for metropolitan area",
                "keywords": ["landuse", "urban", "classification"],
                "source": "City Planning Department",
                "license": "CC-BY-4.0",
                "attribution": "City Planning Department",
                "tags": {"project": "ALPHA", "period": "2021-01-01/2021-12-31"},
                "header_mapping": None,
                "column_schema": None,
                "geometry_type": "MULTIPOLYGON",
            }
        }
    )


# =============================================================================
# Asset Model
# =============================================================================


class Asset(BaseModel):
    """
    Asset registry model - represents a processed dataset (spatial or tabular).

    This is the core model for tracking processed assets in the data lake.
    Each asset represents a versioned dataset stored in MinIO with metadata
    tracked in MongoDB.

    Attributes:
        s3_key: S3 object key (e.g., "data-lake/dataset_001/v1/data.parquet")
        dataset_id: Unique dataset identifier
        version: Asset version number (>= 1)
        content_hash: SHA256 hash of the asset content
        run_id: MongoDB run document ObjectId (links to runs collection)
        kind: Asset kind (spatial, tabular, or joined)
        format: Output format (geoparquet, cog, geojson, or parquet)
        crs: Coordinate Reference System (required for spatial/joined, None for tabular)
        bounds: Geographic bounding box (required for spatial/joined, None for tabular)
        metadata: Asset metadata (title, description, source, license, tags, header_mapping)
        created_at: Creation timestamp
        updated_at: Last update timestamp (optional)
    """

    s3_key: S3Key = Field(
        ..., description="S3 object key (e.g., 'data-lake/dataset_001/v1/data.parquet')"
    )
    dataset_id: str = Field(..., description="Unique dataset identifier")
    version: int = Field(..., ge=1, description="Asset version number (>= 1)")
    content_hash: ContentHash = Field(..., description="SHA256 content hash")
    run_id: str = Field(..., description="MongoDB run document ObjectId")
    kind: AssetKind = Field(..., description="Asset kind (spatial, tabular, or joined)")
    format: OutputFormat = Field(..., description="Output format")
    crs: CRS | None = Field(
        None,
        description="Coordinate Reference System (required for spatial/joined, None for tabular)",
    )
    bounds: Bounds | None = Field(
        None,
        description="Geographic bounding box (required for spatial/joined, None for tabular)",
    )
    metadata: AssetMetadata = Field(..., description="Asset metadata")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime | None = Field(None, description="Last update timestamp")

    @model_validator(mode="after")
    def validate_kind_crs_bounds(self) -> "Asset":
        """
        Validate kind/crs/bounds coherence.

        Rules:
        - kind == TABULAR → crs is None and bounds is None
        - kind in {SPATIAL, JOINED} → crs is not None

        Raises:
            ValueError: If kind/crs/bounds are inconsistent
        """
        if self.kind == AssetKind.TABULAR:
            if self.crs is not None:
                raise ValueError(
                    f"Asset with kind 'tabular' must have crs=None, got crs={self.crs}"
                )
            if self.bounds is not None:
                raise ValueError(
                    f"Asset with kind 'tabular' must have bounds=None, got bounds={self.bounds}"
                )
        else:  # SPATIAL or JOINED
            if self.crs is None:
                raise ValueError(
                    f"Asset with kind '{self.kind.value}' must have crs set, got crs=None"
                )
        return self

    @model_validator(mode="after")
    def validate_metadata_completeness(self) -> "Asset":
        """
        Validate kind-specific metadata requirements.

        Rules:
        - kind == SPATIAL or JOINED → metadata.geometry_type MUST be set (ERROR - M2)
        - kind in {TABULAR, SPATIAL, JOINED} → metadata.column_schema MUST be set (ERROR - M3)

        This enforces that system-derived metadata is populated based on asset kind.
        """
        # Spatial and joined assets MUST have geometry_type (enforced in M2)
        if self.kind in {AssetKind.SPATIAL, AssetKind.JOINED}:
            if self.metadata.geometry_type is None:
                raise ValueError(
                    f"Asset with kind '{self.kind.value}' requires metadata.geometry_type"
                )

        # ALL columnar assets MUST have column_schema (enforced in M3)
        if self.kind in {AssetKind.TABULAR, AssetKind.SPATIAL, AssetKind.JOINED}:
            if self.metadata.column_schema is None:
                raise ValueError(
                    f"Asset with kind '{self.kind.value}' requires metadata.column_schema"
                )

        return self

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
                "run_id": "507f1f77bcf86cd799439011",
                "kind": "spatial",
                "format": "geoparquet",
                "crs": "EPSG:4326",
                "bounds": {"minx": -180.0, "miny": -90.0, "maxx": 180.0, "maxy": 90.0},
                "metadata": {
                    "title": "Sample Dataset",
                    "description": "Test dataset",
                    "source": "Test Source",
                    "license": "CC-BY-4.0",
                    "tags": {},
                },
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": None,
            }
        }
    )
