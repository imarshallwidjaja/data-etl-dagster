# =============================================================================
# Data Models Library
# =============================================================================
# Pydantic models and schemas for the Spatial ETL Pipeline.
# See AGENTS.md for detailed documentation.
# =============================================================================

"""
Data models for the ETL pipeline.

This library provides:
- Manifest: Ingestion manifest schema
- Asset: Asset registry schema
- Spatial types: Bounds, CRS, etc.
- Configuration models
"""

__version__ = "0.1.0"

# Spatial types
from .spatial import (
    CRS,
    Bounds,
    FileType,
    OutputFormat,
    validate_crs,
)

# Manifest models
from .manifest import (
    ComplexSpreadsheetConfig,
    ComplexSpreadsheetConfigV1,
    ComplexSpreadsheetConfigV2,
    ComplexSpreadsheetTemplateParamsV1,
    ComplexSpreadsheetTemplateParamsV2,
    FileEntry,
    JoinConfig,
    ManifestMetadata,
    Manifest,
    ManifestStatus,
    ManifestRecord,
    S3Path,
    TagValue,
)

# Base models
from .base import HumanMetadataMixin

# Asset models
from .asset import (
    S3Key,
    ContentHash,
    AssetKind,
    ColumnInfo,
    AssetMetadata,
    Asset,
)

# Blob models
from .blob import Blob

# Run models
from .run import (
    Run,
    RunStatus,
)

# Activity models
from .activity import (
    ActivityLog,
    ActivityAction,
    ActivityResourceType,
)

# Artifact models
from .artifact import Artifact

# Configuration models
from .config import (
    MinIOSettings,
    MongoSettings,
    PostGISSettings,
    DagsterPostgresSettings,
    GDALSettings,
)

__all__ = [
    # Spatial types
    "CRS",
    "Bounds",
    "FileType",
    "OutputFormat",
    "validate_crs",
    # Manifest models
    "ComplexSpreadsheetConfig",
    "ComplexSpreadsheetConfigV1",
    "ComplexSpreadsheetConfigV2",
    "ComplexSpreadsheetTemplateParamsV1",
    "ComplexSpreadsheetTemplateParamsV2",
    "FileEntry",
    "JoinConfig",
    "ManifestMetadata",
    "Manifest",
    "ManifestStatus",
    "ManifestRecord",
    "S3Path",
    "TagValue",
    # Base models
    "HumanMetadataMixin",
    # Asset models
    "S3Key",
    "ContentHash",
    "AssetKind",
    "ColumnInfo",
    "AssetMetadata",
    "Asset",
    # Blob models
    "Blob",
    # Run models
    "Run",
    "RunStatus",
    # Activity models
    "ActivityLog",
    "ActivityAction",
    "ActivityResourceType",
    # Artifact models
    "Artifact",
    # Configuration models
    "MinIOSettings",
    "MongoSettings",
    "PostGISSettings",
    "DagsterPostgresSettings",
    "GDALSettings",
]
