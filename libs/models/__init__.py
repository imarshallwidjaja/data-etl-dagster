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
    FileEntry,
    JoinConfig,
    ManifestMetadata,
    Manifest,
    ManifestStatus,
    ManifestRecord,
    S3Path,
    TagValue,
)

# Asset models
from .asset import (
    S3Key,
    ContentHash,
    AssetKind,
    AssetMetadata,
    Asset,
)

# Run models
from .run import (
    Run,
    RunStatus,
)

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
    "FileEntry",
    "JoinConfig",
    "ManifestMetadata",
    "Manifest",
    "ManifestStatus",
    "ManifestRecord",
    "S3Path",
    "TagValue",
    # Asset models
    "S3Key",
    "ContentHash",
    "AssetKind",
    "AssetMetadata",
    "Asset",
    # Run models
    "Run",
    "RunStatus",
    # Configuration models
    "MinIOSettings",
    "MongoSettings",
    "PostGISSettings",
    "DagsterPostgresSettings",
    "GDALSettings",
]
