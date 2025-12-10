# =============================================================================
# Data Models Library
# =============================================================================
# Pydantic models and schemas for the Spatial ETL Pipeline.
# See CONTEXT.md for detailed documentation.
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
    ManifestMetadata,
    Manifest,
    ManifestStatus,
    ManifestRecord,
    S3Path,
)

# Asset models
from .asset import (
    S3Key,
    ContentHash,
    AssetMetadata,
    Asset,
)

# Configuration models
from .config import (
    MinIOSettings,
    MongoSettings,
    PostGISSettings,
    DagsterPostgresSettings,
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
    "ManifestMetadata",
    "Manifest",
    "ManifestStatus",
    "ManifestRecord",
    "S3Path",
    # Asset models
    "S3Key",
    "ContentHash",
    "AssetMetadata",
    "Asset",
    # Configuration models
    "MinIOSettings",
    "MongoSettings",
    "PostGISSettings",
    "DagsterPostgresSettings",
]

