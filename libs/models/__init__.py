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

# Configuration models
from .config import (
    MinIOSettings,
    MongoSettings,
    PostGISSettings,
    DagsterPostgresSettings,
)

__all__ = [
    "MinIOSettings",
    "MongoSettings",
    "PostGISSettings",
    "DagsterPostgresSettings",
]

