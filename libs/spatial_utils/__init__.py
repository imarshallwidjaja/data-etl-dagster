# =============================================================================
# Spatial Utils Library
# =============================================================================
# Python wrappers for GDAL command-line tools and spatial operations.
# See AGENTS.md for detailed documentation.
# =============================================================================

"""
Spatial utilities for the ETL pipeline.

This library provides:
- RunIdSchemaMapping: Bidirectional run_id ↔ schema_name conversion
- GDALWrapper: Mockable wrapper for GDAL CLI operations
- Format converters: SHP → GeoParquet, TIFF → COG, etc.
- CRS utilities: Coordinate reference system handling
- Validators: Spatial file validation
"""

from .schema_mapper import RunIdSchemaMapping
from .tabular_headers import normalize_headers

__version__ = "0.1.0"

__all__ = [
    "RunIdSchemaMapping",
    "normalize_headers",
]

