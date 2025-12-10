# =============================================================================
# Spatial Utils Library
# =============================================================================
# Python wrappers for GDAL command-line tools and spatial operations.
# See CONTEXT.md for detailed documentation.
# =============================================================================

"""
Spatial utilities for the ETL pipeline.

This library provides:
- GDALWrapper: Mockable wrapper for GDAL CLI operations
- Format converters: SHP → GeoParquet, TIFF → COG, etc.
- CRS utilities: Coordinate reference system handling
- Validators: Spatial file validation
"""

__version__ = "0.1.0"

