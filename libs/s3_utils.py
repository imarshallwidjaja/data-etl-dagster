# =============================================================================
# S3 Path Utilities
# =============================================================================
# Shared utilities for parsing and manipulating S3 paths.
# Used by both spatial and tabular pipelines.
# =============================================================================

"""
S3 path utilities for the ETL pipeline.

This module provides functions for:
- Parsing S3 paths into bucket and key components
- Extracting keys from S3 paths
- Converting S3 paths to GDAL's vsis3 format
"""

from typing import Tuple

__all__ = [
    "parse_s3_path",
    "extract_s3_key",
    "s3_to_vsis3",
]


def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    """
    Parse S3 path into bucket and key components.
    
    Args:
        s3_path: Full S3 path (e.g., "s3://landing-zone/batch_001/data.csv")
    
    Returns:
        Tuple of (bucket, key) e.g., ("landing-zone", "batch_001/data.csv")
    
    Raises:
        ValueError: If path is not valid s3:// format or missing key
    
    Examples:
        >>> parse_s3_path("s3://landing-zone/batch_001/data.csv")
        ('landing-zone', 'batch_001/data.csv')
        >>> parse_s3_path("s3://data-lake/dataset_abc/v1/output.parquet")
        ('data-lake', 'dataset_abc/v1/output.parquet')
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(
            f"Invalid S3 path format: '{s3_path}'. Must start with 's3://'"
        )
    
    path_without_prefix = s3_path[5:]  # Remove "s3://"
    parts = path_without_prefix.split("/", 1)
    
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(
            f"Invalid S3 path format: '{s3_path}'. Expected 's3://bucket/key'"
        )
    
    return parts[0], parts[1]


def extract_s3_key(s3_path: str) -> str:
    """
    Extract the key portion from an S3 path.
    
    For paths that are already keys (not s3:// format), returns them unchanged.
    
    Args:
        s3_path: Full S3 path (e.g., "s3://landing-zone/batch_001/data.csv")
                 or already a key (e.g., "batch_001/data.csv")
    
    Returns:
        S3 key (e.g., "batch_001/data.csv")
    
    Examples:
        >>> extract_s3_key("s3://landing-zone/batch_001/data.csv")
        'batch_001/data.csv'
        >>> extract_s3_key("batch_001/data.csv")
        'batch_001/data.csv'
    """
    if s3_path.startswith("s3://"):
        _, key = parse_s3_path(s3_path)
        return key
    return s3_path


def s3_to_vsis3(s3_path: str) -> str:
    """
    Convert S3 path to GDAL's vsis3 virtual file system format.
    
    For paths that are already in vsis3 format or other formats,
    returns them unchanged.
    
    Args:
        s3_path: Full S3 path (e.g., "s3://landing-zone/batch_001/data.geojson")
    
    Returns:
        vsis3 path (e.g., "/vsis3/landing-zone/batch_001/data.geojson")
    
    Examples:
        >>> s3_to_vsis3("s3://landing-zone/batch_001/data.geojson")
        '/vsis3/landing-zone/batch_001/data.geojson'
        >>> s3_to_vsis3("/vsis3/landing-zone/batch_001/data.geojson")
        '/vsis3/landing-zone/batch_001/data.geojson'
    """
    if s3_path.startswith("s3://"):
        return s3_path.replace("s3://", "/vsis3/", 1)
    return s3_path

