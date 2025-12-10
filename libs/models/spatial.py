# =============================================================================
# Spatial Types Module
# =============================================================================
# Provides reusable spatial data types with validation:
# - CRS: Coordinate Reference System (EPSG, WKT, PROJ)
# - Bounds: Geographic bounding box
# - FileType: Raster or vector file type
# - OutputFormat: Supported output formats
# =============================================================================

import re
from enum import Enum
from typing import Annotated
from pydantic import BaseModel, Field, BeforeValidator, model_validator

__all__ = ["CRS", "Bounds", "FileType", "OutputFormat", "validate_crs"]


# =============================================================================
# Enums
# =============================================================================

class FileType(str, Enum):
    """File type classification: raster (imagery) or vector (features)."""
    RASTER = "raster"
    VECTOR = "vector"


class OutputFormat(str, Enum):
    """Supported output formats for processed spatial data."""
    GEOPARQUET = "geoparquet"
    COG = "cog"
    GEOJSON = "geojson"


# =============================================================================
# CRS (Coordinate Reference System)
# =============================================================================

def validate_crs(value: str) -> str:
    """
    Validate and normalize a Coordinate Reference System string.
    
    Supports three formats:
    1. EPSG codes: "EPSG:4326", "epsg:28354" (case-insensitive, normalized to uppercase)
    2. WKT strings: "PROJCS[...]", "GEOGCS[...]", "COMPD_CS[...]", "GEOCCS[...]"
    3. PROJ strings: "+proj=utm +zone=55 +south ..."
    
    Args:
        value: CRS string to validate
        
    Returns:
        Normalized CRS string (EPSG codes are uppercased)
        
    Raises:
        TypeError: If the value is not a string
        ValueError: If the CRS format is invalid
    """
    # Explicit type checking
    if not isinstance(value, str):
        raise TypeError(f"CRS must be a string, got {type(value).__name__}")
    
    if not value:
        raise ValueError("CRS must be a non-empty string")
    
    value = value.strip()
    
    if not value:
        raise ValueError("CRS cannot be empty or whitespace only")
    
    # 1. Check for EPSG code (e.g., "EPSG:4326", "epsg:28354")
    epsg_pattern = r'^EPSG:\d{4,6}$'
    if re.match(epsg_pattern, value, re.IGNORECASE):
        return value.upper()  # Normalize to uppercase
    
    # 2. Check for WKT (Well-Known Text) format
    # WKT strings start with PROJCS[, GEOGCS[, COMPD_CS[, or GEOCCS[
    # and end with a closing bracket ]
    wkt_starts = ('PROJCS[', 'GEOGCS[', 'COMPD_CS[', 'GEOCCS[')
    if value.startswith(wkt_starts) and value.endswith(']'):
        # Basic validation: check for balanced brackets (heuristic)
        if value.count('[') == value.count(']'):
            return value
    
    # 3. Check for PROJ string format (starts with +proj=)
    if value.startswith('+proj='):
        # Basic validation: should contain at least one parameter
        if '=' in value[6:]:  # After "+proj="
            return value
    
    # If none of the formats match, raise an error
    raise ValueError(
        f"Invalid CRS format. Must be one of:\n"
        f"  - EPSG code: 'EPSG:4326'\n"
        f"  - WKT string: 'PROJCS[...]' or 'GEOGCS[...]'\n"
        f"  - PROJ string: '+proj=utm +zone=55 ...'\n"
        f"Got: {value[:100]}{'...' if len(value) > 100 else ''}"
    )


CRS = Annotated[
    str,
    Field(..., description="Coordinate Reference System"),
    BeforeValidator(validate_crs)
]
"""
Coordinate Reference System type.

Validates and normalizes CRS strings supporting:
- EPSG codes: "EPSG:4326" (case-insensitive, normalized to uppercase)
- WKT strings: "PROJCS[...]", "GEOGCS[...]", etc.
- PROJ strings: "+proj=utm +zone=55 +south ..."

Examples:
    >>> crs: CRS = "EPSG:4326"  # Valid
    >>> crs: CRS = "epsg:28354"  # Valid, normalized to "EPSG:28354"
    >>> crs: CRS = "PROJCS[...]"  # Valid WKT
    >>> crs: CRS = "+proj=utm +zone=55"  # Valid PROJ string
"""


# =============================================================================
# Bounds (Geographic Bounding Box)
# =============================================================================

class Bounds(BaseModel):
    """
    Geographic bounding box defining a rectangular area.
    
    Represents a bounding box with minimum and maximum X/Y coordinates.
    Validates that minx <= maxx and miny <= maxy (allows point bounds).
    
    Attributes:
        minx: Minimum X coordinate (west)
        miny: Minimum Y coordinate (south)
        maxx: Maximum X coordinate (east)
        maxy: Maximum Y coordinate (north)
    """
    
    minx: float = Field(..., description="Minimum X coordinate (west)")
    miny: float = Field(..., description="Minimum Y coordinate (south)")
    maxx: float = Field(..., description="Maximum X coordinate (east)")
    maxy: float = Field(..., description="Maximum Y coordinate (north)")
    
    @model_validator(mode='after')
    def validate_bounds(self) -> 'Bounds':
        """
        Validate that minx <= maxx and miny <= maxy.
        
        Allows point bounds (minx == maxx and/or miny == maxy).
        
        Raises:
            ValueError: If bounds are invalid (min > max)
        """
        if self.minx > self.maxx:
            raise ValueError(
                f"Invalid bounds: minx ({self.minx}) must be less than or equal to maxx ({self.maxx})"
            )
        if self.miny > self.maxy:
            raise ValueError(
                f"Invalid bounds: miny ({self.miny}) must be less than or equal to maxy ({self.maxy})"
            )
        return self
    
    @property
    def width(self) -> float:
        """Calculate the width (east-west extent) of the bounding box."""
        return self.maxx - self.minx
    
    @property
    def height(self) -> float:
        """Calculate the height (north-south extent) of the bounding box."""
        return self.maxy - self.miny
    
    @property
    def area(self) -> float:
        """Calculate the area of the bounding box (width × height)."""
        return self.width * self.height

