# =============================================================================
# Transformations Library
# =============================================================================
# Recipe-based transformation architecture for spatial ETL pipeline.
# See CONTEXT.md for detailed documentation.
# =============================================================================

"""
Transformations library for the ETL pipeline.

This library provides:
- TransformStep: Base class for all transformation steps
- VectorStep: Base class for vector transformation steps
- Vector transformation steps: NormalizeCRSStep, SimplifyGeometryStep, CreateSpatialIndexStep
- RecipeRegistry: Intent-based recipe lookup
"""

from .base import TransformStep, VectorStep
from .vector import NormalizeCRSStep, SimplifyGeometryStep, CreateSpatialIndexStep
from .registry import RecipeRegistry

__all__ = [
    "TransformStep",
    "VectorStep",
    "NormalizeCRSStep",
    "SimplifyGeometryStep",
    "CreateSpatialIndexStep",
    "RecipeRegistry",
]

