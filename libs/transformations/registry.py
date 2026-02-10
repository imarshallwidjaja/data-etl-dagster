# =============================================================================
# Recipe Registry
# =============================================================================
# Intent-based recipe lookup for transformation steps.
# =============================================================================

from typing import Callable, List

import pyarrow as pa

from .base import VectorStep
from .tabular_readers import read_csv_to_arrow, read_parquet_to_arrow
from .vector import NormalizeCRSStep, SimplifyGeometryStep, CreateSpatialIndexStep

__all__ = [
    "RecipeRegistry",
    "normalize_tabular_format",
    "get_tabular_reader",
]


def normalize_tabular_format(file_format: str) -> str:
    """Normalize tabular format names into canonical lowercase values."""
    normalized = file_format.strip().lower() if isinstance(file_format, str) else ""
    if normalized in {"csv", "parquet"}:
        return normalized

    raise ValueError(
        f"Unsupported tabular format '{file_format}'. Supported formats: CSV, Parquet"
    )


def get_tabular_reader(file_format: str) -> Callable[[str], pa.Table]:
    """Resolve tabular file reader by format (case-insensitive)."""
    normalized_format = normalize_tabular_format(file_format)
    readers: dict[str, Callable[[str], pa.Table]] = {
        "csv": read_csv_to_arrow,
        "parquet": read_parquet_to_arrow,
    }
    return readers[normalized_format]


class RecipeRegistry:
    """
    Registry for transformation recipes by intent.

    Maps manifest intent fields to lists of transformation steps.
    Provides default recipe for unknown intents to maintain backward compatibility.
    """

    @staticmethod
    def get_vector_recipe(intent: str, geom_column: str = "geom") -> List[VectorStep]:
        """
        Get vector transformation recipe for given intent.

        Returns a list of step instances that will be executed in order.
        Steps are instantiated fresh each time (no shared state).

        Args:
            intent: Manifest intent field (e.g., "ingest_vector", "ingest_road_network")
            geom_column: Name of geometry column to operate on (default: "geom")

        Returns:
            List of VectorStep instances to execute
        """
        # Default recipe with configurable geometry column
        default_recipe = [
            NormalizeCRSStep(target_crs=4326, geom_column=geom_column),
            SimplifyGeometryStep(tolerance=0.0001, geom_column=geom_column),
            CreateSpatialIndexStep(geom_column=geom_column),
        ]

        # Building footprints recipe with stronger simplification for visible geometry changes
        # Tolerance of 0.001 degrees ≈ 111m at equator, produces obviously simplified outlines
        building_footprints_recipe = [
            NormalizeCRSStep(target_crs=4326, geom_column=geom_column),
            SimplifyGeometryStep(tolerance=0.001, geom_column=geom_column),
            CreateSpatialIndexStep(geom_column=geom_column),
        ]

        # Intent-specific recipes
        recipes = {
            "ingest_vector": default_recipe,
            "ingest_road_network": default_recipe,  # Can customize later
            "ingest_building_footprints": building_footprints_recipe,
        }

        return recipes.get(intent, default_recipe)

    @staticmethod
    def normalize_tabular_format(file_format: str) -> str:
        """Compatibility wrapper around module-level normalizer."""
        return normalize_tabular_format(file_format)

    @staticmethod
    def get_tabular_reader(file_format: str) -> Callable[[str], pa.Table]:
        """Compatibility wrapper around module-level reader lookup."""
        return get_tabular_reader(file_format)
