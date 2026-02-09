# =============================================================================
# Recipe Registry
# =============================================================================
# Intent-based recipe lookup for transformation steps.
# Format-based tabular reader and suffix lookup.
# =============================================================================

from typing import Callable, List

import pyarrow as pa

from .base import VectorStep
from .vector import NormalizeCRSStep, SimplifyGeometryStep, CreateSpatialIndexStep
from .tabular_readers import read_csv_to_arrow, read_parquet_to_arrow

__all__ = ["RecipeRegistry"]

# Tabular format registry: normalised-lowercase key → (reader, suffix)
_TABULAR_FORMATS: dict[str, tuple[Callable[[str], pa.Table], str]] = {
    "csv": (read_csv_to_arrow, ".csv"),
    "parquet": (read_parquet_to_arrow, ".parquet"),
}


class RecipeRegistry:
    """
    Registry for transformation recipes by intent.

    Maps manifest intent fields to lists of transformation steps.
    Provides default recipe for unknown intents to maintain backward compatibility.

    Also resolves tabular file-format strings to reader callables and temp-file
    suffixes (case-insensitive).
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

    # --------------------------------------------------------------------- #
    # Tabular format helpers
    # --------------------------------------------------------------------- #

    @staticmethod
    def get_tabular_reader(file_format: str) -> Callable[[str], pa.Table]:
        """
        Resolve a reader callable for *file_format* (case-insensitive).

        Args:
            file_format: Format string from ``FileEntry.format``
                         (e.g. ``"CSV"``, ``"Parquet"``).

        Returns:
            A callable ``(path: str) -> pa.Table``.

        Raises:
            ValueError: If *file_format* is not supported.
        """
        key = file_format.strip().lower()
        entry = _TABULAR_FORMATS.get(key)
        if entry is None:
            supported = ", ".join(sorted(_TABULAR_FORMATS))
            raise ValueError(
                f"Unsupported tabular format '{file_format}'. Supported: {supported}"
            )
        return entry[0]

    @staticmethod
    def get_tabular_suffix(file_format: str) -> str:
        """
        Return the temp-file suffix for *file_format* (case-insensitive).

        Args:
            file_format: Format string from ``FileEntry.format``.

        Returns:
            File extension including the dot (e.g. ``".csv"``).

        Raises:
            ValueError: If *file_format* is not supported.
        """
        key = file_format.strip().lower()
        entry = _TABULAR_FORMATS.get(key)
        if entry is None:
            supported = ", ".join(sorted(_TABULAR_FORMATS))
            raise ValueError(
                f"Unsupported tabular format '{file_format}'. Supported: {supported}"
            )
        return entry[1]
