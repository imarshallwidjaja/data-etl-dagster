# =============================================================================
# Recipe Registry
# =============================================================================
# Intent-based recipe lookup for transformation steps.
# =============================================================================

from typing import List
from .base import VectorStep
from .vector import NormalizeCRSStep, SimplifyGeometryStep, CreateSpatialIndexStep

__all__ = ["RecipeRegistry"]


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

