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
    def get_vector_recipe(intent: str) -> List[VectorStep]:
        """
        Get vector transformation recipe for given intent.
        
        Returns a list of step instances that will be executed in order.
        Steps are instantiated fresh each time (no shared state).
        
        Args:
            intent: Manifest intent field (e.g., "ingest_vector", "ingest_road_network")
        
        Returns:
            List of VectorStep instances to execute
        """
        # Default recipe (same as current hardcoded behavior)
        default_recipe = [
            NormalizeCRSStep(),
            SimplifyGeometryStep(),
            CreateSpatialIndexStep(),
        ]
        
        # Future: intent-specific recipes
        recipes = {
            "ingest_vector": default_recipe,
            "ingest_road_network": default_recipe,  # Can customize later
        }
        
        return recipes.get(intent, default_recipe)

