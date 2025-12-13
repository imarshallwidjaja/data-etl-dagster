# =============================================================================
# Unit Tests: Recipe Registry
# =============================================================================

from libs.transformations import (
    RecipeRegistry,
    NormalizeCRSStep,
    SimplifyGeometryStep,
    CreateSpatialIndexStep,
)


# =============================================================================
# Test: get_vector_recipe
# =============================================================================

def test_registry_returns_default_recipe_for_unknown_intent():
    """Test that registry returns default recipe for unknown intent."""
    recipe = RecipeRegistry.get_vector_recipe("unknown_intent")
    
    assert len(recipe) == 3
    assert isinstance(recipe[0], NormalizeCRSStep)
    assert isinstance(recipe[1], SimplifyGeometryStep)
    assert isinstance(recipe[2], CreateSpatialIndexStep)


def test_registry_returns_correct_recipe_for_ingest_vector():
    """Test that registry returns correct recipe for ingest_vector intent."""
    recipe = RecipeRegistry.get_vector_recipe("ingest_vector")
    
    assert len(recipe) == 3
    assert isinstance(recipe[0], NormalizeCRSStep)
    assert isinstance(recipe[1], SimplifyGeometryStep)
    assert isinstance(recipe[2], CreateSpatialIndexStep)


def test_registry_returns_correct_recipe_for_ingest_road_network():
    """Test that registry returns correct recipe for ingest_road_network intent."""
    recipe = RecipeRegistry.get_vector_recipe("ingest_road_network")
    
    assert len(recipe) == 3
    assert isinstance(recipe[0], NormalizeCRSStep)
    assert isinstance(recipe[1], SimplifyGeometryStep)
    assert isinstance(recipe[2], CreateSpatialIndexStep)


def test_registry_returns_steps_in_correct_order():
    """Test that registry returns steps in the correct order."""
    recipe = RecipeRegistry.get_vector_recipe("ingest_vector")
    
    # Verify order: NormalizeCRSStep -> SimplifyGeometryStep -> CreateSpatialIndexStep
    assert isinstance(recipe[0], NormalizeCRSStep)
    assert isinstance(recipe[1], SimplifyGeometryStep)
    assert isinstance(recipe[2], CreateSpatialIndexStep)


def test_registry_returns_new_instances():
    """Test that registry returns new step instances (not shared)."""
    recipe1 = RecipeRegistry.get_vector_recipe("ingest_vector")
    recipe2 = RecipeRegistry.get_vector_recipe("ingest_vector")
    
    # Steps should be different instances
    assert recipe1[0] is not recipe2[0]
    assert recipe1[1] is not recipe2[1]
    assert recipe1[2] is not recipe2[2]
    
    # But should be same type
    assert type(recipe1[0]) == type(recipe2[0])
    assert type(recipe1[1]) == type(recipe2[1])
    assert type(recipe1[2]) == type(recipe2[2])


def test_registry_default_recipe_matches_known_intents():
    """Test that default recipe matches known intent recipes."""
    default_recipe = RecipeRegistry.get_vector_recipe("unknown")
    ingest_vector_recipe = RecipeRegistry.get_vector_recipe("ingest_vector")
    
    # Should have same structure
    assert len(default_recipe) == len(ingest_vector_recipe)
    assert type(default_recipe[0]) == type(ingest_vector_recipe[0])
    assert type(default_recipe[1]) == type(ingest_vector_recipe[1])
    assert type(default_recipe[2]) == type(ingest_vector_recipe[2])

