# =============================================================================
# Unit Tests: Transformations Base Classes
# =============================================================================

import pytest
from abc import ABC

from libs.transformations import TransformStep, VectorStep


# =============================================================================
# Test: TransformStep Abstract Base Class
# =============================================================================

def test_transform_step_is_abstract():
    """Test that TransformStep cannot be instantiated directly."""
    with pytest.raises(TypeError):
        TransformStep()


def test_transform_step_is_abc():
    """Test that TransformStep is an ABC."""
    assert issubclass(TransformStep, ABC)


# =============================================================================
# Test: VectorStep Base Class
# =============================================================================

def test_vector_step_is_subclass_of_transform_step():
    """Test that VectorStep is a subclass of TransformStep."""
    assert issubclass(VectorStep, TransformStep)


def test_vector_step_can_be_subclassed():
    """Test that VectorStep can be subclassed."""
    
    class TestStep(VectorStep):
        def generate_sql(self, schema: str, input_table: str, output_table: str) -> str:
            return f"SELECT * FROM {input_table};"
    
    step = TestStep()
    assert isinstance(step, VectorStep)
    assert isinstance(step, TransformStep)
    assert step.generate_sql("test_schema", "test_table", "output") == "SELECT * FROM test_table;"

