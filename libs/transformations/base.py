# =============================================================================
# Base Classes for Transformation Steps
# =============================================================================
# Abstract base classes for all transformation steps.
# =============================================================================

from abc import ABC, abstractmethod

__all__ = ["TransformStep", "VectorStep"]


class TransformStep(ABC):
    """
    Base class for all transformation steps.
    
    All transformation steps must implement the `generate_sql` method
    to produce SQL that will be executed in the PostGIS schema context.
    """

    @abstractmethod
    def generate_sql(self, schema: str, input_table: str, output_table: str) -> str:
        """
        Generate SQL for this transformation step.
        
        Args:
            schema: Schema name (for potential future use, currently not needed
                    since execute_sql sets search_path)
            input_table: Input table name (unqualified, search_path is set)
            output_table: Output table name (unqualified, search_path is set)
        
        Returns:
            SQL string to execute
        """
        pass


class VectorStep(TransformStep):
    """
    Base class for vector transformation steps.
    
    Marker class for type hints and future extensibility.
    No additional methods beyond TransformStep interface.
    """
    pass

