# =============================================================================
# Vector Transformation Steps
# =============================================================================
# Concrete vector transformation step implementations.
# =============================================================================

from .base import VectorStep

__all__ = ["NormalizeCRSStep", "SimplifyGeometryStep", "CreateSpatialIndexStep"]


class NormalizeCRSStep(VectorStep):
    """
    Transform geometries to a target CRS (default: EPSG:4326).
    
    Creates a new table with geometries transformed to the target CRS.
    The original geometry column is replaced with the transformed geometry.
    """

    def __init__(self, target_crs: int = 4326):
        """
        Initialize CRS normalization step.
        
        Args:
            target_crs: Target EPSG code (default: 4326)
        """
        self.target_crs = target_crs

    def generate_sql(self, schema: str, input_table: str, output_table: str) -> str:
        """
        Generate SQL to transform geometries to target CRS.
        
        Args:
            schema: Schema name (unused, kept for interface consistency)
            input_table: Input table name
            output_table: Output table name
        
        Returns:
            SQL string to create table with transformed geometries
        """
        return f"""
        CREATE TABLE {output_table} AS
        SELECT *, ST_Transform(geom, {self.target_crs}) as geom
        FROM {input_table};
        """


class SimplifyGeometryStep(VectorStep):
    """
    Simplify geometries while preserving topology.
    
    Creates a new table with simplified geometries using ST_SimplifyPreserveTopology.
    Adds a new column `geom_simple` with the simplified geometry.
    """

    def __init__(self, tolerance: float = 0.0001):
        """
        Initialize geometry simplification step.
        
        Args:
            tolerance: Simplification tolerance (default: 0.0001)
        """
        self.tolerance = tolerance

    def generate_sql(self, schema: str, input_table: str, output_table: str) -> str:
        """
        Generate SQL to simplify geometries.
        
        Args:
            schema: Schema name (unused, kept for interface consistency)
            input_table: Input table name
            output_table: Output table name
        
        Returns:
            SQL string to create table with simplified geometries
        """
        return f"""
        CREATE TABLE {output_table} AS
        SELECT *, ST_SimplifyPreserveTopology(geom, {self.tolerance}) as geom_simple
        FROM {input_table};
        """


class CreateSpatialIndexStep(VectorStep):
    """
    Create GIST spatial index on geometry column.
    
    This step operates on an existing table and does not create a new table.
    The output_table parameter is ignored for index steps.
    """

    def generate_sql(self, schema: str, input_table: str, output_table: str) -> str:
        """
        Generate SQL to create spatial index.
        
        Args:
            schema: Schema name (unused, kept for interface consistency)
            input_table: Table name to create index on
            output_table: Ignored for index steps
        
        Returns:
            SQL string to create GIST spatial index
        """
        return f"CREATE INDEX idx_{input_table}_geom ON {input_table} USING GIST (geom);"

