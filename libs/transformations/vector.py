# =============================================================================
# Vector Transformation Steps
# =============================================================================
# Concrete vector transformation step implementations.
# =============================================================================

import re

from .base import VectorStep

__all__ = ["NormalizeCRSStep", "SimplifyGeometryStep", "CreateSpatialIndexStep"]


def _validate_identifier(identifier: str, name: str) -> None:
    """
    Validate that an identifier matches PostgreSQL identifier allowlist.
    
    Args:
        identifier: Identifier to validate
        name: Name of the identifier (for error messages)
    
    Raises:
        ValueError: If identifier doesn't match allowlist pattern
    """
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', identifier):
        raise ValueError(
            f"Invalid {name}: {identifier}. "
            f"Must match pattern: ^[A-Za-z_][A-Za-z0-9_]*$"
        )


class NormalizeCRSStep(VectorStep):
    """
    Transform geometries to a target CRS (default: EPSG:4326).
    
    Creates a new table with geometries transformed to the target CRS.
    The original geometry column is replaced with the transformed geometry.
    """

    def __init__(self, target_crs: int = 4326, geom_column: str = "geom"):
        """
        Initialize CRS normalization step.

        Args:
            target_crs: Target EPSG code (default: 4326)
            geom_column: Name of geometry column to transform (default: "geom")
        """
        self.target_crs = target_crs
        self.geom_column = geom_column

    def generate_sql(self, schema: str, input_table: str, output_table: str) -> str:
        """
        Generate SQL to transform geometries to target CRS.

        Args:
            schema: Schema name
            input_table: Input table name
            output_table: Output table name

        Returns:
            SQL string to create table with transformed geometries
        
        Raises:
            ValueError: If any identifier is invalid
        """
        # Validate identifiers for SQL safety
        _validate_identifier(schema, "schema")
        _validate_identifier(input_table, "input_table")
        _validate_identifier(output_table, "output_table")
        _validate_identifier(self.geom_column, "geom_column")
        
        return f"""
        DO $$
        DECLARE
            col_list TEXT;
            select_prefix TEXT;
            geom_count INTEGER;
        BEGIN
            -- Validate exactly one geometry column exists and matches expected name
            SELECT COUNT(*) INTO geom_count
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = '{input_table}'
              AND udt_name = 'geometry';

            IF geom_count != 1 THEN
                RAISE EXCEPTION 'Table %.% must have exactly one geometry column, found %',
                    '{schema}', '{input_table}', geom_count;
            END IF;

            -- Check if the geometry column has the expected name
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = '{input_table}'
                  AND column_name = '{self.geom_column}'
                  AND udt_name = 'geometry'
            ) THEN
                RAISE EXCEPTION 'Geometry column "%" not found in table %.%',
                    '{self.geom_column}', '{schema}', '{input_table}';
            END IF;

            -- Build column list excluding the geometry column (with deterministic ordering)
            SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
            INTO col_list
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = '{input_table}'
              AND column_name != '{self.geom_column}';

            -- Normalize column list (handle NULL for geometry-only tables)
            col_list := COALESCE(col_list, '');

            -- Build safe select prefix (no dangling comma for geometry-only tables)
            select_prefix := CASE WHEN col_list = '' THEN '' ELSE col_list || ', ' END;

            -- Create output table with transformed geometry
            EXECUTE format(
                'CREATE TABLE %I AS SELECT %sST_Transform(%I, %s) AS %I FROM %I.%I',
                '{output_table}', select_prefix, '{self.geom_column}', {self.target_crs},
                '{self.geom_column}', '{schema}', '{input_table}'
            );
        END $$;
        """


class SimplifyGeometryStep(VectorStep):
    """
    Simplify geometries while preserving topology.

    Creates a new table with simplified geometries using ST_SimplifyPreserveTopology.
    Overwrites the geometry column with the simplified geometry.
    """

    def __init__(self, tolerance: float = 0.0001, geom_column: str = "geom"):
        """
        Initialize geometry simplification step.

        Args:
            tolerance: Simplification tolerance (default: 0.0001)
            geom_column: Name of geometry column to simplify (default: "geom")
        """
        self.tolerance = tolerance
        self.geom_column = geom_column

    def generate_sql(self, schema: str, input_table: str, output_table: str) -> str:
        """
        Generate SQL to simplify geometries.

        Args:
            schema: Schema name
            input_table: Input table name
            output_table: Output table name

        Returns:
            SQL string to create table with simplified geometries
        
        Raises:
            ValueError: If any identifier is invalid
        """
        # Validate identifiers for SQL safety
        _validate_identifier(schema, "schema")
        _validate_identifier(input_table, "input_table")
        _validate_identifier(output_table, "output_table")
        _validate_identifier(self.geom_column, "geom_column")
        
        return f"""
        DO $$
        DECLARE
            col_list TEXT;
            select_prefix TEXT;
            geom_count INTEGER;
        BEGIN
            -- Validate exactly one geometry column exists and matches expected name
            SELECT COUNT(*) INTO geom_count
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = '{input_table}'
              AND udt_name = 'geometry';

            IF geom_count != 1 THEN
                RAISE EXCEPTION 'Table %.% must have exactly one geometry column, found %',
                    '{schema}', '{input_table}', geom_count;
            END IF;

            -- Check if the geometry column has the expected name
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = '{input_table}'
                  AND column_name = '{self.geom_column}'
                  AND udt_name = 'geometry'
            ) THEN
                RAISE EXCEPTION 'Geometry column "%" not found in table %.%',
                    '{self.geom_column}', '{schema}', '{input_table}';
            END IF;

            -- Build column list excluding the geometry column (with deterministic ordering)
            SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
            INTO col_list
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = '{input_table}'
              AND column_name != '{self.geom_column}';

            -- Normalize column list (handle NULL for geometry-only tables)
            col_list := COALESCE(col_list, '');

            -- Build safe select prefix (no dangling comma for geometry-only tables)
            select_prefix := CASE WHEN col_list = '' THEN '' ELSE col_list || ', ' END;

            -- Create output table with simplified geometry
            EXECUTE format(
                'CREATE TABLE %I AS SELECT %sST_SimplifyPreserveTopology(%I, %s) AS %I FROM %I.%I',
                '{output_table}', select_prefix, '{self.geom_column}', {self.tolerance},
                '{self.geom_column}', '{schema}', '{input_table}'
            );
        END $$;
        """


class CreateSpatialIndexStep(VectorStep):
    """
    Create GIST spatial index on geometry column.

    This step operates on an existing table and does not create a new table.
    The output_table parameter is ignored for index steps.
    """

    def __init__(self, geom_column: str = "geom"):
        """
        Initialize spatial index creation step.

        Args:
            geom_column: Name of geometry column to index (default: "geom")
        """
        self.geom_column = geom_column

    def generate_sql(self, schema: str, input_table: str, output_table: str) -> str:
        """
        Generate SQL to create spatial index.

        Args:
            schema: Schema name
            input_table: Table name to create index on
            output_table: Ignored for index steps

        Returns:
            SQL string to create GIST spatial index
        
        Raises:
            ValueError: If any identifier is invalid
        """
        # Validate identifiers for SQL safety
        _validate_identifier(schema, "schema")
        _validate_identifier(input_table, "input_table")
        _validate_identifier(self.geom_column, "geom_column")
        
        return f"""
        DO $$
        DECLARE
            geom_count INTEGER;
        BEGIN
            -- Validate exactly one geometry column exists and matches expected name
            SELECT COUNT(*) INTO geom_count
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name = '{input_table}'
              AND udt_name = 'geometry';

            IF geom_count != 1 THEN
                RAISE EXCEPTION 'Table %.% must have exactly one geometry column, found %',
                    '{schema}', '{input_table}', geom_count;
            END IF;

            -- Check if the geometry column has the expected name
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = '{schema}'
                  AND table_name = '{input_table}'
                  AND column_name = '{self.geom_column}'
                  AND udt_name = 'geometry'
            ) THEN
                RAISE EXCEPTION 'Geometry column "%" not found in table %.%',
                    '{self.geom_column}', '{schema}', '{input_table}';
            END IF;

            -- Create spatial index
            EXECUTE format(
                'CREATE INDEX idx_%I_%I ON %I.%I USING GIST (%I)',
                '{input_table}', '{self.geom_column}', '{schema}', '{input_table}', '{self.geom_column}'
            );
        END $$;
        """

