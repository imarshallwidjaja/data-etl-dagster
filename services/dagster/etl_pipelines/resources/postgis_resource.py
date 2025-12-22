# =============================================================================
# PostGIS Resource - Ephemeral Schema Spatial Compute
# =============================================================================
# Manages ephemeral schema lifecycle for spatial operations.
# Implements the compute engine pattern: Load → Transform → Dump → Drop
# =============================================================================

from contextlib import contextmanager
from typing import Generator, Optional

from dagster import ConfigurableResource
from pydantic import Field
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import logging

from libs.models import Bounds
from libs.spatial_utils import RunIdSchemaMapping

__all__ = ["PostGISResource"]

logger = logging.getLogger(__name__)


class PostGISResource(ConfigurableResource):
    """
    Dagster resource for PostGIS spatial compute operations.

    Manages ephemeral schema lifecycle for spatial processing:
    - Create temporary schemas per Dagster run
    - Execute spatial SQL transformations
    - Automatic cleanup on run completion

    PostGIS is used as a transient compute engine, NOT for data persistence.
    All permanent data resides in MinIO/MongoDB.

    Configuration matches PostGISSettings from libs.models.config.

    Attributes:
        host: PostgreSQL host (default: "postgis")
        port: PostgreSQL port (default: 5432)
        user: PostgreSQL username
        password: PostgreSQL password
        database: Target database name (default: "spatial_compute")

    Example:
        >>> postgis = PostGISResource(host="postgis", user="pguser", password="secret")
        >>> with postgis.ephemeral_schema("abc12345-def6-7890-abcd-ef1234567890") as schema:
        ...     postgis.execute_sql("CREATE TABLE raw_input AS ...", schema)
        ...     postgis.execute_sql("CREATE TABLE processed AS ...", schema)
        ... # Schema automatically dropped
    """

    host: str = Field("postgis", description="PostgreSQL host")
    port: int = Field(5432, description="PostgreSQL port")
    user: str = Field(..., description="PostgreSQL username")
    password: str = Field(..., description="PostgreSQL password")
    database: str = Field("spatial_compute", description="Target database name")

    # Private attributes for lazy engine initialization
    _engine: Optional[Engine] = None

    def _get_connection_string(self) -> str:
        """
        Build PostgreSQL connection URI.

        Format: postgresql://user:password@host:port/database

        Returns:
            PostgreSQL connection string
        """
        return (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )

    def get_engine(self) -> Engine:
        """
        Get or create SQLAlchemy engine for PostGIS connections.

        Uses connection pooling with pre-ping validation to ensure
        connections are healthy before use.

        Returns:
            Configured SQLAlchemy Engine instance
        """
        if self._engine is None:
            self._engine = create_engine(
                self._get_connection_string(),
                pool_pre_ping=True,  # Validate connection health before use
                echo=False,  # Set to True for SQL debugging
            )
        return self._engine

    @staticmethod
    def _sanitize_schema_name(schema: str) -> str:
        """
        Escape PostgreSQL schema identifier for safe SQL.

        For use with raw SQL; ensures identifier doesn't break queries.

        Args:
            schema: Schema name to escape

        Returns:
            Escaped identifier (ready for SQL interpolation)
        """
        # PostgreSQL quotes protect against reserved words and special chars
        return f'"{schema}"'

    @contextmanager
    def ephemeral_schema(self, run_id: str) -> Generator[str, None, None]:
        """
        Context manager for ephemeral schema lifecycle.

        Creates a temporary processing schema on entry, automatically drops
        it on exit (even if an exception occurs). Guarantees cleanup.

        Schema naming follows the pattern: proc_{run_id_sanitized}
        where hyphens in the run_id are replaced with underscores.

        Args:
            run_id: Dagster run ID (UUID format)

        Yields:
            Schema name (safe for use in subsequent operations)

        Example:
            >>> with postgis.ephemeral_schema(context.run_id) as schema:
            ...     # schema = "proc_abc12345_def6_7890_abcd_ef1234567890"
            ...     postgis.execute_sql("CREATE TABLE raw AS ...", schema)
            ...     postgis.execute_sql("CREATE TABLE processed AS ...", schema)
            ... # Schema automatically dropped here, even on exception

        Raises:
            ValueError: If run_id format is invalid
            Exception: Any exception from database operations (wrapped, cleanup still runs)
        """
        # Map run_id to safe schema name
        mapping = RunIdSchemaMapping.from_run_id(run_id)
        schema_name = mapping.schema_name

        engine = self.get_engine()

        try:
            # Create schema
            self._create_schema(schema_name)
            logger.info(f"Created ephemeral schema: {schema_name}")

            yield schema_name

        finally:
            # Drop schema (guaranteed cleanup)
            self._drop_schema(schema_name)
            logger.info(f"Dropped ephemeral schema: {schema_name}")

    def _create_schema(self, schema: str) -> None:
        """
        Create a processing schema if it doesn't exist.

        Args:
            schema: Schema name to create

        Raises:
            Exception: If schema creation fails
        """
        engine = self.get_engine()

        with engine.connect() as conn:
            # Use parameterized identifier escaping
            safe_schema = self._sanitize_schema_name(schema)
            sql = f"CREATE SCHEMA IF NOT EXISTS {safe_schema}"

            conn.execute(text(sql))
            conn.commit()

    def _drop_schema(self, schema: str) -> None:
        """
        Drop a processing schema and all its contents.

        Uses CASCADE to drop all dependent objects (tables, views, etc).
        Idempotent - tolerates missing schema (already dropped).

        Args:
            schema: Schema name to drop

        Raises:
            Exception: If DROP fails for reasons other than schema not existing
        """
        engine = self.get_engine()

        try:
            with engine.connect() as conn:
                safe_schema = self._sanitize_schema_name(schema)
                sql = f"DROP SCHEMA IF EXISTS {safe_schema} CASCADE"

                conn.execute(text(sql))
                conn.commit()
        except Exception as e:
            # Log but don't raise - cleanup failures shouldn't propagate
            logger.warning(f"Failed to drop schema {schema}: {e}")

    def execute_sql(self, sql: str, schema: str) -> None:
        """
        Execute SQL within a specific schema.

        Sets the schema in the search_path before execution to ensure
        tables are created/accessed in the correct schema.

        Args:
            sql: SQL statement to execute (DDL, DML, or queries)
            schema: Schema name for search_path

        Raises:
            Exception: If SQL execution fails

        Example:
            >>> postgis.execute_sql(
            ...     "CREATE TABLE raw_data AS SELECT * FROM source_table",
            ...     "proc_abc12345_def6_7890_abcd_ef1234567890"
            ... )
        """
        engine = self.get_engine()

        with engine.connect() as conn:
            # Set search_path to target schema
            safe_schema = self._sanitize_schema_name(schema)
            conn.execute(text(f"SET search_path TO {safe_schema}, public"))

            # Execute user SQL
            conn.execute(text(sql))
            conn.commit()

    def table_exists(self, schema: str, table: str) -> bool:
        """
        Check if a table exists in the given schema.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            True if table exists, False otherwise

        Example:
            >>> if postgis.table_exists("proc_abc123_def456_...", "raw_input"):
            ...     print("Table found")
        """
        engine = self.get_engine()

        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 
                        FROM information_schema.tables 
                        WHERE table_schema = :schema 
                        AND table_name = :table
                    )
                    """
                ),
                {"schema": schema, "table": table},
            )

            exists = result.scalar()
            return bool(exists)

    def get_table_bounds(
        self, schema: str, table: str, geom_column: str = "geom"
    ) -> Optional[Bounds]:
        """
        Get the spatial bounding box of a table's geometry column.

        Computes ST_Extent() on the specified geometry column.
        Returns None for empty tables or tables with no geometry data.

        Args:
            schema: Schema name
            table: Table name
            geom_column: Name of geometry column (default: "geom")

        Returns:
            Bounds object with minx, miny, maxx, maxy, or None if empty/no geometry

        Raises:
            ValueError: If table doesn't exist or geometry column doesn't exist
            Exception: If query execution fails

        Example:
            >>> bounds = postgis.get_table_bounds(
            ...     "proc_abc123_def456_...",
            ...     "processed_features"
            ... )
            >>> if bounds:
            ...     print(f"Extent: {bounds.minx}, {bounds.miny}, {bounds.maxx}, {bounds.maxy}")
            ... else:
            ...     print("Empty geometry")
        """
        if not self.table_exists(schema, table):
            raise ValueError(f"Table does not exist: {schema}.{table}")

        # Validate geometry column identifier (strict allowlist per Phase 4.5 plan)
        import re

        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", geom_column):
            raise ValueError(
                f"Invalid geometry column name: {geom_column}. Must match pattern: ^[A-Za-z_][A-Za-z0-9_]*$"
            )

        engine = self.get_engine()

        with engine.connect() as conn:
            # Check if geometry column exists
            column_check = conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = :schema
                          AND table_name = :table
                          AND column_name = :geom_column
                          AND udt_name = 'geometry'
                    )
                    """
                ),
                {"schema": schema, "table": table, "geom_column": geom_column},
            )

            if not column_check.scalar():
                raise ValueError(
                    f"Geometry column '{geom_column}' not found in table {schema}.{table}"
                )

            # Compute extent on specified geometry column
            result = conn.execute(
                text(
                    f"""
                    SELECT
                        ST_XMin(extent) as minx,
                        ST_YMin(extent) as miny,
                        ST_XMax(extent) as maxx,
                        ST_YMax(extent) as maxy
                    FROM (
                        SELECT ST_Extent("{geom_column}") as extent
                        FROM "{schema}"."{table}"
                    ) bounds
                    """
                )
            )

            row = result.fetchone()
            if row is None or row[0] is None:
                # Empty geometry - return None instead of raising (Option C)
                return None

            minx, miny, maxx, maxy = row

            return Bounds(minx=minx, miny=miny, maxx=maxx, maxy=maxy)

    def get_geometry_type(
        self, schema: str, table: str, geom_column: str = "geom"
    ) -> str:
        """
        Get the geometry type of a table's geometry column.

        Uses a two-tier strategy:
        1. Query geometry_columns catalog view (fast, no table scan)
        2. Sample-based detection if catalog returns generic type

        Args:
            schema: Schema name
            table: Table name
            geom_column: Name of geometry column (default: "geom")

        Returns:
            Normalized OGC geometry type (uppercase): POINT, POLYGON,
            MULTIPOLYGON, LINESTRING, MULTILINESTRING, GEOMETRYCOLLECTION,
            GEOMETRY (mixed), or UNKNOWN (empty table)

        Raises:
            ValueError: If table doesn't exist

        Example:
            >>> geom_type = postgis.get_geometry_type(
            ...     "proc_abc123_def456_...",
            ...     "processed_features"
            ... )
            >>> print(f"Geometry type: {geom_type}")
            Geometry type: MULTIPOLYGON
        """
        if not self.table_exists(schema, table):
            raise ValueError(f"Table does not exist: {schema}.{table}")

        # Validate geometry column identifier
        import re

        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", geom_column):
            raise ValueError(
                f"Invalid geometry column name: {geom_column}. "
                f"Must match pattern: ^[A-Za-z_][A-Za-z0-9_]*$"
            )

        engine = self.get_engine()

        with engine.connect() as conn:
            # Tier 1: Query geometry_columns catalog
            result = conn.execute(
                text("""
                    SELECT type
                    FROM geometry_columns
                    WHERE f_table_schema = :schema
                      AND f_table_name = :table
                      AND f_geometry_column = :geom_column
                """),
                {"schema": schema, "table": table, "geom_column": geom_column},
            )
            row = result.fetchone()

            if row and row[0]:
                catalog_type = row[0].upper()
                # If catalog has specific type (not generic GEOMETRY), use it
                if catalog_type not in ("GEOMETRY", ""):
                    logger.debug(f"Got geometry type from catalog: {catalog_type}")
                    return catalog_type

            # Tier 2: Sample-based detection (for generic or missing catalog entry)
            logger.debug(f"Catalog returned generic type, sampling {schema}.{table}")
            sample_result = conn.execute(
                text(f"""
                    SELECT DISTINCT ST_GeometryType("{geom_column}") as gtype
                    FROM "{schema}"."{table}"
                    WHERE "{geom_column}" IS NOT NULL
                    LIMIT 100
                """)
            )
            types = [r[0] for r in sample_result.fetchall()]

            if not types:
                # Empty table or all null geometries
                logger.warning(
                    f"No geometry data found in {schema}.{table}, returning UNKNOWN"
                )
                return "UNKNOWN"

            # Normalize: ST_Point -> POINT, ST_MultiPolygon -> MULTIPOLYGON
            normalized = set()
            for t in types:
                if t:
                    # Strip "ST_" prefix if present
                    norm = t.upper().replace("ST_", "")
                    normalized.add(norm)

            if len(normalized) == 0:
                return "UNKNOWN"
            elif len(normalized) == 1:
                return normalized.pop()
            else:
                # Mixed geometry types
                logger.info(
                    f"Mixed geometry types detected in {schema}.{table}: {normalized}"
                )
                return "GEOMETRY"
