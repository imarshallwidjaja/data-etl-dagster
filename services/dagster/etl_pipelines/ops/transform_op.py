# =============================================================================
# Transform Op - Spatial SQL Transformations
# =============================================================================
# Executes spatial transformations in PostGIS ephemeral schema.
# =============================================================================

from typing import Dict, Any
from dagster import op, OpExecutionContext, In, Out

from libs.models import Bounds


def _spatial_transform(
    postgis,
    schema_info: Dict[str, Any],
    log,
) -> Dict[str, Any]:
    """
    Core logic for spatial transformations.
    
    This function is extracted for easier unit testing without Dagster context.
    
    Args:
        postgis: PostGISResource instance
        schema_info: Schema info dict from load_op
        log: Logger instance (context.log)
    
    Returns:
        Transform result dict with schema, table, manifest, bounds, crs, run_id
    
    Raises:
        ValueError: If table doesn't exist or geometry column not found
        RuntimeError: If bounds calculation fails
    """
    schema = schema_info["schema"]
    manifest = schema_info["manifest"]
    run_id = schema_info["run_id"]
    
    # Verify table exists
    if not postgis.table_exists(schema, "raw_data"):
        raise ValueError(f"Table {schema}.raw_data does not exist")
    
    log.info(f"Transforming data in schema: {schema}")
    
    # Execute spatial SQL transformations (hardcoded)
    # Normalize CRS to EPSG:4326, simplify geometries, create spatial indexes
    # Note: execute_sql sets search_path, so we can reference tables without schema prefix
    # But CREATE TABLE needs explicit schema, so we use the schema parameter context
    transform_sql = """
    CREATE TABLE processed AS
    SELECT 
        *,
        ST_Transform(geom, 4326) as geom,
        ST_SimplifyPreserveTopology(geom, 0.0001) as geom_simple
    FROM raw_data;
    """
    
    # Execute transformation (execute_sql sets search_path to schema)
    postgis.execute_sql(transform_sql, schema)
    log.info(f"Created processed table in schema: {schema}")
    
    # Create spatial index on processed geometry
    index_sql = """
    CREATE INDEX idx_processed_geom ON processed USING GIST (geom);
    """
    postgis.execute_sql(index_sql, schema)
    log.info(f"Created spatial index on processed table")
    
    # Compute spatial bounds
    bounds = postgis.get_table_bounds(schema, "processed")
    
    # Return transform result
    return {
        "schema": schema,
        "table": "processed",
        "manifest": manifest,
        "bounds": {
            "minx": bounds.minx,
            "miny": bounds.miny,
            "maxx": bounds.maxx,
            "maxy": bounds.maxy,
        },
        "crs": "EPSG:4326",
        "run_id": run_id,
    }


@op(
    ins={"schema_info": In(dagster_type=dict)},
    out={"transform_result": Out(dagster_type=dict)},
    required_resource_keys={"postgis"},
)
def spatial_transform(context: OpExecutionContext, schema_info: dict) -> dict:
    """
    Execute spatial transformations in PostGIS ephemeral schema.
    
    Performs hardcoded spatial transformations:
    - Normalizes CRS to EPSG:4326
    - Simplifies geometries for performance
    - Creates spatial indexes
    - Computes spatial bounds
    
    Args:
        context: Dagster op execution context
        schema_info: Schema info dict from load_to_postgis op
    
    Returns:
        Transform result dict containing:
        - schema: Schema name
        - table: Table name ("processed")
        - manifest: Full manifest dict
        - bounds: Geographic bounding box (minx, miny, maxx, maxy)
        - crs: Coordinate reference system ("EPSG:4326")
        - run_id: Dagster run ID
    
    Raises:
        ValueError: If table doesn't exist or geometry column not found
        RuntimeError: If bounds calculation fails
    """
    return _spatial_transform(
        postgis=context.resources.postgis,
        schema_info=schema_info,
        log=context.log,
    )

