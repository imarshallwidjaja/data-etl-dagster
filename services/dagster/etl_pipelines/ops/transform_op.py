# =============================================================================
# Transform Op - Spatial SQL Transformations
# =============================================================================
# Executes spatial transformations in PostGIS ephemeral schema.
# =============================================================================

import re
from typing import Dict, Any
from dagster import op, OpExecutionContext, In, Out

from libs.transformations import RecipeRegistry, CreateSpatialIndexStep


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
        bounds may be None for empty geometry datasets

    Raises:
        ValueError: If table doesn't exist or geometry column not found
        RuntimeError: If bounds calculation fails
    """
    schema = schema_info["schema"]
    manifest = schema_info["manifest"]
    run_id = schema_info["run_id"]
    intent = manifest.get("intent", "ingest_vector")
    geom_column = schema_info.get("geom_column", "geom")

    try:
        # Verify input table exists
        if not postgis.table_exists(schema, "raw_data"):
            raise ValueError(f"Table {schema}.raw_data does not exist")

        log.info(f"Transforming data in schema: {schema}")

        # Get recipe from registry
        recipe = RecipeRegistry.get_vector_recipe(intent, geom_column=geom_column)
        log.info(f"Using recipe with {len(recipe)} steps for intent: {intent}")

        # Execute steps with table chaining
        current_table = "raw_data"
        for i, step in enumerate(recipe):
            if isinstance(step, CreateSpatialIndexStep):
                # Index steps don't create new tables
                sql = step.generate_sql(schema, current_table, current_table)
                postgis.execute_sql(sql, schema)
                log.info(f"Step {i}: Created index on {current_table}")
            else:
                next_table = f"step_{i}"
                sql = step.generate_sql(schema, current_table, next_table)
                postgis.execute_sql(sql, schema)
                log.info(f"Step {i}: {current_table} -> {next_table}")
                current_table = next_table

        # Rename final table to "processed"
        # Validate and quote current_table for safety
        if not re.match(r"^(raw_data|step_\d+)$", current_table):
            raise ValueError(f"Invalid table name for rename: {current_table}")
        rename_sql = f'ALTER TABLE "{current_table}" RENAME TO "processed";'
        postgis.execute_sql(rename_sql, schema)
        log.info(f"Renamed {current_table} to processed")

        # Compute spatial bounds
        bounds = postgis.get_table_bounds(schema, "processed", geom_column=geom_column)

        # Extract geometry type for spatial metadata capture (Milestone 2)
        try:
            geometry_type = postgis.get_geometry_type(
                schema, "processed", geom_column=geom_column
            )
            log.info(f"Captured geometry type: {geometry_type}")
        except Exception as e:
            log.warning(f"Failed to extract geometry type: {e}. Using UNKNOWN.")
            geometry_type = "UNKNOWN"

        # Return transform result
        bounds_dict = None
        if bounds is not None:
            bounds_dict = {
                "minx": bounds.minx,
                "miny": bounds.miny,
                "maxx": bounds.maxx,
                "maxy": bounds.maxy,
            }

        return {
            "schema": schema,
            "table": "processed",
            "manifest": manifest,
            "bounds": bounds_dict,
            "crs": "EPSG:4326",
            "run_id": run_id,
            "geometry_type": geometry_type,  # Milestone 2: spatial metadata capture
        }
    except Exception:
        # Clean up schema on failure to maintain architectural law
        log.warning(f"Transform failed, cleaning up schema: {schema}")
        try:
            postgis._drop_schema(schema)
        except Exception as cleanup_error:
            log.warning(f"Failed to cleanup schema {schema}: {cleanup_error}")
        raise


@op(
    ins={"schema_info": In(dagster_type=dict)},
    out={"transform_result": Out(dagster_type=dict)},
    required_resource_keys={"postgis"},
)
def spatial_transform(context: OpExecutionContext, schema_info: dict) -> dict:
    """
    Execute spatial transformations in PostGIS ephemeral schema.

    Uses recipe-based transformation architecture:
    - Gets recipe from registry based on manifest intent
    - Executes transformation steps with table chaining
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
        - bounds: Geographic bounding box (minx, miny, maxx, maxy) or None if empty
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
