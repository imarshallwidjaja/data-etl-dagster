# =============================================================================
# Load Op - S3 to PostGIS
# =============================================================================
# Loads spatial data from MinIO landing zone to PostGIS ephemeral schema
# using GDAL ogr2ogr.
# =============================================================================

from typing import Dict, Any
from dagster import op, OpExecutionContext, In, Out
from sqlalchemy import text

from libs.spatial_utils import RunIdSchemaMapping
from services.dagster.etl_pipelines.resources.gdal_resource import GDALResult


def _load_files_to_postgis(
    gdal,
    postgis,
    manifest: Dict[str, Any],
    run_id: str,
    log,
) -> Dict[str, Any]:
    """
    Core logic for loading files from S3 to PostGIS.
    
    This function is extracted for easier unit testing without Dagster context.
    
    Args:
        gdal: GDALResource instance
        postgis: PostGISResource instance
        manifest: Manifest dict with files list
        run_id: Dagster run ID
        log: Logger instance (context.log)
    
    Returns:
        Schema info dict with schema name, manifest, tables, and run_id
    
    Raises:
        RuntimeError: If any ogr2ogr call fails
    """
    # Map run_id to schema name
    mapping = RunIdSchemaMapping.from_run_id(run_id)
    schema = mapping.schema_name
    
    # Create ephemeral schema (will persist across ops, cleaned up later)
    # Use execute_sql to create schema - need to escape schema name for SQL
    # PostGISResource.execute_sql sets search_path, but CREATE SCHEMA needs to be
    # executed in a different context, so we use the engine directly
    engine = postgis.get_engine()
    with engine.connect() as conn:
        # Escape schema name for SQL (add quotes)
        safe_schema = f'"{schema}"'
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {safe_schema}"))
        conn.commit()
    log.info(f"Created ephemeral schema: {schema}")
    
    # Build PostgreSQL connection string for ogr2ogr
    # Format: PG:host={host} dbname={database} user={user} password={password}
    pg_conn_str = (
        f"PG:host={postgis.host} "
        f"dbname={postgis.database} "
        f"user={postgis.user} "
        f"password={postgis.password}"
    )
    
    # Load all files into the same table: raw_data
    layer_name = f"{schema}.raw_data"
    
    for file_entry in manifest["files"]:
        s3_path = file_entry["path"]
        
        # Convert s3:// path to /vsis3/ path
        # s3://landing-zone/path -> /vsis3/landing-zone/path
        if s3_path.startswith("s3://"):
            vsis3_path = s3_path.replace("s3://", "/vsis3/", 1)
        else:
            # Already in /vsis3/ format or unexpected format
            vsis3_path = s3_path
        
        log.info(f"Loading file: {s3_path} -> {layer_name}")
        
        # Get target CRS if provided
        target_crs = file_entry.get("crs")
        
        # Execute ogr2ogr
        result: GDALResult = gdal.ogr2ogr(
            input_path=vsis3_path,
            output_path=pg_conn_str,
            output_format="PostgreSQL",
            layer_name=layer_name,
            target_crs=target_crs,
            options={"-overwrite": ""},
        )
        
        # Check for failure
        if not result.success:
            raise RuntimeError(
                f"ogr2ogr failed for file {s3_path}: {result.stderr}"
            )
        
        log.info(f"Successfully loaded {s3_path} to {layer_name}")
    
    # Collect table names (single table approach)
    tables = ["raw_data"]
    
    # Return schema info (schema persists for use by subsequent ops)
    return {
        "schema": schema,
        "manifest": manifest,
        "tables": tables,
        "run_id": run_id,
    }


@op(
    ins={"manifest": In(dagster_type=dict)},
    out={"schema_info": Out(dagster_type=dict)},
    required_resource_keys={"gdal", "postgis", "minio"},
)
def load_to_postgis(context: OpExecutionContext, manifest: dict) -> dict:
    """
    Load spatial data from MinIO landing zone to PostGIS ephemeral schema.
    
    Uses GDAL ogr2ogr to load files from S3 to PostGIS. All files in the
    manifest are loaded into a single table: raw_data.
    
    Args:
        context: Dagster op execution context
        manifest: Manifest dict with batch_id, files, metadata, etc.
    
    Returns:
        Schema info dict containing:
        - schema: Schema name (e.g., "proc_abc12345_def6_7890_...")
        - manifest: Full manifest dict
        - tables: List of table names created (["raw_data"])
        - run_id: Dagster run ID
    
    Raises:
        RuntimeError: If any ogr2ogr call fails
    """
    return _load_files_to_postgis(
        gdal=context.resources.gdal,
        postgis=context.resources.postgis,
        manifest=manifest,
        run_id=context.run_id,
        log=context.log,
    )

