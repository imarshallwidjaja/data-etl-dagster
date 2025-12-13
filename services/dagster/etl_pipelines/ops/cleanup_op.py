# =============================================================================
# Cleanup Op - PostGIS Schema Cleanup
# =============================================================================
# Drops ephemeral PostGIS schemas after processing completes.
# Ensures PostGIS remains transient compute only (architectural law).
# =============================================================================

from typing import Dict, Any
from dagster import op, OpExecutionContext, In, Out, success_hook, failure_hook, HookContext

from libs.spatial_utils import RunIdSchemaMapping


def _cleanup_schema(
    postgis,
    schema_info: Dict[str, Any],
    log,
) -> None:
    """
    Core logic for dropping PostGIS schema.
    
    This function is extracted for easier unit testing without Dagster context.
    
    Args:
        postgis: PostGISResource instance
        schema_info: Schema info dict with schema name and run_id
        log: Logger instance (context.log)
    
    Raises:
        Exception: If schema drop fails (logged but may not propagate)
    """
    schema = schema_info["schema"]
    run_id = schema_info.get("run_id")
    
    log.info(f"Cleaning up ephemeral schema: {schema}")
    
    try:
        # Use PostGISResource's internal drop method
        postgis._drop_schema(schema)
        log.info(f"Successfully dropped schema: {schema}")
    except Exception as e:
        # Log but don't raise - cleanup failures shouldn't break the pipeline
        log.warning(f"Failed to drop schema {schema}: {e}")


@op(
    ins={"schema_info": In(dagster_type=dict)},
    required_resource_keys={"postgis"},
)
def cleanup_postgis_schema(context: OpExecutionContext, schema_info: dict) -> None:
    """
    Drop ephemeral PostGIS schema after processing completes.
    
    This op ensures that PostGIS schemas are cleaned up after the ETL pipeline
    completes, maintaining the architectural law that PostGIS is transient
    compute only.
    
    Args:
        context: Dagster op execution context
        schema_info: Schema info dict from load_to_postgis op (must contain "schema" key)
    
    Raises:
        KeyError: If schema_info doesn't contain "schema" key
    """
    _cleanup_schema(
        postgis=context.resources.postgis,
        schema_info=schema_info,
        log=context.log,
    )


@success_hook(required_resource_keys={"postgis"})
def cleanup_schema_on_success(context: HookContext):
    """
    Hook that runs after export_to_datalake succeeds.
    
    Derives schema name from run_id and drops the schema to ensure cleanup.
    """
    try:
        run_id = context.run_id
        mapping = RunIdSchemaMapping.from_run_id(run_id)
        schema = mapping.schema_name
        
        postgis = context.resources.postgis
        context.log.info(f"Cleaning up schema on success: {schema}")
        postgis._drop_schema(schema)
        context.log.info(f"Successfully dropped schema: {schema}")
    except Exception as e:
        context.log.warning(f"Failed to cleanup schema on success: {e}")


@failure_hook(required_resource_keys={"postgis"})
def cleanup_schema_on_failure(context: HookContext):
    """
    Hook that runs after export_to_datalake fails.
    
    Derives schema name from run_id and drops the schema to ensure cleanup
    even when the export op fails.
    """
    try:
        run_id = context.run_id
        mapping = RunIdSchemaMapping.from_run_id(run_id)
        schema = mapping.schema_name
        
        postgis = context.resources.postgis
        context.log.info(f"Cleaning up schema on failure: {schema}")
        postgis._drop_schema(schema)
        context.log.info(f"Successfully dropped schema: {schema}")
    except Exception as e:
        context.log.warning(f"Failed to cleanup schema on failure: {e}")

