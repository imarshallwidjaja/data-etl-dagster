# =============================================================================
# Common Ops - Shared operations for all jobs
# =============================================================================
# Operations that are shared across multiple jobs, including run initialization.
# =============================================================================

"""Common operations shared across all jobs."""

from dagster import op, OpExecutionContext

from libs.models import Manifest, ManifestRecord, ManifestStatus


__all__ = ["init_mongo_run_op"]


@op(required_resource_keys={"mongodb"})
def init_mongo_run_op(context: OpExecutionContext, payload: dict) -> dict:
    """
    Synchronously create the run document in MongoDB at job start.

    This op MUST run before any ops that need to link to the run document
    (e.g., export_to_datalake). It ensures the manifest is persisted with
    status=RUNNING.

    Accepts either:
    - A raw manifest dict (legacy usage from sensors)
    - A dict containing a "manifest" key (e.g., transform_result from spatial_transform)

    Flow:
    1. Extract manifest dict from payload (either directly or via "manifest" key)
    2. Extract batch_id, job_name, partition_key from run tags
    3. Upsert manifest with status=RUNNING (if not already persisted)
    4. Create run document with status=RUNNING

    Args:
        context: Dagster op execution context
        payload: Either a manifest dict directly, or a dict containing a "manifest" key

    Returns:
        The payload unchanged (passthrough for downstream ops)

    Side effects:
        - Creates/updates manifest document in MongoDB
        - Creates run document in MongoDB
    """
    mongodb = context.resources.mongodb
    tags = context.run.tags
    dagster_run_id = context.run_id

    # Extract manifest from payload - supports both raw manifest and wrapped formats
    if "manifest" in payload:
        # Wrapped format (e.g., transform_result dict containing "manifest" key)
        manifest_json = payload["manifest"]
    else:
        # Raw manifest dict (legacy usage)
        manifest_json = payload

    batch_id = tags.get("batch_id", "unknown")
    job_name = context.job_name
    partition_key = tags.get("partition_key")

    context.log.info(
        f"Initializing run in MongoDB: run_id={dagster_run_id}, "
        f"batch_id={batch_id}, job={job_name}"
    )

    # Step 1: Ensure manifest exists in MongoDB with status=RUNNING
    existing_manifest = mongodb.get_manifest(batch_id)
    if existing_manifest is None:
        # Create manifest from the manifest_json if it wasn't persisted by sensor
        manifest = Manifest(**manifest_json)
        record = ManifestRecord.from_manifest(manifest, status=ManifestStatus.RUNNING)
        mongodb.insert_manifest(record)
        context.log.info(f"Created manifest document for batch_id={batch_id}")
    else:
        # Update status to RUNNING if not already
        mongodb.update_manifest_status(
            batch_id=batch_id,
            status=ManifestStatus.RUNNING,
        )
        context.log.info(f"Updated manifest {batch_id} to RUNNING")

    # Step 2: Create run document
    run_id = mongodb.insert_run(
        dagster_run_id=dagster_run_id,
        batch_id=batch_id,
        job_name=job_name,
        partition_key=partition_key,
    )
    context.log.info(f"Created run document: {run_id}")

    # Return the original payload unchanged for downstream ops
    return payload
