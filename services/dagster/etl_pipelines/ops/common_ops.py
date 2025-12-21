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
def init_mongo_run_op(context: OpExecutionContext, manifest_json: dict) -> dict:
    """
    Synchronously create the run document in MongoDB at job start.

    This op MUST be the first step in every job to ensure the run document
    exists before any assets attempt to link to it. It also ensures the
    manifest is persisted with status=PROCESSING.

    Flow:
    1. Extract batch_id, job_name, partition_key from run tags
    2. Upsert manifest with status=PROCESSING (if not already persisted)
    3. Create run document with status=RUNNING

    Args:
        context: Dagster op execution context
        manifest_json: Validated manifest dict from upstream op

    Returns:
        The manifest_json unchanged (passthrough for downstream ops)

    Side effects:
        - Creates/updates manifest document in MongoDB
        - Creates run document in MongoDB
    """
    mongodb = context.resources.mongodb
    tags = context.run.tags
    dagster_run_id = context.run_id

    batch_id = tags.get("batch_id", "unknown")
    job_name = context.job_name
    partition_key = tags.get("partition_key")

    context.log.info(
        f"Initializing run in MongoDB: run_id={dagster_run_id}, "
        f"batch_id={batch_id}, job={job_name}"
    )

    # Step 1: Ensure manifest exists in MongoDB with status=PROCESSING
    existing_manifest = mongodb.get_manifest(batch_id)
    if existing_manifest is None:
        # Create manifest from the manifest_json if it wasn't persisted by sensor
        manifest = Manifest(**manifest_json)
        record = ManifestRecord.from_manifest(
            manifest, status=ManifestStatus.PROCESSING
        )
        mongodb.insert_manifest(record)
        context.log.info(f"Created manifest document for batch_id={batch_id}")
    else:
        # Update status to PROCESSING if not already
        mongodb.update_manifest_status(
            batch_id=batch_id,
            status=ManifestStatus.PROCESSING,
            dagster_run_id=dagster_run_id,
        )
        context.log.info(f"Updated manifest {batch_id} to PROCESSING")

    # Step 2: Create run document
    run_id = mongodb.insert_run(
        dagster_run_id=dagster_run_id,
        batch_id=batch_id,
        job_name=job_name,
        partition_key=partition_key,
    )
    context.log.info(f"Created run document: {run_id}")

    return manifest_json
