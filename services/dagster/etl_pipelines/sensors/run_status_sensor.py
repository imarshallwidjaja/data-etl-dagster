# =============================================================================
# Run Status Sensor - Lifecycle tracking for manifest/run updates
# =============================================================================
# Monitors Dagster run lifecycle and updates manifest/run status in MongoDB.
# =============================================================================

"""Run status sensor for manifest and run lifecycle tracking."""

from dagster import (
    DagsterRunStatus,
    RunFailureSensorContext,
    RunStatusSensorContext,
    run_failure_sensor,
    run_status_sensor,
)

from libs.models import ManifestStatus, RunStatus


__all__ = ["manifest_run_failure_sensor", "manifest_run_success_sensor"]


# Jobs we should track (these are the ones that process manifests)
TRACKED_JOBS = frozenset(
    [
        "spatial_asset_job",
        "tabular_asset_job",
        "join_asset_job",
        "ingest_job",
    ]
)


def _get_batch_id_from_run(run_tags: dict) -> str | None:
    """Extract batch_id from run tags."""
    return run_tags.get("batch_id")


def _get_mongodb_client():
    """Create a MongoDB client using settings from environment."""
    from libs.models.config import MongoSettings
    from pymongo import MongoClient

    settings = MongoSettings()
    client = MongoClient(settings.connection_string)
    return client, settings.database


@run_failure_sensor(
    name="manifest_run_failure_sensor",
    description="Updates manifest status to FAILED when runs fail or are canceled",
)
def manifest_run_failure_sensor(context: RunFailureSensorContext):
    """
    Handle run failures and cancellations.

    Updates both the run document and manifest document to reflect failure.
    """
    dagster_run = context.dagster_run
    job_name = dagster_run.job_name

    # Only track our manifest-processing jobs
    if job_name not in TRACKED_JOBS:
        context.log.debug(f"Skipping untracked job: {job_name}")
        return

    dagster_run_id = dagster_run.run_id
    batch_id = _get_batch_id_from_run(dagster_run.tags)

    if not batch_id:
        context.log.warning(
            f"Run {dagster_run_id} has no batch_id tag, cannot update manifest"
        )
        return

    context.log.info(
        f"Run {dagster_run_id} failed for batch_id={batch_id}, updating MongoDB"
    )

    # Determine status based on Dagster status
    if dagster_run.status == DagsterRunStatus.CANCELED:
        run_status = RunStatus.CANCELED
        error_message = f"Run canceled. See Dagster UI for details: {dagster_run_id}"
    else:
        run_status = RunStatus.FAILURE
        error_message = f"Run failed. See Dagster UI for details: {dagster_run_id}"

    # Access MongoDB via direct client
    client, db_name = _get_mongodb_client()
    db = client[db_name]

    # Update run document
    db["runs"].update_one(
        {"dagster_run_id": dagster_run_id},
        {
            "$set": {
                "status": run_status.value,
                "error_message": error_message,
                "completed_at": dagster_run.end_time,
            }
        },
    )

    # Update manifest document
    db["manifests"].update_one(
        {"batch_id": batch_id},
        {
            "$set": {
                "status": ManifestStatus.FAILED.value,
                "error_message": error_message,
                "dagster_run_id": dagster_run_id,
            }
        },
    )

    context.log.info(f"Updated manifest {batch_id} to FAILED")
    client.close()


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    name="manifest_run_success_sensor",
    description="Updates manifest status to COMPLETED when runs succeed",
)
def manifest_run_success_sensor(context: RunStatusSensorContext):
    """
    Handle successful run completions.

    Updates both the run document and manifest document to reflect success.
    """
    dagster_run = context.dagster_run
    job_name = dagster_run.job_name

    # Only track our manifest-processing jobs
    if job_name not in TRACKED_JOBS:
        context.log.debug(f"Skipping untracked job: {job_name}")
        return

    dagster_run_id = dagster_run.run_id
    batch_id = _get_batch_id_from_run(dagster_run.tags)

    if not batch_id:
        context.log.warning(
            f"Run {dagster_run_id} has no batch_id tag, cannot update manifest"
        )
        return

    context.log.info(
        f"Run {dagster_run_id} succeeded for batch_id={batch_id}, updating MongoDB"
    )

    # Access MongoDB via direct client
    client, db_name = _get_mongodb_client()
    db = client[db_name]

    # Update run document
    db["runs"].update_one(
        {"dagster_run_id": dagster_run_id},
        {
            "$set": {
                "status": RunStatus.SUCCESS.value,
                "completed_at": dagster_run.end_time,
            }
        },
    )

    # Update manifest document
    db["manifests"].update_one(
        {"batch_id": batch_id},
        {
            "$set": {
                "status": ManifestStatus.COMPLETED.value,
                "dagster_run_id": dagster_run_id,
            }
        },
    )

    context.log.info(f"Updated manifest {batch_id} to COMPLETED")
    client.close()
