# =============================================================================
# Run Status Sensor - Lifecycle tracking for manifest/run updates
# =============================================================================
# Monitors Dagster run lifecycle and updates manifest/run status in MongoDB.
# Also logs run lifecycle events (started, success, failure, canceled) to
# activity_logs for audit trail.
# =============================================================================

"""Run status sensor for manifest and run lifecycle tracking."""

from datetime import datetime, timezone
from collections.abc import Mapping
import os
from typing import Literal

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunStatusSensorContext,
    run_failure_sensor,
    run_status_sensor,
)
from pymongo import MongoClient
from pymongo.database import Database

from libs.models import ManifestStatus, RunStatus


__all__ = [
    "manifest_run_failure_sensor",
    "manifest_run_success_sensor",
    "manifest_run_started_sensor",
    # Internal helpers exposed for testing
    "_handle_run_started",
    "_handle_run_success",
    "_handle_run_failure",
]


# Jobs we should track (these are the ones that process manifests)
TRACKED_JOBS = frozenset(
    [
        "spatial_asset_job",
        "tabular_asset_job",
        "join_asset_job",
        "ingest_job",
    ]
)

# Collection name for activity logs
ACTIVITY_LOGS_COLLECTION = "activity_logs"

# Type aliases matching libs.models.activity
ActivityAction = Literal[
    "create_manifest",
    "rerun_manifest",
    "delete_manifest",
    "upload_file",
    "delete_file",
    "download_asset",
    "run_started",
    "run_success",
    "run_failure",
    "run_canceled",
]
ActivityResourceType = Literal["manifest", "file", "asset", "run"]


def _get_batch_id_from_run(run_tags: Mapping[str, str]) -> str | None:
    """Extract batch_id from run tags."""
    return run_tags.get("batch_id")


def _get_mongodb_client() -> tuple[MongoClient[dict[str, object]], str]:
    """Create a MongoDB client using settings from environment."""
    from libs.models.config import MongoSettings

    # Read env vars with same defaults as MongoSettings expects
    settings = MongoSettings.model_validate(
        {
            "host": os.environ.get("MONGO_HOST", "mongodb"),
            "port": int(os.environ.get("MONGO_PORT", "27017")),
            "username": os.environ.get("MONGO_INITDB_ROOT_USERNAME", ""),
            "password": os.environ.get("MONGO_INITDB_ROOT_PASSWORD", ""),
            "database": os.environ.get("MONGO_DATABASE", "spatial_etl"),
            "auth_source": os.environ.get("MONGO_AUTH_SOURCE", "admin"),
        }
    )
    client: MongoClient[dict[str, object]] = MongoClient(
        host=settings.connection_string
    )
    return client, settings.database


def _write_activity_log(
    db: Database[dict[str, object]],
    action: ActivityAction,
    resource_id: str,
    user: str,
    timestamp: datetime,
    details: dict[str, object],
) -> None:
    """
    Write an activity log entry with idempotent upsert.

    Uses (action, resource_type, resource_id) as the compound key for deduplication.
    For run lifecycle events, resource_type is always "run".

    Args:
        db: MongoDB database instance
        action: The action being logged (run_started, run_success, etc.)
        resource_id: The Dagster run ID
        user: The operator/user who initiated the run
        timestamp: When the event occurred
        details: Additional context (batch_id, job_name, source, etc.)
    """
    resource_type: ActivityResourceType = "run"

    # Use upsert with $setOnInsert to ensure idempotency
    _ = db[ACTIVITY_LOGS_COLLECTION].update_one(
        {
            "action": action,
            "resource_type": resource_type,
            "resource_id": resource_id,
        },
        {
            "$setOnInsert": {
                "timestamp": timestamp,
                "user": user,
                "action": action,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "details": details,
            }
        },
        upsert=True,
    )


def _build_activity_details(
    run_tags: Mapping[str, str],
    job_name: str,
    error_message: str | None = None,
) -> dict[str, object]:
    """
    Build the details dict for activity log entries.

    Extracts batch_id, operator, source from run tags and includes job_name.
    Optionally includes error_message for failure events.

    Args:
        run_tags: Tags from the Dagster run
        job_name: Name of the Dagster job
        error_message: Error message (for failure/canceled events)

    Returns:
        Details dictionary for activity log
    """
    details: dict[str, object] = {
        "job_name": job_name,
    }

    # Add optional fields from tags
    batch_id = run_tags.get("batch_id")
    if batch_id:
        details["batch_id"] = batch_id

    operator = run_tags.get("operator")
    if operator:
        details["operator"] = operator

    source = run_tags.get("source")
    if source:
        details["source"] = source

    if error_message:
        details["error_message"] = error_message

    return details


def _handle_run_failure(context: RunFailureSensorContext) -> None:
    """
    Handle run failures and cancellations (internal logic).

    Updates both the run document and manifest document to reflect failure.
    Logs run_failure or run_canceled to activity_logs.

    This function is exposed for direct testing. The decorated sensor
    simply delegates to this function.
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
    is_canceled = dagster_run.status == DagsterRunStatus.CANCELED
    if is_canceled:
        run_status = RunStatus.CANCELED
        error_message = f"Run canceled. See Dagster UI for details: {dagster_run_id}"
        activity_action: ActivityAction = "run_canceled"
    else:
        run_status = RunStatus.FAILURE
        error_message = f"Run failed. See Dagster UI for details: {dagster_run_id}"
        activity_action = "run_failure"

    # Access MongoDB via direct client
    client, db_name = _get_mongodb_client()
    db = client[db_name]

    # Get accurate end time from Dagster run storage
    run_stats = context.instance.get_run_stats(dagster_run_id)
    end_time = (
        datetime.fromtimestamp(run_stats.end_time, tz=timezone.utc)
        if run_stats and run_stats.end_time
        else datetime.now(timezone.utc)
    )

    # Update run document
    _ = db["runs"].update_one(
        {"dagster_run_id": dagster_run_id},
        {
            "$set": {
                "status": run_status.value,
                "error_message": error_message,
                "completed_at": end_time,
            }
        },
    )

    # Update manifest document
    manifest_status = ManifestStatus.CANCELED if is_canceled else ManifestStatus.FAILURE
    _ = db["manifests"].update_one(
        {"batch_id": batch_id},
        {
            "$set": {
                "status": manifest_status.value,
                "error_message": error_message,
                "completed_at": end_time,
            }
        },
    )

    # Log to activity_logs
    operator = dagster_run.tags.get("operator", "system")
    details = _build_activity_details(dagster_run.tags, job_name, error_message)
    _write_activity_log(
        db=db,
        action=activity_action,
        resource_id=dagster_run_id,
        user=operator,
        timestamp=end_time,
        details=details,
    )

    context.log.info(
        f"Updated manifest {batch_id} to {manifest_status.value} "
        + f"and logged {activity_action}"
    )
    client.close()


@run_failure_sensor(
    name="manifest_run_failure_sensor",
    description="Updates manifest status to FAILURE when runs fail or are canceled",
    default_status=DefaultSensorStatus.RUNNING,
)
def manifest_run_failure_sensor(context: RunFailureSensorContext) -> None:
    """Decorated sensor that delegates to _handle_run_failure."""
    _handle_run_failure(context)


def _handle_run_success(context: RunStatusSensorContext) -> None:
    """
    Handle successful run completions (internal logic).

    Updates both the run document and manifest document to reflect success.
    Logs run_success to activity_logs.

    This function is exposed for direct testing. The decorated sensor
    simply delegates to this function.
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

    # Get accurate end time from Dagster run storage
    run_stats = context.instance.get_run_stats(dagster_run_id)
    end_time = (
        datetime.fromtimestamp(run_stats.end_time, tz=timezone.utc)
        if run_stats and run_stats.end_time
        else datetime.now(timezone.utc)
    )

    # Update run document
    _ = db["runs"].update_one(
        {"dagster_run_id": dagster_run_id},
        {
            "$set": {
                "status": RunStatus.SUCCESS.value,
                "completed_at": end_time,
            }
        },
    )

    # Update manifest document
    _ = db["manifests"].update_one(
        {"batch_id": batch_id},
        {
            "$set": {
                "status": ManifestStatus.SUCCESS.value,
                "completed_at": end_time,
            }
        },
    )

    # Log to activity_logs
    operator = dagster_run.tags.get("operator", "system")
    details = _build_activity_details(dagster_run.tags, job_name)
    _write_activity_log(
        db=db,
        action="run_success",
        resource_id=dagster_run_id,
        user=operator,
        timestamp=end_time,
        details=details,
    )

    context.log.info(f"Updated manifest {batch_id} to SUCCESS and logged run_success")
    client.close()


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    name="manifest_run_success_sensor",
    description="Updates manifest status to SUCCESS when runs succeed",
    default_status=DefaultSensorStatus.RUNNING,
)
def manifest_run_success_sensor(context: RunStatusSensorContext) -> None:
    """Decorated sensor that delegates to _handle_run_success."""
    _handle_run_success(context)


def _handle_run_started(context: RunStatusSensorContext) -> None:
    """
    Handle run start events (internal logic).

    Logs run_started to activity_logs for audit trail.
    Does NOT update manifest/run status (those are initialized elsewhere).

    This function is exposed for direct testing. The decorated sensor
    simply delegates to this function.
    """
    dagster_run = context.dagster_run
    job_name = dagster_run.job_name

    # Only track our manifest-processing jobs
    if job_name not in TRACKED_JOBS:
        context.log.debug(f"Skipping untracked job: {job_name}")
        return

    dagster_run_id = dagster_run.run_id
    batch_id = _get_batch_id_from_run(dagster_run.tags)

    context.log.info(
        f"Run {dagster_run_id} started for batch_id={batch_id or 'N/A'}, "
        + "logging to activity_logs"
    )

    # Access MongoDB via direct client
    client, db_name = _get_mongodb_client()
    db = client[db_name]

    # Get start time from Dagster run storage
    run_stats = context.instance.get_run_stats(dagster_run_id)
    start_time = (
        datetime.fromtimestamp(run_stats.start_time, tz=timezone.utc)
        if run_stats and run_stats.start_time
        else datetime.now(timezone.utc)
    )

    # Log to activity_logs
    operator = dagster_run.tags.get("operator", "system")
    details = _build_activity_details(dagster_run.tags, job_name)
    _write_activity_log(
        db=db,
        action="run_started",
        resource_id=dagster_run_id,
        user=operator,
        timestamp=start_time,
        details=details,
    )

    context.log.info(f"Logged run_started for {dagster_run_id}")
    client.close()


@run_status_sensor(
    run_status=DagsterRunStatus.STARTED,
    name="manifest_run_started_sensor",
    description="Logs run_started to activity_logs when runs begin",
    default_status=DefaultSensorStatus.RUNNING,
)
def manifest_run_started_sensor(context: RunStatusSensorContext) -> None:
    """Decorated sensor that delegates to _handle_run_started."""
    _handle_run_started(context)
