"""Manifest sensor for detecting new manifest files in MinIO landing zone."""

from dagster import (
    sensor,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    DefaultSensorStatus,
)
from pydantic import ValidationError

from ..resources import MinIOResource
from ..jobs import ingest_job
from libs.models import Manifest


@sensor(
    job=ingest_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    name="manifest_sensor",
    description="Polls MinIO landing zone for new manifest files and triggers ingestion jobs",
)
def manifest_sensor(context: SensorEvaluationContext, minio: MinIOResource):
    """
    Poll landing-zone/manifests/ for new JSON files.
    
    Flow:
    1. List all .json files in manifests/ prefix
    2. Filter out already-processed (check cursor)
    3. For each new manifest:
       a. Download and validate against Manifest model
       b. Yield RunRequest with manifest data as run config
       c. Update cursor to track processed manifests
    
    Args:
        context: Dagster sensor evaluation context
        minio: MinIOResource instance (injected by Dagster)
    
    Yields:
        RunRequest: For each valid new manifest
        SkipReason: If no new manifests found
    """
    # Get list of all manifests
    try:
        manifests = minio.list_manifests()
    except Exception as e:
        context.log.error(f"Failed to list manifests: {e}")
        yield SkipReason(f"Error listing manifests: {e}")
        return
    
    # Parse cursor to get processed manifests
    processed = set()
    if context.cursor:
        try:
            processed = set(context.cursor.split(","))
        except Exception as e:
            context.log.warning(f"Failed to parse cursor, resetting: {e}")
            processed = set()
    
    # Filter to new manifests only
    new_manifests = [m for m in manifests if m not in processed]
    
    if not new_manifests:
        yield SkipReason("No new manifests found")
        return
    
    # Process each new manifest
    processed_this_run = set()
    for manifest_key in new_manifests:
        try:
            # Download manifest
            manifest_data = minio.get_manifest(manifest_key)
            
            # Validate against Pydantic model
            try:
                manifest = Manifest(**manifest_data)
            except ValidationError as e:
                context.log.error(
                    f"Invalid manifest '{manifest_key}': {e.errors()}. "
                    f"Marking as processed to prevent retry. "
                    f"To retry, re-upload with a new key or use retry mechanism."
                )
                # Add to processed even if invalid - only try once
                processed_this_run.add(manifest_key)
                continue
            
            # Yield RunRequest
            yield RunRequest(
                run_key=manifest.batch_id,  # Unique run key per batch
                run_config={
                    "ops": {
                        "ingest_placeholder": {
                            "config": {
                                "manifest": manifest.model_dump(mode="json"),
                                "manifest_key": manifest_key,
                            }
                        }
                    }
                },
                tags={
                    "batch_id": manifest.batch_id,
                    "uploader": manifest.uploader,
                    "intent": manifest.intent,
                    "manifest_key": manifest_key,
                },
            )
            
            context.log.info(
                f"Triggered ingestion job for manifest '{manifest_key}' "
                f"(batch_id: {manifest.batch_id})"
            )
            
            # Mark as processed
            processed_this_run.add(manifest_key)
            
        except Exception as e:
            # Log error but continue processing other manifests
            context.log.error(
                f"Error processing manifest '{manifest_key}': {e}. "
                f"Marking as processed to prevent retry. "
                f"To retry, re-upload with a new key or use retry mechanism."
            )
            # Add to processed even on error - only try once
            processed_this_run.add(manifest_key)
    
    # Update cursor with all processed manifests (old + new)
    if processed_this_run:
        all_processed = processed | processed_this_run
        context.update_cursor(",".join(sorted(all_processed)))

