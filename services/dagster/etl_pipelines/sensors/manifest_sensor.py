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


"""Manifest sensor for detecting new manifest files in MinIO landing zone.

Multi-lane router that routes manifests to different jobs based on intent.
Controlled by MANIFEST_ROUTER_ENABLED_LANES environment variable.
"""

import json
import os
from enum import Enum
from typing import Literal

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


# =============================================================================
# Lane Definitions
# =============================================================================

class Lane(str, Enum):
    """Processing lanes for manifest routing."""
    INGEST = "ingest"
    TABULAR = "tabular"
    JOIN = "join"


# Lane to job name mapping
LANE_TO_JOB: dict[Lane, str] = {
    Lane.INGEST: "ingest_job",
    Lane.TABULAR: "ingest_tabular_job",
    Lane.JOIN: "join_datasets_job",
}


# =============================================================================
# Cursor Format
# =============================================================================

CURSOR_VERSION = 1
MAX_CURSOR_KEYS = 500


def parse_cursor(cursor: str | None) -> set[str]:
    """
    Parse cursor into set of processed manifest keys.
    
    Supports migration from old comma-separated format to JSON format.
    
    Args:
        cursor: Cursor string (JSON v1 format or comma-separated legacy format)
        
    Returns:
        Set of processed manifest keys
    """
    if not cursor:
        return set()
    
    try:
        # Try parsing as JSON (v1 format)
        cursor_data = json.loads(cursor)
        if isinstance(cursor_data, dict) and cursor_data.get("v") == CURSOR_VERSION:
            processed_keys = cursor_data.get("processed_keys", [])
            return set(processed_keys)
    except (json.JSONDecodeError, TypeError, AttributeError):
        pass
    
    # Fall back to legacy comma-separated format
    try:
        return set(cursor.split(",")) if cursor else set()
    except Exception:
        return set()


def build_cursor(processed_keys: set[str]) -> str:
    """
    Build JSON cursor from processed keys.
    
    Caps to MAX_CURSOR_KEYS to prevent unbounded growth.
    
    Args:
        processed_keys: Set of processed manifest keys
        
    Returns:
        JSON cursor string
    """
    # Cap to max keys (keep most recent)
    keys_list = sorted(processed_keys)
    if len(keys_list) > MAX_CURSOR_KEYS:
        keys_list = keys_list[-MAX_CURSOR_KEYS:]
        # Log warning if we're capping (would need logger, but keeping simple for now)
    
    cursor_data = {
        "v": CURSOR_VERSION,
        "processed_keys": keys_list,
        "max_keys": MAX_CURSOR_KEYS,
    }
    return json.dumps(cursor_data)


# =============================================================================
# Lane Routing
# =============================================================================

def determine_lane(manifest: Manifest) -> Lane:
    """
    Determine processing lane based on manifest intent.
    
    Args:
        manifest: Validated manifest
        
    Returns:
        Lane enum value
        
    Raises:
        ValueError: If join lane is selected but join_config is missing
    """
    intent = manifest.intent
    
    if intent == "ingest_tabular":
        return Lane.TABULAR
    elif intent == "join_datasets":
        if manifest.metadata.join_config is None:
            raise ValueError(
                f"Manifest with intent 'join_datasets' requires metadata.join_config. "
                f"batch_id: {manifest.batch_id}"
            )
        return Lane.JOIN
    else:
        # Default to ingest lane for all other intents
        return Lane.INGEST


def enabled_lanes() -> set[Lane]:
    """
    Get set of enabled lanes from MANIFEST_ROUTER_ENABLED_LANES env var.
    
    Defaults to {INGEST} when unset.
    
    Returns:
        Set of enabled Lane enums
    """
    env_value = os.getenv("MANIFEST_ROUTER_ENABLED_LANES", "").strip()
    
    if not env_value:
        return {Lane.INGEST}
    
    # Parse comma-separated values
    enabled_strs = [s.strip().lower() for s in env_value.split(",")]
    enabled = set()
    
    for lane_str in enabled_strs:
        try:
            lane = Lane(lane_str)
            enabled.add(lane)
        except ValueError:
            # Skip invalid lane names
            continue
    
    # Always include ingest if nothing else is enabled
    if not enabled:
        enabled = {Lane.INGEST}
    
    return enabled


# =============================================================================
# Run Request Building
# =============================================================================

def build_run_request(
    lane: Lane,
    manifest: Manifest,
    manifest_key: str,
) -> RunRequest:
    """
    Build RunRequest for a manifest based on its lane.
    
    Args:
        lane: Processing lane
        manifest: Validated manifest
        manifest_key: S3 key of the manifest
        
    Returns:
        RunRequest with lane-prefixed run_key and appropriate job config
    """
    job_name = LANE_TO_JOB[lane]
    run_key = f"{lane.value}:{manifest.batch_id}"
    archive_key = f"archive/{manifest_key}"
    
    # Build run config based on lane
    if lane == Lane.INGEST:
        # ingest_job expects manifest as op input to load_to_postgis
        run_config = {
            "ops": {
                "load_to_postgis": {
                    "inputs": {
                        "manifest": {
                            "value": manifest.model_dump(mode="json"),
                        }
                    }
                }
            }
        }
    else:
        # Placeholder jobs expect manifest as op input to their placeholder op
        op_name = "validate_and_log"  # Placeholder op name
        run_config = {
            "ops": {
                op_name: {
                    "inputs": {
                        "manifest": {
                            "value": manifest.model_dump(mode="json"),
                        }
                    }
                }
            }
        }
    
    tags = {
        "batch_id": manifest.batch_id,
        "uploader": manifest.uploader,
        "intent": manifest.intent,
        "manifest_key": manifest_key,
        "lane": lane.value,
        "manifest_archive_key": archive_key,
    }
    
    return RunRequest(
        run_key=run_key,
        job_name=job_name,
        run_config=run_config,
        tags=tags,
    )


# =============================================================================
# Sensor Implementation
# =============================================================================

@sensor(
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    name="manifest_sensor",
    description="Multi-lane router: Polls MinIO landing zone for new manifest files and routes to appropriate jobs",
)
def manifest_sensor(context: SensorEvaluationContext, minio: MinIOResource):
    """
    Poll landing-zone/manifests/ for new JSON files and route to appropriate jobs.
    
    Flow:
    1. List all .json files in manifests/ prefix
    2. Filter out already-processed (check cursor)
    3. For each new manifest:
       a. Download and validate against Manifest model
       b. Determine processing lane based on intent
       c. Check if lane is enabled (Traffic Controller)
       d. If enabled: yield RunRequest with lane-prefixed run_key
       e. If disabled: log and mark as processed (one-shot)
       f. Archive manifest after processing
       g. Update cursor to track processed manifests
    
    Args:
        context: Dagster sensor evaluation context
        minio: MinIOResource instance (injected by Dagster)
    
    Yields:
        RunRequest: For each valid new manifest with enabled lane
        SkipReason: If no new manifests found or errors occur
    """
    # Get list of all manifests
    try:
        manifests = minio.list_manifests()
    except Exception as e:
        context.log.error(f"Failed to list manifests: {e}")
        yield SkipReason(f"Error listing manifests: {e}")
        return
    
    # Parse cursor to get processed manifests
    processed = parse_cursor(context.cursor)
    
    # Filter to new manifests only
    new_manifests = [m for m in manifests if m not in processed]
    
    if not new_manifests:
        yield SkipReason("No new manifests found")
        return
    
    # Get enabled lanes (Traffic Controller)
    enabled = enabled_lanes()
    
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
                    f"To retry, rerun/re-execute the Dagster run (sensor is one-shot)."
                )
                # Add to processed even if invalid - only try once
                processed_this_run.add(manifest_key)
                # Archive invalid manifest
                try:
                    minio.move_to_archive(manifest_key)
                except Exception as archive_error:
                    context.log.warning(f"Failed to archive invalid manifest '{manifest_key}': {archive_error}")
                continue
            
            # Determine lane
            try:
                lane = determine_lane(manifest)
            except ValueError as e:
                context.log.error(
                    f"Lane determination failed for manifest '{manifest_key}': {e}. "
                    f"Marking as processed to prevent retry."
                )
                processed_this_run.add(manifest_key)
                # Archive manifest with invalid lane config
                try:
                    minio.move_to_archive(manifest_key)
                except Exception as archive_error:
                    context.log.warning(f"Failed to archive manifest '{manifest_key}': {archive_error}")
                continue
            
            # Check if lane is enabled (Traffic Controller)
            if lane not in enabled:
                context.log.info(
                    f"Lane '{lane.value}' is disabled for manifest '{manifest_key}' "
                    f"(batch_id: {manifest.batch_id}). "
                    f"Enabled lanes: {[l.value for l in enabled]}. "
                    f"Marking as processed (one-shot)."
                )
                processed_this_run.add(manifest_key)
                # Archive disabled lane manifest
                try:
                    minio.move_to_archive(manifest_key)
                except Exception as archive_error:
                    context.log.warning(f"Failed to archive manifest '{manifest_key}': {archive_error}")
                continue
            
            # Build and yield RunRequest
            run_request = build_run_request(lane, manifest, manifest_key)
            yield run_request
            
            context.log.info(
                f"Triggered {lane.value} job for manifest '{manifest_key}' "
                f"(batch_id: {manifest.batch_id}, run_key: {run_request.run_key})"
            )
            
            # Mark as processed
            processed_this_run.add(manifest_key)
            
            # Archive manifest after successful routing
            try:
                minio.move_to_archive(manifest_key)
            except Exception as archive_error:
                context.log.warning(f"Failed to archive manifest '{manifest_key}': {archive_error}")
            
        except Exception as e:
            # Log error but continue processing other manifests
            context.log.error(
                f"Error processing manifest '{manifest_key}': {e}. "
                f"Marking as processed to prevent retry. "
                f"To retry, rerun/re-execute the Dagster run (sensor is one-shot)."
            )
            # Add to processed even on error - only try once
            processed_this_run.add(manifest_key)
            # Archive manifest even on error
            try:
                minio.move_to_archive(manifest_key)
            except Exception as archive_error:
                context.log.warning(f"Failed to archive manifest '{manifest_key}': {archive_error}")
    
    # Update cursor with all processed manifests (old + new)
    if processed_this_run:
        all_processed = processed | processed_this_run
        cursor_str = build_cursor(all_processed)
        context.update_cursor(cursor_str)

