"""Legacy manifest sensor for detecting new manifest files in MinIO landing zone.

This sensor is intentionally **legacy** and only triggers the op-based `ingest_job`
for non-tabular, non-join intents.

Tabular and join manifests are handled by dedicated asset sensors:
- `tabular_sensor` -> `tabular_asset_job` (materializes `raw_tabular_asset`)
- `join_sensor` -> `join_asset_job` (materializes `joined_spatial_asset`)

The sensor remains one-shot (cursor-based) for safety and traceability.
"""

import json
import os
from enum import Enum

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


def _derive_source_tag(manifest: Manifest) -> str:
    """Derive source tag from manifest metadata.tags.

    Priority:
    1. ingestion_source (if present and string)
    2. source (if present and string)
    3. "unknown" (fallback)
    """
    tags = manifest.metadata.tags
    if tags:
        ingestion_source = tags.get("ingestion_source")
        if isinstance(ingestion_source, str) and ingestion_source:
            return ingestion_source
        source = tags.get("source")
        if isinstance(source, str) and source:
            return source
    return "unknown"


# =============================================================================
# Lane Definitions
# =============================================================================


class Lane(str, Enum):
    """Processing lanes for legacy manifest routing."""

    INGEST = "ingest"


# Lane to job name mapping
LANE_TO_JOB: dict[Lane, str] = {
    Lane.INGEST: "ingest_job",
}


# =============================================================================
# Cursor Format
# =============================================================================

CURSOR_VERSION = 1
MAX_CURSOR_KEYS = 500


def merge_processed_keys(existing: list[str], new: list[str]) -> list[str]:
    """
    Merge existing processed keys with new ones, preserving order.

    Keys that appear in both lists are moved to the end (most recent occurrence wins).
    Maintains uniqueness while preserving processing order.

    Args:
        existing: Existing processed keys in order
        new: New processed keys in order

    Returns:
        Merged list with preserved order and uniqueness
    """
    # Start with existing keys
    result = existing[:]

    # Add new keys, moving duplicates to end
    for key in new:
        if key in result:
            result.remove(key)
        result.append(key)

    return result


def parse_cursor(cursor: str | None) -> list[str]:
    """
    Parse cursor into ordered list of processed manifest keys.

    Supports migration from old comma-separated format to JSON format.
    Preserves processing order for "most recently processed" semantics.

    Args:
        cursor: Cursor string (JSON v1 format or comma-separated legacy format)

    Returns:
        Ordered list of processed manifest keys
    """
    if not cursor:
        return []

    try:
        # Try parsing as JSON (v1 format)
        cursor_data = json.loads(cursor)
        if isinstance(cursor_data, dict) and cursor_data.get("v") == CURSOR_VERSION:
            processed_keys = cursor_data.get("processed_keys", [])
            # Filter to strings, drop blanks, de-dupe preserving order
            seen = set()
            result = []
            for key in processed_keys:
                if isinstance(key, str) and key.strip() and key not in seen:
                    result.append(key)
                    seen.add(key)
            return result
    except (json.JSONDecodeError, TypeError, AttributeError):
        pass

    # Fall back to legacy comma-separated format
    try:
        keys = cursor.split(",")
        # Filter out blanks, de-dupe preserving order
        seen = set()
        result = []
        for key in keys:
            key = key.strip()
            if key and key not in seen:
                result.append(key)
                seen.add(key)
        return result
    except Exception:
        return []


def build_cursor(processed_keys: list[str]) -> str:
    """
    Build JSON cursor from ordered processed keys.

    Caps to MAX_CURSOR_KEYS by keeping the most recently processed keys (tail).
    Preserves processing order for "most recently processed" semantics.

    Args:
        processed_keys: Ordered list of processed manifest keys

    Returns:
        JSON cursor string
    """
    # Cap to max keys (keep most recent - tail of the list)
    keys_list = processed_keys
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
    Determine processing lane for the **legacy** ingest_job only.

    Returns None for intents handled by asset sensors.

    Args:
        manifest: Validated manifest

    Returns:
        Lane enum value, or None if the manifest is handled by asset sensors.
    """
    intent = manifest.intent

    # Skip intents handled by dedicated asset sensors
    # - spatial_sensor: ingest_vector, ingest_raster
    # - tabular_sensor: ingest_tabular
    # - join_sensor: join_datasets
    if intent in ("ingest_tabular", "join_datasets", "ingest_vector", "ingest_raster"):
        return None

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
        # Only INGEST is supported by this legacy sensor.
        if lane_str == Lane.INGEST.value:
            enabled.add(Lane.INGEST)

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

    tags = {
        "batch_id": manifest.batch_id,
        "uploader": manifest.uploader,
        "intent": manifest.intent,
        "manifest_key": manifest_key,
        "lane": lane.value,
        "manifest_archive_key": archive_key,
        "operator": manifest.uploader,
        "source": _derive_source_tag(manifest),
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
    job=ingest_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    name="manifest_sensor",
    description="Legacy sensor: routes non-tabular, non-join manifests to ingest_job (op-based). Tabular/join handled by asset sensors.",
)
def manifest_sensor(context: SensorEvaluationContext, minio: MinIOResource):
    """
    Poll landing-zone/manifests/ for new JSON files and route to appropriate jobs.

    Flow:
    1. List all .json files in manifests/ prefix
    2. Filter out already-processed (check cursor)
    3. For each new manifest:
       a. Download and validate against Manifest model
       b. Determine lane for legacy ingest_job (or None for asset-handled intents)
       c. If lane is None: mark as processed but do NOT archive (asset sensors will process + archive)
       d. Check if lane is enabled (Traffic Controller)
       e. If enabled: yield RunRequest with lane-prefixed run_key
       f. If disabled: log and mark as processed (one-shot)
       g. Archive manifest after processing (legacy ingest lane only)
       h. Update cursor to track processed manifests

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

    # Parse cursor to get processed manifests (ordered list)
    processed_order = parse_cursor(context.cursor)
    processed_set = set(processed_order)

    # Filter to new manifests only
    new_manifests = [m for m in manifests if m not in processed_set]

    if not new_manifests:
        yield SkipReason("No new manifests found")
        return

    # Get enabled lanes (Traffic Controller)
    enabled = enabled_lanes()

    # Process each new manifest (track in order)
    processed_this_run = []
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
                processed_this_run.append(manifest_key)
                # Archive invalid manifest
                try:
                    minio.move_to_archive(manifest_key)
                except Exception as archive_error:
                    context.log.warning(
                        f"Failed to archive invalid manifest '{manifest_key}': {archive_error}"
                    )
                continue

            # Determine lane for legacy ingest_job
            lane = determine_lane(manifest)

            # Skip manifests handled by asset sensors (tabular/join). We still mark as
            # processed for this sensor to avoid re-scanning forever, but we do NOT
            # archive here (dedicated sensors will archive after processing).
            if lane is None:
                context.log.info(
                    f"Manifest '{manifest_key}' (intent={manifest.intent}) handled by asset sensor, skipping legacy ingest routing"
                )
                processed_this_run.append(manifest_key)
                continue

            # Check if lane is enabled (Traffic Controller)
            if lane not in enabled:
                context.log.info(
                    f"Lane '{lane.value}' is disabled for manifest '{manifest_key}' "
                    f"(batch_id: {manifest.batch_id}). "
                    f"Enabled lanes: {[l.value for l in enabled]}. "
                    f"Marking as processed (one-shot)."
                )
                processed_this_run.append(manifest_key)
                # Archive disabled lane manifest
                try:
                    minio.move_to_archive(manifest_key)
                except Exception as archive_error:
                    context.log.warning(
                        f"Failed to archive manifest '{manifest_key}': {archive_error}"
                    )
                continue

            # Build and yield RunRequest
            run_request = build_run_request(lane, manifest, manifest_key)
            yield run_request

            context.log.info(
                f"Triggered {lane.value} job for manifest '{manifest_key}' "
                f"(batch_id: {manifest.batch_id}, run_key: {run_request.run_key})"
            )

            # Mark as processed
            processed_this_run.append(manifest_key)

            # Archive manifest after successful routing
            try:
                minio.move_to_archive(manifest_key)
            except Exception as archive_error:
                context.log.warning(
                    f"Failed to archive manifest '{manifest_key}': {archive_error}"
                )

        except Exception as e:
            # Log error but continue processing other manifests
            context.log.error(
                f"Error processing manifest '{manifest_key}': {e}. "
                f"Marking as processed to prevent retry. "
                f"To retry, rerun/re-execute the Dagster run (sensor is one-shot)."
            )
            # Add to processed even on error - only try once
            processed_this_run.append(manifest_key)
            # Archive manifest even on error
            try:
                minio.move_to_archive(manifest_key)
            except Exception as archive_error:
                context.log.warning(
                    f"Failed to archive manifest '{manifest_key}': {archive_error}"
                )

    # Update cursor with all processed manifests (old + new)
    if processed_this_run:
        all_processed = merge_processed_keys(processed_order, processed_this_run)
        cursor_str = build_cursor(all_processed)
        context.update_cursor(cursor_str)
