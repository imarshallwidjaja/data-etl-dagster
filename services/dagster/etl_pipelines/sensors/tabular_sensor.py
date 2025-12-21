"""Tabular asset sensor.

Polls `landing-zone/manifests/` for tabular ingestion manifests (`intent=ingest_tabular`)
and triggers `tabular_asset_job` to materialize `raw_tabular_asset`.
"""

from __future__ import annotations

import json

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from pydantic import ValidationError

from libs.models import Manifest
from ..partitions import dataset_partitions, extract_partition_key
from ..resources import MinIOResource


CURSOR_VERSION = 1
MAX_CURSOR_KEYS = 500


def _parse_cursor(cursor: str | None) -> list[str]:
    if not cursor:
        return []
    try:
        cursor_data = json.loads(cursor)
        if isinstance(cursor_data, dict) and cursor_data.get("v") == CURSOR_VERSION:
            processed_keys = cursor_data.get("processed_keys", [])
            seen: set[str] = set()
            result: list[str] = []
            for key in processed_keys:
                if isinstance(key, str) and key.strip() and key not in seen:
                    seen.add(key)
                    result.append(key)
            return result
    except (json.JSONDecodeError, TypeError, AttributeError):
        return []
    return []


def _build_cursor(processed_keys: list[str]) -> str:
    keys_list = processed_keys[-MAX_CURSOR_KEYS:]
    return json.dumps(
        {"v": CURSOR_VERSION, "processed_keys": keys_list, "max_keys": MAX_CURSOR_KEYS}
    )


@sensor(
    job_name="tabular_asset_job",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    name="tabular_sensor",
    description="Polls for tabular manifests (ingest_tabular) and materializes raw_tabular_asset",
)
def tabular_sensor(context: SensorEvaluationContext, minio: MinIOResource):
    try:
        manifests = minio.list_manifests()
    except Exception as e:
        context.log.error(f"Failed to list manifests: {e}")
        yield SkipReason(f"Error listing manifests: {e}")
        return

    processed_order = _parse_cursor(context.cursor)
    processed_set = set(processed_order)
    new_manifests = [m for m in manifests if m not in processed_set]

    if not new_manifests:
        yield SkipReason("No new manifests found")
        return

    processed_this_run: list[str] = []

    for manifest_key in new_manifests:
        try:
            manifest_data = minio.get_manifest(manifest_key)
            try:
                manifest = Manifest(**manifest_data)
            except ValidationError as e:
                context.log.error(
                    f"Invalid manifest '{manifest_key}': {e.errors()}. Marking as processed to prevent retry."
                )
                processed_this_run.append(manifest_key)
                try:
                    minio.move_to_archive(manifest_key)
                except Exception as archive_error:
                    context.log.warning(
                        f"Failed to archive invalid manifest '{manifest_key}': {archive_error}"
                    )
                continue

            if manifest.intent != "ingest_tabular":
                continue

            partition_key = extract_partition_key(manifest)
            # Register dynamic partition key before creating RunRequest (idempotent)
            context.instance.add_dynamic_partitions(
                partitions_def_name=dataset_partitions.name,
                partition_keys=[partition_key],
            )
            run_request = RunRequest(
                run_key=f"tabular_asset:{manifest.batch_id}:{partition_key}",
                run_config={
                    "ops": {
                        "raw_manifest_json": {
                            "config": {"manifest": manifest.model_dump(mode="json")}
                        },
                    }
                },
                partition_key=partition_key,
                tags={
                    "batch_id": manifest.batch_id,
                    "uploader": manifest.uploader,
                    "intent": manifest.intent,
                    "manifest_key": manifest_key,
                    "sensor": "tabular_sensor",
                    "partition_key": partition_key,
                },
            )
            yield run_request

            processed_this_run.append(manifest_key)
            try:
                minio.move_to_archive(manifest_key)
            except Exception as archive_error:
                context.log.warning(
                    f"Failed to archive manifest '{manifest_key}': {archive_error}"
                )

            context.log.info(
                f"Triggered tabular_asset_job for manifest '{manifest_key}' "
                f"(batch_id={manifest.batch_id}, partition={partition_key})"
            )
        except Exception as e:
            context.log.error(
                f"Error processing manifest '{manifest_key}': {e}. Marking as processed to prevent retry."
            )
            processed_this_run.append(manifest_key)
            try:
                minio.move_to_archive(manifest_key)
            except Exception as archive_error:
                context.log.warning(
                    f"Failed to archive manifest '{manifest_key}': {archive_error}"
                )

    if processed_this_run:
        all_processed = processed_order + [
            k for k in processed_this_run if k not in processed_set
        ]
        context.update_cursor(_build_cursor(all_processed))
