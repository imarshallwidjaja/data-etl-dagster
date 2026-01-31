"""Join asset sensor.

Polls `landing-zone/manifests/` for join manifests (`intent=join_datasets`) and triggers
`join_asset_job` to materialize `joined_spatial_asset`.

Joins are **explicit only**: they require `metadata.join_config.spatial_dataset_id` and
`metadata.join_config.tabular_dataset_id` to identify the datasets to join.
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
from .tag_helpers import derive_testing_tag


CURSOR_VERSION = 1


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
    job_name="join_asset_job",
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    name="join_sensor",
    description="Polls for join manifests (join_datasets) and materializes joined_spatial_asset",
)
def join_sensor(context: SensorEvaluationContext, minio: MinIOResource):
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

            if manifest.intent != "join_datasets":
                continue

            join_config = manifest.metadata.join_config
            if join_config is None:
                context.log.warning(
                    f"Manifest '{manifest_key}' has intent=join_datasets but no join_config, skipping"
                )
                processed_this_run.append(manifest_key)
                try:
                    minio.move_to_archive(manifest_key)
                except Exception as archive_error:
                    context.log.warning(
                        f"Failed to archive manifest '{manifest_key}': {archive_error}"
                    )
                continue

            # Note: spatial_dataset_id and tabular_dataset_id are required by the model,
            # and validation will fail if they're missing.

            partition_key = extract_partition_key(manifest)
            # Register dynamic partition key before creating RunRequest (idempotent)
            context.instance.add_dynamic_partitions(
                partitions_def_name=dataset_partitions.name,
                partition_keys=[partition_key],
            )
            run_request = RunRequest(
                run_key=f"join_asset:{manifest.batch_id}:{partition_key}",
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
                    "sensor": "join_sensor",
                    "partition_key": partition_key,
                    "spatial_dataset_id": join_config.spatial_dataset_id,
                    "tabular_dataset_id": join_config.tabular_dataset_id,
                    "operator": manifest.uploader,
                    "source": _derive_source_tag(manifest),
                    **(
                        {"testing": testing_tag}
                        if (testing_tag := derive_testing_tag(manifest))
                        else {}
                    ),
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

            spatial_version_str = (
                f"v{join_config.spatial_version}"
                if join_config.spatial_version
                else "latest"
            )
            tabular_version_str = (
                f"v{join_config.tabular_version}"
                if join_config.tabular_version
                else "latest"
            )
            context.log.info(
                f"Triggered join_asset_job for manifest '{manifest_key}' "
                f"(batch_id={manifest.batch_id}, partition={partition_key}, "
                f"spatial={join_config.spatial_dataset_id}@{spatial_version_str}, "
                f"tabular={join_config.tabular_dataset_id}@{tabular_version_str})"
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
