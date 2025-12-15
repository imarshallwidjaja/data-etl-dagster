"""Manifest sensor for detecting new manifest files in MinIO landing zone."""

import json
import os
from enum import Enum
from typing import Iterable

from dagster import (
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from pydantic import ValidationError

from libs.models import Manifest
from ..resources import MinIOResource

CURSOR_VERSION = 1
DEFAULT_MAX_CURSOR_KEYS = 500
ROUTER_ENV_VAR = "MANIFEST_ROUTER_ENABLED_ROUTES"


class Route(Enum):
    LEGACY_JOB = "legacy"
    TABULAR_INGEST = "tabular"
    EXPLICIT_JOIN = "join"


def _dedupe_preserve_order(keys: Iterable[str]) -> list[str]:
    seen = set()
    ordered: list[str] = []
    for key in keys:
        if key not in seen:
            seen.add(key)
            ordered.append(key)
    return ordered


def load_cursor_state(cursor: str | None) -> tuple[list[str], int]:
    """
    Load cursor state from JSON (v1) or legacy comma-separated string.

    Returns:
        processed_keys: ordered list of processed manifest keys
        max_keys: maximum keys to retain
    """
    if not cursor:
        return [], DEFAULT_MAX_CURSOR_KEYS

    try:
        parsed = json.loads(cursor)
        if isinstance(parsed, dict) and parsed.get("v") == CURSOR_VERSION:
            processed_keys = parsed.get("processed_keys", [])
            max_keys = int(parsed.get("max_keys", DEFAULT_MAX_CURSOR_KEYS))
            return _dedupe_preserve_order(processed_keys), max(max_keys, 1)
    except (json.JSONDecodeError, TypeError, ValueError):
        # Fall back to legacy parsing
        pass

    legacy_keys = [k for k in cursor.split(",") if k]
    return _dedupe_preserve_order(legacy_keys), DEFAULT_MAX_CURSOR_KEYS


def save_cursor_state(processed_keys: list[str], max_keys: int) -> str:
    """Persist cursor state as bounded JSON structure."""
    bounded_keys = _dedupe_preserve_order(processed_keys)[-max_keys:]
    return json.dumps(
        {"v": CURSOR_VERSION, "processed_keys": bounded_keys, "max_keys": max_keys}
    )


def upsert_processed_key(processed_keys: list[str], key: str, max_keys: int) -> None:
    """Insert key at the end, drop oldest when exceeding max_keys."""
    if key in processed_keys:
        processed_keys.remove(key)
    processed_keys.append(key)
    overflow = len(processed_keys) - max_keys
    if overflow > 0:
        del processed_keys[0:overflow]


def determine_route(manifest: Manifest) -> Route:
    """Select processing route based on manifest intent."""
    if manifest.intent == "ingest_tabular":
        return Route.TABULAR_INGEST
    if manifest.intent == "join_datasets":
        if manifest.metadata.join_config is None:
            raise ValueError("join_datasets intent requires metadata.join_config")
        return Route.EXPLICIT_JOIN
    return Route.LEGACY_JOB


def _enabled_routes() -> set[str]:
    raw = os.getenv(ROUTER_ENV_VAR, Route.LEGACY_JOB.value)
    return {r.strip().lower() for r in raw.split(",") if r.strip()}


def is_route_enabled(route: Route) -> bool:
    """Traffic controller: gate routes via env var."""
    return route.value in _enabled_routes()


def build_run_request(route: Route, manifest: Manifest, manifest_key: str) -> RunRequest:
    """Construct RunRequest with route-aware job name and wiring."""
    manifest_payload = manifest.model_dump(mode="json")
    archive_key = f"archive/{manifest_key}"
    tags = {
        "batch_id": manifest.batch_id,
        "uploader": manifest.uploader,
        "intent": manifest.intent,
        "manifest_key": manifest_key,
        "manifest_archive_key": archive_key,
        "route": route.value,
    }

    if route is Route.LEGACY_JOB:
        job_name = "ingest_job"
        run_config = {
            "ops": {
                "load_to_postgis": {
                    "inputs": {"manifest": {"value": manifest_payload}}
                }
            }
        }
    elif route is Route.TABULAR_INGEST:
        job_name = "ingest_tabular_job"
        run_config = {
            "ops": {
                "tabular_ingest_placeholder": {
                    "inputs": {"manifest": {"value": manifest_payload}}
                }
            }
        }
    else:
        job_name = "join_datasets_job"
        run_config = {
            "ops": {
                "join_datasets_placeholder": {
                    "inputs": {"manifest": {"value": manifest_payload}}
                }
            }
        }

    run_key = f"{route.value}:{manifest.batch_id}"
    return RunRequest(
        job_name=job_name,
        run_key=run_key,
        run_config=run_config,
        tags=tags,
    )


@sensor(
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
    name="manifest_sensor",
    description="Polls MinIO landing zone for new manifest files and routes to appropriate jobs",
)
def manifest_sensor(context: SensorEvaluationContext, minio: MinIOResource):
    """
    Poll landing-zone/manifests/ for new JSON files and route them to the correct job.

    One-shot semantics: each manifest key is acknowledged once (valid, invalid, or errored),
    recorded in the cursor, and moved to archive.
    """
    try:
        manifests = minio.list_manifests()
    except Exception as exc:
        context.log.error(f"Failed to list manifests: {exc}")
        yield SkipReason(f"Error listing manifests: {exc}")
        return

    processed_keys, max_keys = load_cursor_state(context.cursor)
    processed_set = set(processed_keys)

    new_manifests = [m for m in manifests if m not in processed_set]
    if not new_manifests:
        yield SkipReason("No new manifests found")
        return

    processed_changed = False
    for manifest_key in new_manifests:
        try:
            manifest_data = minio.get_manifest(manifest_key)
            manifest = Manifest(**manifest_data)

            route = determine_route(manifest)
            if not is_route_enabled(route):
                context.log.info(
                    f"Route '{route.value}' disabled by {ROUTER_ENV_VAR}; "
                    f"acknowledging manifest '{manifest_key}' without launching a run."
                )
                continue

            yield build_run_request(route, manifest, manifest_key)
            context.log.info(
                f"Triggered route '{route.value}' for manifest '{manifest_key}' "
                f"(batch_id: {manifest.batch_id})"
            )
        except ValidationError as exc:
            context.log.error(
                f"Invalid manifest '{manifest_key}': {exc.errors()}. "
                "Marking as processed to prevent retry."
            )
        except Exception as exc:
            context.log.error(
                f"Error processing manifest '{manifest_key}': {exc}. "
                "Marking as processed to prevent retry."
            )
        finally:
            upsert_processed_key(processed_keys, manifest_key, max_keys)
            processed_changed = True
            try:
                minio.move_to_archive(manifest_key)
            except Exception as archive_exc:
                context.log.warning(
                    f"Failed to archive manifest '{manifest_key}': {archive_exc}. "
                    "Manual cleanup may be required."
                )

    if processed_changed:
        context.update_cursor(save_cursor_state(processed_keys, max_keys))

