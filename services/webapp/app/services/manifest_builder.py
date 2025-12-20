# =============================================================================
# Manifest Builder - Form to Manifest Conversion
# =============================================================================
# Converts webapp form data to Manifest model for pipeline processing.
# =============================================================================

import re
import uuid
from typing import Any

from libs.models import FileEntry, Manifest, ManifestMetadata

from app.services.mongodb_service import get_mongodb_service


def generate_batch_id(prefix: str = "batch") -> str:
    """
    Generate a unique batch ID.

    Args:
        prefix: Optional prefix for the batch ID

    Returns:
        Unique batch ID (e.g., "batch_abc123def456")
    """
    unique_part = uuid.uuid4().hex[:12]
    return f"{prefix}_{unique_part}"


def generate_dataset_id() -> str:
    """
    Generate a unique dataset ID.

    Returns:
        Unique dataset ID (e.g., "dataset_abc123def456")
    """
    unique_part = uuid.uuid4().hex[:12]
    return f"dataset_{unique_part}"


def create_rerun_batch_id(original_batch_id: str) -> str:
    """
    Create a versioned batch ID for re-running a manifest.

    Smart versioning:
    - If original is "batch_abc", tries "batch_abc_v2", "batch_abc_v3", etc.
    - If original is already "batch_abc_v2", increments to "batch_abc_v3"
    - Queries MongoDB to find highest existing version

    Args:
        original_batch_id: The original batch ID

    Returns:
        New batch ID with version suffix
    """
    mongodb = get_mongodb_service()
    next_version = mongodb.get_next_rerun_version(original_batch_id)

    # Strip existing version suffix
    base_id = re.sub(r"_v\d+$", "", original_batch_id)

    return f"{base_id}_v{next_version}"


def build_manifest(
    asset_type: str,
    form_data: dict[str, Any],
    uploader: str = "webapp",
) -> Manifest:
    """
    Build a Manifest model from form data.

    Args:
        asset_type: Type of asset ("spatial", "tabular", "joined")
        form_data: Form data dictionary
        uploader: User/system identifier

    Returns:
        Validated Manifest model

    Raises:
        ValueError: If form data is invalid
    """
    # Extract common fields
    batch_id = form_data.get("batch_id") or generate_batch_id()
    project = form_data.get("project", "")
    description = form_data.get("description", "")

    # Extract tags
    tags: dict[str, Any] = {}
    dataset_id = form_data.get("dataset_id")
    if dataset_id:
        tags["dataset_id"] = dataset_id
    else:
        tags["dataset_id"] = generate_dataset_id()

    # Add any custom tags from form
    custom_tags = form_data.get("tags", {})
    if isinstance(custom_tags, dict):
        for key, value in custom_tags.items():
            # Only accept primitive values
            if isinstance(value, (str, int, float, bool)):
                tags[key] = value

    # Build metadata
    metadata = ManifestMetadata(
        project=project,
        description=description if description else None,
        tags=tags if tags else None,
    )

    # Asset-type specific logic
    if asset_type == "spatial":
        return _build_spatial_manifest(form_data, batch_id, uploader, metadata)
    elif asset_type == "tabular":
        return _build_tabular_manifest(form_data, batch_id, uploader, metadata)
    elif asset_type == "joined":
        return _build_joined_manifest(form_data, batch_id, uploader, metadata)
    else:
        raise ValueError(f"Unknown asset type: {asset_type}")


def _build_spatial_manifest(
    form_data: dict[str, Any],
    batch_id: str,
    uploader: str,
    metadata: ManifestMetadata,
) -> Manifest:
    """Build a spatial asset manifest."""
    # Get intent (spatial can have multiple intents)
    intent = form_data.get("intent", "ingest_vector")
    valid_intents = {
        "ingest_vector",
        "ingest_raster",
        "ingest_building_footprints",
        "ingest_road_network",
    }
    if intent not in valid_intents:
        raise ValueError(f"Invalid spatial intent: {intent}")

    # Build file entries
    files = _get_file_entries(form_data)

    return Manifest(
        batch_id=batch_id,
        uploader=uploader,
        intent=intent,
        files=files,
        metadata=metadata,
    )


def _build_tabular_manifest(
    form_data: dict[str, Any],
    batch_id: str,
    uploader: str,
    metadata: ManifestMetadata,
) -> Manifest:
    """Build a tabular asset manifest."""
    # Fixed intent for tabular
    intent = "ingest_tabular"

    # Build file entries (tabular requires exactly one CSV)
    files = _get_file_entries(form_data)

    if len(files) != 1:
        raise ValueError("Tabular manifest requires exactly one file")

    return Manifest(
        batch_id=batch_id,
        uploader=uploader,
        intent=intent,
        files=files,
        metadata=metadata,
    )


def _build_joined_manifest(
    form_data: dict[str, Any],
    batch_id: str,
    uploader: str,
    metadata: ManifestMetadata,
) -> Manifest:
    """Build a joined asset manifest."""
    # Fixed intent for join
    intent = "join_datasets"

    # No files for join (uses existing assets)
    files: list[FileEntry] = []

    # Get join_config - already validated by Pydantic at API boundary
    join_config = form_data.get("join_config")
    if not join_config:
        raise ValueError("Join manifest requires join_config")

    # Add join_config to metadata
    metadata_with_join = ManifestMetadata(
        project=metadata.project,
        description=metadata.description,
        tags=metadata.tags,
        join_config=join_config,
    )

    return Manifest(
        batch_id=batch_id,
        uploader=uploader,
        intent=intent,
        files=files,
        metadata=metadata_with_join,
    )


def _get_file_entries(form_data: dict[str, Any]) -> list[FileEntry]:
    """
    Get FileEntry list from form data.

    If files are already FileEntry objects (from validated API request),
    returns them directly. Otherwise builds from dict data.

    Args:
        form_data: Form data with "files" key

    Returns:
        List of FileEntry
    """
    files_data = form_data.get("files", [])
    if not files_data:
        return []

    # If already FileEntry objects (from validated request), return directly
    if files_data and isinstance(files_data[0], FileEntry):
        return files_data

    # Otherwise build from dict data (legacy compatibility)
    entries = []
    for file_info in files_data:
        if isinstance(file_info, dict):
            path = file_info.get("path", "")
            file_format = file_info.get("format", _infer_format(path))
            file_type = file_info.get("type", "vector")
        else:
            path = str(file_info)
            file_format = _infer_format(path)
            file_type = "vector"

        if path:
            entries.append(
                FileEntry(
                    path=path,
                    type=file_type,
                    format=file_format,
                )
            )

    return entries


def _infer_format(path: str) -> str:
    """Infer file format from path extension."""
    ext = path.lower().split(".")[-1] if "." in path else ""

    format_map = {
        "geojson": "GeoJSON",
        "json": "GeoJSON",
        "shp": "SHP",
        "gpkg": "GPKG",
        "tif": "GTiff",
        "tiff": "GTiff",
        "csv": "CSV",
        "parquet": "Parquet",
    }

    return format_map.get(ext, "GeoJSON")
