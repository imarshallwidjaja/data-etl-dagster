# =============================================================================
# Manifests Router
# =============================================================================
# Endpoints for manifest management, creation, and re-run.
# =============================================================================

import json
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from app.auth.dependencies import AuthenticatedUser, get_current_user
from app.services.minio_service import get_minio_service
from app.services.mongodb_service import get_mongodb_service, ManifestSummary
from app.services.manifest_builder import (
    build_manifest,
    create_rerun_batch_id,
    generate_batch_id,
)

router = APIRouter(prefix="/manifests", tags=["manifests"])


class ManifestListResponse(BaseModel):
    """Response for manifest listing."""

    manifests: list[dict]
    count: int


class ManifestDetailResponse(BaseModel):
    """Response for manifest details."""

    manifest: dict


class ManifestCreateRequest(BaseModel):
    """Request for creating a manifest."""

    batch_id: Optional[str] = None
    project: str
    description: Optional[str] = None
    dataset_id: Optional[str] = None
    tags: Optional[dict] = None
    # Spatial-specific
    intent: Optional[str] = None
    files: Optional[list[dict]] = None
    # Join-specific
    join_config: Optional[dict] = None


class ManifestCreateResponse(BaseModel):
    """Response for manifest creation."""

    batch_id: str
    manifest_key: str
    message: str


class ManifestDeleteResponse(BaseModel):
    """Response for manifest deletion."""

    batch_id: str
    message: str


class ManifestRerunResponse(BaseModel):
    """Response for manifest re-run."""

    original_batch_id: str
    new_batch_id: str
    manifest_key: str
    message: str


@router.get("/", response_model=ManifestListResponse)
async def list_manifests(
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=100, description="Maximum results"),
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> ManifestListResponse:
    """
    List manifests from MongoDB.

    Optionally filter by status (pending, processing, completed, failed).
    """
    mongodb = get_mongodb_service()
    manifests = mongodb.list_manifests(status=status, limit=limit)

    manifest_dicts = [
        {
            "batch_id": m.batch_id,
            "intent": m.intent,
            "status": m.status,
            "uploader": m.uploader,
            "project": m.project,
            "file_count": m.file_count,
            "created_at": m.created_at.isoformat(),
            "dagster_run_id": m.dagster_run_id,
        }
        for m in manifests
    ]

    return ManifestListResponse(
        manifests=manifest_dicts,
        count=len(manifest_dicts),
    )


@router.get("/new")
async def select_asset_type(
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> dict:
    """
    Return available asset types for manifest creation.

    In Phase 4, this will render an HTML template.
    """
    return {
        "asset_types": [
            {
                "id": "spatial",
                "name": "Spatial Asset",
                "description": "Vector or raster spatial data",
                "intents": [
                    "ingest_vector",
                    "ingest_raster",
                    "ingest_building_footprints",
                    "ingest_road_network",
                ],
            },
            {
                "id": "tabular",
                "name": "Tabular Asset",
                "description": "CSV tabular data",
                "intents": ["ingest_tabular"],
            },
            {
                "id": "joined",
                "name": "Joined Asset",
                "description": "Join existing spatial and tabular assets",
                "intents": ["join_datasets"],
            },
        ],
    }


@router.get("/new/{asset_type}")
async def get_asset_form(
    asset_type: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> dict:
    """
    Return form schema for the specified asset type.

    In Phase 4, this will render an HTML form.
    """
    if asset_type not in ("spatial", "tabular", "joined"):
        raise HTTPException(status_code=404, detail=f"Unknown asset type: {asset_type}")

    # Get existing assets for join dropdowns
    mongodb = get_mongodb_service()

    base_form = {
        "asset_type": asset_type,
        "suggested_batch_id": generate_batch_id(),
        "fields": {
            "batch_id": {"type": "text", "required": False},
            "project": {"type": "text", "required": True},
            "description": {"type": "textarea", "required": False},
            "dataset_id": {"type": "text", "required": False},
        },
    }

    if asset_type == "spatial":
        base_form["fields"]["intent"] = {
            "type": "select",
            "required": True,
            "options": [
                "ingest_vector",
                "ingest_raster",
                "ingest_building_footprints",
                "ingest_road_network",
            ],
        }
        base_form["fields"]["files"] = {
            "type": "file",
            "required": True,
            "multiple": True,
        }

    elif asset_type == "tabular":
        base_form["fields"]["files"] = {
            "type": "file",
            "required": True,
            "multiple": False,
        }

    elif asset_type == "joined":
        # Get available assets for dropdowns
        spatial_assets = mongodb.list_assets(kind="spatial", limit=100)
        tabular_assets = mongodb.list_assets(kind="tabular", limit=100)

        base_form["fields"]["join_config"] = {
            "type": "group",
            "fields": {
                "spatial_dataset_id": {
                    "type": "select",
                    "required": True,
                    "options": [
                        {
                            "value": a.dataset_id,
                            "label": f"{a.dataset_id} (v{a.version})",
                        }
                        for a in spatial_assets
                    ],
                },
                "tabular_dataset_id": {
                    "type": "select",
                    "required": True,
                    "options": [
                        {
                            "value": a.dataset_id,
                            "label": f"{a.dataset_id} (v{a.version})",
                        }
                        for a in tabular_assets
                    ],
                },
                "left_key": {"type": "text", "required": True},
                "right_key": {"type": "text", "required": False},
                "how": {
                    "type": "select",
                    "required": False,
                    "options": ["left", "inner", "right", "outer"],
                    "default": "left",
                },
            },
        }

    return base_form


@router.post("/new/{asset_type}", response_model=ManifestCreateResponse)
async def create_manifest(
    asset_type: str,
    request: ManifestCreateRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> ManifestCreateResponse:
    """
    Create a new manifest and upload to landing zone.

    The manifest will be placed in manifests/ prefix to trigger sensors.
    """
    if asset_type not in ("spatial", "tabular", "joined"):
        raise HTTPException(status_code=404, detail=f"Unknown asset type: {asset_type}")

    try:
        # Build manifest from form data
        form_data = request.model_dump(exclude_none=True)
        manifest = build_manifest(
            asset_type=asset_type,
            form_data=form_data,
            uploader=current_user.username,
        )

        # Upload to MinIO
        minio = get_minio_service()
        manifest_key = f"manifests/{manifest.batch_id}.json"
        manifest_json = manifest.model_dump_json(indent=2)

        from io import BytesIO

        minio.upload_to_landing(
            file=BytesIO(manifest_json.encode("utf-8")),
            key=manifest_key,
            content_type="application/json",
        )

        return ManifestCreateResponse(
            batch_id=manifest.batch_id,
            manifest_key=manifest_key,
            message=f"Manifest created and uploaded to {manifest_key}",
        )

    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{batch_id}", response_model=ManifestDetailResponse)
async def get_manifest(
    batch_id: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> ManifestDetailResponse:
    """
    Get manifest details by batch_id.

    Queries MongoDB for the manifest record.
    """
    mongodb = get_mongodb_service()
    manifest = mongodb.get_manifest(batch_id)

    if not manifest:
        raise HTTPException(status_code=404, detail=f"Manifest not found: {batch_id}")

    return ManifestDetailResponse(manifest=manifest)


@router.post("/{batch_id}/delete", response_model=ManifestDeleteResponse)
async def delete_manifest(
    batch_id: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> ManifestDeleteResponse:
    """
    Delete a manifest from the landing zone.

    Only pending manifests can be deleted.
    """
    minio = get_minio_service()
    manifest_key = f"manifests/{batch_id}.json"

    try:
        minio.delete_from_landing(manifest_key)
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=404, detail=f"Manifest not found: {batch_id}"
        ) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc

    return ManifestDeleteResponse(
        batch_id=batch_id,
        message=f"Manifest deleted: {batch_id}",
    )


@router.post("/{batch_id}/rerun", response_model=ManifestRerunResponse)
async def rerun_manifest(
    batch_id: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> ManifestRerunResponse:
    """
    Create a new manifest from an archived manifest for re-processing.

    - Loads original manifest from archive
    - Generates new batch_id with version suffix
    - Copies original data with new batch_id
    - Uploads new manifest to trigger sensors
    """
    minio = get_minio_service()

    # Try to get manifest from archive
    archive_key = f"archive/manifests/{batch_id}.json"

    try:
        original = minio.get_archived_manifest(archive_key)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(
            status_code=404,
            detail=f"Archived manifest not found: {batch_id}",
        ) from exc

    # Generate new batch_id
    new_batch_id = create_rerun_batch_id(batch_id)

    # Create new manifest with updated batch_id
    new_manifest = dict(original)
    new_manifest["batch_id"] = new_batch_id
    new_manifest["uploader"] = current_user.username

    # Upload new manifest
    manifest_key = f"manifests/{new_batch_id}.json"
    manifest_json = json.dumps(new_manifest, indent=2, default=str)

    from io import BytesIO

    minio.upload_to_landing(
        file=BytesIO(manifest_json.encode("utf-8")),
        key=manifest_key,
        content_type="application/json",
    )

    return ManifestRerunResponse(
        original_batch_id=batch_id,
        new_batch_id=new_batch_id,
        manifest_key=manifest_key,
        message=f"Re-run manifest created: {new_batch_id}",
    )
