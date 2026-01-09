# =============================================================================
# Manifests Router
# =============================================================================
# Endpoints for manifest management, creation, and re-run.
# =============================================================================

import json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from libs.models import FileEntry, JoinConfig, TagValue

from app.auth.dependencies import AuthenticatedUser, get_current_user
from app.services.minio_service import get_minio_service
from app.services.mongodb_service import get_mongodb_service
from app.services.manifest_builder import (
    build_manifest,
    create_rerun_batch_id,
    generate_batch_id,
)

router = APIRouter(prefix="/manifests", tags=["manifests"])

# Templates
BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


class ManifestListResponse(BaseModel):
    """Response for manifest listing."""

    manifests: list[dict]
    count: int


class ManifestDetailResponse(BaseModel):
    """Response for manifest details."""

    manifest: dict


class ManifestCreateRequest(BaseModel):
    """Request for creating a manifest.

    Uses validated Pydantic models from libs.models for files, tags, and join_config.
    Includes all HumanMetadataMixin fields required by the pipeline:
    - title (required)
    - description, keywords, source, license, attribution (required, can be empty)
    """

    # Batch identification
    batch_id: Optional[str] = None
    dataset_id: Optional[str] = None

    # Human metadata fields (HumanMetadataMixin contract)
    title: str  # Required, human-readable dataset title
    description: str = ""  # Required but can be empty
    keywords: list[str] = []  # Required but can be empty list
    source: str = ""  # Required but can be empty
    license: str = ""  # Required but can be empty
    attribution: str = ""  # Required but can be empty

    # Optional project (demoted from required in M1)
    project: Optional[str] = None

    # Processing configuration
    intent: Optional[str] = None
    files: Optional[list[FileEntry]] = None
    join_config: Optional[JoinConfig] = None
    tags: Optional[dict[str, TagValue]] = None


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


@router.get("/", response_model=None)
async def list_manifests(
    request: Request,
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=100, description="Maximum results"),
    format: str = Query("html", description="Response format: html or json"),
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    """List manifests from MongoDB."""
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

    if format == "json":
        return ManifestListResponse(manifests=manifest_dicts, count=len(manifest_dicts))

    return templates.TemplateResponse(
        "manifests/list.html",
        {
            "request": request,
            "user": current_user,
            "manifests": manifest_dicts,
            "status": status,
        },
    )


@router.get("/new")
async def select_asset_type(
    request: Request,
    format: str = Query("html", description="Response format"),
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    """Select asset type for manifest creation."""
    asset_types = [
        {"id": "spatial", "name": "Spatial Asset", "description": "Vector or raster"},
        {"id": "tabular", "name": "Tabular Asset", "description": "CSV data"},
        {
            "id": "joined",
            "name": "Joined Asset",
            "description": "Join spatial + tabular",
        },
    ]

    if format == "json":
        return {"asset_types": asset_types}

    return templates.TemplateResponse(
        "manifests/select_type.html",
        {"request": request, "user": current_user, "asset_types": asset_types},
    )


@router.get("/new/{asset_type}")
async def get_asset_form(
    request: Request,
    asset_type: str,
    format: str = Query("html", description="Response format"),
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    """Return form for the specified asset type."""
    if asset_type not in ("spatial", "tabular", "joined"):
        raise HTTPException(status_code=404, detail=f"Unknown asset type: {asset_type}")

    mongodb = get_mongodb_service()
    suggested_batch_id = generate_batch_id()

    # Get assets for join form
    spatial_assets = mongodb.list_assets(kind="spatial", limit=100)
    tabular_assets = mongodb.list_assets(kind="tabular", limit=100)

    if format == "json":
        return {"asset_type": asset_type, "suggested_batch_id": suggested_batch_id}

    template_name = f"manifests/new_{asset_type}.html"
    return templates.TemplateResponse(
        template_name,
        {
            "request": request,
            "user": current_user,
            "asset_type": asset_type,
            "suggested_batch_id": suggested_batch_id,
            "spatial_assets": spatial_assets,
            "tabular_assets": tabular_assets,
        },
    )


@router.post("/new/{asset_type}", response_model=ManifestCreateResponse)
async def create_manifest(
    asset_type: str,
    request: ManifestCreateRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> ManifestCreateResponse:
    """Create a new manifest and upload to landing zone."""
    if asset_type not in ("spatial", "tabular", "joined"):
        raise HTTPException(status_code=404, detail=f"Unknown asset type: {asset_type}")

    try:
        form_data = request.model_dump(exclude_none=True)
        manifest = build_manifest(
            asset_type=asset_type,
            form_data=form_data,
            uploader=current_user.username,
        )

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


@router.get("/schemas/{asset_type}")
async def get_manifest_schema(
    asset_type: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    """Return JSON Schema for manifest creation forms.

    Exposes the ManifestCreateRequest schema for client-side validation.
    The asset_type is included as x-asset-type metadata hint.
    """
    if asset_type not in ("spatial", "tabular", "joined"):
        raise HTTPException(status_code=404, detail=f"Unknown asset type: {asset_type}")

    # Generate JSON Schema from Pydantic model
    schema = ManifestCreateRequest.model_json_schema()

    # Add asset-type specific hints for frontend use
    schema["x-asset-type"] = asset_type

    return schema


@router.get("/{batch_id}")
async def get_manifest(
    request: Request,
    batch_id: str,
    format: str = Query("html", description="Response format"),
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    """Get manifest details by batch_id."""
    mongodb = get_mongodb_service()
    manifest = mongodb.get_manifest(batch_id)

    if not manifest:
        raise HTTPException(status_code=404, detail=f"Manifest not found: {batch_id}")

    if format == "json":
        return ManifestDetailResponse(manifest=manifest)

    return templates.TemplateResponse(
        "manifests/view.html",
        {"request": request, "user": current_user, "manifest": manifest},
    )


@router.post("/{batch_id}/delete", response_model=ManifestDeleteResponse)
async def delete_manifest(
    batch_id: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> ManifestDeleteResponse:
    """Delete a manifest from the landing zone."""
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
        batch_id=batch_id, message=f"Manifest deleted: {batch_id}"
    )


@router.post("/{batch_id}/rerun", response_model=ManifestRerunResponse)
async def rerun_manifest(
    batch_id: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> ManifestRerunResponse:
    """Create a new manifest from an archived manifest for re-processing."""
    minio = get_minio_service()
    archive_key = f"archive/manifests/{batch_id}.json"

    try:
        original = minio.get_archived_manifest(archive_key)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(
            status_code=404, detail=f"Archived manifest not found: {batch_id}"
        ) from exc

    new_batch_id = create_rerun_batch_id(batch_id)
    new_manifest = dict(original)
    new_manifest["batch_id"] = new_batch_id
    new_manifest["uploader"] = current_user.username

    manifest_key = f"manifests/{new_batch_id}.json"
    manifest_json = json.dumps(new_manifest, indent=2, default=str)

    from io import BytesIO

    try:
        minio.upload_if_not_exists(
            file=BytesIO(manifest_json.encode("utf-8")),
            key=manifest_key,
            content_type="application/json",
        )
    except FileExistsError:
        raise HTTPException(
            status_code=409,
            detail=f"Version conflict: {new_batch_id} already exists. Please retry.",
        )

    return ManifestRerunResponse(
        original_batch_id=batch_id,
        new_batch_id=new_batch_id,
        manifest_key=manifest_key,
        message=f"Re-run manifest created: {new_batch_id}",
    )
