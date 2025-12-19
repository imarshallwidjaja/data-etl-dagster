# =============================================================================
# Assets Router
# =============================================================================
# Endpoints for asset browsing, download, and lineage.
# =============================================================================

from pathlib import Path
from typing import Optional, Union

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.auth.dependencies import AuthenticatedUser, get_current_user
from app.services.minio_service import get_minio_service
from app.services.mongodb_service import get_mongodb_service

router = APIRouter(prefix="/assets", tags=["assets"])

# Templates
BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


class AssetListResponse(BaseModel):
    """Response for asset listing."""

    assets: list[dict]
    count: int


class AssetDetailResponse(BaseModel):
    """Response for asset details."""

    asset: dict
    versions: list[dict]


class LineageResponse(BaseModel):
    """Response for asset lineage."""

    asset: dict
    parents: list[dict]


@router.get("/", response_model=None)
async def list_assets(
    request: Request,
    kind: Optional[str] = Query(None, description="Filter by kind"),
    limit: int = Query(50, ge=1, le=100, description="Maximum results"),
    format: str = Query("html", description="Response format: html or json"),
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    """List assets from MongoDB."""
    mongodb = get_mongodb_service()
    assets = mongodb.list_assets(kind=kind, limit=limit)

    asset_dicts = [
        {
            "id": a.id,
            "dataset_id": a.dataset_id,
            "version": a.version,
            "kind": a.kind,
            "s3_key": a.s3_key,
            "created_at": a.created_at.isoformat(),
            "project": a.project,
        }
        for a in assets
    ]

    if format == "json":
        return AssetListResponse(assets=asset_dicts, count=len(asset_dicts))

    return templates.TemplateResponse(
        "assets/list.html",
        {"request": request, "user": current_user, "assets": asset_dicts, "kind": kind},
    )


@router.get("/{dataset_id}")
async def get_asset(
    request: Request,
    dataset_id: str,
    format: str = Query("html", description="Response format"),
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    """Get asset details and all versions."""
    mongodb = get_mongodb_service()

    asset = mongodb.get_asset(dataset_id)
    if not asset:
        raise HTTPException(status_code=404, detail=f"Asset not found: {dataset_id}")

    # Convert datetime to ISO string for template rendering
    if asset.get("created_at"):
        asset["created_at"] = asset["created_at"].isoformat()

    versions = mongodb.get_asset_versions(dataset_id)
    version_dicts = [
        {
            "version": v.get("version"),
            "s3_key": v.get("s3_key"),
            "created_at": v.get("created_at").isoformat()
            if v.get("created_at")
            else None,
            "content_hash": v.get("content_hash"),
        }
        for v in versions
    ]

    if format == "json":
        return AssetDetailResponse(asset=asset, versions=version_dicts)

    return templates.TemplateResponse(
        "assets/detail.html",
        {
            "request": request,
            "user": current_user,
            "asset": asset,
            "versions": version_dicts,
        },
    )


@router.get("/{dataset_id}/v{version}/download")
async def download_asset(
    dataset_id: str,
    version: int,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> StreamingResponse:
    """Download an asset file from the data lake."""
    mongodb = get_mongodb_service()
    minio = get_minio_service()

    asset = mongodb.get_asset(dataset_id, version=version)
    if not asset:
        raise HTTPException(
            status_code=404, detail=f"Asset not found: {dataset_id} v{version}"
        )

    s3_key = asset.get("s3_key")
    if not s3_key:
        raise HTTPException(
            status_code=404, detail=f"Asset has no s3_key: {dataset_id} v{version}"
        )

    try:
        data = minio.download_from_lake(s3_key)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    filename = s3_key.split("/")[-1]
    content_type = "application/octet-stream"
    if filename.endswith(".parquet"):
        content_type = "application/vnd.apache.parquet"
    elif filename.endswith(".json") or filename.endswith(".geojson"):
        content_type = "application/geo+json"

    return StreamingResponse(
        data,
        media_type=content_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/{dataset_id}/v{version}/lineage")
async def get_lineage(
    request: Request,
    dataset_id: str,
    version: int,
    format: str = Query("html", description="Response format"),
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    """Get lineage (parent assets) for an asset."""
    mongodb = get_mongodb_service()

    asset = mongodb.get_asset(dataset_id, version=version)
    if not asset:
        raise HTTPException(
            status_code=404, detail=f"Asset not found: {dataset_id} v{version}"
        )

    asset_id = asset.get("_id")
    parents = mongodb.get_parent_assets(asset_id) if asset_id else []

    parent_dicts = [
        {
            "id": p.get("_id"),
            "dataset_id": p.get("dataset_id"),
            "version": p.get("version"),
            "kind": p.get("kind"),
            "s3_key": p.get("s3_key"),
            "transformation": p.get("_lineage_transformation"),
        }
        for p in parents
    ]

    if format == "json":
        return LineageResponse(asset=asset, parents=parent_dicts)

    return templates.TemplateResponse(
        "assets/lineage.html",
        {
            "request": request,
            "user": current_user,
            "asset": asset,
            "parents": parent_dicts,
        },
    )
