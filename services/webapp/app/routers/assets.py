# =============================================================================
# Assets Router
# =============================================================================
# Endpoints for asset browsing, download, and lineage.
# =============================================================================

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.auth.dependencies import AuthenticatedUser, get_current_user
from app.services.minio_service import get_minio_service
from app.services.mongodb_service import get_mongodb_service

router = APIRouter(prefix="/assets", tags=["assets"])


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


@router.get("/", response_model=AssetListResponse)
async def list_assets(
    kind: Optional[str] = Query(
        None,
        description="Filter by kind (spatial, tabular, joined)",
    ),
    limit: int = Query(50, ge=1, le=100, description="Maximum results"),
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> AssetListResponse:
    """
    List assets from MongoDB.

    Optionally filter by kind.
    """
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

    return AssetListResponse(
        assets=asset_dicts,
        count=len(asset_dicts),
    )


@router.get("/{dataset_id}", response_model=AssetDetailResponse)
async def get_asset(
    dataset_id: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> AssetDetailResponse:
    """
    Get asset details and all versions.
    """
    mongodb = get_mongodb_service()

    # Get latest version
    asset = mongodb.get_asset(dataset_id)
    if not asset:
        raise HTTPException(status_code=404, detail=f"Asset not found: {dataset_id}")

    # Get all versions
    versions = mongodb.get_asset_versions(dataset_id)

    # Format versions for response
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

    return AssetDetailResponse(
        asset=asset,
        versions=version_dicts,
    )


@router.get("/{dataset_id}/v{version}/download")
async def download_asset(
    dataset_id: str,
    version: int,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> StreamingResponse:
    """
    Download an asset file from the data lake.
    """
    mongodb = get_mongodb_service()
    minio = get_minio_service()

    # Get the specific version
    asset = mongodb.get_asset(dataset_id, version=version)
    if not asset:
        raise HTTPException(
            status_code=404,
            detail=f"Asset not found: {dataset_id} v{version}",
        )

    s3_key = asset.get("s3_key")
    if not s3_key:
        raise HTTPException(
            status_code=404,
            detail=f"Asset has no s3_key: {dataset_id} v{version}",
        )

    try:
        data = minio.download_from_lake(s3_key)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    # Determine filename
    filename = s3_key.split("/")[-1]

    # Determine content type
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


@router.get("/{dataset_id}/v{version}/lineage", response_model=LineageResponse)
async def get_lineage(
    dataset_id: str,
    version: int,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> LineageResponse:
    """
    Get lineage (parent assets) for an asset.
    """
    mongodb = get_mongodb_service()

    # Get the specific version
    asset = mongodb.get_asset(dataset_id, version=version)
    if not asset:
        raise HTTPException(
            status_code=404,
            detail=f"Asset not found: {dataset_id} v{version}",
        )

    asset_id = asset.get("_id")
    if not asset_id:
        # If no _id, lineage lookup won't work
        return LineageResponse(asset=asset, parents=[])

    # Get parent assets
    parents = mongodb.get_parent_assets(asset_id)

    # Format parents for response
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

    return LineageResponse(
        asset=asset,
        parents=parent_dicts,
    )
