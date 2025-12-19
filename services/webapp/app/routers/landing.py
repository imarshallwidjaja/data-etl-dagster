# =============================================================================
# Landing Zone Router
# =============================================================================
# Endpoints for landing zone file management.
# =============================================================================

from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.auth.dependencies import AuthenticatedUser, get_current_user
from app.services.minio_service import get_minio_service, LandingZoneObject

router = APIRouter(prefix="/landing", tags=["landing"])


class FileListResponse(BaseModel):
    """Response for file listing."""

    prefix: str
    files: list[dict]
    count: int


class UploadResponse(BaseModel):
    """Response for file upload."""

    key: str
    message: str


class DeleteResponse(BaseModel):
    """Response for file deletion."""

    key: str
    message: str


@router.get("/", response_model=FileListResponse)
async def list_landing_zone(
    prefix: str = Query("", description="Optional prefix to filter files"),
    include_archive: bool = Query(False, description="Include archived files"),
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> FileListResponse:
    """
    List files in the landing zone.

    Returns all files, optionally filtered by prefix.
    """
    minio = get_minio_service()
    objects = minio.list_landing_zone(prefix=prefix, include_archive=include_archive)

    files = [
        {
            "key": obj.key,
            "size": obj.size,
            "last_modified": obj.last_modified.isoformat(),
            "is_dir": obj.is_dir,
        }
        for obj in objects
    ]

    return FileListResponse(
        prefix=prefix,
        files=files,
        count=len(files),
    )


@router.get("/browse/{path:path}", response_model=FileListResponse)
async def browse_prefix(
    path: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> FileListResponse:
    """
    Browse files at a specific prefix.

    Similar to list but for a specific directory path.
    """
    minio = get_minio_service()

    # Ensure path ends with / for directory browsing
    prefix = path if path.endswith("/") else f"{path}/"

    objects = minio.list_landing_zone(prefix=prefix)

    files = [
        {
            "key": obj.key,
            "size": obj.size,
            "last_modified": obj.last_modified.isoformat(),
            "is_dir": obj.is_dir,
        }
        for obj in objects
    ]

    return FileListResponse(
        prefix=prefix,
        files=files,
        count=len(files),
    )


@router.post("/upload", response_model=UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    prefix: str = Query("", description="Optional prefix for the upload path"),
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> UploadResponse:
    """
    Upload a file to the landing zone.

    The file will be uploaded to the specified prefix.
    """
    minio = get_minio_service()

    # Build the key
    filename = file.filename or "unnamed"
    key = f"{prefix}/{filename}" if prefix else filename
    key = key.lstrip("/")  # Remove leading slash

    # Determine content type
    content_type = file.content_type or "application/octet-stream"

    # Upload
    minio.upload_to_landing(
        file=file.file,
        key=key,
        content_type=content_type,
    )

    return UploadResponse(
        key=key,
        message=f"File uploaded successfully to {key}",
    )


@router.get("/download/{path:path}")
async def download_file(
    path: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> StreamingResponse:
    """
    Download a file from the landing zone or archive.

    Supports both active files and archived files (archive/ prefix).
    """
    minio = get_minio_service()

    try:
        data = minio.download_from_landing(path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    # Determine filename for download
    filename = path.split("/")[-1]

    # Infer content type
    content_type = "application/octet-stream"
    if filename.endswith(".json"):
        content_type = "application/json"
    elif filename.endswith(".csv"):
        content_type = "text/csv"
    elif filename.endswith(".geojson"):
        content_type = "application/geo+json"

    return StreamingResponse(
        data,
        media_type=content_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.post("/delete/{path:path}", response_model=DeleteResponse)
async def delete_file(
    path: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> DeleteResponse:
    """
    Delete a file from the landing zone.

    Only allows deletion of active files, not archived files.
    """
    minio = get_minio_service()

    try:
        minio.delete_from_landing(path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc

    return DeleteResponse(
        key=path,
        message=f"File deleted successfully: {path}",
    )
