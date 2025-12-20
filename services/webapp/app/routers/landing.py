# =============================================================================
# Landing Zone Router
# =============================================================================
# Endpoints for landing zone file management.
# =============================================================================

import re
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, field_validator

from app.auth.dependencies import AuthenticatedUser, get_current_user
from app.services.minio_service import get_minio_service

router = APIRouter(prefix="/landing", tags=["landing"])

# Templates
BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Folder name validation pattern: alphanumeric, dashes, underscores
FOLDER_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


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


class FolderCreateRequest(BaseModel):
    """Request for creating a folder."""

    name: str = Field(
        ..., description="Folder name (alphanumeric, dashes, underscores)"
    )
    prefix: str = Field("", description="Parent folder prefix")

    @field_validator("name")
    @classmethod
    def validate_folder_name(cls, v: str) -> str:
        if not v:
            raise ValueError("Folder name cannot be empty")
        if not FOLDER_NAME_PATTERN.match(v):
            raise ValueError(
                "Folder name must contain only letters, numbers, dashes, and underscores"
            )
        return v


class FolderResponse(BaseModel):
    """Response for folder operations."""

    key: str
    message: str


@router.get("/", response_model=None)
async def list_landing_zone(
    request: Request,
    prefix: str = Query("", description="Optional prefix to filter files"),
    include_archive: bool = Query(False, description="Include archived files"),
    format: str = Query("html", description="Response format: html or json"),
    current_user: AuthenticatedUser = Depends(get_current_user),
):
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

    if format == "json":
        return FileListResponse(
            prefix=prefix,
            files=files,
            count=len(files),
        )

    return templates.TemplateResponse(
        "landing/list.html",
        {
            "request": request,
            "user": current_user,
            "prefix": prefix,
            "files": files,
            "include_archive": include_archive,
        },
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


@router.post("/folder", response_model=FolderResponse)
async def create_folder(
    request: FolderCreateRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> FolderResponse:
    """
    Create a folder in the landing zone.

    Folder names must contain only letters, numbers, dashes, and underscores.
    """
    minio = get_minio_service()

    # Build full path
    if request.prefix:
        folder_path = f"{request.prefix.rstrip('/')}/{request.name}"
    else:
        folder_path = request.name

    try:
        key = minio.create_folder(folder_path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return FolderResponse(
        key=key,
        message=f"Folder created successfully: {key}",
    )


@router.post("/delete-folder/{path:path}", response_model=FolderResponse)
async def delete_folder(
    path: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> FolderResponse:
    """
    Delete an empty folder from the landing zone.

    Only empty folders can be deleted.
    Protected folders (archive/, manifests/) cannot be deleted.
    """
    minio = get_minio_service()

    try:
        minio.delete_folder(path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return FolderResponse(
        key=path,
        message=f"Folder deleted successfully: {path}",
    )
