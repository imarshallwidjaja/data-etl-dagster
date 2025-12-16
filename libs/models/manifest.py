# =============================================================================
# Manifest Models Module
# =============================================================================
# Defines models for ingestion manifests:
# - FileEntry: Individual file metadata
# - ManifestMetadata: User-supplied metadata
# - Manifest: Input contract (what users upload)
# - ManifestRecord: Persisted contract (what gets stored in MongoDB)
# =============================================================================

import re
from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Literal
from pydantic import BaseModel, Field, BeforeValidator, ConfigDict, model_validator

from .spatial import FileType

__all__ = [
    "FileEntry",
    "JoinConfig",
    "ManifestMetadata",
    "Manifest",
    "ManifestStatus",
    "ManifestRecord",
    "S3Path",
    "TagValue",
]


# =============================================================================
# S3 Path Validation
# =============================================================================

def validate_s3_path(value: str) -> str:
    """
    Validate S3 path format.
    
    Accepts paths in formats:
    - s3://bucket-name/path/to/file.ext
    - bucket-name/path/to/file.ext (without s3:// prefix)
    
    Args:
        value: S3 path string to validate
        
    Returns:
        Normalized S3 path (ensures s3:// prefix)
        
    Raises:
        ValueError: If the path format is invalid
    """
    if not isinstance(value, str):
        raise TypeError(f"S3 path must be a string, got {type(value).__name__}")
    
    if not value:
        raise ValueError("S3 path cannot be empty")
    
    value = value.strip()
    
    # Remove s3:// prefix if present for normalization
    if value.startswith("s3://"):
        path_without_prefix = value[5:]
    else:
        path_without_prefix = value
    
    # Validate bucket/path structure
    # S3 paths should have: bucket-name/path/to/file
    # Bucket names: 3-63 chars, lowercase, numbers, dots, hyphens
    # Cannot start/end with dot or hyphen
    if not path_without_prefix:
        raise ValueError("S3 path must include bucket name and object key")
    
    parts = path_without_prefix.split("/", 1)
    if len(parts) < 2:
        raise ValueError("S3 path must include both bucket name and object key")
    
    bucket_name = parts[0]
    object_key = parts[1] if len(parts) > 1 else ""
    
    # Basic bucket name validation
    if not bucket_name:
        raise ValueError("S3 bucket name cannot be empty")
    
    if len(bucket_name) < 3 or len(bucket_name) > 63:
        raise ValueError(f"S3 bucket name must be 3-63 characters, got {len(bucket_name)}")
    
    # Bucket name: lowercase, numbers, dots, hyphens only
    if not re.match(r'^[a-z0-9][a-z0-9.-]*[a-z0-9]$', bucket_name):
        raise ValueError(
            f"Invalid S3 bucket name '{bucket_name}'. "
            "Must be lowercase alphanumeric with dots/hyphens, 3-63 chars"
        )
    
    if not object_key:
        raise ValueError("S3 object key cannot be empty")
    
    # Return normalized path with s3:// prefix
    return f"s3://{path_without_prefix}"


S3Path = Annotated[
    str,
    Field(..., description="S3 object path (e.g., s3://bucket-name/path/to/file.ext)"),
    BeforeValidator(validate_s3_path)
]
"""S3 path type with validation. Normalizes paths to include s3:// prefix."""


# =============================================================================
# Tag Value Type
# =============================================================================

TagValue = str | int | float | bool
"""Allowed tag value types for manifest metadata."""


# =============================================================================
# Manifest Status Enum
# =============================================================================

class ManifestStatus(str, Enum):
    """Processing status for manifests in MongoDB."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


# =============================================================================
# File Entry Model
# =============================================================================

class FileEntry(BaseModel):
    """
    Metadata for a single file in a manifest.
    
    Represents a data file (raster, vector, or tabular) with its location,
    format, and type.
    
    Attributes:
        path: S3 path to the file (validated format)
        type: File type classification (raster, vector, or tabular)
        format: Input format string (e.g., "GTiff", "GPKG", "SHP", "CSV", "GeoJSON")
    """
    
    path: S3Path = Field(..., description="S3 path to the file")
    type: FileType = Field(..., description="File type: raster, vector, or tabular")
    format: str = Field(..., description="Input format (e.g., GTiff, GPKG, SHP, GeoJSON, CSV)")
    
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "path": "s3://landing-zone/batch_001/image.tif",
                "type": "raster",
                "format": "GTiff"
            }
        }
    )


# =============================================================================
# Join Config Model
# =============================================================================

class JoinConfig(BaseModel):
    """
    Optional join configuration for associating ingested data with existing assets.
    
    Attributes:
        target_asset_id: Identifier of the asset to join against (optional)
        left_key: Field in the incoming dataset used for the join
        right_key: Field in the target asset (defaults to left_key when omitted)
        how: Join strategy (defaults to left join)
    """
    
    target_asset_id: str | None = Field(
        None,
        description="Identifier of the existing asset to join against (optional)",
    )
    left_key: str = Field(..., description="Field in incoming data used for join")
    right_key: str | None = Field(
        None,
        description="Field in target asset used for join (defaults to left_key)",
    )
    how: Literal["left", "inner", "right", "outer"] = Field(
        "left",
        description="Join strategy",
    )
    
    @model_validator(mode='after')
    def default_right_key(self) -> 'JoinConfig':
        """Default right_key to left_key when not provided."""
        if self.right_key is None:
            self.right_key = self.left_key
        return self
    
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "target_asset_id": "dataset_ab12cd34ef56",
                "left_key": "parcel_id",
                "right_key": "parcel_id",
                "how": "left",
            }
        }
    )


# =============================================================================
# Manifest Metadata Model
# =============================================================================

class ManifestMetadata(BaseModel):
    """
    User-supplied metadata for a manifest.
    
    Provides context about the data being ingested, such as project
    affiliation and description.
    
    Attributes:
        project: Project identifier or name
        description: Optional description of the data
        tags: Arbitrary queryable tags (primitive scalars only)
        join_config: Optional join instructions for downstream matching
    """
    
    project: str = Field(..., description="Project identifier or name")
    description: str | None = Field(None, description="Optional description of the data")
    tags: dict[str, TagValue] = Field(
        default_factory=dict,
        description="User-supplied tags (str/int/float/bool values only)",
    )
    join_config: JoinConfig | None = Field(
        None,
        description="Optional join configuration for downstream matching",
    )
    
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "project": "ALPHA",
                "description": "Satellite imagery for urban analysis",
                "tags": {"priority": 1, "source": "user"},
                "join_config": {
                    "target_asset_id": "dataset_ab12cd34ef56",
                    "left_key": "parcel_id",
                    "right_key": "parcel_id",
                    "how": "left",
                }
            }
        }
    )


# =============================================================================
# Manifest Model (Input Contract)
# =============================================================================

class Manifest(BaseModel):
    """
    Input manifest contract - what users upload to trigger ETL pipelines.
    
    This is the schema for JSON files uploaded to s3://landing-zone/manifests/.
    The Dagster sensor detects these files and initiates processing.
    
    Attributes:
        batch_id: Unique batch identifier
        uploader: User or system identifier that uploaded the manifest
        intent: Processing intent (e.g., "ingest_raster", "ingest_vector", "ingest_tabular")
        files: List of files to process
        metadata: User-supplied metadata
    """
    
    batch_id: str = Field(..., description="Unique batch identifier")
    uploader: str = Field(..., description="User or system identifier")
    intent: str = Field(..., description="Processing intent (e.g., 'ingest_raster', 'ingest_tabular')")
    files: list[FileEntry] = Field(..., min_length=1, description="List of files to process")
    metadata: ManifestMetadata = Field(..., description="User-supplied metadata")
    
    @model_validator(mode='after')
    def validate_unique_paths(self) -> 'Manifest':
        """
        Validate that all file paths in the manifest are unique.
        
        Raises:
            ValueError: If duplicate file paths are found
        """
        paths = [f.path for f in self.files]
        if len(paths) != len(set(paths)):
            duplicates = [p for p in paths if paths.count(p) > 1]
            raise ValueError(
                f"Duplicate file paths found in manifest '{self.batch_id}': {set(duplicates)}"
            )
        return self
    
    @model_validator(mode='after')
    def validate_intent_type_coherence(self) -> 'Manifest':
        """
        Enforce intent/type coherence to prevent routing errors.
        
        Rules:
        - If intent == "ingest_tabular" → all files[].type must be "tabular"
        - Otherwise → forbid "tabular" (prevents accidental routing to spatial pipeline)
        
        Raises:
            ValueError: If intent and file types are inconsistent
        """
        from .spatial import FileType
        
        if self.intent == "ingest_tabular":
            # All files must be tabular
            non_tabular = [f for f in self.files if f.type != FileType.TABULAR]
            if non_tabular:
                raise ValueError(
                    f"Manifest with intent 'ingest_tabular' must have all files with type 'tabular'. "
                    f"Found non-tabular files: {[f.path for f in non_tabular]}"
                )
        else:
            # Forbid tabular files in non-tabular intents
            tabular_files = [f for f in self.files if f.type == FileType.TABULAR]
            if tabular_files:
                raise ValueError(
                    f"Manifest with intent '{self.intent}' cannot contain tabular files. "
                    f"Use intent 'ingest_tabular' for tabular data. "
                    f"Found tabular files: {[f.path for f in tabular_files]}"
                )
        return self
    
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "batch_id": "batch_001",
                "uploader": "system",
                "intent": "ingest_vector",
                "files": [
                    {
                        "path": "s3://landing-zone/batch_001/file.gpkg",
                        "type": "vector",
                        "format": "GPKG"
                    }
                ],
                "metadata": {
                    "project": "ALPHA",
                    "description": "Test dataset",
                    "tags": {"source": "user", "priority": 1},
                    "join_config": {
                        "left_key": "parcel_id",
                        "right_key": "parcel_id",
                        "how": "left"
                    }
                }
            }
        }
    )


# =============================================================================
# Manifest Record Model (Persisted Contract)
# =============================================================================

class ManifestRecord(Manifest):
    """
    Persisted manifest record - what gets stored in MongoDB.
    
    Extends the input Manifest with runtime tracking fields:
    - Processing status
    - Dagster run ID (if processing has started)
    - Error messages (if processing failed)
    - Timestamps for ingestion and completion
    
    Attributes:
        status: Current processing status
        dagster_run_id: Dagster run ID that processed this manifest (if any)
        error_message: Error message if processing failed (if any)
        ingested_at: Timestamp when manifest was ingested
        completed_at: Timestamp when processing completed (if completed)
    """
    
    status: ManifestStatus = Field(..., description="Current processing status")
    dagster_run_id: str | None = Field(None, description="Dagster run ID that processed this manifest")
    error_message: str | None = Field(None, description="Error message if processing failed")
    ingested_at: datetime = Field(..., description="Timestamp when manifest was ingested")
    completed_at: datetime | None = Field(None, description="Timestamp when processing completed")
    
    @classmethod
    def from_manifest(
        cls,
        manifest: Manifest,
        status: ManifestStatus = ManifestStatus.PENDING,
        ingested_at: datetime | None = None
    ) -> 'ManifestRecord':
        """
        Create a ManifestRecord from a Manifest.
        
        Convenience method to convert an input manifest to a persisted record
        with default status and timestamp.
        
        Args:
            manifest: Input manifest to convert
            status: Initial processing status (default: PENDING)
            ingested_at: Ingestion timestamp (default: current UTC time)
            
        Returns:
            ManifestRecord instance ready for MongoDB storage
        """
        if ingested_at is None:
            ingested_at = datetime.now(timezone.utc)
        
        return cls(
            **manifest.model_dump(),
            status=status,
            ingested_at=ingested_at
        )
    
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "batch_id": "batch_001",
                "uploader": "system",
                "intent": "ingest_vector",
                "files": [
                    {
                        "path": "s3://landing-zone/batch_001/file.gpkg",
                        "type": "vector",
                        "format": "GPKG"
                    }
                ],
                "metadata": {
                    "project": "ALPHA",
                    "description": "Test dataset",
                    "tags": {"source": "user", "priority": 1},
                    "join_config": {
                        "left_key": "parcel_id",
                        "right_key": "parcel_id",
                        "how": "left"
                    }
                },
                "status": "completed",
                "dagster_run_id": "run_12345",
                "error_message": None,
                "ingested_at": "2024-01-01T00:00:00Z",
                "completed_at": "2024-01-01T00:05:00Z"
            }
        }
    )

