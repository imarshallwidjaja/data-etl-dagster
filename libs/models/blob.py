# =============================================================================
# Blob Models Module
# =============================================================================
# Defines models for content-addressed blobs stored in object storage.
# =============================================================================

"""Models for content-addressed blobs in object storage."""

import re
from datetime import datetime
from typing import Annotated
from pydantic import BaseModel, Field, BeforeValidator, ConfigDict

from .asset import ContentHash, S3Key

__all__ = ["Blob"]


# =============================================================================
# S3 Bucket Validation
# =============================================================================


def validate_s3_bucket(value: str) -> str:
    """
    Validate S3 bucket name format.

    Bucket names should:
    - Be 3-63 characters
    - Use lowercase letters, numbers, dots, and hyphens
    - Not start or end with dot or hyphen

    Args:
        value: Bucket name string to validate

    Returns:
        Normalized bucket name (trimmed)

    Raises:
        TypeError: If the value is not a string
        ValueError: If the bucket name format is invalid
    """
    if not isinstance(value, str):
        raise TypeError(f"S3 bucket name must be a string, got {type(value).__name__}")

    if not value:
        raise ValueError("S3 bucket name cannot be empty")

    value = value.strip()

    if len(value) < 3 or len(value) > 63:
        raise ValueError(f"S3 bucket name must be 3-63 characters, got {len(value)}")

    if not re.match(r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$", value):
        raise ValueError(
            f"Invalid S3 bucket name '{value}'. "
            "Must be lowercase alphanumeric with dots/hyphens, 3-63 chars"
        )

    return value


S3Bucket = Annotated[
    str,
    Field(..., description="S3 bucket name for the stored blob"),
    BeforeValidator(validate_s3_bucket),
]
"""S3 bucket type with validation."""


# =============================================================================
# Blob Model
# =============================================================================


class Blob(BaseModel):
    """
    Content-addressed blob metadata for object storage.

    Each blob represents a single stored object, identified by content hash,
    and located by bucket + key.

    Attributes:
        content_hash: SHA256 hash of the blob content
        bucket: S3 bucket name
        key: S3 object key
        size_bytes: Optional size of the object in bytes
        content_type: Optional MIME content type
        created_at: Creation timestamp
    """

    content_hash: ContentHash = Field(..., description="SHA256 content hash")
    bucket: S3Bucket = Field(..., description="S3 bucket name")
    key: S3Key = Field(..., description="S3 object key")
    size_bytes: int | None = Field(None, ge=0, description="Blob size in bytes")
    content_type: str | None = Field(None, description="MIME content type")
    created_at: datetime = Field(..., description="Creation timestamp")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "content_hash": "sha256:abc123def4567890abcdef1234567890abcdef1234567890abcdef1234567890",
                "bucket": "data-lake",
                "key": "dataset_001/v1/data.parquet",
                "size_bytes": 1024,
                "content_type": "application/parquet",
                "created_at": "2024-01-01T00:00:00Z",
            }
        }
    )
