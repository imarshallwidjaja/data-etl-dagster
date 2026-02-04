# =============================================================================
# Artifact Model
# =============================================================================
# Defines per-upload artifacts for raw and intermediate processing.
# =============================================================================

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

__all__ = ["Artifact"]


class Artifact(BaseModel):
    """
    Per-upload artifact model for raw source and intermediate outputs.

    Attributes:
        kind: Artifact type (raw_source or intermediate)
        blob_id: Storage blob identifier
        batch_id: Manifest batch identifier
        run_id: MongoDB run document ObjectId (optional)
        created_at: Artifact creation timestamp
        source_s3_path: Source object path for raw uploads (required for raw_source)
        original_filename: Original filename from upload (required for raw_source)
        uploader: User identifier for raw upload (required for raw_source)
        content_type: MIME content type (optional)
        producer: System component that produced intermediate (required for intermediate)
        label: Short label for intermediate artifact (required for intermediate)
        parameters: Producer parameters (defaults to {})
    """

    kind: Literal["raw_source", "intermediate"] = Field(
        ..., description="Artifact kind"
    )
    blob_id: str = Field(..., description="Storage blob identifier")
    batch_id: str = Field(..., description="Manifest batch identifier")
    run_id: str | None = Field(None, description="MongoDB run ObjectId")
    created_at: datetime = Field(..., description="Creation timestamp")

    # Raw source fields
    source_s3_path: str | None = Field(
        None, description="Source object path (required for raw_source)"
    )
    original_filename: str | None = Field(
        None, description="Original filename (required for raw_source)"
    )
    uploader: str | None = Field(
        None, description="Uploader identifier (required for raw_source)"
    )
    content_type: str | None = Field(None, description="MIME content type")

    # Intermediate fields
    producer: str | None = Field(
        None, description="Producer component (required for intermediate)"
    )
    label: str | None = Field(
        None, description="Intermediate label (required for intermediate)"
    )
    parameters: dict[str, Any] = Field(
        default_factory=dict, description="Producer parameters"
    )

    @field_validator("parameters", mode="before")
    @classmethod
    def normalize_parameters(cls, value: Any) -> dict[str, Any]:
        """Normalize None parameters to empty dict."""
        if value is None:
            return {}
        return value

    @model_validator(mode="after")
    def validate_kind_specific_fields(self) -> "Artifact":
        """Enforce kind-specific required fields."""
        if self.kind == "raw_source":
            missing = [
                field
                for field in ["source_s3_path", "original_filename", "uploader"]
                if getattr(self, field) is None
            ]
            if missing:
                raise ValueError(
                    "raw_source artifacts require fields: " + ", ".join(missing)
                )

        if self.kind == "intermediate":
            missing = [
                field for field in ["producer", "label"] if getattr(self, field) is None
            ]
            if missing:
                raise ValueError(
                    "intermediate artifacts require fields: " + ", ".join(missing)
                )

        return self

    model_config = ConfigDict(extra="forbid")
