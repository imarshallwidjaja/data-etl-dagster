# =============================================================================
# Base Models and Mixins
# =============================================================================
# Shared base models and mixins for metadata fields.
# =============================================================================

"""Base models and mixins for shared metadata fields."""

from pydantic import BaseModel, Field, field_validator

__all__ = ["HumanMetadataMixin"]


class HumanMetadataMixin(BaseModel):
    """
    Shared human-readable metadata fields for manifests and assets.

    Enforces consistent metadata capture across ingestion and storage layers.
    These fields represent the "what" of the data from a human perspective.

    Attributes:
        title: Human-readable title (required, can be empty)
        description: Detailed description (required, can be empty)
        keywords: Subject keywords for discovery (required, can be empty list)
        source: Data lineage/provenance statement (required, can be empty)
        license: License identifier or statement (required, can be empty)
        attribution: Credit/attribution statement (required, can be empty)
    """

    title: str = Field(..., description="Human-readable title")
    description: str = Field(..., description="Detailed description of the data")
    keywords: list[str] = Field(
        default_factory=list,
        description="Subject keywords for discovery and cataloging",
    )
    source: str = Field(..., description="Data lineage/provenance statement")
    license: str = Field(..., description="License identifier or statement (free-form)")
    attribution: str = Field(..., description="Credit/attribution statement")

    @field_validator("keywords")
    @classmethod
    def validate_keywords(cls, v: list[str]) -> list[str]:
        """
        Clean and deduplicate keywords list.

        - Removes empty/whitespace-only keywords
        - Trims whitespace from valid keywords
        - Removes duplicates (preserves order)
        - No count limit enforced
        """
        if not isinstance(v, list):
            raise TypeError("keywords must be a list")

        # Clean: trim whitespace, filter empty
        cleaned = [kw.strip() for kw in v if kw and isinstance(kw, str) and kw.strip()]

        # Deduplicate while preserving order
        unique = list(dict.fromkeys(cleaned))

        return unique

    model_config = {
        "json_schema_extra": {
            "example": {
                "title": "Urban Building Footprints",
                "description": "High-resolution building outlines from 2024 aerial survey",
                "keywords": ["buildings", "urban", "footprints", "gis"],
                "source": "City Planning Department Aerial Survey 2024",
                "license": "CC-BY-4.0",
                "attribution": "City Planning Department",
            }
        }
    }
