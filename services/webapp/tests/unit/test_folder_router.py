# =============================================================================
# Landing Zone Folder Router Unit Tests
# =============================================================================
# Tests for folder name validation.
# =============================================================================

import pytest
import re
from pydantic import BaseModel, Field, field_validator, ValidationError

# Folder name validation pattern: alphanumeric, dashes, underscores
# Copied from router to test independently without FastAPI
FOLDER_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


class FolderCreateRequest(BaseModel):
    """Request for creating a folder - copied for testing without FastAPI."""

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


class TestFolderNameValidation:
    """Tests for folder name validation."""

    def test_valid_alphanumeric_name(self):
        """Alphanumeric folder names should be valid."""
        request = FolderCreateRequest(name="test123")
        assert request.name == "test123"

    def test_valid_name_with_underscore(self):
        """Folder names with underscores should be valid."""
        request = FolderCreateRequest(name="batch_001")
        assert request.name == "batch_001"

    def test_valid_name_with_dash(self):
        """Folder names with dashes should be valid."""
        request = FolderCreateRequest(name="my-folder")
        assert request.name == "my-folder"

    def test_valid_name_mixed(self):
        """Folder names with mixed valid characters should be valid."""
        request = FolderCreateRequest(name="Test_Folder-123")
        assert request.name == "Test_Folder-123"

    def test_invalid_name_with_space(self):
        """Folder names with spaces should be rejected."""
        with pytest.raises(ValidationError) as exc_info:
            FolderCreateRequest(name="my folder")

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert "letters, numbers, dashes, and underscores" in errors[0]["msg"]

    def test_invalid_name_with_special_chars(self):
        """Folder names with special characters should be rejected."""
        invalid_names = [
            "test@folder",
            "my.folder",
            "folder/name",
            "test#123",
            "folder!",
        ]
        for name in invalid_names:
            with pytest.raises(ValidationError):
                FolderCreateRequest(name=name)

    def test_invalid_empty_name(self):
        """Empty folder names should be rejected."""
        with pytest.raises(ValidationError) as exc_info:
            FolderCreateRequest(name="")

        errors = exc_info.value.errors()
        # Should contain error about empty name
        assert len(errors) >= 1

    def test_prefix_is_optional(self):
        """Prefix should be optional and default to empty string."""
        request = FolderCreateRequest(name="test_folder")
        assert request.prefix == ""

    def test_prefix_can_be_set(self):
        """Prefix can be set for nested folders."""
        request = FolderCreateRequest(name="child", prefix="parent/")
        assert request.prefix == "parent/"


class TestFolderNamePattern:
    """Tests for folder name regex pattern."""

    def test_pattern_matches_valid_names(self):
        """Pattern should match valid folder names."""
        valid_names = [
            "test",
            "TEST",
            "Test123",
            "batch_001",
            "my-folder",
            "a",
            "123",
            "Test_Folder-123",
        ]
        for name in valid_names:
            assert FOLDER_NAME_PATTERN.match(name), f"Pattern should match: {name}"

    def test_pattern_rejects_invalid_names(self):
        """Pattern should reject invalid folder names."""
        invalid_names = [
            "my folder",
            "test.folder",
            "folder/name",
            "test@123",
            "",
            " test",
            "test ",
        ]
        for name in invalid_names:
            result = FOLDER_NAME_PATTERN.match(name)
            assert not result, f"Pattern should reject: {name}"
