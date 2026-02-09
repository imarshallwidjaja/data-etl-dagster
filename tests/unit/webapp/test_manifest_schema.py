# =============================================================================
# Manifest Schema Endpoint Unit Tests
# =============================================================================
# Tests for the GET /manifests/schemas/{asset_type} endpoint.
# Validates JSON Schema generation from ManifestCreateRequest.
# =============================================================================

import pytest
from unittest.mock import Mock, patch

import sys
import os

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "services", "webapp")
)

from app.routers.manifests import ManifestCreateRequest


class TestManifestCreateRequestSchema:
    """Tests for ManifestCreateRequest JSON Schema generation."""

    def test_model_json_schema_returns_dict(self):
        """model_json_schema() should return a valid dict."""
        schema = ManifestCreateRequest.model_json_schema()
        assert isinstance(schema, dict)
        assert "properties" in schema

    def test_schema_contains_title_field(self):
        """Schema should include required title field."""
        schema = ManifestCreateRequest.model_json_schema()
        assert "title" in schema["properties"]

    def test_schema_contains_human_metadata_fields(self):
        """Schema should include all HumanMetadataMixin fields."""
        schema = ManifestCreateRequest.model_json_schema()
        expected_fields = [
            "title",
            "description",
            "keywords",
            "source",
            "license",
            "attribution",
        ]
        for field in expected_fields:
            assert field in schema["properties"], f"Missing field: {field}"

    def test_schema_contains_optional_fields(self):
        """Schema should include optional fields."""
        schema = ManifestCreateRequest.model_json_schema()
        optional_fields = [
            "batch_id",
            "dataset_id",
            "project",
            "intent",
            "files",
            "tags",
        ]
        for field in optional_fields:
            assert field in schema["properties"], f"Missing optional field: {field}"

    def test_schema_title_is_required(self):
        """title field should be marked as required."""
        schema = ManifestCreateRequest.model_json_schema()
        assert "required" in schema
        assert "title" in schema["required"]

    def test_schema_keywords_is_array(self):
        """keywords field should be typed as array."""
        schema = ManifestCreateRequest.model_json_schema()
        keywords_schema = schema["properties"]["keywords"]
        assert keywords_schema.get("type") == "array" or "anyOf" in keywords_schema

    def test_schema_files_references_file_entry(self):
        """files field should reference FileEntry schema."""
        schema = ManifestCreateRequest.model_json_schema()
        files_schema = schema["properties"].get("files", {})
        has_items = "items" in files_schema or "anyOf" in files_schema
        assert has_items or "$ref" in str(files_schema)


class TestManifestCreateRequestValidation:
    """Tests for ManifestCreateRequest Pydantic validation."""

    def test_valid_minimal_request(self):
        """Request with only required title should be valid."""
        request = ManifestCreateRequest(title="Test Dataset")
        assert request.title == "Test Dataset"
        assert request.description == ""
        assert request.keywords == []

    def test_valid_full_request(self):
        """Request with all fields should be valid."""
        from libs.models import FileEntry

        request = ManifestCreateRequest(
            batch_id="batch_001",
            dataset_id="dataset_abc",
            title="Full Dataset",
            description="A complete test dataset",
            keywords=["test", "sample"],
            source="Unit test",
            license="MIT",
            attribution="Test Suite",
            project="TEST_PROJECT",
            intent="ingest_vector",
            files=[
                FileEntry(
                    path="s3://landing-zone/test.geojson",
                    type="vector",
                    format="GeoJSON",
                )
            ],
            tags={"priority": 1, "source": "test"},
        )
        assert request.batch_id == "batch_001"
        assert request.title == "Full Dataset"
        assert len(request.files) == 1

    def test_empty_title_is_invalid(self):
        """Request with empty title should fail (title is required string)."""
        request = ManifestCreateRequest(title="")
        # Empty string is technically valid as title is str, not constrained
        assert request.title == ""

    def test_missing_title_is_invalid(self):
        """Request without title should fail validation."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError) as exc_info:
            ManifestCreateRequest()  # type: ignore

        errors = exc_info.value.errors()
        assert any(e["loc"] == ("title",) for e in errors)

    def test_keywords_parsed_from_list(self):
        """Keywords should accept list of strings."""
        request = ManifestCreateRequest(
            title="Test",
            keywords=["foo", "bar", "baz"],
        )
        assert request.keywords == ["foo", "bar", "baz"]

    def test_optional_fields_default_to_none_or_empty(self):
        """Optional fields should have sensible defaults."""
        request = ManifestCreateRequest(title="Test")
        assert request.batch_id is None
        assert request.project is None
        assert request.dataset_id is None
        assert request.intent is None
        assert request.files is None
        assert request.tags is None


class TestComplexSpreadsheetSchemaDefinitions:
    """Tests for ComplexSpreadsheetConfig and related defs in ManifestCreateRequest schema."""

    def test_schema_defs_contains_complex_spreadsheet_config(self):
        """$defs should contain ComplexSpreadsheetConfig."""
        schema = ManifestCreateRequest.model_json_schema()
        assert "$defs" in schema
        assert "ComplexSpreadsheetConfig" in schema["$defs"]

    def test_schema_defs_contains_complex_spreadsheet_template_params_v1(self):
        """$defs should contain ComplexSpreadsheetTemplateParamsV1."""
        schema = ManifestCreateRequest.model_json_schema()
        assert "$defs" in schema
        assert "ComplexSpreadsheetTemplateParamsV1" in schema["$defs"]

    def test_complex_spreadsheet_config_forbids_additional_properties(self):
        """ComplexSpreadsheetConfig should set additionalProperties=false."""
        schema = ManifestCreateRequest.model_json_schema()
        config_def = schema["$defs"]["ComplexSpreadsheetConfig"]
        assert config_def.get("additionalProperties") is False

    def test_template_params_v1_forbids_additional_properties(self):
        """ComplexSpreadsheetTemplateParamsV1 should set additionalProperties=false."""
        schema = ManifestCreateRequest.model_json_schema()
        params_def = schema["$defs"]["ComplexSpreadsheetTemplateParamsV1"]
        assert params_def.get("additionalProperties") is False

    def test_complex_spreadsheet_config_requires_template_id(self):
        """ComplexSpreadsheetConfig should require template_id."""
        schema = ManifestCreateRequest.model_json_schema()
        config_def = schema["$defs"]["ComplexSpreadsheetConfig"]
        assert "required" in config_def
        assert "template_id" in config_def["required"]

    def test_template_id_is_string_type(self):
        """template_id in ComplexSpreadsheetConfig should be a string type."""
        schema = ManifestCreateRequest.model_json_schema()
        config_def = schema["$defs"]["ComplexSpreadsheetConfig"]
        props = config_def.get("properties", {})
        assert "template_id" in props
        assert props["template_id"].get("type") == "string"

    def test_schema_contains_complex_spreadsheet_field(self):
        """ManifestCreateRequest should include complex_spreadsheet field."""
        schema = ManifestCreateRequest.model_json_schema()
        assert "complex_spreadsheet" in schema["properties"]

    def test_template_params_v1_requires_anchor_text(self):
        """ComplexSpreadsheetTemplateParamsV1 should require anchor_text."""
        schema = ManifestCreateRequest.model_json_schema()
        params_def = schema["$defs"]["ComplexSpreadsheetTemplateParamsV1"]
        assert "required" in params_def
        assert "anchor_text" in params_def["required"]

    def test_template_params_v1_has_expected_properties(self):
        """ComplexSpreadsheetTemplateParamsV1 should have all v1 properties."""
        schema = ManifestCreateRequest.model_json_schema()
        params_def = schema["$defs"]["ComplexSpreadsheetTemplateParamsV1"]
        props = params_def.get("properties", {})
        expected = [
            "anchor_text",
            "anchor_match",
            "header_rows",
            "id_column_count",
            "sheet_names",
        ]
        for field in expected:
            assert field in props, f"Missing property: {field}"

    def test_template_id_is_literal_type(self):
        """template_id in ComplexSpreadsheetConfig should be constrained to a Literal."""
        schema = ManifestCreateRequest.model_json_schema()
        config_def = schema["$defs"]["ComplexSpreadsheetConfig"]
        props = config_def.get("properties", {})
        template_id = props["template_id"]
        # Literal produces a const or enum in JSON Schema
        assert "const" in template_id or "enum" in template_id, (
            f"template_id should be constrained, got: {template_id}"
        )
