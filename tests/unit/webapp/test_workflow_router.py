import pytest
from unittest.mock import MagicMock, patch
from app.routers.workflows import form_to_manifest_dict, validate_step, parse_wizard_state

def test_parse_wizard_state_valid():
    form_data = {"_wizard_state": '{"title": "test", "keywords": ["a"]}'}
    state = parse_wizard_state(form_data)
    assert state == {"title": "test", "keywords": ["a"]}

def test_parse_wizard_state_invalid():
    form_data = {"_wizard_state": "invalid json"}
    state = parse_wizard_state(form_data)
    assert state == {}

def test_form_to_manifest_dict_nesting_files():
    data = {
        "title": "My Dataset",
        "file_path": "s3://landing/data.geojson",
        "file_format": "GeoJSON",
        "file_type": "vector"
    }
    manifest = form_to_manifest_dict("ingest-vector", data)
    assert manifest["title"] == "My Dataset"
    assert len(manifest["files"]) == 1
    assert manifest["files"][0]["path"] == "s3://landing/data.geojson"
    assert manifest["files"][0]["format"] == "GeoJSON"

def test_form_to_manifest_dict_join_config():
    data = {
        "title": "Joined",
        "spatial_dataset_id": "sp1",
        "tabular_dataset_id": "tab1",
        "left_key": "id",
        "right_key": "sid",
        "how": "inner"
    }
    manifest = form_to_manifest_dict("join-datasets", data)
    assert manifest["join_config"]["spatial_dataset_id"] == "sp1"
    assert manifest["join_config"]["tabular_dataset_id"] == "tab1"
    assert manifest["join_config"]["left_key"] == "id"
    assert manifest["join_config"]["right_key"] == "sid"
    assert manifest["join_config"]["how"] == "inner"
    assert manifest["files"] == []

def test_validate_step_metadata_missing_title():
    data = {"description": "test"}
    errors = validate_step("ingest-vector", 0, data) # Step 0 is metadata
    assert "title" in errors
    assert "Title is required" in errors["title"]

def test_validate_step_metadata_valid():
    data = {"title": "My Title"}
    errors = validate_step("ingest-vector", 0, data)
    assert errors == {}

def test_validate_step_files_missing():
    data = {"file_format": "GeoJSON"}
    errors = validate_step("ingest-vector", 1, data) # Step 1 is files
    assert "file_path" in errors
