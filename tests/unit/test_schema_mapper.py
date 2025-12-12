"""
Unit tests for RunIdSchemaMapping.

Tests bidirectional conversion between Dagster run_id and PostgreSQL schema names.
"""

import pytest
from libs.spatial_utils import RunIdSchemaMapping


# =============================================================================
# Test: from_run_id - Creation from Run ID
# =============================================================================

def test_from_run_id_creates_schema_name():
    """Test that from_run_id creates correct schema name."""
    run_id = "abc12345-def6-7890-abcd-ef1234567890"
    mapping = RunIdSchemaMapping.from_run_id(run_id)
    
    assert mapping.run_id == run_id
    assert mapping.schema_name == "proc_abc12345_def6_7890_abcd_ef1234567890"


def test_from_run_id_replaces_hyphens_with_underscores():
    """Test that hyphens are replaced with underscores."""
    run_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    mapping = RunIdSchemaMapping.from_run_id(run_id)
    
    assert "-" not in mapping.schema_name
    assert mapping.schema_name.count("_") == 5  # proc_ prefix + 4 hyphens become underscores


def test_from_run_id_adds_proc_prefix():
    """Test that schema_name starts with 'proc_'."""
    run_id = "12345678-abcd-ef01-2345-6789abcdef01"
    mapping = RunIdSchemaMapping.from_run_id(run_id)
    
    assert mapping.schema_name.startswith("proc_")


def test_from_run_id_with_various_hex_formats():
    """Test that from_run_id works with different hex case."""
    # UUIDs can have both uppercase and lowercase hex
    run_id = "ABCDEF01-2345-6789-ABCD-EF0123456789"
    mapping = RunIdSchemaMapping.from_run_id(run_id)
    
    assert mapping.schema_name == "proc_ABCDEF01_2345_6789_ABCD_EF0123456789"


# =============================================================================
# Test: from_run_id - Validation
# =============================================================================

def test_from_run_id_rejects_empty_string():
    """Test that from_run_id rejects empty run_id."""
    with pytest.raises(ValueError, match="empty"):
        RunIdSchemaMapping.from_run_id("")


def test_from_run_id_rejects_invalid_uuid_format():
    """Test that from_run_id rejects non-UUID strings."""
    with pytest.raises(ValueError, match="UUID format"):
        RunIdSchemaMapping.from_run_id("not-a-valid-uuid")


def test_from_run_id_rejects_uuid_with_wrong_part_count():
    """Test that from_run_id rejects UUIDs with wrong segment count."""
    with pytest.raises(ValueError, match="UUID format"):
        RunIdSchemaMapping.from_run_id("abc12345-def6-7890-ijkl")  # Only 4 parts


def test_from_run_id_rejects_uuid_with_wrong_segment_length():
    """Test that from_run_id rejects UUIDs with wrong segment lengths."""
    # First segment should be 8 chars, but we provide 7
    with pytest.raises(ValueError, match="wrong length"):
        RunIdSchemaMapping.from_run_id("abc1234-def6-7890-ijkl-mnop12345678")


def test_from_run_id_rejects_non_hex_characters():
    """Test that from_run_id rejects non-hex characters in UUID."""
    with pytest.raises(ValueError, match="non-hex"):
        RunIdSchemaMapping.from_run_id("zzzzzzzz-xxxx-yyyy-wwww-vvvvvvvvvvvv")


# =============================================================================
# Test: from_schema_name - Reverse Mapping
# =============================================================================

def test_from_schema_name_extracts_run_id():
    """Test that from_schema_name correctly extracts original run_id."""
    schema_name = "proc_abc12345_def6_7890_abcd_ef1234567890"
    mapping = RunIdSchemaMapping.from_schema_name(schema_name)
    
    assert mapping.run_id == "abc12345-def6-7890-abcd-ef1234567890"
    assert mapping.schema_name == schema_name


def test_from_schema_name_restores_hyphens():
    """Test that from_schema_name restores hyphens at correct positions."""
    schema_name = "proc_aaaaaaaa_bbbb_cccc_dddd_eeeeeeeeeeee"
    mapping = RunIdSchemaMapping.from_schema_name(schema_name)
    
    run_id = mapping.run_id
    
    # Check hyphen positions (standard UUID positions: 8, 13, 18, 23)
    assert run_id == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    assert run_id[8] == "-"
    assert run_id[13] == "-"
    assert run_id[18] == "-"
    assert run_id[23] == "-"


def test_from_schema_name_requires_proc_prefix():
    """Test that from_schema_name rejects schema without 'proc_' prefix."""
    with pytest.raises(ValueError, match="must start with 'proc_'"):
        RunIdSchemaMapping.from_schema_name("something_abc12345_def6_7890_ijkl_mnop12345678")


def test_from_schema_name_rejects_malformed_schema_name():
    """Test that from_schema_name rejects truncated/malformed names."""
    # Too short - missing UUID segments
    with pytest.raises(ValueError):  # Will fail validation during reconstruction
        RunIdSchemaMapping.from_schema_name("proc_short")


# =============================================================================
# Test: Round-Trip Conversion
# =============================================================================

def test_round_trip_run_id_to_schema_name_to_run_id():
    """Test bidirectional conversion: run_id → schema_name → run_id."""
    original_run_id = "12345678-abcd-ef01-2345-6789abcdef01"
    
    # Forward: run_id → schema_name
    forward_mapping = RunIdSchemaMapping.from_run_id(original_run_id)
    schema_name = forward_mapping.schema_name
    assert schema_name == "proc_12345678_abcd_ef01_2345_6789abcdef01"
    
    # Reverse: schema_name → run_id
    reverse_mapping = RunIdSchemaMapping.from_schema_name(schema_name)
    recovered_run_id = reverse_mapping.run_id
    
    # Verify round-trip succeeds
    assert recovered_run_id == original_run_id


def test_round_trip_schema_name_to_run_id_to_schema_name():
    """Test bidirectional conversion: schema_name → run_id → schema_name."""
    original_schema = "proc_abc12345_def6_7890_abcd_ef1234567890"
    
    # Forward: schema_name → run_id
    forward_mapping = RunIdSchemaMapping.from_schema_name(original_schema)
    run_id = forward_mapping.run_id
    assert run_id == "abc12345-def6-7890-abcd-ef1234567890"
    
    # Reverse: run_id → schema_name
    reverse_mapping = RunIdSchemaMapping.from_run_id(run_id)
    recovered_schema = reverse_mapping.schema_name
    
    # Verify round-trip succeeds
    assert recovered_schema == original_schema


# =============================================================================
# Test: Model Validation
# =============================================================================

def test_model_validation_requires_valid_schema_name():
    """Test that model validation rejects invalid schema names."""
    # Missing 'proc_' prefix
    with pytest.raises(ValueError, match="must start with 'proc_'"):
        RunIdSchemaMapping(
            run_id="abc12345-def6-7890-ijkl-mnop12345678",
            schema_name="invalid_abc12345_def6_7890_ijkl_mnop12345678"
        )


def test_model_validation_rejects_schema_with_invalid_chars():
    """Test that model validation rejects schema names with invalid characters."""
    with pytest.raises(ValueError, match="invalid character"):
        RunIdSchemaMapping(
            run_id="abc12345-def6-7890-ijkl-mnop12345678",
            schema_name="proc_abc-12345_def6"  # Hyphens not allowed
        )


def test_model_validation_requires_valid_run_id():
    """Test that model validation rejects invalid run_ids."""
    with pytest.raises(ValueError, match="UUID format"):
        RunIdSchemaMapping(
            run_id="invalid-run-id",
            schema_name="proc_abc12345_def6_7890_ijkl_mnop12345678"
        )


# =============================================================================
# Test: Edge Cases
# =============================================================================

def test_handles_uppercase_hex_in_uuid():
    """Test that both uppercase and lowercase hex work."""
    run_id = "ABCD1234-EF56-7890-ABCD-EF1234567890"
    mapping = RunIdSchemaMapping.from_run_id(run_id)
    
    # Should preserve case in schema name
    assert "ABCD1234" in mapping.schema_name
    
    # Reverse should also work
    reverse = RunIdSchemaMapping.from_schema_name(mapping.schema_name)
    assert reverse.run_id == run_id


def test_different_uuid_values():
    """Test with various realistic Dagster run IDs."""
    test_uuids = [
        "00000000-0000-0000-0000-000000000000",  # All zeros
        "ffffffff-ffff-ffff-ffff-ffffffffffff",  # All ones (hex)
        "f47ac10b-58cc-4372-a567-0e02b2c3d479",  # Real-world example
        "550e8400-e29b-41d4-a716-446655440000",  # Real-world example
    ]
    
    for run_id in test_uuids:
        mapping = RunIdSchemaMapping.from_run_id(run_id)
        reverse = RunIdSchemaMapping.from_schema_name(mapping.schema_name)
        assert reverse.run_id == run_id, f"Round-trip failed for {run_id}"
