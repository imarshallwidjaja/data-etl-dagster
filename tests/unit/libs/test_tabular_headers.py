"""
Unit tests for tabular header normalization.
"""

import pytest
from libs.spatial_utils import normalize_tabular_headers


class TestNormalizeTabularHeaders:
    """Test header normalization functionality."""

    def test_basic_normalization(self):
        """Test basic case/whitespace/punctuation normalization."""
        headers = ["First Name", "Last Name", "Age", "Email Address"]
        result = normalize_tabular_headers(headers)

        expected = {
            "First Name": "first_name",
            "Last Name": "last_name",
            "Age": "age",
            "Email Address": "email_address"
        }
        assert result == expected

    def test_case_insensitive_and_whitespace(self):
        """Test case normalization and whitespace handling."""
        headers = ["FIRST NAME", "  Last   Name  ", "age"]
        result = normalize_tabular_headers(headers)

        expected = {
            "FIRST NAME": "first_name",
            "  Last   Name  ": "last_name",
            "age": "age"
        }
        assert result == expected

    def test_punctuation_handling(self):
        """Test punctuation replacement with underscores."""
        headers = ["Name (First)", "Name.Last", "Qty: 100", "A/B/C"]
        result = normalize_tabular_headers(headers)

        expected = {
            "Name (First)": "name_first",
            "Name.Last": "name_last",
            "Qty: 100": "qty_100",
            "A/B/C": "a_b_c"
        }
        assert result == expected

    def test_stop_words_removal(self):
        """Test stop word removal."""
        headers = ["The First Name", "Last Name Of Person", "Age In Years"]
        stop_words = {"the", "of", "in"}
        result = normalize_tabular_headers(headers, stop_words=stop_words)

        expected = {
            "The First Name": "first_name",
            "Last Name Of Person": "last_name_person",
            "Age In Years": "age_years"
        }
        assert result == expected

    def test_abbreviations(self):
        """Test abbreviation expansion."""
        headers = ["St Name", "Qty", "Addr"]
        abbreviations = {"st": "street", "qty": "quantity", "addr": "address"}
        result = normalize_tabular_headers(headers, abbreviations=abbreviations)

        expected = {
            "St Name": "street_name",
            "Qty": "quantity",
            "Addr": "address"
        }
        assert result == expected

    def test_empty_headers(self):
        """Test handling of empty headers."""
        headers = ["Name", "", "Age", ""]
        result = normalize_tabular_headers(headers)

        expected = {
            "Name": "name",
            "": "col_1",
            "Age": "age",
            "": "col_3"  # Second empty gets col_3 (index 3)
        }
        assert result == expected

    def test_collision_resolution(self):
        """Test deterministic collision resolution."""
        headers = ["Name", "name"]  # Both normalize to "name" base
        result = normalize_tabular_headers(headers)

        expected = {
            "Name": "name",      # "Name" normalizes to "name", gets it first
            "name": "name_2"     # "name" also normalizes to "name", but collides, gets "name_2"
        }
        assert result == expected

    def test_postgres_identifier_rules(self):
        """Test enforcement of Postgres identifier rules."""
        # Headers that would start with numbers
        headers = ["123abc", "9lives", "_underscore", "valid_name"]
        result = normalize_tabular_headers(headers)

        expected = {
            "123abc": "col_0",  # Invalid start -> col_0
            "9lives": "col_1",  # Invalid start -> col_1
            "_underscore": "_underscore",  # Valid
            "valid_name": "valid_name"  # Valid
        }
        assert result == expected

    def test_length_limit(self):
        """Test 63 character limit enforcement."""
        long_header = "a" * 100
        headers = [long_header]
        result = normalize_tabular_headers(headers)

        cleaned = result[long_header]
        assert len(cleaned) <= 63
        assert cleaned == "a" * 63  # Should be truncated

    def test_complex_collision_with_length_limit(self):
        """Test collision resolution when truncation is involved."""
        # Create different headers that all normalize to the same long string
        headers = ["a" * 65 + "x", "a" * 65 + "y", "a" * 65 + "z"]
        result = normalize_tabular_headers(headers)

        # All get truncated to 63 chars, then collision resolution
        values = list(result.values())
        assert len(values) == 3
        assert "a" * 63 in values  # First one gets full 63 chars
        # Others get truncated to make room for suffix
        assert any(v.startswith("a" * 62) for v in values)  # Truncated for suffix

    def test_mixed_invalid_characters(self):
        """Test handling of various invalid characters."""
        headers = ["name@example.com", "user-name", "field.value", "data+test"]
        result = normalize_tabular_headers(headers)

        expected = {
            "name@example.com": "name_example_com",
            "user-name": "user_name",
            "field.value": "field_value",
            "data+test": "data_test"
        }
        assert result == expected

    def test_empty_after_stop_words(self):
        """Test headers that become empty after stop word removal."""
        headers = ["the", "and", "or"]
        stop_words = {"the", "and", "or"}
        result = normalize_tabular_headers(headers, stop_words=stop_words)

        expected = {
            "the": "col_0",
            "and": "col_1",
            "or": "col_2"
        }
        assert result == expected

    def test_abbreviations_before_stop_words(self):
        """Test that abbreviations are applied before stop word removal."""
        headers = ["the st name"]
        abbreviations = {"st": "street"}
        stop_words = {"the"}
        result = normalize_tabular_headers(headers, stop_words=stop_words, abbreviations=abbreviations)

        expected = {
            "the st name": "street_name"
        }
        assert result == expected

    def test_no_modification_of_valid_identifiers(self):
        """Test that valid Postgres identifiers are left unchanged."""
        headers = ["id", "user_id", "created_at", "first_name", "last_name"]
        result = normalize_tabular_headers(headers)

        # Should remain unchanged
        for header in headers:
            assert result[header] == header
