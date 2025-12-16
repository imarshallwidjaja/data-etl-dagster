"""
Unit tests for tabular header normalization.

Tests header cleaning logic, Postgres identifier validation, collision handling,
and edge cases.
"""

import pytest

from libs.spatial_utils.tabular_headers import normalize_headers


class TestNormalizeHeaders:
    """Test header normalization logic."""
    
    def test_basic_normalization(self):
        """Test basic header normalization (lowercase, spaces to underscores)."""
        headers = ["First Name", "Last Name", "Age"]
        mapping, cleaned = normalize_headers(headers)
        
        assert cleaned == ["first_name", "last_name", "age"]
        assert mapping["First Name"] == "first_name"
        assert mapping["Last Name"] == "last_name"
        assert mapping["Age"] == "age"
    
    def test_punctuation_removal(self):
        """Test that punctuation is removed."""
        headers = ["Name (Primary)", "Age-Years", "Email.Address"]
        mapping, cleaned = normalize_headers(headers)
        
        assert cleaned == ["name_primary", "age_years", "email_address"]
    
    def test_multiple_underscores_collapsed(self):
        """Test that multiple underscores are collapsed."""
        headers = ["First  Name", "Last___Name"]
        mapping, cleaned = normalize_headers(headers)
        
        assert cleaned == ["first_name", "last_name"]
    
    def test_leading_trailing_underscores_removed(self):
        """Test that leading/trailing underscores are removed."""
        headers = ["_Name", "Name_", "__Name__"]
        mapping, cleaned = normalize_headers(headers)
        
        # All normalize to "name", so collisions are handled: name, name_1, name_2
        assert cleaned == ["name", "name_1", "name_2"]
    
    def test_empty_headers_become_col_index(self):
        """Test that empty headers become col_{index}."""
        headers = ["Name", "", "Age"]
        mapping, cleaned = normalize_headers(headers)
        
        assert cleaned[0] == "name"
        assert cleaned[1] == "col_1"
        assert cleaned[2] == "age"
        assert mapping[""] == "col_1"
    
    def test_collision_handling(self):
        """Test that collisions are handled with numeric suffixes."""
        headers = ["Name", "Name", "Name"]
        mapping, cleaned = normalize_headers(headers)
        
        # First occurrence is "name", subsequent ones get name_1, name_2
        assert cleaned == ["name", "name_1", "name_2"]
        # Mapping stores the last occurrence for duplicate keys (dict behavior)
        # But we can verify all cleaned headers are unique
        assert len(set(cleaned)) == 3
        assert all(h.startswith("name") for h in cleaned)
    
    def test_truncation_to_63_chars(self):
        """Test that headers are truncated to 63 characters (Postgres limit)."""
        long_header = "a" * 100
        headers = [long_header]
        mapping, cleaned = normalize_headers(headers)
        
        assert len(cleaned[0]) == 63
        assert cleaned[0].startswith("a")
    
    def test_truncation_with_collision_suffix(self):
        """Test that truncation accounts for collision suffix length."""
        # Create headers that will collide after truncation
        header1 = "a" * 65
        header2 = "a" * 66
        headers = [header1, header2]
        mapping, cleaned = normalize_headers(headers)
        
        # Both should be valid Postgres identifiers
        assert len(cleaned[0]) <= 63
        assert len(cleaned[1]) <= 63
        assert cleaned[0] != cleaned[1]
    
    def test_starts_with_letter_or_underscore(self):
        """Test that headers starting with numbers get underscore prefix."""
        headers = ["123Column", "4th Item"]
        mapping, cleaned = normalize_headers(headers)
        
        assert cleaned[0].startswith("_")
        assert cleaned[1].startswith("_")
        assert cleaned[0] == "_123column"
        assert cleaned[1] == "_4th_item"
    
    def test_stop_words_removal(self):
        """Test that stop words are removed."""
        headers = ["The Name", "A Description", "An Item"]
        mapping, cleaned = normalize_headers(headers, stop_words=["the", "a", "an"])
        
        assert cleaned == ["name", "description", "item"]
    
    def test_abbreviation_expansion(self):
        """Test that abbreviations are expanded."""
        headers = ["St Name", "Qty Items"]
        mapping, cleaned = normalize_headers(
            headers,
            abbreviations={"st": "street", "qty": "quantity"}
        )
        
        assert cleaned == ["street_name", "quantity_items"]
    
    def test_abbreviation_whole_word_only(self):
        """Test that abbreviations only match whole words."""
        headers = ["St Name", "Qty Items"]
        mapping, cleaned = normalize_headers(
            headers,
            abbreviations={"st": "street", "qty": "quantity"}
        )
        
        # Should expand "st" and "qty" as whole words
        assert "street" in cleaned[0].lower()
        assert "quantity" in cleaned[1].lower()
        
        # But "Street" (capitalized) should not match "st" abbreviation
        headers2 = ["Street Name", "Quantity Items"]
        mapping2, cleaned2 = normalize_headers(
            headers2,
            abbreviations={"st": "street", "qty": "quantity"}
        )
        # After lowercasing, "street" in "Street Name" becomes "street name"
        # The abbreviation matching is case-insensitive, so it will match
        # This is expected behavior - abbreviations match whole words case-insensitively
        assert cleaned2[0] == "street_name"
    
    def test_postgres_identifier_pattern(self):
        """Test that all cleaned headers match Postgres identifier pattern."""
        headers = [
            "Name",
            "Age (Years)",
            "Email Address",
            "123Column",
            "Special!@#Chars",
            "",
            "Very Long Header Name That Should Be Truncated",
        ]
        mapping, cleaned = normalize_headers(headers)
        
        import re
        pattern = r'^[a-z_][a-z0-9_]*$'
        for header in cleaned:
            assert re.match(pattern, header), f"Header '{header}' doesn't match Postgres pattern"
            assert len(header) <= 63, f"Header '{header}' exceeds 63 characters"
    
    def test_complex_real_world_example(self):
        """Test with complex real-world header names."""
        headers = [
            "First Name",
            "Last Name",
            "Email Address",
            "Phone Number",
            "Date of Birth",
            "Postal Code",
            "Country/Region",
            "Subscription Status",
            "Last Login (UTC)",
            "Account Balance ($)",
        ]
        mapping, cleaned = normalize_headers(headers)
        
        assert len(cleaned) == len(headers)
        assert all(len(h) <= 63 for h in cleaned)
        assert all(h.islower() or h.startswith("_") for h in cleaned)
        assert "first_name" in cleaned
        assert "last_name" in cleaned
        assert "email_address" in cleaned
    
    def test_all_empty_headers(self):
        """Test handling when all headers are empty."""
        headers = ["", "", ""]
        mapping, cleaned = normalize_headers(headers)
        
        assert cleaned == ["col_0", "col_1", "col_2"]
        assert all(h.startswith("col_") for h in cleaned)
    
    def test_unicode_characters_removed(self):
        """Test that unicode characters are removed."""
        headers = ["Nom (Français)", "Age (年)"]
        mapping, cleaned = normalize_headers(headers)
        
        # Should only contain ASCII alphanumeric and underscores
        import re
        pattern = r'^[a-z0-9_]+$'
        for header in cleaned:
            assert re.match(pattern, header), f"Header '{header}' contains non-ASCII characters"

