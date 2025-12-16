"""
Unit tests for tabular header normalization.

Tests header cleaning logic, Postgres identifier validation, collision handling,
edge cases, and advanced smart compression features.
"""

import pytest

from libs.spatial_utils.tabular_headers import (
    normalize_headers,
    normalize_headers_advanced,
    deduplicate_words,
    remove_inner_vowels,
    DEFAULT_ETL_ABBREVIATIONS,
)


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
        """Test that headers are truncated to 63 characters (Postgres limit).
        
        Uses consonants to avoid vowel removal during smart compression.
        """
        # Use consonants (not vowels) so smart compression vowel-removal doesn't affect result
        long_header = "b" * 100
        headers = [long_header]
        mapping, cleaned = normalize_headers(headers)
        
        assert len(cleaned[0]) == 63
        assert cleaned[0].startswith("b")
    
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


class TestHelperFunctions:
    """Test helper functions for smart compression."""
    
    def test_deduplicate_words_basic(self):
        """Test basic word deduplication."""
        assert deduplicate_words("user_id_user") == "user_id"
        assert deduplicate_words("date_date_date") == "date"
        assert deduplicate_words("name_value_name_value") == "name_value"
    
    def test_deduplicate_words_no_duplicates(self):
        """Test that non-duplicate words are preserved."""
        assert deduplicate_words("first_name") == "first_name"
        assert deduplicate_words("user_id") == "user_id"
    
    def test_deduplicate_words_empty(self):
        """Test empty string handling."""
        assert deduplicate_words("") == ""
        assert deduplicate_words("_") == ""
    
    def test_remove_inner_vowels_basic(self):
        """Test basic vowel removal."""
        assert remove_inner_vowels("revenue") == "rvne"
        assert remove_inner_vowels("service") == "srvce"
        assert remove_inner_vowels("identification") == "idntfctn"
    
    def test_remove_inner_vowels_short_words(self):
        """Test that short words (<=2 chars) are preserved."""
        assert remove_inner_vowels("id") == "id"
        assert remove_inner_vowels("a") == "a"
        assert remove_inner_vowels("to") == "to"
    
    def test_remove_inner_vowels_multiple_words(self):
        """Test vowel removal across multiple underscore-separated words."""
        assert remove_inner_vowels("first_name") == "frst_nme"
        assert remove_inner_vowels("user_id") == "usr_id"
    
    def test_remove_inner_vowels_no_vowels(self):
        """Test words with no inner vowels - nothing to remove."""
        # "xyz" has no vowels in the middle, so stays the same
        # (function only removes vowels, not consonants)
        assert remove_inner_vowels("xyz") == "xyz"
        assert remove_inner_vowels("bcdfg") == "bcdfg"


class TestNormalizeHeadersAdvanced:
    """Test the advanced normalization function with smart compression."""
    
    def test_default_abbreviations_applied(self):
        """Test that default ETL abbreviations are applied."""
        headers = ["International Revenue", "Quantity Number"]
        mapping, cleaned = normalize_headers_advanced(headers)
        
        # "international" -> "intl", "revenue" -> "rev"
        # "quantity" -> "qty", "number" -> "num"
        assert "intl" in cleaned[0]
        assert "qty" in cleaned[1] or "num" in cleaned[1]
    
    def test_custom_abbreviations(self):
        """Test custom abbreviations override defaults."""
        headers = ["Customer Identifier"]
        custom_abbrevs = {"customer": "cust", "identifier": "id"}
        mapping, cleaned = normalize_headers_advanced(
            headers, abbreviations=custom_abbrevs
        )
        
        assert cleaned[0] == "cust_id"
    
    def test_smart_compression_deduplication(self):
        """Test that word deduplication is applied for long headers."""
        # Create a header that's > 63 chars with repeated words
        long_header = "user_id_" * 10 + "user_id"  # Has many "user" and "id" repeats
        headers = [long_header]
        mapping, cleaned = normalize_headers_advanced(headers, abbreviations={})
        
        # After deduplication, should be much shorter
        assert len(cleaned[0]) <= 63
        assert "user" in cleaned[0]
        assert "id" in cleaned[0]
    
    def test_smart_compression_vowel_removal(self):
        """Test that vowel removal is applied when deduplication isn't enough."""
        # Create a header > 63 chars with unique words (no deduplication possible)
        # Using words that have vowels in the middle
        long_header = "revenue_service_organization_department_configuration_application"
        headers = [long_header]
        mapping, cleaned = normalize_headers_advanced(headers, abbreviations={})
        
        # Should be compressed to <= 63 chars
        assert len(cleaned[0]) <= 63
    
    def test_hard_truncation_fallback(self):
        """Test that hard truncation is used when smart compression isn't enough."""
        # Use all consonants so vowel removal doesn't help
        long_header = "b" * 100
        headers = [long_header]
        mapping, cleaned = normalize_headers_advanced(headers, abbreviations={})
        
        assert len(cleaned[0]) == 63
        assert cleaned[0] == "b" * 63
    
    def test_collision_resolution_with_suffix(self):
        """Test collision resolution adds numeric suffix."""
        headers = ["Name", "Name", "Name", "Name"]
        mapping, cleaned = normalize_headers_advanced(headers, abbreviations={})
        
        assert cleaned == ["name", "name_1", "name_2", "name_3"]
    
    def test_collision_suffix_with_truncation(self):
        """Test that collision suffix fits within 63 chars."""
        # Create many colliding long headers
        long_header = "b" * 65
        headers = [long_header] * 15  # 15 duplicates to get _14 suffix
        mapping, cleaned = normalize_headers_advanced(headers, abbreviations={})
        
        # All should be <= 63 chars and unique
        assert all(len(h) <= 63 for h in cleaned)
        assert len(set(cleaned)) == 15  # All unique
    
    def test_stop_words_with_nltk_fallback(self):
        """Test stop words handling."""
        headers = ["The Name", "A Description"]
        custom_stop_words = {"the", "a"}
        mapping, cleaned = normalize_headers_advanced(
            headers, stop_words=custom_stop_words, abbreviations={}
        )
        
        assert cleaned == ["name", "description"]
    
    def test_empty_stop_words(self):
        """Test with empty stop words set."""
        headers = ["The Name"]
        mapping, cleaned = normalize_headers_advanced(
            headers, stop_words=set(), abbreviations={}
        )
        
        # "the" should NOT be removed
        assert "the" in cleaned[0]
    
    def test_numeric_start_protection(self):
        """Test that headers starting with numbers get underscore prefix."""
        headers = ["123Column", "4th Item", "2024_data"]
        mapping, cleaned = normalize_headers_advanced(headers, abbreviations={})
        
        for header in cleaned:
            assert header.startswith("_") or header[0].isalpha()
            assert header.startswith("_")  # All should have underscore prefix
    
    def test_symbols_only_fallback(self):
        """Test that symbols-only headers fall back to col_{index}."""
        headers = ["%", "?!", "@#$"]
        mapping, cleaned = normalize_headers_advanced(headers, abbreviations={})
        
        assert cleaned == ["col_0", "col_1", "col_2"]
    
    def test_postgres_identifier_compliance(self):
        """Test that all outputs match Postgres identifier pattern."""
        import re
        
        headers = [
            "International Revenue Service Identification Number",
            "User ID User ID User",
            "123 Test Header",
            "Special!@#Chars%^&",
            "",
            "Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",  # 70 a's
        ]
        mapping, cleaned = normalize_headers_advanced(headers)
        
        pattern = r'^[a-z_][a-z0-9_]*$'
        for header in cleaned:
            assert re.match(pattern, header), f"Header '{header}' doesn't match Postgres pattern"
            assert len(header) <= 63, f"Header '{header}' exceeds 63 characters"


class TestNltkIntegration:
    """Test NLTK integration (NLTK is required)."""
    
    def test_nltk_import_available(self):
        """Test that NLTK stopwords can be imported."""
        from nltk.corpus import stopwords
        assert stopwords is not None
    
    def test_default_abbreviations_defined(self):
        """Test that DEFAULT_ETL_ABBREVIATIONS is properly defined."""
        assert isinstance(DEFAULT_ETL_ABBREVIATIONS, dict)
        assert len(DEFAULT_ETL_ABBREVIATIONS) > 0
        
        # Check some expected abbreviations
        assert DEFAULT_ETL_ABBREVIATIONS.get("quantity") == "qty"
        assert DEFAULT_ETL_ABBREVIATIONS.get("number") == "num"
        assert DEFAULT_ETL_ABBREVIATIONS.get("international") == "intl"

