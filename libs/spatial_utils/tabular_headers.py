# =============================================================================
# Tabular Headers Module
# =============================================================================
# Provides deterministic header normalization for tabular data ingestion.
# Ensures headers conform to Postgres identifier rules for reliable joins.
# Uses "smart compression" techniques to preserve semantics over hard truncation.
# =============================================================================

import re
from typing import Dict, List, Optional, Set, Tuple

from nltk.corpus import stopwords as _nltk_stopwords_module

__all__ = ["normalize_headers", "normalize_headers_advanced"]

# -----------------------------------------------------------------------------
# NLTK stopwords loading
# -----------------------------------------------------------------------------
_nltk_stopwords: Optional[Set[str]] = None


def _get_nltk_stopwords() -> Set[str]:
    """
    Load NLTK English stopwords.

    Raises:
        LookupError: If NLTK stopwords data is not downloaded. Run:
            python -c "import nltk; nltk.download('stopwords')"
    """
    global _nltk_stopwords
    if _nltk_stopwords is None:
        _nltk_stopwords = set(_nltk_stopwords_module.words("english"))
    return _nltk_stopwords


# -----------------------------------------------------------------------------
# Default ETL abbreviations (long form -> short form for compression)
# -----------------------------------------------------------------------------
DEFAULT_ETL_ABBREVIATIONS: Dict[str, str] = {
    "quantity": "qty",
    "number": "num",
    "amount": "amt",
    "estimated": "est",
    "average": "avg",
    "international": "intl",
    "identifier": "id",
    "identification": "id",
    "description": "desc",
    "information": "info",
    "organization": "org",
    "department": "dept",
    "configuration": "config",
    "application": "app",
    "reference": "ref",
    "transaction": "txn",
    "maximum": "max",
    "minimum": "min",
    "percentage": "pct",
    "address": "addr",
    "telephone": "tel",
    "category": "cat",
    "coordinate": "coord",
    "longitude": "lon",
    "latitude": "lat",
    "geometry": "geom",
    "service": "svc",
    "revenue": "rev",
}


# -----------------------------------------------------------------------------
# Helper Functions for Smart Compression
# -----------------------------------------------------------------------------
def deduplicate_words(text: str) -> str:
    """
    Remove redundant words in a string while preserving order.

    Args:
        text: Underscore-separated string (e.g., "user_id_user").

    Returns:
        String with duplicate words removed (e.g., "user_id").

    Examples:
        >>> deduplicate_words("user_id_user")
        'user_id'
        >>> deduplicate_words("date_date_date")
        'date'
    """
    words = text.split("_")
    seen: Set[str] = set()
    result: List[str] = []
    for word in words:
        if word and word not in seen:
            seen.add(word)
            result.append(word)
    return "_".join(result)


def remove_inner_vowels(text: str) -> str:
    """
    Shorten words by removing inner vowels while preserving readability.

    For each word:
    - If length <= 2: keep as-is
    - Else: keep first char + remove vowels from middle + keep last char

    Args:
        text: Underscore-separated string (e.g., "revenue").

    Returns:
        String with inner vowels removed (e.g., "rvne").

    Examples:
        >>> remove_inner_vowels("revenue")
        'rvne'
        >>> remove_inner_vowels("data")
        'dta'
        >>> remove_inner_vowels("id")
        'id'
    """
    vowels = set("aeiou")
    words = text.split("_")
    result: List[str] = []
    for word in words:
        if len(word) <= 2:
            result.append(word)
        else:
            # Keep first char, remove vowels from middle, keep last char
            middle = word[1:-1]
            compressed_middle = "".join(c for c in middle if c not in vowels)
            result.append(word[0] + compressed_middle + word[-1])
    return "_".join(result)


# -----------------------------------------------------------------------------
# Main Normalization Function (Advanced)
# -----------------------------------------------------------------------------
def normalize_headers_advanced(
    headers: List[str],
    stop_words: Optional[Set[str]] = None,
    abbreviations: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, str], List[str]]:
    """
    Normalize headers to valid Postgres identifiers with smart compression.

    Applies deterministic normalization with semantic preservation:

    Phase 1 - Standard Sanitization:
        1. Lowercase conversion and whitespace stripping
        2. Abbreviation application (long -> short, e.g., "quantity" -> "qty")
        3. Special character replacement with underscores
        4. Stop word removal
        5. Numeric start protection (prepend underscore)
        6. Multiple underscore collapse and cleanup

    Phase 2 - Smart Compression (if length > 63):
        A. Word deduplication (e.g., "user_id_user" -> "user_id")
        B. Inner vowel removal (e.g., "revenue" -> "rvne")
        C. Hard truncation (last resort)

    Phase 3 - Collision Resolution:
        Deterministic suffix addition for duplicates (_1, _2, ...)

    Args:
        headers: List of original header strings.
        stop_words: Optional set of stop words to remove.
                    If None and NLTK available, uses English stopwords.
                    If None and NLTK unavailable, uses empty set.
        abbreviations: Optional dict mapping long words to abbreviations.
                       (e.g., {"quantity": "qty", "number": "num"})
                       If None, uses DEFAULT_ETL_ABBREVIATIONS.

    Returns:
        Tuple of:
        - header_mapping: Dict mapping original header -> cleaned header
        - cleaned_headers: List of cleaned header strings (in original order)

    Examples:
        >>> mapping, cleaned = normalize_headers_advanced(["First Name", "Last Name"])
        >>> cleaned
        ['first_name', 'last_name']

        >>> mapping, cleaned = normalize_headers_advanced(["Name", "Name", "Name"])
        >>> cleaned
        ['name', 'name_1', 'name_2']

        >>> mapping, cleaned = normalize_headers_advanced(["1st Quarter"])
        >>> cleaned
        ['_1st_quarter']
    """
    # Step 1: Initialize
    if stop_words is None:
        stop_words = _get_nltk_stopwords()
    if abbreviations is None:
        abbreviations = DEFAULT_ETL_ABBREVIATIONS

    header_mapping: Dict[str, str] = {}
    cleaned_headers: List[str] = []
    seen_cleaned: Dict[str, int] = {}  # Track collisions: cleaned_name -> count

    for idx, original_header in enumerate(headers):
        # =====================================================================
        # PHASE 1: Standard Sanitization
        # =====================================================================

        # Step 1.1: Basic clean - lowercase and strip
        normalized = original_header.strip().lower()

        # Step 1.2: Handle empty headers early
        if not normalized:
            candidate = f"col_{idx}"
        else:
            # Step 1.3: Apply abbreviations (long -> short)
            for long_word, short_abbrev in abbreviations.items():
                pattern = rf"\b{re.escape(long_word)}\b"
                normalized = re.sub(pattern, short_abbrev, normalized, flags=re.IGNORECASE)

            # Step 1.4: Replace special chars with underscores
            normalized = re.sub(r"[^a-z0-9]", "_", normalized)

            # Step 1.5: Remove stop words
            words = normalized.split("_")
            words = [w for w in words if w and w not in stop_words]
            normalized = "_".join(words)

            # Step 1.6: Cleanup - collapse underscores and strip
            normalized = re.sub(r"_+", "_", normalized)
            normalized = normalized.strip("_")

            # Step 1.7: Empty fallback after cleanup
            if not normalized:
                candidate = f"col_{idx}"
            else:
                # Step 1.8: Numeric start protection (AFTER stripping)
                if normalized[0].isdigit():
                    normalized = f"_{normalized}"
                candidate = normalized

        # =====================================================================
        # PHASE 2: Smart Compression (if > 63 chars)
        # =====================================================================
        if len(candidate) > 63:
            # Strategy A: Deduplicate words
            candidate = deduplicate_words(candidate)

        if len(candidate) > 63:
            # Strategy B: Remove inner vowels
            candidate = remove_inner_vowels(candidate)

        if len(candidate) > 63:
            # Strategy C: Hard truncate (last resort)
            candidate = candidate[:63]
            # Ensure no trailing underscore after truncation
            candidate = candidate.rstrip("_")

        # =====================================================================
        # PHASE 3: Deterministic Collision Resolution
        # =====================================================================
        if candidate in seen_cleaned:
            seen_cleaned[candidate] += 1
            suffix = f"_{seen_cleaned[candidate]}"

            # Safety check: ensure final name fits in 63 chars
            max_base_len = 63 - len(suffix)
            if max_base_len < 1:
                max_base_len = 1
            truncated_candidate = candidate[:max_base_len].rstrip("_")
            final_name = f"{truncated_candidate}{suffix}"
        else:
            seen_cleaned[candidate] = 0
            final_name = candidate

        # =====================================================================
        # Final Validation
        # =====================================================================
        if not re.match(r"^[a-z_][a-z0-9_]*$", final_name):
            # Fallback: use col_{index} if normalization failed
            fallback = f"col_{idx}"
            if fallback in seen_cleaned:
                seen_cleaned[fallback] += 1
                final_name = f"{fallback}_{seen_cleaned[fallback]}"
            else:
                seen_cleaned[fallback] = 0
                final_name = fallback

        header_mapping[original_header] = final_name
        cleaned_headers.append(final_name)

    return header_mapping, cleaned_headers


# -----------------------------------------------------------------------------
# Backward-Compatible Wrapper
# -----------------------------------------------------------------------------
def normalize_headers(
    headers: List[str],
    stop_words: Optional[List[str]] = None,
    abbreviations: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, str], List[str]]:
    """
    Normalize headers to valid Postgres identifiers.

    This is a backward-compatible wrapper around normalize_headers_advanced.

    Note: The `abbreviations` parameter here performs EXPANSION (short -> long),
    e.g., {"st": "street"} expands "st" to "street". This is the opposite of
    normalize_headers_advanced which uses CONTRACTION (long -> short).

    Args:
        headers: List of original header strings.
        stop_words: Optional list of stop words to remove (default: None = no removal).
        abbreviations: Optional dict mapping abbreviations to full words
                       (e.g., {"st": "street", "qty": "quantity"}).
                       These are applied BEFORE the standard contraction phase.

    Returns:
        Tuple of:
        - header_mapping: Dict mapping original header -> cleaned header
        - cleaned_headers: List of cleaned header strings (in original order)

    Examples:
        >>> mapping, cleaned = normalize_headers(["First Name", "Last Name", "Age"])
        >>> cleaned
        ['first_name', 'last_name', 'age']

        >>> mapping, cleaned = normalize_headers(["St Name"], abbreviations={"st": "street"})
        >>> cleaned
        ['street_name']
    """
    # Convert stop_words list to set
    stop_words_set: Optional[Set[str]] = None
    if stop_words is not None:
        stop_words_set = set(stop_words)
    else:
        # For backward compatibility: don't use NLTK stopwords by default
        stop_words_set = set()

    # Pre-process headers to apply expansion abbreviations
    processed_headers = list(headers)
    if abbreviations:
        for i, header in enumerate(processed_headers):
            normalized = header.strip().lower()
            for abbrev, full_word in abbreviations.items():
                pattern = rf"\b{re.escape(abbrev)}\b"
                normalized = re.sub(pattern, full_word, normalized, flags=re.IGNORECASE)
            processed_headers[i] = normalized

    # Call advanced function with no default contractions (empty dict)
    # This preserves backward compatibility
    mapping_internal, cleaned = normalize_headers_advanced(
        processed_headers,
        stop_words=stop_words_set,
        abbreviations={},  # No contractions for backward compat
    )

    # Rebuild mapping with original headers as keys
    header_mapping: Dict[str, str] = {}
    for original, cleaned_name in zip(headers, cleaned):
        header_mapping[original] = cleaned_name

    return header_mapping, cleaned
