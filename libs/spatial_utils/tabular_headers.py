# =============================================================================
# Tabular Headers Module
# =============================================================================
# Provides deterministic header normalization for tabular data ingestion.
# Ensures headers conform to Postgres identifier rules for reliable joins.
# Uses "smart compression" techniques to preserve semantics over hard truncation.
# =============================================================================

import logging
import re
from typing import Callable, Dict, List, Optional, Pattern, Set, Tuple

from nltk.corpus import stopwords as _nltk_stopwords_module

__all__ = [
    "normalize_headers",
    "normalize_headers_advanced",
    "DEFAULT_ETL_ABBREVIATIONS",
    "deduplicate_words",
    "remove_inner_vowels",
]

log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# NLTK stopwords loading
# -----------------------------------------------------------------------------
_nltk_stopwords: Optional[Set[str]] = None
_nltk_load_attempted: bool = False


def _get_nltk_stopwords() -> Set[str]:
    """
    Load NLTK English stopwords with graceful fallback.

    Returns:
        Set of English stopwords, or empty set if NLTK data unavailable.

    Note:
        If NLTK stopwords data is not downloaded, logs a warning and returns
        an empty set. To download stopwords data, run:
            python -c "import nltk; nltk.download('stopwords')"
    """
    global _nltk_stopwords, _nltk_load_attempted

    if _nltk_stopwords is not None:
        return _nltk_stopwords

    if _nltk_load_attempted:
        # Already tried and failed, return empty set
        return set()

    _nltk_load_attempted = True
    try:
        _nltk_stopwords = set(_nltk_stopwords_module.words("english"))
        return _nltk_stopwords
    except LookupError:
        log.warning(
            "NLTK stopwords data not found. Stop word removal will be skipped. "
            "To enable, run: python -c \"import nltk; nltk.download('stopwords')\""
        )
        _nltk_stopwords = set()
        return _nltk_stopwords


# -----------------------------------------------------------------------------
# Default ETL abbreviations (long form -> short form for compression)
# -----------------------------------------------------------------------------
# NOTE: This dictionary is the canonical source for ETL abbreviations.
# If abbreviations are needed elsewhere in the pipeline (e.g., documentation,
# reverse mapping), import this constant rather than duplicating it.
# Keep entries sorted alphabetically by key for maintainability.
DEFAULT_ETL_ABBREVIATIONS: Dict[str, str] = {
    "address": "addr",
    "amount": "amt",
    "application": "app",
    "average": "avg",
    "category": "cat",
    "configuration": "config",
    "coordinate": "coord",
    "department": "dept",
    "description": "desc",
    "estimated": "est",
    "geometry": "geom",
    "identification": "id",
    "identifier": "id",
    "information": "info",
    "international": "intl",
    "latitude": "lat",
    "longitude": "lon",
    "maximum": "max",
    "minimum": "min",
    "number": "num",
    "organization": "org",
    "percentage": "pct",
    "quantity": "qty",
    "reference": "ref",
    "revenue": "rev",
    "service": "svc",
    "telephone": "tel",
    "transaction": "txn",
}

# Pre-compiled regex pattern for default abbreviations (optimization)
_default_abbrev_pattern: Optional[Pattern[str]] = None
_default_abbrev_replacer: Optional[Callable[[re.Match[str]], str]] = None


def _build_abbreviation_regex(
    abbreviations: Dict[str, str],
) -> Tuple[Pattern[str], Callable[[re.Match[str]], str]]:
    """
    Build a compiled regex pattern and replacer function for abbreviations.

    This compiles all abbreviation keys into a single regex pattern with
    alternation (e.g., r'\\b(quantity|number|amount)\\b'), enabling O(1)
    replacement passes regardless of dictionary size.

    Args:
        abbreviations: Dict mapping long words to short abbreviations.

    Returns:
        Tuple of (compiled_pattern, replacer_function)
    """
    if not abbreviations:
        # Return a pattern that matches nothing
        return re.compile(r"(?!x)x"), lambda m: m.group(0)

    # Sort by length (longest first) to avoid partial matches
    # e.g., "identification" should match before "identifier"
    sorted_keys = sorted(abbreviations.keys(), key=len, reverse=True)
    escaped_keys = [re.escape(k) for k in sorted_keys]
    pattern = re.compile(rf"\b({'|'.join(escaped_keys)})\b", re.IGNORECASE)

    # Create lowercase lookup for case-insensitive replacement
    lowercase_abbrevs = {k.lower(): v for k, v in abbreviations.items()}

    def replacer(match: re.Match[str]) -> str:
        matched_word = match.group(1).lower()
        return lowercase_abbrevs.get(matched_word, match.group(0))

    return pattern, replacer


def _get_default_abbrev_regex() -> Tuple[Pattern[str], Callable[[re.Match[str]], str]]:
    """
    Get the pre-compiled regex for DEFAULT_ETL_ABBREVIATIONS (cached).

    Returns:
        Tuple of (compiled_pattern, replacer_function)
    """
    global _default_abbrev_pattern, _default_abbrev_replacer

    if _default_abbrev_pattern is None:
        _default_abbrev_pattern, _default_abbrev_replacer = _build_abbreviation_regex(
            DEFAULT_ETL_ABBREVIATIONS
        )

    return _default_abbrev_pattern, _default_abbrev_replacer


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


# Pre-compiled patterns for common operations (optimization)
_SPECIAL_CHAR_PATTERN = re.compile(r"[^a-z0-9]")
_MULTI_UNDERSCORE_PATTERN = re.compile(r"_+")
_POSTGRES_IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")


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
                    If None, uses NLTK English stopwords (with graceful fallback).
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
    # Step 1: Initialize stop words (with graceful NLTK fallback)
    if stop_words is None:
        stop_words = _get_nltk_stopwords()

    # Step 2: Build or use cached abbreviation regex (O(1) compilation)
    if abbreviations is None:
        abbrev_pattern, abbrev_replacer = _get_default_abbrev_regex()
    elif abbreviations:
        abbrev_pattern, abbrev_replacer = _build_abbreviation_regex(abbreviations)
    else:
        # Empty dict - no abbreviations
        abbrev_pattern, abbrev_replacer = None, None

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
            # Step 1.3: Apply abbreviations in single pass (optimized)
            if abbrev_pattern is not None:
                normalized = abbrev_pattern.sub(abbrev_replacer, normalized)

            # Step 1.4: Replace special chars with underscores
            normalized = _SPECIAL_CHAR_PATTERN.sub("_", normalized)

            # Step 1.5: Remove stop words
            words = normalized.split("_")
            words = [w for w in words if w and w not in stop_words]
            normalized = "_".join(words)

            # Step 1.6: Cleanup - collapse underscores and strip
            normalized = _MULTI_UNDERSCORE_PATTERN.sub("_", normalized)
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
        if not _POSTGRES_IDENTIFIER_PATTERN.match(final_name):
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

    # Pre-process headers to apply expansion abbreviations (optimized)
    processed_headers = list(headers)
    if abbreviations:
        # Build optimized regex for expansion abbreviations
        abbrev_pattern, abbrev_replacer = _build_abbreviation_regex(abbreviations)
        for i, header in enumerate(processed_headers):
            normalized = header.strip().lower()
            normalized = abbrev_pattern.sub(abbrev_replacer, normalized)
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
