# =============================================================================
# Tabular Headers Module
# =============================================================================
# Provides deterministic header normalization for tabular data ingestion.
# Ensures headers are valid Postgres column identifiers.
# =============================================================================

import re
from typing import Dict, List, Optional, Set


__all__ = ["normalize_tabular_headers"]


def normalize_tabular_headers(
    headers: List[str],
    stop_words: Optional[Set[str]] = None,
    abbreviations: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """
    Normalize tabular headers to valid Postgres column identifiers.

    Performs deterministic header cleaning with collision resolution:
    1. Normalize case, whitespace, and punctuation
    2. Apply optional stop-word removal and abbreviations
    3. Enforce Postgres identifier rules (^ [a-z_][a-z0-9_]* $, max 63 chars)
    4. Resolve collisions deterministically (name, name_2, name_3, ...)
    5. Handle empty headers as col_{index}

    Args:
        headers: List of original header strings
        stop_words: Optional set of words to remove (e.g., {"the", "and"})
        abbreviations: Optional dict of replacements (e.g., {"st": "street", "qty": "quantity"})

    Returns:
        Dict mapping original headers to cleaned headers

    Examples:
        >>> normalize_tabular_headers(["First Name", "Last Name", "Age"])
        {'First Name': 'first_name', 'Last Name': 'last_name', 'Age': 'age'}

        >>> normalize_tabular_headers(["ID", "", "Name"], stop_words={"the"})
        {'ID': 'id', '': 'col_1', 'Name': 'name'}
    """
    if stop_words is None:
        stop_words = set()
    if abbreviations is None:
        abbreviations = {}

    header_mapping = {}
    used_names = set()

    for i, header in enumerate(headers):
        # Start with the original header
        cleaned = header

        # 1. Basic normalization: lowercase, strip whitespace
        cleaned = cleaned.lower().strip()

        # 2. Replace punctuation with underscores, collapse multiple underscores
        cleaned = re.sub(r'[^\w\s]', '_', cleaned)
        cleaned = re.sub(r'_+', '_', cleaned)
        cleaned = cleaned.rstrip('_')

        # 3. Apply abbreviations (before stop word removal)
        for abbr, replacement in abbreviations.items():
            # Use word boundaries to avoid partial matches
            cleaned = re.sub(rf'\b{re.escape(abbr.lower())}\b', replacement.lower(), cleaned)

        # 4. Remove stop words (split, filter, rejoin)
        words = cleaned.split()
        words = [word for word in words if word not in stop_words]
        cleaned = '_'.join(words)

        # 5. Handle empty result - use col_{index}
        if not cleaned:
            cleaned = f"col_{i}"

        # 6. Enforce Postgres identifier rules
        # Replace invalid characters with underscores
        cleaned = re.sub(r'[^a-z0-9_]', '_', cleaned)

        # Remove multiple consecutive underscores
        cleaned = re.sub(r'_+', '_', cleaned)

        # Strip trailing underscores only (leading underscores are valid)
        cleaned = cleaned.rstrip('_')

        # Must start with letter or underscore
        if not cleaned:
            cleaned = f"col_{i}"
        elif not re.match(r'^[a-z_]', cleaned):
            cleaned = f"col_{i}"

        # Truncate to 63 characters (Postgres limit)
        if len(cleaned) > 63:
            cleaned = cleaned[:63].rstrip('_')
            # Handle empty after truncation
            if not cleaned:
                cleaned = f"col_{i}"

        # 7. Resolve collisions deterministically
        base_name = cleaned
        counter = 2
        original_cleaned = cleaned
        while cleaned in used_names:
            # Try name_2, name_3, etc.
            candidate = f"{base_name}_{counter}"
            if len(candidate) > 63:
                # If too long, truncate base and add counter
                available_chars = 63 - len(str(counter)) - 1  # -1 for underscore
                base_name = base_name[:available_chars].rstrip('_')
                if not base_name:
                    # If base becomes empty, use col_i_counter format
                    candidate = f"col_{i}_{counter}"
                else:
                    candidate = f"{base_name}_{counter}"

            cleaned = candidate
            counter += 1

        used_names.add(cleaned)
        header_mapping[header] = cleaned

    return header_mapping


def validate_header_mapping(header_mapping: Dict[str, str]) -> None:
    """
    Validate that all cleaned headers are valid Postgres identifiers.

    Args:
        header_mapping: Dict mapping original to cleaned headers

    Raises:
        ValueError: If any cleaned header is invalid
    """
    postgres_pattern = re.compile(r'^[a-z_][a-z0-9_]*$')

    for original, cleaned in header_mapping.items():
        if not postgres_pattern.match(cleaned):
            raise ValueError(f"Invalid Postgres identifier '{cleaned}' for header '{original}'")

        if len(cleaned) > 63:
            raise ValueError(f"Header '{original}' cleaned to '{cleaned}' exceeds 63 char limit")

        if not cleaned:
            raise ValueError(f"Header '{original}' resulted in empty cleaned name")
