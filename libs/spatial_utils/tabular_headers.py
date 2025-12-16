# =============================================================================
# Tabular Headers Module
# =============================================================================
# Provides deterministic header normalization for tabular data ingestion.
# Ensures headers conform to Postgres identifier rules for reliable joins.
# =============================================================================

import re
from typing import Dict, List, Optional

__all__ = ["normalize_headers"]


def normalize_headers(
    headers: List[str],
    stop_words: Optional[List[str]] = None,
    abbreviations: Optional[Dict[str, str]] = None,
) -> tuple[Dict[str, str], List[str]]:
    """
    Normalize headers to valid Postgres identifiers.
    
    Applies deterministic normalization:
    1. Lowercase conversion
    2. Whitespace/punctuation normalization
    3. Optional stop-word removal
    4. Optional abbreviation expansion
    5. Postgres identifier validation (^[a-z_][a-z0-9_]*$, length ≤ 63)
    6. Collision handling (name, name_2, name_3, ...)
    7. Empty header handling (col_{index})
    
    Args:
        headers: List of original header strings
        stop_words: Optional list of stop words to remove (default: empty)
        abbreviations: Optional dict mapping abbreviations to full words
                     (e.g., {"st": "street", "qty": "quantity"})
    
    Returns:
        Tuple of:
        - header_mapping: Dict mapping original header → cleaned header
        - cleaned_headers: List of cleaned header strings (in original order)
    
    Examples:
        >>> mapping, cleaned = normalize_headers(["First Name", "Last Name", "Age"])
        >>> cleaned
        ['first_name', 'last_name', 'age']
        >>> mapping["First Name"]
        'first_name'
        
        >>> mapping, cleaned = normalize_headers(["Name", "Name", ""])
        >>> cleaned
        ['name', 'name_2', 'col_2']
    """
    if stop_words is None:
        stop_words = []
    if abbreviations is None:
        abbreviations = {}
    
    header_mapping: Dict[str, str] = {}
    cleaned_headers: List[str] = []
    seen_cleaned: Dict[str, int] = {}  # Track collisions: cleaned_name -> count
    
    for idx, original_header in enumerate(headers):
        # Step 1: Normalize to lowercase and strip whitespace
        normalized = original_header.strip().lower()
        
        # Step 2: Handle empty headers
        if not normalized:
            base_name = f"col_{idx}"
        else:
            # Step 3: Apply abbreviations (before other normalization)
            for abbrev, full_word in abbreviations.items():
                # Replace whole word matches (word boundaries)
                pattern = r'\b' + re.escape(abbrev) + r'\b'
                normalized = re.sub(pattern, full_word, normalized, flags=re.IGNORECASE)
            
            # Step 4: Remove stop words
            words = normalized.split()
            words = [w for w in words if w not in stop_words]
            normalized = " ".join(words)
            
            # Step 5: Normalize whitespace and punctuation
            # Replace spaces and common punctuation with underscores
            normalized = re.sub(r'[\s\-\.]+', '_', normalized)
            # Remove any remaining non-alphanumeric/underscore characters
            normalized = re.sub(r'[^a-z0-9_]', '', normalized)
            # Collapse multiple underscores
            normalized = re.sub(r'_+', '_', normalized)
            # Remove leading/trailing underscores
            normalized = normalized.strip('_')
            
            # Step 6: Ensure it starts with a letter or underscore
            if not normalized:
                base_name = f"col_{idx}"
            elif not re.match(r'^[a-z_]', normalized):
                # Prepend underscore if it doesn't start with letter/underscore
                normalized = f"_{normalized}"
                base_name = normalized
            else:
                base_name = normalized
        
        # Step 7: Truncate to 63 characters (Postgres limit)
        if len(base_name) > 63:
            base_name = base_name[:63]
            # Ensure it still ends with alphanumeric/underscore (not cut mid-char)
            base_name = re.sub(r'[^a-z0-9_]+$', '', base_name)
        
        # Step 8: Handle collisions deterministically
        if base_name in seen_cleaned:
            seen_cleaned[base_name] += 1
            suffix = seen_cleaned[base_name]
            cleaned_name = f"{base_name}_{suffix}"
            # Truncate again if collision suffix makes it too long
            if len(cleaned_name) > 63:
                # Keep base name short enough for suffix
                max_base_len = 63 - len(str(suffix)) - 1  # -1 for underscore
                if max_base_len < 1:
                    max_base_len = 1
                base_name = base_name[:max_base_len]
                cleaned_name = f"{base_name}_{suffix}"
        else:
            seen_cleaned[base_name] = 0
            cleaned_name = base_name
        
        # Step 9: Final validation - ensure it matches Postgres identifier pattern
        if not re.match(r'^[a-z_][a-z0-9_]*$', cleaned_name):
            # Fallback: use col_{index} if normalization failed
            cleaned_name = f"col_{idx}"
            seen_cleaned[cleaned_name] = seen_cleaned.get(cleaned_name, -1) + 1
            if seen_cleaned[cleaned_name] > 0:
                cleaned_name = f"{cleaned_name}_{seen_cleaned[cleaned_name]}"
        
        header_mapping[original_header] = cleaned_name
        cleaned_headers.append(cleaned_name)
    
    return header_mapping, cleaned_headers

