# libs/spatial_utils — Agent Guide

## Scope
Lightweight spatial-adjacent utilities (schema mapping and header normalization).

## Key invariants
- Schema names must be SQL-safe and reversible for run_id mapping.
- Keep utilities dependency-light; GDAL stays in the user-code container.
- `normalize_headers_advanced` requires NLTK stopwords (download in dev if used).

## References
- Models/settings: `libs/models/AGENTS.md`
