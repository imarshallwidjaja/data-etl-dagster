from __future__ import annotations

from libs.models import Manifest


def derive_testing_tag(manifest: Manifest) -> str | None:
    tags = manifest.metadata.tags
    if not tags:
        return None
    raw = tags.get("testing")
    if isinstance(raw, bool):
        return "true" if raw else "false"
    if isinstance(raw, (int, float)):
        return "true" if raw else "false"
    if isinstance(raw, str):
        value = raw.strip().lower()
        if value in {"1", "true", "yes", "y", "on"}:
            return "true"
        if value in {"0", "false", "no", "n", "off"}:
            return "false"
    return None
