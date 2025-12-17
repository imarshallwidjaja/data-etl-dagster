"""
Dynamic partitions for the ETL pipeline.

Assets are partitioned by dataset_id to enable:
- Partition-level materialization tracking
- Fan-in patterns for derived assets (joins)
- Granular re-processing
"""

from __future__ import annotations

import uuid
from typing import Any

from dagster import DynamicPartitionsDefinition

from libs.models import Manifest

# Dynamic partitions keyed by dataset_id
# Partition keys are added at runtime when assets are materialized
dataset_partitions = DynamicPartitionsDefinition(name="dataset_id")


def extract_partition_key(manifest_like: Manifest | dict[str, Any]) -> str:
    """
    Extract or generate partition key (dataset_id).

    Handles both Pydantic Manifest objects and dictionaries (for raw assets / normalized JSON).
    """
    if isinstance(manifest_like, dict):
        metadata = manifest_like.get("metadata", {})
        tags = metadata.get("tags", {})
        dataset_id = tags.get("dataset_id")
    else:
        dataset_id = manifest_like.metadata.tags.get("dataset_id")

    if dataset_id and isinstance(dataset_id, str) and dataset_id.strip():
        return dataset_id.strip()
    return f"dataset_{uuid.uuid4().hex[:12]}"


__all__ = ["dataset_partitions", "extract_partition_key"]


