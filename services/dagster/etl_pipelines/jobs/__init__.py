"""Dagster Jobs - Executable Workflows.

Note: Asset jobs (spatial_asset_job, tabular_asset_job, join_asset_job) are
defined in definitions.py, not here. This module only contains legacy op-based jobs.
"""

from .ingest_job import ingest_job

__all__ = ["ingest_job"]

