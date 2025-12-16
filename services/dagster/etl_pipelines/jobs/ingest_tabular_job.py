"""Ingestion job for tabular data (Phase 3)."""

from dagster import job, OpExecutionContext

from ..ops import (
    download_tabular_from_landing,
    load_and_clean_tabular,
    export_tabular_parquet_to_datalake,
)


@job(
    name="ingest_tabular_job",
    description="Ingest tabular data from CSV to Parquet in data lake (Phase 3).",
)
def ingest_tabular_job(manifest: dict):
    """Tabular ingestion job: CSV → cleaned headers → Parquet → data lake."""
    # Download CSV from landing zone
    local_path = download_tabular_from_landing(manifest)

    # Load and clean tabular data
    tabular_data = load_and_clean_tabular(local_path, manifest)

    # Export to Parquet in data lake
    asset_info = export_tabular_parquet_to_datalake(tabular_data, manifest)
