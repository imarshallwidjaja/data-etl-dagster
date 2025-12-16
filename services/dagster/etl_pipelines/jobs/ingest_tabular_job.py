"""Tabular ingestion job for CSV data (Phase 3)."""

from dagster import job

from ..ops.tabular_ops import (
    download_tabular_from_landing,
    load_and_clean_tabular,
    export_tabular_parquet_to_datalake,
)


@job(
    name="ingest_tabular_job",
    description="Ingest tabular data (CSV) to Parquet format. Downloads from landing zone, cleans headers, exports to data lake, and registers in MongoDB.",
)
def ingest_tabular_job():
    """
    Tabular ingestion pipeline.
    
    Flow:
    1. Download CSV from landing zone to temp file
    2. Load CSV into Arrow Table and clean headers
    3. Export to Parquet, upload to data lake, register in MongoDB
    """
    # Chain: manifest -> download -> load_and_clean -> export
    # manifest comes from run_config (set by sensor)
    download_result = download_tabular_from_landing()
    table_info = load_and_clean_tabular(download_result)
    export_tabular_parquet_to_datalake(table_info)
