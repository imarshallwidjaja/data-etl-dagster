"""Op-based job for complex spreadsheet splitting."""

from dagster import job

from ..ops import (
    archive_raw_sources_op,
    init_mongo_run_op,
    split_complex_spreadsheet_op,
)


@job(
    name="complex_table_splitter_job",
    description="Archives XLSX raw source, splits configured sheets into long-form Parquet intermediates, and publishes child tabular manifests.",
)
def complex_table_splitter_job():
    """Split an ingest_complex_spreadsheet manifest into per-sheet child manifests."""
    manifest = init_mongo_run_op()
    archived_manifest = archive_raw_sources_op(manifest)
    split_complex_spreadsheet_op(archived_manifest)
