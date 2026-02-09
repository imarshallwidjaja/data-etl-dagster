"""Complex table splitter job (op-based).

Splits a multi-table XLSX into per-sheet parquet intermediates,
registers artifacts, and publishes child manifests to the landing zone
for downstream tabular ingestion.
"""

from dagster import job

from ..ops import (
    init_mongo_run_op,
    archive_raw_sources_op,
    split_complex_spreadsheet_op,
)


@job(
    name="complex_table_splitter_job",
    description=(
        "Split a complex multi-table XLSX into per-sheet parquet intermediates, "
        "archive raw sources, register artifacts, and publish child manifests"
    ),
)
def complex_table_splitter_job():
    """
    Op-based job for complex spreadsheet splitting.

    Pipeline flow:
    1. init_mongo_run_op: Create run document in MongoDB
    2. archive_raw_sources_op: Archive the raw XLSX source
    3. split_complex_spreadsheet_op: Split sheets, register intermediates,
       publish child manifests
    """
    manifest = init_mongo_run_op()
    archived_manifest = archive_raw_sources_op(manifest)
    split_complex_spreadsheet_op(archived_manifest)
