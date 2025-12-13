"""Placeholder ingestion job for Phase 3 manifest sensor implementation."""

from dagster import job, op, OpExecutionContext


@op
def ingest_placeholder(context: OpExecutionContext):
    """
    Placeholder op for ingest job.
    
    This will be replaced with the full ETL pipeline in Phase 4/5.
    For now, it just logs the manifest data from run config.
    """
    manifest_data = context.op_config.get("manifest", {})
    manifest_key = context.op_config.get("manifest_key", "unknown")
    context.log.info(f"Processing manifest: {manifest_data.get('batch_id', 'unknown')}")
    context.log.info(f"Manifest key: {manifest_key}")
    context.log.info(f"Manifest data: {manifest_data}")
    return manifest_data


@job(
    name="ingest_job",
    description="Main ingestion job triggered by manifest sensor (placeholder for Phase 3)",
)
def ingest_job():
    """Placeholder ingestion job."""
    ingest_placeholder()

