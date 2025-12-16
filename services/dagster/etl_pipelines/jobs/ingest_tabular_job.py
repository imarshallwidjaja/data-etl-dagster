"""Placeholder ingestion job for tabular data (Phase 2)."""

from dagster import job, op, OpExecutionContext
from libs.models import Manifest


@op
def tabular_validate_and_log(context: OpExecutionContext, manifest: dict) -> dict:
    """
    Placeholder op for tabular ingest job.
    
    Validates manifest and logs key fields. No PostGIS/MinIO/Mongo writes in Phase 2.
    
    Args:
        context: Dagster op execution context
        manifest: Manifest dict from run config
        
    Returns:
        Manifest dict (for potential future chaining)
    """
    # Validate manifest
    validated_manifest = Manifest(**manifest)
    
    # Log key fields
    context.log.info(f"Processing tabular ingest manifest: {validated_manifest.batch_id}")
    context.log.info(f"Uploader: {validated_manifest.uploader}")
    context.log.info(f"Intent: {validated_manifest.intent}")
    context.log.info(f"Project: {validated_manifest.metadata.project}")
    if validated_manifest.metadata.description:
        context.log.info(f"Description: {validated_manifest.metadata.description}")
    if validated_manifest.metadata.tags:
        context.log.info(f"Tags: {validated_manifest.metadata.tags}")
    context.log.info(f"Files: {len(validated_manifest.files)} file(s)")
    
    return manifest


@job(
    name="ingest_tabular_job",
    description="Placeholder job for tabular data ingestion (Phase 2). Validates and logs manifest only.",
)
def ingest_tabular_job():
    """Placeholder tabular ingestion job."""
    tabular_validate_and_log()
