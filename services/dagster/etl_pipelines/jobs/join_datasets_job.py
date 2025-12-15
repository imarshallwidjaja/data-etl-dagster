"""Placeholder job for dataset joins (Phase 2)."""

from dagster import job, op, OpExecutionContext
from libs.models import Manifest


@op
def validate_and_log(context: OpExecutionContext, manifest: dict) -> dict:
    """
    Placeholder op for join datasets job.
    
    Validates manifest (including join_config requirement) and logs key fields.
    No PostGIS/MinIO/Mongo writes in Phase 2.
    
    Args:
        context: Dagster op execution context
        manifest: Manifest dict from run config
        
    Returns:
        Manifest dict (for potential future chaining)
    """
    # Validate manifest
    validated_manifest = Manifest(**manifest)
    
    # Validate join_config is present (should be enforced by router, but double-check)
    if validated_manifest.metadata.join_config is None:
        raise ValueError(
            f"join_datasets job requires metadata.join_config. "
            f"batch_id: {validated_manifest.batch_id}"
        )
    
    join_config = validated_manifest.metadata.join_config
    
    # Log key fields
    context.log.info(f"Processing join datasets manifest: {validated_manifest.batch_id}")
    context.log.info(f"Uploader: {validated_manifest.uploader}")
    context.log.info(f"Intent: {validated_manifest.intent}")
    context.log.info(f"Project: {validated_manifest.metadata.project}")
    if validated_manifest.metadata.description:
        context.log.info(f"Description: {validated_manifest.metadata.description}")
    if validated_manifest.metadata.tags:
        context.log.info(f"Tags: {validated_manifest.metadata.tags}")
    context.log.info(f"Files: {len(validated_manifest.files)} file(s)")
    
    # Log join config summary
    context.log.info(f"Join config:")
    context.log.info(f"  Target asset ID: {join_config.target_asset_id}")
    context.log.info(f"  Left key: {join_config.left_key}")
    context.log.info(f"  Right key: {join_config.right_key}")
    context.log.info(f"  Join type: {join_config.how}")
    
    return manifest


@job(
    name="join_datasets_job",
    description="Placeholder job for dataset joins (Phase 2). Validates and logs manifest (including join_config) only.",
)
def join_datasets_job():
    """Placeholder join datasets job."""
    validate_and_log()
