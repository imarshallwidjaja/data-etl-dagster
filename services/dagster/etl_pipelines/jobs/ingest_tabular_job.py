"""Placeholder tabular ingestion job for manifest router rollout."""

from dagster import OpExecutionContext, job, op

from libs.models import Manifest


@op
def tabular_ingest_placeholder(context: OpExecutionContext, manifest: dict) -> dict:
    """
    Validate manifest and log intent for tabular ingestion.

    This is a safe placeholder; real tabular ETL will replace it in a later phase.
    """
    manifest_obj = Manifest(**manifest)
    metadata = manifest_obj.metadata

    context.log.info(f"[tabular] batch_id={manifest_obj.batch_id}, project={metadata.project}")
    context.log.info(f"[tabular] files={len(manifest_obj.files)}, tags={metadata.tags}")

    return {
        "batch_id": manifest_obj.batch_id,
        "intent": manifest_obj.intent,
        "project": metadata.project,
    }


@job(
    name="ingest_tabular_job",
    description="Placeholder job for tabular ingestion route (validate and log only).",
)
def ingest_tabular_job():
    tabular_ingest_placeholder()

