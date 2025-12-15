"""Placeholder explicit join job for manifest router rollout."""

from dagster import OpExecutionContext, job, op

from libs.models import Manifest


@op
def join_datasets_placeholder(context: OpExecutionContext, manifest: dict) -> dict:
    """
    Validate manifest and log join configuration.

    This is a safe placeholder; real join execution will replace it in a later phase.
    """
    manifest_obj = Manifest(**manifest)
    join_config = manifest_obj.metadata.join_config

    context.log.info(
        "[join] batch_id=%s, project=%s, target_asset_id=%s, how=%s",
        manifest_obj.batch_id,
        manifest_obj.metadata.project,
        join_config.target_asset_id if join_config else None,
        join_config.how if join_config else None,
    )

    return {
        "batch_id": manifest_obj.batch_id,
        "intent": manifest_obj.intent,
        "project": manifest_obj.metadata.project,
        "join_config": join_config.model_dump(mode="json") if join_config else None,
    }


@job(
    name="join_datasets_job",
    description="Placeholder job for explicit dataset joins (validate and log only).",
)
def join_datasets_job():
    join_datasets_placeholder()

