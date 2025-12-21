# =============================================================================
# Run Model
# =============================================================================
# Defines the Run model for tracking Dagster run execution in MongoDB.
# =============================================================================

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


__all__ = ["Run", "RunStatus"]


class RunStatus(str, Enum):
    """Status of a Dagster run in the MongoDB ledger."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"
    CANCELED = "canceled"


class Run(BaseModel):
    """
    Run document model for MongoDB tracking.

    Represents a Dagster run and its relationship to manifests and assets.
    The run document is created synchronously when a job starts (via init_mongo_run_op)
    and updated when the run completes (via run_status_sensor).

    Attributes:
        dagster_run_id: Dagster's internal run ID (unique index)
        batch_id: Links to the manifest that triggered this run
        job_name: Name of the Dagster job executed
        partition_key: Dynamic partition key (dataset_id) if applicable
        status: Current run status (running/success/failure/canceled)
        asset_ids: ObjectIds of assets produced by this run
        error_message: Error details if run failed
        started_at: Timestamp when run started
        completed_at: Timestamp when run completed (if finished)
    """

    dagster_run_id: str = Field(..., description="Dagster run ID (unique)")
    batch_id: str = Field(..., description="Manifest batch_id that triggered this run")
    job_name: str = Field(..., description="Name of the job executed")
    partition_key: Optional[str] = Field(None, description="Dataset partition key")
    status: RunStatus = Field(..., description="Current run status")
    asset_ids: list[str] = Field(
        default_factory=list,
        description="ObjectId strings of assets produced by this run",
    )
    error_message: Optional[str] = Field(None, description="Error message if failed")
    started_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Run start timestamp",
    )
    completed_at: Optional[datetime] = Field(
        None, description="Run completion timestamp"
    )
