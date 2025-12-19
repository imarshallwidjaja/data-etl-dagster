# =============================================================================
# Runs Router
# =============================================================================
# Endpoints for Dagster run tracking.
# =============================================================================

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.auth.dependencies import AuthenticatedUser, get_current_user
from app.services.dagster_service import get_dagster_service

router = APIRouter(prefix="/runs", tags=["runs"])


class RunListResponse(BaseModel):
    """Response for run listing."""

    runs: list[dict]
    count: int


class RunDetailResponse(BaseModel):
    """Response for run details."""

    run: dict
    events: list[dict]


@router.get("/", response_model=RunListResponse)
async def list_runs(
    status: Optional[str] = Query(
        None,
        description="Filter by status (STARTED, SUCCESS, FAILURE, CANCELED, etc.)",
    ),
    limit: int = Query(25, ge=1, le=100, description="Maximum results"),
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> RunListResponse:
    """
    List Dagster runs.

    Optionally filter by status.
    """
    dagster = get_dagster_service()

    try:
        runs = dagster.get_runs(status=status, limit=limit)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to query Dagster: {exc}",
        ) from exc

    run_dicts = [
        {
            "run_id": r.run_id,
            "status": r.status,
            "job_name": r.job_name,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "ended_at": r.ended_at.isoformat() if r.ended_at else None,
            "tags": r.tags,
            "batch_id": r.tags.get("batch_id"),
            "intent": r.tags.get("intent"),
        }
        for r in runs
    ]

    return RunListResponse(
        runs=run_dicts,
        count=len(run_dicts),
    )


@router.get("/{run_id}", response_model=RunDetailResponse)
async def get_run_details(
    run_id: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
) -> RunDetailResponse:
    """
    Get detailed run information including events and logs.
    """
    dagster = get_dagster_service()

    try:
        details = dagster.get_run_details(run_id)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to query Dagster: {exc}",
        ) from exc

    if not details:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    run_dict = {
        "run_id": details.run_id,
        "status": details.status,
        "job_name": details.job_name,
        "started_at": details.started_at.isoformat() if details.started_at else None,
        "ended_at": details.ended_at.isoformat() if details.ended_at else None,
        "tags": details.tags,
        "error_message": details.error_message,
        "batch_id": details.tags.get("batch_id"),
        "intent": details.tags.get("intent"),
    }

    event_dicts = [
        {
            "timestamp": e.timestamp.isoformat(),
            "event_type": e.event_type,
            "message": e.message,
            "step_key": e.step_key,
        }
        for e in details.events
    ]

    return RunDetailResponse(
        run=run_dict,
        events=event_dicts,
    )
