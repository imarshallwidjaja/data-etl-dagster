# =============================================================================
# Dagster Service - GraphQL API Operations
# =============================================================================
# Service wrapper for Dagster GraphQL operations in the webapp.
# =============================================================================

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

import httpx

from app.config import get_settings


@dataclass
class RunSummary:
    """Summary of a Dagster run for list views."""

    run_id: str
    status: str
    job_name: str
    started_at: Optional[datetime]
    ended_at: Optional[datetime]
    tags: dict


@dataclass
class RunEvent:
    """A single event from a Dagster run."""

    timestamp: datetime
    event_type: str
    message: str
    step_key: Optional[str] = None


@dataclass
class RunDetails:
    """Detailed run information including events."""

    run_id: str
    status: str
    job_name: str
    started_at: Optional[datetime]
    ended_at: Optional[datetime]
    tags: dict
    events: list[RunEvent]
    error_message: Optional[str] = None


class DagsterService:
    """Service for Dagster GraphQL operations."""

    def __init__(self) -> None:
        settings = get_settings()
        self._graphql_url = settings.dagster_graphql_url

    def _execute_query(self, query: str, variables: Optional[dict] = None) -> dict:
        """Execute a GraphQL query."""
        response = httpx.post(
            self._graphql_url,
            json={"query": query, "variables": variables or {}},
            timeout=30.0,
        )
        response.raise_for_status()
        result = response.json()

        if "errors" in result:
            raise RuntimeError(f"GraphQL error: {result['errors']}")

        return result.get("data", {})

    def get_runs(
        self,
        status: Optional[str] = None,
        limit: int = 25,
    ) -> list[RunSummary]:
        """
        Get list of Dagster runs.

        Args:
            status: Optional status filter (STARTED, SUCCESS, FAILURE, etc.)
            limit: Maximum number of results

        Returns:
            List of RunSummary
        """
        query = """
        query RunsQuery($limit: Int!, $filter: RunsFilter) {
            runsOrError(limit: $limit, filter: $filter) {
                ... on Runs {
                    results {
                        runId
                        status
                        pipelineName
                        startTime
                        endTime
                        tags {
                            key
                            value
                        }
                    }
                }
                ... on InvalidPipelineRunsFilterError {
                    message
                }
                ... on PythonError {
                    message
                }
            }
        }
        """

        variables: dict[str, Any] = {"limit": limit}

        if status:
            variables["filter"] = {"statuses": [status]}

        data = self._execute_query(query, variables)

        runs_result = data.get("runsOrError", {})

        if "message" in runs_result:
            raise RuntimeError(runs_result["message"])

        results = []
        for run in runs_result.get("results", []):
            tags_dict = {tag["key"]: tag["value"] for tag in run.get("tags", [])}

            started_at = None
            if run.get("startTime"):
                started_at = datetime.fromtimestamp(run["startTime"])

            ended_at = None
            if run.get("endTime"):
                ended_at = datetime.fromtimestamp(run["endTime"])

            results.append(
                RunSummary(
                    run_id=run.get("runId", ""),
                    status=run.get("status", "UNKNOWN"),
                    job_name=run.get("pipelineName", ""),
                    started_at=started_at,
                    ended_at=ended_at,
                    tags=tags_dict,
                )
            )

        return results

    def get_run_details(self, run_id: str) -> Optional[RunDetails]:
        """
        Get detailed run information including events.

        Args:
            run_id: Dagster run ID

        Returns:
            RunDetails or None if not found
        """
        query = """
        query RunQuery($runId: ID!) {
            runOrError(runId: $runId) {
                ... on Run {
                    runId
                    status
                    pipelineName
                    startTime
                    endTime
                    tags {
                        key
                        value
                    }
                    eventConnection(afterCursor: null) {
                        events {
                            ... on MessageEvent {
                                message
                                timestamp
                                eventType
                                stepKey
                            }
                        }
                    }
                }
                ... on RunNotFoundError {
                    message
                }
                ... on PythonError {
                    message
                }
            }
        }
        """

        data = self._execute_query(query, {"runId": run_id})

        run_result = data.get("runOrError", {})

        if "message" in run_result and "runId" not in run_result:
            return None

        tags_dict = {tag["key"]: tag["value"] for tag in run_result.get("tags", [])}

        started_at = None
        if run_result.get("startTime"):
            started_at = datetime.fromtimestamp(run_result["startTime"])

        ended_at = None
        if run_result.get("endTime"):
            ended_at = datetime.fromtimestamp(run_result["endTime"])

        events = []
        error_message = None
        event_connection = run_result.get("eventConnection", {})

        for event in event_connection.get("events", []):
            timestamp = datetime.now()
            if event.get("timestamp"):
                try:
                    # Dagster timestamps are in milliseconds, convert to seconds
                    timestamp = datetime.fromtimestamp(
                        float(event["timestamp"]) / 1000.0
                    )
                except (ValueError, TypeError, OSError):
                    pass

            event_obj = RunEvent(
                timestamp=timestamp,
                event_type=event.get("eventType", ""),
                message=event.get("message", ""),
                step_key=event.get("stepKey"),
            )
            events.append(event_obj)

            # Capture error message from failure events
            if event.get("eventType") in ("RUN_FAILURE", "STEP_FAILURE"):
                error_message = event.get("message", "")

        return RunDetails(
            run_id=run_result.get("runId", ""),
            status=run_result.get("status", "UNKNOWN"),
            job_name=run_result.get("pipelineName", ""),
            started_at=started_at,
            ended_at=ended_at,
            tags=tags_dict,
            events=events,
            error_message=error_message,
        )


# Singleton instance
_dagster_service: Optional[DagsterService] = None


def get_dagster_service() -> DagsterService:
    """Get or create the Dagster service singleton."""
    global _dagster_service
    if _dagster_service is None:
        _dagster_service = DagsterService()
    return _dagster_service
