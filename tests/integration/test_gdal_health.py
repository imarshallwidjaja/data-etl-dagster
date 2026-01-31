"""Integration test: GDAL installation health check via Dagster.

This test verifies that GDAL and required libraries are properly installed
and configured in the user-code container by triggering a Dagster job run.

The test monitors the job execution through the Dagster GraphQL API and
validates that the health check asset materializes successfully.

Run with: pytest tests/integration/test_gdal_health.py -v -m integration
"""

import os
import pytest
import requests
import time
from typing import Optional
from requests.exceptions import RequestException, Timeout

from .helpers import build_test_run_tags


pytestmark = pytest.mark.integration


@pytest.fixture
def dagster_graphql_url():
    """Get Dagster GraphQL endpoint URL."""
    port = os.getenv("DAGSTER_WEBSERVER_PORT", "3000")
    return f"http://localhost:{port}/graphql"


@pytest.fixture
def dagster_client(dagster_graphql_url):
    """Create a simple Dagster GraphQL client."""

    class DagsterGraphQLClient:
        def __init__(self, url: str):
            self.url = url

        def query(
            self, query_str: str, variables: Optional[dict] = None, timeout: int = 30
        ) -> dict:
            """Execute a GraphQL query."""
            payload = {"query": query_str}
            if variables:
                payload["variables"] = variables

            try:
                response = requests.post(
                    self.url,
                    json=payload,
                    timeout=timeout,
                )
                if response.status_code != 200:
                    raise RuntimeError(
                        f"GraphQL request failed with status {response.status_code}: "
                        f"{response.text}"
                    )
                return response.json()
            except (RequestException, Timeout) as e:
                raise RuntimeError(
                    f"Failed to communicate with Dagster GraphQL API: {e}"
                )

    return DagsterGraphQLClient(dagster_graphql_url)


class TestGDALHealthCheckJob:
    """Test suite for GDAL health check via Dagster job."""

    def test_gdal_health_check_job_exists(self, dagster_client):
        """Verify that the gdal_health_check_job is registered."""
        query = """
        query JobCheck($repositoryLocationName: String!, $repositoryName: String!, $jobName: String!) {
            pipelineOrError(params: {
                repositoryLocationName: $repositoryLocationName,
                repositoryName: $repositoryName,
                pipelineName: $jobName
            }) {
                ... on Pipeline {
                    name
                    description
                }
                ... on PipelineNotFoundError {
                    message
                }
                ... on InvalidSubsetError {
                    message
                }
                ... on PythonError {
                    message
                }
            }
        }
        """

        variables = {
            "repositoryLocationName": "etl_pipelines",
            "repositoryName": "__repository__",
            "jobName": "gdal_health_check_job",
        }

        result = dagster_client.query(query, variables=variables)

        # Check for errors
        assert "errors" not in result, f"GraphQL errors: {result.get('errors')}"

        job_or_error = result["data"]["pipelineOrError"]
        assert "name" in job_or_error, (
            f"Job not found or error occurred: {job_or_error}"
        )
        assert job_or_error["name"] == "gdal_health_check_job"

    def test_gdal_health_check_asset_materializes(self, dagster_client):
        """
        Trigger gdal_health_check_job and verify the asset materializes successfully.

        This is the primary integration test that validates GDAL is working inside
        the user-code container.
        """
        # 1. Launch the job
        launch_query = """
        mutation LaunchRun(
            $repositoryLocationName: String!
            $repositoryName: String!
            $jobName: String!
            $executionMetadata: ExecutionMetadata
        ) {
            launchRun(
                executionParams: {
                    selector: {
                        repositoryLocationName: $repositoryLocationName,
                        repositoryName: $repositoryName,
                        pipelineName: $jobName
                    }
                    executionMetadata: $executionMetadata
                }
            ) {
                ... on LaunchRunSuccess {
                    run {
                        runId
                        status
                    }
                }
                ... on PipelineNotFoundError {
                    message
                }
                ... on RunConfigValidationInvalid {
                    errors {
                        message
                    }
                }
                ... on PythonError {
                    message
                }
            }
        }
        """

        variables = {
            "repositoryLocationName": "etl_pipelines",
            "repositoryName": "__repository__",
            "jobName": "gdal_health_check_job",
            "executionMetadata": {
                "tags": build_test_run_tags(test_run_id="gdal_health_check")
            },
        }

        launch_result = dagster_client.query(
            launch_query, variables=variables, timeout=10
        )

        assert "errors" not in launch_result, (
            f"Failed to launch job: {launch_result.get('errors')}"
        )

        launch_response = launch_result["data"]["launchRun"]
        assert "run" in launch_response, (
            f"Job launch failed: {launch_response.get('message', 'Unknown error')}"
        )

        run_id = launch_response["run"]["runId"]
        assert run_id, "No run_id returned from job launch"
        print(f"Launched run: {run_id}")

        # 2. Poll for job completion (with timeout)
        max_wait = 120  # 2 minutes max
        poll_interval = 2  # Check every 2 seconds
        elapsed = 0

        status = "STARTING"

        while elapsed < max_wait:
            run_query = """
            query GetRun($runId: ID!) {
                runOrError(runId: $runId) {
                    ... on Run {
                        id
                        status
                    }
                    ... on RunNotFoundError {
                        message
                    }
                }
            }
            """

            run_result = dagster_client.query(
                run_query, variables={"runId": run_id}, timeout=10
            )

            assert "errors" not in run_result, (
                f"Failed to query run status: {run_result.get('errors')}"
            )

            run_or_error = run_result["data"]["runOrError"]
            if "id" not in run_or_error:
                # Run not found yet, wait and retry
                time.sleep(poll_interval)
                elapsed += poll_interval
                continue

            status = run_or_error["status"]

            # Check for terminal states
            if status in ["SUCCESS", "FAILURE", "CANCELED"]:
                break

            time.sleep(poll_interval)
            elapsed += poll_interval

        # 3. Verify final status
        if status != "SUCCESS":
            # Fetch logs to debug failure
            log_query = """
            query GetRunLogs($runId: ID!) {
                logsForRun(runId: $runId) {
                    __typename
                    ... on EventConnection {
                        events {
                            __typename
                            ... on MessageEvent {
                                message
                                level
                            }
                            ... on ExecutionStepFailureEvent {
                                error {
                                    message
                                    stack
                                }
                            }
                        }
                    }
                }
            }
            """

            log_result = dagster_client.query(
                log_query, variables={"runId": run_id}, timeout=10
            )

            error_details = "Unknown error"
            if "data" in log_result and "logsForRun" in log_result["data"]:
                logs = log_result["data"]["logsForRun"]
                if logs.get("__typename") == "EventConnection":
                    events_list = logs.get("events", [])
                    # Filter for errors
                    error_events = [
                        e
                        for e in events_list
                        if e.get("level") == "ERROR"
                        or "Failure" in e.get("__typename", "")
                    ]
                    if error_events:
                        error_details = error_events
                    else:
                        # Just show the last few events if no explicit error found
                        error_details = (
                            events_list[-5:] if events_list else "No events found"
                        )

            assert status == "SUCCESS", (
                f"Job failed with status {status}. Error details: {error_details}"
            )

        # 4. Verify the asset was materialized
        asset_query = """
        query GetAssetMaterializationEvents($runId: ID!) {
            logsForRun(runId: $runId) {
                __typename
                ... on EventConnection {
                    events {
                        __typename
                        ... on MaterializationEvent {
                            assetKey {
                                path
                            }
                        }
                    }
                }
            }
        }
        """

        asset_result = dagster_client.query(
            asset_query, variables={"runId": run_id}, timeout=10
        )

        assert "errors" not in asset_result, (
            f"Failed to query asset events: {asset_result.get('errors')}"
        )

        logs = asset_result["data"]["logsForRun"]
        events = []
        if logs.get("__typename") == "EventConnection":
            events = logs.get("events", [])

        # Find asset materialization events
        asset_events = [
            e
            for e in events
            if e.get("__typename") == "MaterializationEvent"
            and e.get("assetKey")
            and "gdal_health_check" in str(e["assetKey"].get("path", []))
        ]

        assert len(asset_events) > 0, (
            f"No asset materialization events found for gdal_health_check in run {run_id}"
        )

        print("✅ GDAL health check passed successfully!")
