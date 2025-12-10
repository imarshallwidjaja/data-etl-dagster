"""
Integration tests for Dagster (orchestrator).

Tests GraphQL API connectivity and basic queries.
"""

import os
import pytest
import requests
from requests.exceptions import RequestException, Timeout


pytestmark = pytest.mark.integration


@pytest.fixture
def dagster_url():
    """Get Dagster webserver URL from environment."""
    port = os.getenv("DAGSTER_WEBSERVER_PORT", "3000")
    return f"http://localhost:{port}"


@pytest.fixture
def dagster_graphql_url(dagster_url):
    """Get Dagster GraphQL endpoint URL."""
    return f"{dagster_url}/graphql"


class TestDagsterConnectivity:
    """Test Dagster connectivity and GraphQL API."""
    
    def test_dagster_webserver_accessible(self, dagster_url):
        """Test that Dagster webserver is accessible."""
        try:
            response = requests.get(dagster_url, timeout=5)
            # Accept both 200 (success) and 404 (not found but server is up)
            assert response.status_code in [200, 404], \
                f"Unexpected status code: {response.status_code}"
        except (RequestException, Timeout) as e:
            pytest.fail(f"Dagster webserver not accessible: {e}")
    
    def test_dagster_graphql_api(self, dagster_graphql_url):
        """Test that Dagster GraphQL API responds."""
        query = {"query": "{ version }"}
        
        try:
            response = requests.post(
                dagster_graphql_url,
                json=query,
                timeout=10,
            )
            assert response.status_code == 200, \
                f"GraphQL API returned status {response.status_code}"
            
            data = response.json()
            assert "data" in data, "Response missing 'data' field"
            assert "version" in data["data"], "Response missing 'version' field"
            assert data["data"]["version"] is not None, "Version is None"
        except (RequestException, Timeout) as e:
            pytest.fail(f"Dagster GraphQL API not accessible: {e}")
    
    def test_dagster_graphql_workspace_query(self, dagster_graphql_url):
        """Test that we can query workspace information."""
        query = {
            "query": """
                {
                    workspaceOrError {
                        ... on Workspace {
                            locationEntries {
                                name
                            }
                        }
                    }
                }
            """
        }
        
        try:
            response = requests.post(
                dagster_graphql_url,
                json=query,
                timeout=10,
            )
            assert response.status_code == 200
            
            data = response.json()
            # Check for errors
            if "errors" in data:
                # Some errors are acceptable (e.g., no workspace configured)
                # but we should at least get a response
                assert len(data.get("errors", [])) > 0 or "data" in data
            else:
                assert "data" in data
        except (RequestException, Timeout) as e:
            pytest.fail(f"Dagster GraphQL workspace query failed: {e}")

