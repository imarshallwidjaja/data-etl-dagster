"""Unit tests for integration test endpoint env-var overrides.

Verifies that DAGSTER_GRAPHQL_URL and WEBAPP_URL env vars are respected
by the integration test fixtures, enabling in-network execution where
services are reachable via Docker DNS rather than localhost.
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Dagster GraphQL URL fixture
# ---------------------------------------------------------------------------


class TestDagsterGraphQLURL:
    """dagster_graphql_url fixture should prefer DAGSTER_GRAPHQL_URL env var."""

    def test_returns_localhost_default_when_env_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """Without env override, default to localhost:3000/graphql."""
        monkeypatch.delenv("DAGSTER_GRAPHQL_URL", raising=False)
        monkeypatch.delenv("DAGSTER_WEBSERVER_PORT", raising=False)

        from tests.integration.conftest import _dagster_graphql_url

        assert _dagster_graphql_url() == "http://localhost:3000/graphql"

    def test_returns_env_override_when_set(self, monkeypatch: pytest.MonkeyPatch):
        """DAGSTER_GRAPHQL_URL overrides the full URL."""
        monkeypatch.setenv(
            "DAGSTER_GRAPHQL_URL", "http://dagster-webserver:3000/graphql"
        )

        # Re-import to pick up env at fixture-call time
        from tests.integration.conftest import _dagster_graphql_url

        assert _dagster_graphql_url() == "http://dagster-webserver:3000/graphql"

    def test_env_override_wins_over_port_env(self, monkeypatch: pytest.MonkeyPatch):
        """DAGSTER_GRAPHQL_URL takes priority over DAGSTER_WEBSERVER_PORT."""
        monkeypatch.setenv("DAGSTER_GRAPHQL_URL", "http://custom-host:9999/graphql")
        monkeypatch.setenv("DAGSTER_WEBSERVER_PORT", "5555")

        from tests.integration.conftest import _dagster_graphql_url

        assert _dagster_graphql_url() == "http://custom-host:9999/graphql"


# ---------------------------------------------------------------------------
# Dagster base URL fixture
# ---------------------------------------------------------------------------


class TestDagsterURL:
    """dagster_url fixture should derive base URL from graphql URL."""

    def test_returns_localhost_default(self, monkeypatch: pytest.MonkeyPatch):
        """Without env override, default to localhost:3000."""
        monkeypatch.delenv("DAGSTER_GRAPHQL_URL", raising=False)
        monkeypatch.delenv("DAGSTER_WEBSERVER_PORT", raising=False)

        from tests.integration.conftest import _dagster_url

        assert _dagster_url() == "http://localhost:3000"

    def test_respects_dagster_graphql_url_env(self, monkeypatch: pytest.MonkeyPatch):
        """DAGSTER_GRAPHQL_URL should also drive the base URL (strip /graphql)."""
        monkeypatch.setenv(
            "DAGSTER_GRAPHQL_URL", "http://dagster-webserver:3000/graphql"
        )

        from tests.integration.conftest import _dagster_url

        assert _dagster_url() == "http://dagster-webserver:3000"


# ---------------------------------------------------------------------------
# Webapp URL fixture
# ---------------------------------------------------------------------------


class TestWebappURL:
    """webapp_url fixture should prefer WEBAPP_URL env var."""

    def test_returns_localhost_default_when_env_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """Without env override, default to localhost:8080."""
        monkeypatch.delenv("WEBAPP_URL", raising=False)

        from tests.integration.conftest import _webapp_url

        assert _webapp_url() == "http://localhost:8080"

    def test_returns_env_override_when_set(self, monkeypatch: pytest.MonkeyPatch):
        """WEBAPP_URL overrides the full URL."""
        monkeypatch.setenv("WEBAPP_URL", "http://webapp:8080")

        from tests.integration.conftest import _webapp_url

        assert _webapp_url() == "http://webapp:8080"
