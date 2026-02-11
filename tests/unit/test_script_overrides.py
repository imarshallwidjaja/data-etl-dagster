"""Tests for URL overrides in wait_for_services.py and compose-project mode in check_container_stability.py."""

from __future__ import annotations

import os
from unittest.mock import patch


# ---------------------------------------------------------------------------
# wait_for_services: URL override helpers
# ---------------------------------------------------------------------------


class TestDagsterURLOverride:
    """check_dagster and verify_user_code_dagster should honour DAGSTER_GRAPHQL_URL."""

    def test_check_dagster_uses_default_url_when_no_override(self):
        """check_dagster builds URL from localhost + port when no override is set."""
        from scripts.wait_for_services import _dagster_graphql_url

        with patch.dict(os.environ, {}, clear=True):
            url = _dagster_graphql_url()
        assert url == "http://localhost:3000/graphql"

    def test_check_dagster_uses_port_override(self):
        """check_dagster respects DAGSTER_WEBSERVER_PORT."""
        from scripts.wait_for_services import _dagster_graphql_url

        with patch.dict(os.environ, {"DAGSTER_WEBSERVER_PORT": "4000"}, clear=True):
            url = _dagster_graphql_url()
        assert url == "http://localhost:4000/graphql"

    def test_check_dagster_uses_full_url_override(self):
        """DAGSTER_GRAPHQL_URL takes priority over port-based URL."""
        from scripts.wait_for_services import _dagster_graphql_url

        with patch.dict(
            os.environ,
            {
                "DAGSTER_GRAPHQL_URL": "http://dagster-webserver:3000/graphql",
                "DAGSTER_WEBSERVER_PORT": "9999",
            },
            clear=True,
        ):
            url = _dagster_graphql_url()
        assert url == "http://dagster-webserver:3000/graphql"


class TestWebappURLOverride:
    """check_webapp should honour WEBAPP_URL."""

    def test_check_webapp_uses_default_url(self):
        """check_webapp builds URL from localhost:8080 when no override is set."""
        from scripts.wait_for_services import _webapp_url

        with patch.dict(os.environ, {}, clear=True):
            url = _webapp_url()
        assert url == "http://localhost:8080"

    def test_check_webapp_uses_override(self):
        """WEBAPP_URL takes priority over default."""
        from scripts.wait_for_services import _webapp_url

        with patch.dict(os.environ, {"WEBAPP_URL": "http://webapp:8080"}, clear=True):
            url = _webapp_url()
        assert url == "http://webapp:8080"


# ---------------------------------------------------------------------------
# check_container_stability: compose-project mode
# ---------------------------------------------------------------------------


class TestComposeProjectContainerNames:
    """Container names should be derived from COMPOSE_PROJECT_NAME when set."""

    def test_default_container_names_use_directory_project_name(self):
        """Without COMPOSE_PROJECT_NAME, derives names from cwd project name."""
        from scripts.check_container_stability import resolve_container_names

        with (
            patch.dict(os.environ, {}, clear=True),
            patch(
                "scripts.check_container_stability.os.getcwd",
                return_value="/tmp/data-etl-dagster",
            ),
        ):
            names = resolve_container_names()
        assert names == [
            "data-etl-dagster-dagster-webserver-1",
            "data-etl-dagster-dagster-daemon-1",
            "data-etl-dagster-user-code-1",
            "data-etl-dagster-mongodb-1",
            "data-etl-dagster-postgis-1",
            "data-etl-dagster-minio-1",
        ]

    def test_project_scoped_container_names(self):
        """With COMPOSE_PROJECT_NAME, derives <project>-<service>-1 names."""
        from scripts.check_container_stability import resolve_container_names

        with patch.dict(os.environ, {"COMPOSE_PROJECT_NAME": "wt-smoke"}, clear=True):
            names = resolve_container_names()
        assert names == [
            "wt-smoke-dagster-webserver-1",
            "wt-smoke-dagster-daemon-1",
            "wt-smoke-user-code-1",
            "wt-smoke-mongodb-1",
            "wt-smoke-postgis-1",
            "wt-smoke-minio-1",
        ]

    def test_check_containers_env_overrides_project_names(self):
        """CHECK_CONTAINERS still works and overrides project-derived names."""
        from scripts.check_container_stability import resolve_container_names

        with patch.dict(
            os.environ,
            {"COMPOSE_PROJECT_NAME": "wt-smoke", "CHECK_CONTAINERS": "foo,bar"},
            clear=True,
        ):
            names = resolve_container_names()
        assert names == ["foo", "bar"]
