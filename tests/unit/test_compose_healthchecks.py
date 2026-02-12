"""Guard compose health checks used by the containerized test stack."""

from __future__ import annotations

import pathlib

import yaml


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load_yaml(path: pathlib.Path) -> dict:
    data = yaml.safe_load(path.read_text())
    assert isinstance(data, dict), f"Expected mapping at root of {path}"
    return data


def test_dagster_webserver_has_healthcheck():
    compose = _load_yaml(REPO_ROOT / "compose.yaml")
    dagster_web = compose["services"]["dagster-webserver"]
    assert "healthcheck" in dagster_web, (
        "compose.yaml dagster-webserver must define a healthcheck"
    )


def test_dagster_webserver_healthcheck_uses_graphql_query():
    """A bare GET to /graphql returns 400 on Dagster (GraphQL expects a query).

    The healthcheck must send a real GraphQL query — the minimal introspection
    ``{__typename}`` works on every GraphQL server and returns 200.
    """
    compose = _load_yaml(REPO_ROOT / "compose.yaml")
    hc = compose["services"]["dagster-webserver"]["healthcheck"]
    # test can be a list (CMD/CMD-SHELL) — flatten to a single string
    test_cmd = hc["test"] if isinstance(hc["test"], str) else " ".join(hc["test"])

    assert "__typename" in test_cmd, (
        "dagster-webserver healthcheck must include a GraphQL query "
        "(e.g. {__typename}) — a bare GET to /graphql returns 400"
    )


def test_webapp_has_healthcheck():
    compose = _load_yaml(REPO_ROOT / "compose.yaml")
    webapp = compose["services"]["webapp"]
    assert "healthcheck" in webapp, "compose.yaml webapp must define a healthcheck"


def test_test_runner_waits_for_dagster_and_webapp_health():
    overlay = _load_yaml(REPO_ROOT / "compose.test.yaml")
    depends_on = overlay["services"]["test-runner"]["depends_on"]

    assert depends_on["dagster-webserver"]["condition"] == "service_healthy", (
        "compose.test.yaml test-runner must wait for dagster-webserver health"
    )
    assert depends_on["webapp"]["condition"] == "service_healthy", (
        "compose.test.yaml test-runner must wait for webapp health"
    )


def test_test_runner_waits_for_dagster_daemon_start():
    """E2E GraphQL-launched runs require dagster-daemon to dequeue queued runs."""
    overlay = _load_yaml(REPO_ROOT / "compose.test.yaml")
    depends_on = overlay["services"]["test-runner"]["depends_on"]

    assert "dagster-daemon" in depends_on, (
        "compose.test.yaml test-runner must depend on dagster-daemon; "
        "without it, GraphQL-launched runs can remain queued and E2E polling hangs"
    )
    assert depends_on["dagster-daemon"]["condition"] == "service_started", (
        "compose.test.yaml test-runner must wait for dagster-daemon to start"
    )


def test_test_overlay_provides_webapp_session_secret():
    """compose.test.yaml must set WEBAPP_SESSION_SECRET for the webapp service.

    The base compose.yaml uses ``${WEBAPP_SESSION_SECRET:-}`` which resolves to
    empty when no host ``.env`` supplies the value.  Since the test stack sets
    ``ENVIRONMENT=ci`` (via compose.test.yaml or ``.env.ci``), the webapp
    Settings validator requires a non-empty secret.  The test overlay must
    therefore hardcode a stable, non-production secret so the webapp container
    starts healthy without relying on host-side env files.
    """
    overlay = _load_yaml(REPO_ROOT / "compose.test.yaml")
    webapp_env = overlay["services"]["webapp"]["environment"]

    assert "WEBAPP_SESSION_SECRET" in webapp_env, (
        "compose.test.yaml webapp.environment must include WEBAPP_SESSION_SECRET; "
        "without it, ENVIRONMENT=ci causes a Settings ValidationError at startup"
    )
    secret = webapp_env["WEBAPP_SESSION_SECRET"]
    assert secret and len(str(secret).strip()) > 0, (
        "WEBAPP_SESSION_SECRET in compose.test.yaml must be a non-empty string"
    )
