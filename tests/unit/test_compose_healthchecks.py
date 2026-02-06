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
