"""Verify CI workflow files follow project standards.

Checks:
- Python 3.10 (not 3.11)
- uv setup via astral-sh/setup-uv with version-file
- No legacy pip install of legacy requirements files
- No references to deleted docker-compose.yaml
"""

import pathlib
import re

import pytest

WORKFLOWS_DIR = pathlib.Path(__file__).resolve().parents[2] / ".github" / "workflows"

# Legacy pip install pattern (deleted; asserted absent)
_LEGACY_PIP_INSTALL = "pip install -r " + "requirements-test" + ".txt"

# All reusable workflow files that set up Python and/or Docker
WORKFLOW_FILES = [
    "_unit-tests.yml",
    "_integration-infra.yml",
    "_integration-etl.yml",
    "_integration-webapp.yml",
    "_e2e-tests.yml",
]


@pytest.fixture(params=WORKFLOW_FILES)
def workflow_content(request):
    """Return (filename, content) for each workflow file."""
    path = WORKFLOWS_DIR / request.param
    assert path.exists(), f"Workflow file not found: {path}"
    return request.param, path.read_text()


class TestNoPython311:
    """CI must use Python 3.10, not 3.11."""

    def test_no_python_311_in_setup(self, workflow_content):
        name, content = workflow_content
        assert 'python-version: "3.11"' not in content, (
            f"{name} still references python-version 3.11"
        )


class TestUvSetup:
    """CI must install uv via astral-sh/setup-uv with explicit version."""

    def test_uses_setup_uv_action(self, workflow_content):
        name, content = workflow_content
        assert "astral-sh/setup-uv" in content, (
            f"{name} does not use astral-sh/setup-uv action"
        )

    def test_reads_uv_version_step(self, workflow_content):
        name, content = workflow_content
        assert "Read pinned uv version" in content, (
            f"{name} missing step that reads .uv-version"
        )

    def test_uses_explicit_uv_version_input(self, workflow_content):
        name, content = workflow_content
        assert "version: ${{ steps.uv_version.outputs.value }}" in content, (
            f"{name} does not pass explicit uv version from .uv-version"
        )


class TestUvVersionFileFormat:
    """.uv-version must be a plain uv version string."""

    def test_uv_version_file_is_plain_version(self):
        uv_version_path = pathlib.Path(__file__).resolve().parents[2] / ".uv-version"
        content = uv_version_path.read_text().strip()
        assert re.match(r"^\d+\.\d+\.\d+$", content), (
            ".uv-version must be a plain semantic version string (e.g. 0.9.22)"
        )


class TestNoPipInstall:
    """CI must use uv sync, not pip install."""

    def test_no_pip_install_requirements(self, workflow_content):
        name, content = workflow_content
        assert _LEGACY_PIP_INSTALL not in content, (
            f"{name} still uses legacy pip install"
        )

    def test_no_pip_upgrade(self, workflow_content):
        name, content = workflow_content
        assert "pip install --upgrade pip" not in content, (
            f"{name} still runs pip install --upgrade pip"
        )

    def test_uses_uv_sync(self, workflow_content):
        name, content = workflow_content
        assert "uv sync --frozen --group test" in content, (
            f"{name} does not use uv sync --frozen --group test"
        )


class TestNoLegacyDockerCompose:
    """CI must not reference deleted docker-compose.yaml."""

    DOCKER_WORKFLOWS = [
        "_integration-infra.yml",
        "_integration-etl.yml",
        "_integration-webapp.yml",
        "_e2e-tests.yml",
    ]

    @pytest.fixture(params=DOCKER_WORKFLOWS)
    def docker_workflow_content(self, request):
        path = WORKFLOWS_DIR / request.param
        assert path.exists(), f"Workflow file not found: {path}"
        return request.param, path.read_text()

    def test_no_docker_compose_yaml_reference(self, docker_workflow_content):
        name, content = docker_workflow_content
        # Match docker-compose.yaml but not compose.yaml
        matches = re.findall(r"docker-compose\.yaml", content)
        assert not matches, (
            f"{name} still references docker-compose.yaml ({len(matches)} occurrences)"
        )

    def test_uses_compose_yaml(self, docker_workflow_content):
        name, content = docker_workflow_content
        assert "compose.yaml" in content, f"{name} does not reference compose.yaml"


class TestCiOrchestratorForceAll:
    """ci.yml force-all filter must reference compose.yaml, not docker-compose.yaml."""

    def test_force_all_references_compose_yaml(self):
        ci_path = WORKFLOWS_DIR / "ci.yml"
        content = ci_path.read_text()
        assert "docker-compose.yaml" not in content, (
            "ci.yml force-all filter still references docker-compose.yaml"
        )
        assert "compose.yaml" in content, (
            "ci.yml force-all filter does not reference compose.yaml"
        )


class TestCiComposeTestOverlayUsage:
    """Docker integration workflows must use compose.test.yaml in CI."""

    _FILES = [
        "_integration-infra.yml",
        "_integration-etl.yml",
        "_integration-webapp.yml",
        "_e2e-tests.yml",
    ]

    @pytest.fixture(params=_FILES)
    def overlay_workflow_content(self, request):
        path = WORKFLOWS_DIR / request.param
        assert path.exists(), f"Workflow file not found: {path}"
        return request.param, path.read_text()

    def test_compose_commands_include_test_overlay(self, overlay_workflow_content):
        name, content = overlay_workflow_content
        assert "docker compose -f compose.yaml -f compose.test.yaml" in content, (
            f"{name} must include compose.test.yaml in docker compose commands"
        )


class TestCiContainerizedIntegrationExecution:
    """Integration workflows should run pytest in test-runner container."""

    _FILES = [
        "_integration-infra.yml",
        "_integration-etl.yml",
        "_integration-webapp.yml",
        "_e2e-tests.yml",
    ]

    @pytest.fixture(params=_FILES)
    def integration_workflow_content(self, request):
        path = WORKFLOWS_DIR / request.param
        assert path.exists(), f"Workflow file not found: {path}"
        return request.param, path.read_text()

    def test_uses_test_runner_container(self, integration_workflow_content):
        name, content = integration_workflow_content
        assert "run --rm --no-deps test-runner" in content, (
            f"{name} must run tests through test-runner container"
        )


class TestCiWaitForServicesEnvPropagation:
    """Wait-for-services in test-runner must receive WAIT_FOR_SERVICES."""

    _FILES = [
        "_integration-infra.yml",
        "_integration-etl.yml",
        "_integration-webapp.yml",
        "_e2e-tests.yml",
    ]

    @pytest.fixture(params=_FILES)
    def wait_workflow_content(self, request):
        path = WORKFLOWS_DIR / request.param
        assert path.exists(), f"Workflow file not found: {path}"
        return request.param, path.read_text()

    def test_wait_step_passes_wait_for_services_env(self, wait_workflow_content):
        name, content = wait_workflow_content
        assert "run --rm --no-deps -e WAIT_FOR_SERVICES test-runner" in content, (
            f"{name} must pass WAIT_FOR_SERVICES into test-runner for wait_for_services.py"
        )


class TestCiUnstableContainerLogDumping:
    """Unstable container IDs must be dumped with docker logs, not compose logs."""

    _FILES = ["_integration-etl.yml", "_e2e-tests.yml"]

    @pytest.fixture(params=_FILES)
    def unstable_log_workflow_content(self, request):
        path = WORKFLOWS_DIR / request.param
        assert path.exists(), f"Workflow file not found: {path}"
        return request.param, path.read_text()

    def test_uses_docker_logs_for_unstable_container_ids(
        self, unstable_log_workflow_content
    ):
        name, content = unstable_log_workflow_content
        assert 'docker logs "$line" || true' in content, (
            f"{name} must use docker logs for unstable container IDs"
        )
