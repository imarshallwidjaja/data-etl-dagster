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
    """CI must install uv via astral-sh/setup-uv with version-file."""

    def test_uses_setup_uv_action(self, workflow_content):
        name, content = workflow_content
        assert "astral-sh/setup-uv" in content, (
            f"{name} does not use astral-sh/setup-uv action"
        )

    def test_uses_version_file(self, workflow_content):
        name, content = workflow_content
        assert "version-file: .uv-version" in content, (
            f"{name} does not use version-file: .uv-version"
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
