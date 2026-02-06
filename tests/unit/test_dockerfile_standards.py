"""Verify service Dockerfiles follow uv migration standards.

Checks:
- Pinned uv via ARG UV_VERSION and multi-stage copy
- UV_PROJECT_ENVIRONMENT=/opt/venv set before uv sync
- WORKDIR /workspace for dependency install context
- uv sync --frozen --no-install-project --only-group <group>
- Runtime VIRTUAL_ENV + PATH point to /opt/venv
- No pip install or requirements*.txt references
- Python 3.10 (not 3.11)
"""

import pathlib
import re

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

# Dockerfile → expected dependency group mapping
DOCKERFILES = {
    "services/dagster/Dockerfile": "dagster",
    "services/dagster/Dockerfile.user-code": "user-code",
    "services/webapp/Dockerfile": "webapp",
}


@pytest.fixture(params=list(DOCKERFILES.keys()), ids=list(DOCKERFILES.keys()))
def dockerfile_info(request):
    """Return (relative_path, content, expected_group) for each Dockerfile."""
    rel_path = request.param
    path = REPO_ROOT / rel_path
    assert path.exists(), f"Dockerfile not found: {path}"
    content = path.read_text()
    group = DOCKERFILES[rel_path]
    return rel_path, content, group


class TestUvPinned:
    """Dockerfiles must pin uv version via ARG + multi-stage COPY."""

    def test_has_uv_version_arg(self, dockerfile_info):
        rel_path, content, _ = dockerfile_info
        assert re.search(r"ARG\s+UV_VERSION", content), (
            f"{rel_path} missing ARG UV_VERSION"
        )

    def test_has_uv_stage(self, dockerfile_info):
        rel_path, content, _ = dockerfile_info
        assert re.search(
            r"FROM\s+ghcr\.io/astral-sh/uv:\$\{?UV_VERSION\}?\s+AS\s+uv",
            content,
        ), (
            f"{rel_path} missing multi-stage uv image (FROM ghcr.io/astral-sh/uv:${{UV_VERSION}} AS uv)"
        )

    def test_copies_uv_binary(self, dockerfile_info):
        rel_path, content, _ = dockerfile_info
        assert re.search(r"COPY\s+--from=uv\s+/uv", content), (
            f"{rel_path} missing COPY --from=uv /uv"
        )


class TestUvProjectEnvironment:
    """UV_PROJECT_ENVIRONMENT must be set before uv sync."""

    def test_uv_project_environment_set(self, dockerfile_info):
        rel_path, content, _ = dockerfile_info
        assert "UV_PROJECT_ENVIRONMENT=/opt/venv" in content, (
            f"{rel_path} missing ENV UV_PROJECT_ENVIRONMENT=/opt/venv"
        )

    def test_uv_project_env_before_uv_sync(self, dockerfile_info):
        """UV_PROJECT_ENVIRONMENT must appear before the uv sync command."""
        rel_path, content, _ = dockerfile_info
        env_pos = content.find("UV_PROJECT_ENVIRONMENT=/opt/venv")
        sync_pos = content.find("uv sync")
        assert env_pos != -1, f"{rel_path} missing UV_PROJECT_ENVIRONMENT"
        assert sync_pos != -1, f"{rel_path} missing uv sync"
        assert env_pos < sync_pos, (
            f"{rel_path}: UV_PROJECT_ENVIRONMENT must appear before uv sync"
        )


class TestWorkdir:
    """Dockerfiles must use WORKDIR /workspace for dependency context."""

    def test_has_workspace_workdir(self, dockerfile_info):
        rel_path, content, _ = dockerfile_info
        assert re.search(r"WORKDIR\s+/workspace", content), (
            f"{rel_path} missing WORKDIR /workspace"
        )


class TestUvSync:
    """Dockerfiles must install deps via uv sync with correct flags."""

    def test_uses_uv_sync_frozen(self, dockerfile_info):
        rel_path, content, _ = dockerfile_info
        assert "uv sync --frozen" in content, f"{rel_path} missing uv sync --frozen"

    def test_uses_correct_group(self, dockerfile_info):
        rel_path, content, group = dockerfile_info
        assert f"--only-group {group}" in content, (
            f"{rel_path} missing --only-group {group}"
        )

    def test_uses_no_install_project(self, dockerfile_info):
        rel_path, content, _ = dockerfile_info
        assert "--no-install-project" in content, (
            f"{rel_path} missing --no-install-project"
        )

    def test_copies_pyproject_and_lock(self, dockerfile_info):
        """Must copy pyproject.toml and uv.lock into build context."""
        rel_path, content, _ = dockerfile_info
        assert "pyproject.toml" in content, f"{rel_path} does not COPY pyproject.toml"
        assert "uv.lock" in content, f"{rel_path} does not COPY uv.lock"


class TestRuntimeVenv:
    """Runtime must use /opt/venv via VIRTUAL_ENV + PATH."""

    def test_virtual_env_set(self, dockerfile_info):
        rel_path, content, _ = dockerfile_info
        assert re.search(r"ENV\s+VIRTUAL_ENV\s*=\s*/opt/venv", content), (
            f"{rel_path} missing ENV VIRTUAL_ENV=/opt/venv"
        )

    def test_path_includes_venv(self, dockerfile_info):
        rel_path, content, _ = dockerfile_info
        # PATH should include /opt/venv/bin
        assert re.search(r"/opt/venv/bin", content), (
            f"{rel_path} PATH does not include /opt/venv/bin"
        )


class TestNoPipInstall:
    """Dockerfiles must not use pip install or reference requirements*.txt."""

    def test_no_pip_install(self, dockerfile_info):
        rel_path, content, _ = dockerfile_info
        assert "pip install" not in content, f"{rel_path} still uses pip install"

    def test_no_requirements_txt(self, dockerfile_info):
        rel_path, content, _ = dockerfile_info
        assert not re.search(r"requirements.*\.txt", content), (
            f"{rel_path} still references requirements*.txt"
        )


class TestPython310:
    """Webapp Dockerfile must use Python 3.10, not 3.11."""

    def test_webapp_uses_python_310(self):
        path = REPO_ROOT / "services/webapp/Dockerfile"
        content = path.read_text()
        assert "python:3.11" not in content, "Webapp Dockerfile still uses python:3.11"
        assert "python:3.10" in content, "Webapp Dockerfile does not use python:3.10"


class TestBuildContexts:
    """compose.yaml build contexts must support repo-root COPY."""

    def test_dagster_webserver_context_is_repo_root(self):
        compose_path = REPO_ROOT / "compose.yaml"
        content = compose_path.read_text()
        # dagster-webserver build context should be repo root (.)
        # not ./services/dagster
        webserver_section = _extract_service_build(content, "dagster-webserver")
        assert webserver_section is not None, (
            "dagster-webserver build section not found in compose.yaml"
        )
        assert (
            "context: ." in webserver_section or 'context: "."' in webserver_section
        ), "dagster-webserver build context must be repo root (.)"

    def test_dagster_daemon_context_is_repo_root(self):
        compose_path = REPO_ROOT / "compose.yaml"
        content = compose_path.read_text()
        daemon_section = _extract_service_build(content, "dagster-daemon")
        assert daemon_section is not None, (
            "dagster-daemon build section not found in compose.yaml"
        )
        assert "context: ." in daemon_section or 'context: "."' in daemon_section, (
            "dagster-daemon build context must be repo root (.)"
        )


def _extract_service_build(compose_content: str, service_name: str) -> str | None:
    """Extract the build section for a service from compose YAML content.

    Returns the lines from the service definition through its build block,
    or None if not found.
    """
    lines = compose_content.splitlines()
    in_service = False
    indent = 0
    result = []
    for line in lines:
        stripped = line.lstrip()
        current_indent = len(line) - len(stripped)
        if stripped.startswith(f"{service_name}:"):
            in_service = True
            indent = current_indent
            result.append(line)
            continue
        if in_service:
            if stripped and current_indent <= indent and not stripped.startswith("#"):
                break
            result.append(line)
    return "\n".join(result) if result else None
