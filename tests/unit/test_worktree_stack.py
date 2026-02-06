"""Unit tests for scripts/worktree_stack.py — agent-safe compose wrapper."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Import the module under test
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

import worktree_stack  # noqa: E402


# ===========================================================================
# Helpers
# ===========================================================================


def _expected_project_name(worktree_root: str) -> str:
    """Mirror the production algorithm so tests stay in sync."""
    digest = hashlib.sha256(worktree_root.encode()).hexdigest()[:8]
    return f"wt-{digest}"


# ===========================================================================
# resolve_worktree_root
# ===========================================================================


class TestResolveWorktreeRoot:
    """Worktree root resolution via git rev-parse."""

    def test_returns_stripped_stdout(self, tmp_path: Path):
        fake_root = str(tmp_path / "my-repo")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(stdout=f"  {fake_root}  \n", returncode=0)
            result = worktree_stack.resolve_worktree_root()
        assert result == fake_root
        mock_run.assert_called_once_with(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
        )

    def test_raises_on_git_failure(self):
        with patch(
            "subprocess.run", side_effect=subprocess.CalledProcessError(128, "git")
        ):
            with pytest.raises(SystemExit):
                worktree_stack.resolve_worktree_root()


# ===========================================================================
# project_name
# ===========================================================================


class TestProjectName:
    """Deterministic project naming from worktree root."""

    def test_deterministic_hash(self):
        root = "/home/user/repo"
        name = worktree_stack.project_name(root)
        assert name == _expected_project_name(root)

    def test_starts_with_wt_prefix(self):
        name = worktree_stack.project_name("/any/path")
        assert name.startswith("wt-")

    def test_hash_length_is_8(self):
        name = worktree_stack.project_name("/some/worktree")
        # "wt-" = 3 chars, hash = 8 chars
        assert len(name) == 11

    def test_different_paths_different_names(self):
        a = worktree_stack.project_name("/path/a")
        b = worktree_stack.project_name("/path/b")
        assert a != b


# ===========================================================================
# state_file helpers
# ===========================================================================


class TestStateFile:
    """State file at <worktree>/.worktree/stack.json."""

    def test_state_file_path(self, tmp_path: Path):
        path = worktree_stack.state_file_path(str(tmp_path))
        assert path == Path(tmp_path / ".worktree" / "stack.json")

    def test_write_state_creates_dir_and_file(self, tmp_path: Path):
        project = "wt-abcd1234"
        worktree_stack.write_state(str(tmp_path), project)
        sf = tmp_path / ".worktree" / "stack.json"
        assert sf.exists()
        data = json.loads(sf.read_text())
        assert data["project"] == project
        assert "worktree_root" in data

    def test_read_state_returns_data(self, tmp_path: Path):
        project = "wt-abcd1234"
        worktree_stack.write_state(str(tmp_path), project)
        data = worktree_stack.read_state(str(tmp_path))
        assert data["project"] == project

    def test_read_state_returns_none_when_missing(self, tmp_path: Path):
        data = worktree_stack.read_state(str(tmp_path))
        assert data is None

    def test_remove_state_deletes_file(self, tmp_path: Path):
        worktree_stack.write_state(str(tmp_path), "wt-abcd1234")
        sf = tmp_path / ".worktree" / "stack.json"
        assert sf.exists()
        worktree_stack.remove_state(str(tmp_path))
        assert not sf.exists()


# ===========================================================================
# compose_cmd builder
# ===========================================================================


class TestComposeCmd:
    """Builds the correct docker compose command arrays."""

    def test_up_command(self):
        cmd = worktree_stack.compose_cmd("wt-abc12345", "up")
        assert cmd == [
            "docker",
            "compose",
            "-f",
            "compose.yaml",
            "-f",
            "compose.test.yaml",
            "-p",
            "wt-abc12345",
            "up",
            "-d",
            "--build",
        ]

    def test_down_command(self):
        cmd = worktree_stack.compose_cmd("wt-abc12345", "down")
        assert cmd == [
            "docker",
            "compose",
            "-f",
            "compose.yaml",
            "-f",
            "compose.test.yaml",
            "-p",
            "wt-abc12345",
            "down",
            "-v",
            "--remove-orphans",
        ]

    def test_test_command_no_extra_args(self):
        cmd = worktree_stack.compose_cmd("wt-abc12345", "test", pytest_args=[])
        assert cmd == [
            "docker",
            "compose",
            "-f",
            "compose.yaml",
            "-f",
            "compose.test.yaml",
            "-p",
            "wt-abc12345",
            "run",
            "--rm",
            "test-runner",
            "pytest",
        ]

    def test_test_command_with_extra_args(self):
        cmd = worktree_stack.compose_cmd(
            "wt-abc12345", "test", pytest_args=["-q", "--tb=short"]
        )
        assert cmd == [
            "docker",
            "compose",
            "-f",
            "compose.yaml",
            "-f",
            "compose.test.yaml",
            "-p",
            "wt-abc12345",
            "run",
            "--rm",
            "test-runner",
            "pytest",
            "-q",
            "--tb=short",
        ]


# ===========================================================================
# CLI action: up
# ===========================================================================


class TestActionUp:
    """The 'up' action starts the test stack and writes state."""

    @patch("worktree_stack.resolve_worktree_root")
    @patch("subprocess.run")
    def test_up_writes_state_and_runs_compose(self, mock_run, mock_root, tmp_path):
        mock_root.return_value = str(tmp_path)
        mock_run.return_value = MagicMock(returncode=0)

        worktree_stack.action_up()

        expected_project = _expected_project_name(str(tmp_path))

        # Should have run the compose up command
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert args[0:2] == ["docker", "compose"]
        assert "-p" in args
        assert expected_project in args
        assert "up" in args
        assert "-d" in args
        assert "--build" in args

        # State file should exist
        sf = tmp_path / ".worktree" / "stack.json"
        assert sf.exists()
        data = json.loads(sf.read_text())
        assert data["project"] == expected_project

    @patch("worktree_stack.resolve_worktree_root")
    @patch("subprocess.run")
    def test_up_preserves_state_on_compose_failure(self, mock_run, mock_root, tmp_path):
        """When compose up fails, state file must remain so down can clean up."""
        mock_root.return_value = str(tmp_path)
        mock_run.return_value = MagicMock(returncode=1)

        with pytest.raises(SystemExit) as exc_info:
            worktree_stack.action_up()
        assert exc_info.value.code == 1

        # State file must still exist — this is what allows ``down`` to work
        sf = tmp_path / ".worktree" / "stack.json"
        assert sf.exists(), "state file must survive a failed 'up' for teardown"
        data = json.loads(sf.read_text())
        assert data["project"] == _expected_project_name(str(tmp_path))

    @patch("worktree_stack.resolve_worktree_root")
    @patch("subprocess.run")
    def test_down_works_after_failed_up(self, mock_run, mock_root, tmp_path):
        """Full scenario: up fails → down still tears down partial resources."""
        mock_root.return_value = str(tmp_path)
        expected_project = _expected_project_name(str(tmp_path))

        # Simulate failed up (compose returns non-zero)
        mock_run.return_value = MagicMock(returncode=1)
        with pytest.raises(SystemExit):
            worktree_stack.action_up()

        # Now simulate successful down
        mock_run.reset_mock()
        mock_run.return_value = MagicMock(returncode=0)
        worktree_stack.action_down()

        # Should have called compose down with the correct project
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert "down" in args
        assert expected_project in args

        # State file should be removed after successful down
        sf = tmp_path / ".worktree" / "stack.json"
        assert not sf.exists()


# ===========================================================================
# CLI action: test
# ===========================================================================


class TestActionTest:
    """The 'test' action runs pytest inside the compose test-runner."""

    @patch("worktree_stack.resolve_worktree_root")
    @patch("subprocess.run")
    def test_test_runs_compose_run(self, mock_run, mock_root, tmp_path):
        mock_root.return_value = str(tmp_path)
        mock_run.return_value = MagicMock(returncode=0)

        worktree_stack.action_test(pytest_args=["-q"])

        expected_project = _expected_project_name(str(tmp_path))

        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert "run" in args
        assert "--rm" in args
        assert "test-runner" in args
        assert "pytest" in args
        assert "-q" in args
        assert expected_project in args

    @patch("worktree_stack.resolve_worktree_root")
    @patch("subprocess.run")
    def test_test_propagates_exit_code(self, mock_run, mock_root, tmp_path):
        mock_root.return_value = str(tmp_path)
        mock_run.return_value = MagicMock(returncode=1)

        with pytest.raises(SystemExit) as exc_info:
            worktree_stack.action_test(pytest_args=[])
        assert exc_info.value.code == 1


# ===========================================================================
# CLI action: down
# ===========================================================================


class TestActionDown:
    """The 'down' action tears down the test stack and removes state."""

    @patch("worktree_stack.resolve_worktree_root")
    @patch("subprocess.run")
    def test_down_refuses_without_state(self, mock_run, mock_root, tmp_path):
        mock_root.return_value = str(tmp_path)
        # No state file — should refuse
        with pytest.raises(SystemExit):
            worktree_stack.action_down()
        mock_run.assert_not_called()

    @patch("worktree_stack.resolve_worktree_root")
    @patch("subprocess.run")
    def test_down_runs_compose_and_removes_state(self, mock_run, mock_root, tmp_path):
        mock_root.return_value = str(tmp_path)
        mock_run.return_value = MagicMock(returncode=0)

        project = _expected_project_name(str(tmp_path))
        worktree_stack.write_state(str(tmp_path), project)

        worktree_stack.action_down()

        # Should have run compose down
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert "down" in args
        assert "-v" in args
        assert "--remove-orphans" in args
        assert project in args

        # State file should be removed
        sf = tmp_path / ".worktree" / "stack.json"
        assert not sf.exists()


# ===========================================================================
# CLI main (argparse)
# ===========================================================================


class TestCLI:
    """CLI entry-point dispatches to the correct action."""

    @patch("worktree_stack.action_up")
    def test_cli_up(self, mock_up):
        worktree_stack.main(["up"])
        mock_up.assert_called_once()

    @patch("worktree_stack.action_down")
    def test_cli_down(self, mock_down):
        worktree_stack.main(["down"])
        mock_down.assert_called_once()

    @patch("worktree_stack.action_test")
    def test_cli_test_with_args(self, mock_test):
        worktree_stack.main(["test", "--", "-q", "--tb=short"])
        mock_test.assert_called_once_with(pytest_args=["-q", "--tb=short"])

    @patch("worktree_stack.action_test")
    def test_cli_test_no_args(self, mock_test):
        worktree_stack.main(["test"])
        mock_test.assert_called_once_with(pytest_args=[])
