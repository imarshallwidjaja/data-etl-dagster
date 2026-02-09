#!/usr/bin/env python3
"""Agent-safe wrapper to manage worktree Docker test stacks.

Provides deterministic project naming and explicit compose commands so
that multiple worktree stacks can coexist without port or name conflicts.

CLI contract:
    uv run python scripts/worktree_stack.py up
    uv run python scripts/worktree_stack.py test -- <pytest args>
    uv run python scripts/worktree_stack.py down
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path


# =============================================================================
# Worktree root resolution
# =============================================================================


def resolve_worktree_root() -> str:
    """Return the absolute worktree root via ``git rev-parse --show-toplevel``.

    Deterministic regardless of CWD — always resolves the enclosing repo root.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        print("ERROR: not inside a git repository", file=sys.stderr)
        sys.exit(1)


# =============================================================================
# Deterministic project naming
# =============================================================================


def project_name(worktree_root: str) -> str:
    """Derive a compose project name: ``wt-<first8(sha256(abs_path))>``."""
    digest = hashlib.sha256(worktree_root.encode()).hexdigest()[:8]
    return f"wt-{digest}"


# =============================================================================
# State file helpers
# =============================================================================


def state_file_path(worktree_root: str) -> Path:
    """Return ``<worktree>/.worktree/stack.json``."""
    return Path(worktree_root) / ".worktree" / "stack.json"


def write_state(worktree_root: str, project: str) -> None:
    """Persist stack state so ``down`` knows what to tear down."""
    sf = state_file_path(worktree_root)
    sf.parent.mkdir(parents=True, exist_ok=True)
    sf.write_text(
        json.dumps({"project": project, "worktree_root": worktree_root}, indent=2)
    )


def read_state(worktree_root: str) -> dict | None:
    """Read stack state; return *None* if the file does not exist."""
    sf = state_file_path(worktree_root)
    if not sf.exists():
        return None
    return json.loads(sf.read_text())


def remove_state(worktree_root: str) -> None:
    """Delete the state file after a successful ``down``."""
    sf = state_file_path(worktree_root)
    if sf.exists():
        sf.unlink()


# =============================================================================
# Compose command builder
# =============================================================================

_COMPOSE_BASE = [
    "docker",
    "compose",
    "-f",
    "compose.yaml",
    "-f",
    "compose.test.yaml",
]


def compose_cmd(
    project: str,
    action: str,
    *,
    pytest_args: list[str] | None = None,
) -> list[str]:
    """Build the full ``docker compose`` command list.

    Supported *action* values: ``up``, ``down``, ``test``.
    """
    base = [*_COMPOSE_BASE, "-p", project]

    if action == "up":
        return [*base, "up", "-d", "--build"]

    if action == "down":
        return [*base, "down", "-v", "--remove-orphans"]

    if action == "test":
        cmd = [*base, "run", "--rm", "test-runner", "uv", "run", "pytest"]
        if pytest_args:
            cmd.extend(pytest_args)
        return cmd

    raise ValueError(f"Unknown action: {action}")


# =============================================================================
# CLI actions
# =============================================================================


def action_up() -> None:
    """Start the isolated test stack and record state."""
    root = resolve_worktree_root()
    proj = project_name(root)
    cmd = compose_cmd(proj, "up")

    print(f"[worktree_stack] project={proj}")
    print(f"[worktree_stack] {' '.join(cmd)}")

    # Write state *before* compose up so that ``down`` can always clean up
    # partial resources when ``up`` fails midway through.
    write_state(root, proj)

    result = subprocess.run(cmd, cwd=root)
    if result.returncode != 0:
        print(
            f"ERROR: compose up exited {result.returncode} "
            f"(state kept — run 'down' to clean up)",
            file=sys.stderr,
        )
        sys.exit(result.returncode)

    print("[worktree_stack] stack up — state written to .worktree/stack.json")


def action_test(*, pytest_args: list[str]) -> None:
    """Run pytest inside the compose test-runner container."""
    root = resolve_worktree_root()
    proj = project_name(root)
    cmd = compose_cmd(proj, "test", pytest_args=pytest_args)

    print(f"[worktree_stack] project={proj}")
    print(f"[worktree_stack] {' '.join(cmd)}")

    result = subprocess.run(cmd, cwd=root)
    if result.returncode != 0:
        sys.exit(result.returncode)


def action_down() -> None:
    """Tear down the test stack; refuse if no state file exists."""
    root = resolve_worktree_root()
    state = read_state(root)

    if state is None:
        print(
            "ERROR: no stack state found at .worktree/stack.json — "
            "nothing to tear down (was 'up' run first?)",
            file=sys.stderr,
        )
        sys.exit(1)

    proj = state["project"]
    cmd = compose_cmd(proj, "down")

    print(f"[worktree_stack] project={proj}")
    print(f"[worktree_stack] {' '.join(cmd)}")

    result = subprocess.run(cmd, cwd=root)
    if result.returncode != 0:
        print(f"ERROR: compose down exited {result.returncode}", file=sys.stderr)
        sys.exit(result.returncode)

    remove_state(root)
    print("[worktree_stack] stack down — state removed")


# =============================================================================
# CLI entry point
# =============================================================================


def main(argv: list[str] | None = None) -> None:
    """Parse CLI arguments and dispatch to the appropriate action."""
    parser = argparse.ArgumentParser(
        description="Manage isolated Docker test stacks for git worktrees.",
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("up", help="Start the test stack")
    subparsers.add_parser("down", help="Tear down the test stack")

    test_parser = subparsers.add_parser("test", help="Run pytest in the test-runner")
    test_parser.add_argument(
        "pytest_args",
        nargs="*",
        help="Extra pytest arguments (pass after '--')",
    )

    args = parser.parse_args(argv)

    if args.command == "up":
        action_up()
    elif args.command == "down":
        action_down()
    elif args.command == "test":
        # argparse consumes everything after "test"; if the user typed
        # ``test -- -q``, argparse strips the ``--`` automatically.
        action_test(pytest_args=args.pytest_args or [])
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
