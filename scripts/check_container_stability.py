#!/usr/bin/env python3
"""
Container stability check script for CI/CD pipelines.

Monitors container restart counts over a monitoring period (default 30 seconds)
to detect containers that are restarting repeatedly, indicating configuration
or runtime issues.

This script should be run after services are started and have had time to
initialize, but before running integration tests.
"""

import os
import subprocess
import sys
import time
from typing import Dict, List, Optional


# Compose service names to monitor.
# Container names are derived as: <project>-<service>-1
CRITICAL_SERVICES = [
    "dagster-webserver",
    "dagster-daemon",
    "user-code",
    "mongodb",
    "postgis",
    "minio",
]


def default_compose_project_name() -> str:
    """Derive Docker Compose's default project name from current directory."""
    cwd = os.getcwd().rstrip(os.sep)
    project = os.path.basename(cwd)
    return project or "default"


def resolve_compose_project_name() -> str:
    """Resolve compose project name from env or directory-derived default."""
    return (
        os.getenv("COMPOSE_PROJECT_NAME", "").strip() or default_compose_project_name()
    )


def resolve_container_names() -> List[str]:
    """Resolve container names for the current environment.

    If CHECK_CONTAINERS is set, uses those names directly.
    Otherwise derives ``<project>-<service>-1`` names from compose service
    definitions using ``COMPOSE_PROJECT_NAME`` or Compose's default project
    name (current directory basename).
    """
    containers_env = os.getenv("CHECK_CONTAINERS", "").strip()
    if containers_env:
        return [c.strip() for c in containers_env.split(",")]

    project = resolve_compose_project_name()
    return [f"{project}-{svc}-1" for svc in CRITICAL_SERVICES]


def get_restart_count(container_name: str) -> Optional[int]:
    """
    Get the restart count for a container using docker inspect.

    Args:
        container_name: Name of the container to check

    Returns:
        Restart count as integer, or None if container not found
    """
    try:
        result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.RestartCount}}",
                container_name,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return int(result.stdout.strip())
    except (subprocess.CalledProcessError, ValueError, FileNotFoundError):
        return None


def check_container_stability(
    container_names: List[str],
    monitor_duration: int = 30,
) -> Dict[str, bool]:
    """
    Monitor container restart counts over a period of time.

    Args:
        container_names: List of container names to monitor
        monitor_duration: Duration to monitor in seconds (default 30)

    Returns:
        Dictionary mapping container names to stability status (True = stable, False = unstable)
    """
    print("=" * 60)
    print("Container Stability Check")
    print("=" * 60)
    print(f"Monitoring containers for {monitor_duration} seconds...")
    print()

    # Get initial restart counts
    initial_counts: Dict[str, Optional[int]] = {}
    for container in container_names:
        count = get_restart_count(container)
        initial_counts[container] = count
        if count is None:
            print(f"⚠ Warning: Container '{container}' not found")
        else:
            print(f"  {container}: initial restart count = {count}")

    print()
    print(f"Waiting {monitor_duration} seconds...")
    time.sleep(monitor_duration)
    print()

    # Get final restart counts
    final_counts: Dict[str, Optional[int]] = {}
    stability_status: Dict[str, bool] = {}

    for container in container_names:
        final_count = get_restart_count(container)
        final_counts[container] = final_count
        initial_count = initial_counts.get(container)

        if initial_count is None or final_count is None:
            # Container not found - mark as unstable
            stability_status[container] = False
            print(f"✗ {container}: container not found or inaccessible")
        elif final_count > initial_count:
            # Container restarted during monitoring period
            stability_status[container] = False
            restarts = final_count - initial_count
            print(
                f"✗ {container}: UNSTABLE (restarted {restarts} time(s) during monitoring)"
            )
        else:
            # Container is stable
            stability_status[container] = True
            print(f"✓ {container}: stable (restart count = {final_count})")

    return stability_status


def main():
    """
    Main entry point.

    Containers can be configured via CHECK_CONTAINERS environment variable.
    Comma-separated list of container names to check.

    Default: <project>-dagster-webserver-1, <project>-dagster-daemon-1,
    <project>-user-code-1, <project>-mongodb-1, <project>-postgis-1,
    <project>-minio-1
    Example: CHECK_CONTAINERS=wt-smoke-minio-1,wt-smoke-mongodb-1,wt-smoke-postgis-1
    """
    # Get monitoring duration from environment (default 30 seconds)
    monitor_duration = int(os.getenv("CONTAINER_STABILITY_MONITOR_DURATION", "30"))

    # Resolve container names (project-scoped, env override, or defaults)
    containers = resolve_container_names()
    project = resolve_compose_project_name()
    print(f"Using compose project: {project}")
    print(f"Checking containers: {', '.join(containers)}")

    # Check stability of containers
    stability_status = check_container_stability(
        containers,
        monitor_duration=monitor_duration,
    )

    # Collect unstable containers
    unstable_containers = [
        container for container, is_stable in stability_status.items() if not is_stable
    ]

    print()
    print("=" * 60)
    if unstable_containers:
        print(f"FAILED: {len(unstable_containers)} container(s) are unstable:")
        for container in unstable_containers:
            print(f"  - {container}")
        print()
        print("Unstable containers (one per line, for log dumping):")
        for container in unstable_containers:
            print(container)
        sys.exit(1)
    else:
        print("SUCCESS: All containers are stable")
        sys.exit(0)


if __name__ == "__main__":
    main()
