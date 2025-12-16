#!/usr/bin/env python3
"""
Container stability check script for CI/CD pipelines.

Monitors container restart counts over a monitoring period (default 30 seconds)
to detect containers that are restarting repeatedly, which indicates configuration
or dependency issues.

Outputs failing container names to stdout (one per line) for log dumping.
"""

import json
import subprocess
import sys
import time
from typing import Dict, List


# Critical containers to monitor
CRITICAL_CONTAINERS = [
    "dagster-webserver",
    "dagster-daemon",
    "dagster-user-code",
    "mongodb",
    "postgis",
    "minio",
]


def get_container_restart_count(container_name: str) -> int:
    """
    Get the current restart count for a container.
    
    Args:
        container_name: Name of the container to check
        
    Returns:
        Restart count as integer, or -1 if container not found
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
    except (subprocess.CalledProcessError, ValueError):
        return -1


def check_container_stability(
    containers: List[str], monitor_duration: int = 30
) -> List[str]:
    """
    Monitor container restart counts over a period.
    
    Args:
        containers: List of container names to monitor
        monitor_duration: Duration to monitor in seconds (default 30)
        
    Returns:
        List of container names that restarted during monitoring
    """
    print(f"Monitoring container stability for {monitor_duration} seconds...")
    print(f"Checking containers: {', '.join(containers)}")
    
    # Get initial restart counts
    initial_counts: Dict[str, int] = {}
    for container in containers:
        count = get_container_restart_count(container)
        if count == -1:
            print(f"  WARNING: Container '{container}' not found, skipping")
            continue
        initial_counts[container] = count
        print(f"  {container}: restart_count={count}")
    
    if not initial_counts:
        print("ERROR: No containers found to monitor")
        return []
    
    # Wait for monitoring period
    print(f"\nWaiting {monitor_duration} seconds...")
    time.sleep(monitor_duration)
    
    # Get final restart counts and detect restarts
    print("\nChecking restart counts after monitoring period...")
    unstable_containers: List[str] = []
    
    for container in initial_counts:
        final_count = get_container_restart_count(container)
        if final_count == -1:
            print(f"  WARNING: Container '{container}' not found after monitoring")
            unstable_containers.append(container)
            continue
        
        initial = initial_counts[container]
        print(f"  {container}: restart_count={initial} -> {final_count}")
        
        if final_count > initial:
            print(f"    ✗ RESTARTED {final_count - initial} time(s) during monitoring")
            unstable_containers.append(container)
        else:
            print(f"    ✓ Stable")
    
    return unstable_containers


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Monitor container stability over a monitoring period"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Monitoring duration in seconds (default: 30)",
    )
    parser.add_argument(
        "--containers",
        nargs="+",
        default=CRITICAL_CONTAINERS,
        help="Container names to monitor (default: all critical containers)",
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Container Stability Check")
    print("=" * 60)
    
    unstable = check_container_stability(args.containers, args.duration)
    
    print("=" * 60)
    if unstable:
        print(f"FAILED: {len(unstable)} container(s) restarted during monitoring:")
        for container in unstable:
            print(f"  - {container}")
        print("\nUnstable containers (for log dumping):")
        # Output to stdout for workflow to capture
        for container in unstable:
            print(container)
        sys.exit(1)
    else:
        print("SUCCESS: All containers are stable")
        sys.exit(0)


if __name__ == "__main__":
    main()

