"""Dagster Sensors - Event-Driven Triggers.

This module contains sensors that monitor external systems and trigger Dagster jobs.
Currently implements:
- manifest_sensor: Polls MinIO landing zone for new manifest files and triggers ingestion jobs.
"""

from .manifest_sensor import manifest_sensor

__all__ = ["manifest_sensor"]

