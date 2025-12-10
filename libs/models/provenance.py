# =============================================================================
# Provenance Models Module (Placeholder)
# =============================================================================
# This module is a placeholder for future Run and Lineage models.
# =============================================================================

"""
Provenance Models (Run, Lineage)

Deferred: These models will be implemented once the Dagster run/lineage
schema integration approach is determined.

See MongoDB collections 'runs' and 'lineage' in 
services/mongodb/init/01-init-db.js for the target schema.

## Planned Models

### Run Model
Tracks ETL run metadata:
- dagster_run_id: Dagster run ID
- manifest_id: Reference to manifest
- status: running, success, failure
- assets_created: List of asset IDs created
- started_at: Run start time
- completed_at: Run completion time
- error_message: Error message if failed

### Lineage Model
Tracks asset transformation graph:
- source_asset_id: Source asset reference
- target_asset_id: Target asset reference
- dagster_run_id: Run that created this relationship
- transformation: Type of transformation applied
- parameters: Transformation parameters
- created_at: Lineage record creation time
"""

__all__: list[str] = []

