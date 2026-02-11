"""Run-scoped IO manager for raw_manifest_json handoff.

This prevents cross-run contamination when multiple runs materialize the same
asset concurrently by storing outputs under a run-specific path segment.
"""

from __future__ import annotations

import os
from pathlib import Path

from dagster import ConfigurableIOManager, InputContext, OutputContext


class RunScopedManifestIOManager(ConfigurableIOManager):
    """Filesystem IO manager that scopes payloads by Dagster run_id."""

    base_dir: str = "/opt/dagster/dagster_home/storage/artifacts/storage"

    def _get_path(self, context: OutputContext) -> Path:
        run_id = context.run_id or "unknown-run"
        asset_name = (
            "__".join(context.asset_key.path) if context.asset_key else "output"
        )
        return Path(self.base_dir) / asset_name / run_id / "result.pkl"

    def handle_output(self, context: OutputContext, obj):
        path = self._get_path(context)
        os.makedirs(path.parent, exist_ok=True)
        with open(path, "wb") as f:
            import pickle

            pickle.dump(obj, f)

    def load_input(self, context: InputContext):
        upstream = context.upstream_output
        if upstream is None:
            raise FileNotFoundError("No upstream output context available")

        path = self._get_path(upstream)
        with open(path, "rb") as f:
            import pickle

            return pickle.load(f)
