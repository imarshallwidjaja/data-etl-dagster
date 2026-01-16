"""Unit tests for run_status_sensor lifecycle logging to activity_logs."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from services.dagster.etl_pipelines.sensors.run_status_sensor import (
    ACTIVITY_LOGS_COLLECTION,
    TRACKED_JOBS,
    _build_activity_details,
    _get_batch_id_from_run,
    _handle_run_failure,
    _handle_run_started,
    _handle_run_success,
    _write_activity_log,
)


class TestGetBatchIdFromRun:
    def test_returns_batch_id_when_present(self):
        tags = {"batch_id": "batch_123", "other": "value"}
        assert _get_batch_id_from_run(tags) == "batch_123"

    def test_returns_none_when_missing(self):
        tags = {"other": "value"}
        assert _get_batch_id_from_run(tags) is None

    def test_returns_none_for_empty_tags(self):
        assert _get_batch_id_from_run({}) is None


class TestBuildActivityDetails:
    def test_includes_job_name(self):
        details = _build_activity_details({}, "spatial_asset_job")
        assert details["job_name"] == "spatial_asset_job"

    def test_includes_batch_id_when_present(self):
        tags = {"batch_id": "batch_abc"}
        details = _build_activity_details(tags, "spatial_asset_job")
        assert details["batch_id"] == "batch_abc"

    def test_includes_operator_when_present(self):
        tags = {"operator": "admin_user"}
        details = _build_activity_details(tags, "tabular_asset_job")
        assert details["operator"] == "admin_user"

    def test_includes_source_when_present(self):
        tags = {"source": "webapp"}
        details = _build_activity_details(tags, "join_asset_job")
        assert details["source"] == "webapp"

    def test_includes_error_message_when_provided(self):
        details = _build_activity_details({}, "ingest_job", "Run failed!")
        assert details["error_message"] == "Run failed!"

    def test_excludes_missing_optional_fields(self):
        details = _build_activity_details({}, "spatial_asset_job")
        assert "batch_id" not in details
        assert "operator" not in details
        assert "source" not in details
        assert "error_message" not in details

    def test_full_tags(self):
        tags = {
            "batch_id": "batch_xyz",
            "operator": "test_user",
            "source": "cli",
        }
        details = _build_activity_details(tags, "spatial_asset_job", "Error occurred")
        assert details == {
            "job_name": "spatial_asset_job",
            "batch_id": "batch_xyz",
            "operator": "test_user",
            "source": "cli",
            "error_message": "Error occurred",
        }


class TestWriteActivityLog:
    @pytest.fixture
    def mock_db(self):
        db = MagicMock()
        db[ACTIVITY_LOGS_COLLECTION] = MagicMock()
        return db

    def test_uses_upsert_with_set_on_insert(self, mock_db):
        timestamp = datetime.now(timezone.utc)
        _write_activity_log(
            db=mock_db,
            action="run_started",
            resource_id="run_123",
            user="test_user",
            timestamp=timestamp,
            details={"job_name": "spatial_asset_job"},
        )

        mock_db[ACTIVITY_LOGS_COLLECTION].update_one.assert_called_once()
        call_args = mock_db[ACTIVITY_LOGS_COLLECTION].update_one.call_args

        filter_doc = call_args[0][0]
        assert filter_doc["action"] == "run_started"
        assert filter_doc["resource_type"] == "run"
        assert filter_doc["resource_id"] == "run_123"

        update_doc = call_args[0][1]
        assert "$setOnInsert" in update_doc
        set_on_insert = update_doc["$setOnInsert"]
        assert set_on_insert["timestamp"] == timestamp
        assert set_on_insert["user"] == "test_user"
        assert set_on_insert["action"] == "run_started"
        assert set_on_insert["resource_type"] == "run"
        assert set_on_insert["resource_id"] == "run_123"
        assert set_on_insert["details"] == {"job_name": "spatial_asset_job"}

        assert call_args[1]["upsert"] is True

    def test_idempotent_key_is_action_resource_type_resource_id(self, mock_db):
        _write_activity_log(
            db=mock_db,
            action="run_success",
            resource_id="run_456",
            user="admin",
            timestamp=datetime.now(timezone.utc),
            details={},
        )

        call_args = mock_db[ACTIVITY_LOGS_COLLECTION].update_one.call_args
        filter_doc = call_args[0][0]
        assert set(filter_doc.keys()) == {"action", "resource_type", "resource_id"}


class TestTrackedJobs:
    def test_tracked_jobs_includes_expected_jobs(self):
        assert "spatial_asset_job" in TRACKED_JOBS
        assert "tabular_asset_job" in TRACKED_JOBS
        assert "join_asset_job" in TRACKED_JOBS
        assert "ingest_job" in TRACKED_JOBS

    def test_tracked_jobs_is_frozen(self):
        assert isinstance(TRACKED_JOBS, frozenset)


class TestRunStatusSensorIntegration:
    @pytest.fixture
    def mock_run(self):
        run = MagicMock()
        run.run_id = "dagster_run_123"
        run.job_name = "spatial_asset_job"
        run.tags = {
            "batch_id": "batch_test",
            "operator": "test_operator",
            "source": "webapp",
        }
        return run

    @pytest.fixture
    def mock_run_stats(self):
        stats = MagicMock()
        stats.start_time = 1704067200.0
        stats.end_time = 1704067260.0
        return stats

    @patch(
        "services.dagster.etl_pipelines.sensors.run_status_sensor._get_mongodb_client"
    )
    def test_started_sensor_logs_run_started(
        self, mock_get_client, mock_run, mock_run_stats
    ):
        mock_activity_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(return_value=mock_activity_collection)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_get_client.return_value = (mock_client, "spatial_etl")

        mock_context = MagicMock()
        mock_context.dagster_run = mock_run
        mock_context.instance.get_run_stats.return_value = mock_run_stats
        mock_context.log = MagicMock()

        _handle_run_started(mock_context)

        mock_activity_collection.update_one.assert_called_once()
        call_args = mock_activity_collection.update_one.call_args
        filter_doc = call_args[0][0]
        assert filter_doc["action"] == "run_started"
        assert filter_doc["resource_id"] == "dagster_run_123"

        mock_client.close.assert_called_once()

    @patch(
        "services.dagster.etl_pipelines.sensors.run_status_sensor._get_mongodb_client"
    )
    def test_success_sensor_logs_run_success(
        self, mock_get_client, mock_run, mock_run_stats
    ):
        mock_activity_collection = MagicMock()
        mock_runs_collection = MagicMock()

        def getitem_side_effect(key: str) -> MagicMock:
            if key == ACTIVITY_LOGS_COLLECTION:
                return mock_activity_collection
            return mock_runs_collection

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(side_effect=getitem_side_effect)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_get_client.return_value = (mock_client, "spatial_etl")

        mock_context = MagicMock()
        mock_context.dagster_run = mock_run
        mock_context.instance.get_run_stats.return_value = mock_run_stats
        mock_context.log = MagicMock()

        _handle_run_success(mock_context)

        mock_activity_collection.update_one.assert_called_once()
        call_args = mock_activity_collection.update_one.call_args
        filter_doc = call_args[0][0]
        assert filter_doc["action"] == "run_success"
        assert filter_doc["resource_id"] == "dagster_run_123"

        mock_client.close.assert_called_once()

    @patch(
        "services.dagster.etl_pipelines.sensors.run_status_sensor._get_mongodb_client"
    )
    def test_failure_sensor_logs_run_failure(
        self, mock_get_client, mock_run, mock_run_stats
    ):
        from dagster import DagsterRunStatus

        mock_run.status = DagsterRunStatus.FAILURE

        mock_activity_collection = MagicMock()
        mock_runs_collection = MagicMock()

        def getitem_side_effect(key: str) -> MagicMock:
            if key == ACTIVITY_LOGS_COLLECTION:
                return mock_activity_collection
            return mock_runs_collection

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(side_effect=getitem_side_effect)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_get_client.return_value = (mock_client, "spatial_etl")

        mock_context = MagicMock()
        mock_context.dagster_run = mock_run
        mock_context.instance.get_run_stats.return_value = mock_run_stats
        mock_context.log = MagicMock()

        _handle_run_failure(mock_context)

        mock_activity_collection.update_one.assert_called_once()
        call_args = mock_activity_collection.update_one.call_args
        filter_doc = call_args[0][0]
        assert filter_doc["action"] == "run_failure"
        assert filter_doc["resource_id"] == "dagster_run_123"

        update_doc = call_args[0][1]
        assert "error_message" in update_doc["$setOnInsert"]["details"]

        mock_client.close.assert_called_once()

    @patch(
        "services.dagster.etl_pipelines.sensors.run_status_sensor._get_mongodb_client"
    )
    def test_canceled_sensor_logs_run_canceled(
        self, mock_get_client, mock_run, mock_run_stats
    ):
        from dagster import DagsterRunStatus

        mock_run.status = DagsterRunStatus.CANCELED

        mock_activity_collection = MagicMock()
        mock_runs_collection = MagicMock()

        def getitem_side_effect(key: str) -> MagicMock:
            if key == ACTIVITY_LOGS_COLLECTION:
                return mock_activity_collection
            return mock_runs_collection

        mock_db = MagicMock()
        mock_db.__getitem__ = MagicMock(side_effect=getitem_side_effect)
        mock_client = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_get_client.return_value = (mock_client, "spatial_etl")

        mock_context = MagicMock()
        mock_context.dagster_run = mock_run
        mock_context.instance.get_run_stats.return_value = mock_run_stats
        mock_context.log = MagicMock()

        _handle_run_failure(mock_context)

        mock_activity_collection.update_one.assert_called_once()
        call_args = mock_activity_collection.update_one.call_args
        filter_doc = call_args[0][0]
        assert filter_doc["action"] == "run_canceled"
        assert filter_doc["resource_id"] == "dagster_run_123"

        mock_client.close.assert_called_once()

    @patch(
        "services.dagster.etl_pipelines.sensors.run_status_sensor._get_mongodb_client"
    )
    def test_untracked_job_skips_logging(self, mock_get_client):
        mock_run = MagicMock()
        mock_run.job_name = "some_other_job"
        mock_run.run_id = "run_xyz"

        mock_context = MagicMock()
        mock_context.dagster_run = mock_run
        mock_context.log = MagicMock()

        _handle_run_started(mock_context)

        mock_get_client.assert_not_called()
