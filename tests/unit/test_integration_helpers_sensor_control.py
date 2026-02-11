from unittest.mock import Mock

import pytest

from tests.integration.helpers import start_sensor, stop_sensor


def test_start_sensor_uses_repository_selector_defaults() -> None:
    client = Mock()
    client.query.side_effect = [
        {
            "data": {
                "sensorOrError": {
                    "__typename": "Sensor",
                    "sensorState": {"id": "state-1", "status": "STOPPED"},
                }
            }
        },
        {
            "data": {
                "startSensor": {
                    "__typename": "Sensor",
                    "sensorState": {"id": "state-1", "status": "RUNNING"},
                }
            }
        },
    ]

    start_sensor(client, "tabular_sensor")

    assert client.query.call_count == 2

    first_query = client.query.call_args_list[0].args[0]
    first_variables = client.query.call_args_list[0].kwargs["variables"]
    assert "sensorOrError" in first_query
    assert first_variables == {
        "repositoryLocationName": "etl_pipelines",
        "repositoryName": "__repository__",
        "sensorName": "tabular_sensor",
    }

    second_query = client.query.call_args_list[1].args[0]
    second_variables = client.query.call_args_list[1].kwargs["variables"]
    assert "startSensor" in second_query
    assert second_variables == {
        "repositoryLocationName": "etl_pipelines",
        "repositoryName": "__repository__",
        "sensorName": "tabular_sensor",
    }


def test_start_sensor_skips_mutation_when_sensor_already_running() -> None:
    client = Mock()
    client.query.return_value = {
        "data": {
            "sensorOrError": {
                "__typename": "Sensor",
                "sensorState": {"id": "state-1", "status": "RUNNING"},
            }
        }
    }

    start_sensor(client, "tabular_sensor")

    assert client.query.call_count == 1


def test_stop_sensor_uses_sensor_state_id() -> None:
    client = Mock()
    client.query.side_effect = [
        {
            "data": {
                "sensorOrError": {
                    "__typename": "Sensor",
                    "sensorState": {"id": "state-1", "status": "RUNNING"},
                }
            }
        },
        {
            "data": {
                "stopSensor": {
                    "__typename": "StopSensorMutationResult",
                    "instigationState": {"id": "state-1", "status": "STOPPED"},
                }
            }
        },
    ]

    stop_sensor(client, "tabular_sensor")

    assert client.query.call_count == 2

    first_query = client.query.call_args_list[0].args[0]
    first_variables = client.query.call_args_list[0].kwargs["variables"]
    assert "sensorOrError" in first_query
    assert first_variables == {
        "repositoryLocationName": "etl_pipelines",
        "repositoryName": "__repository__",
        "sensorName": "tabular_sensor",
    }

    second_query = client.query.call_args_list[1].args[0]
    second_variables = client.query.call_args_list[1].kwargs["variables"]
    assert "stopSensor" in second_query
    assert second_variables == {"id": "state-1"}


def test_stop_sensor_skips_mutation_when_sensor_already_stopped() -> None:
    client = Mock()
    client.query.return_value = {
        "data": {
            "sensorOrError": {
                "__typename": "Sensor",
                "sensorState": {"id": "state-1", "status": "STOPPED"},
            }
        }
    }

    stop_sensor(client, "tabular_sensor")

    assert client.query.call_count == 1


def test_start_sensor_raises_on_graphql_errors() -> None:
    client = Mock()
    client.query.return_value = {"errors": [{"message": "boom"}]}

    with pytest.raises(RuntimeError, match="Failed to query sensor state"):
        start_sensor(client, "tabular_sensor")


def test_stop_sensor_raises_on_stop_failure_union() -> None:
    client = Mock()
    client.query.side_effect = [
        {
            "data": {
                "sensorOrError": {
                    "__typename": "Sensor",
                    "sensorState": {"id": "state-1", "status": "RUNNING"},
                }
            }
        },
        {
            "data": {
                "stopSensor": {
                    "__typename": "PythonError",
                    "message": "permission denied",
                }
            }
        },
    ]

    with pytest.raises(RuntimeError, match="Failed to stop sensor 'tabular_sensor'"):
        stop_sensor(client, "tabular_sensor")
