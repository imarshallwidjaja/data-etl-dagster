"""Dagster Definitions - Repository Configuration."""

from dagster import Definitions

# Import assets, jobs, resources, schedules, and sensors as they are created
# Example:
# from .ops import my_asset
# from .jobs import my_job
# from .resources import my_resource
# from .sensors import my_sensor

defs = Definitions(
    assets=[],
    jobs=[],
    resources={},
    schedules=[],
    sensors=[],
)

