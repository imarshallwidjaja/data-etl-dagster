from dataclasses import dataclass, field
from typing import Any


@dataclass
class WorkflowStep:
    """A single step in a workflow wizard."""

    id: str
    title: str
    description: str
    fields: list[str]  # Field names to show in this step
    template: str | None = None


@dataclass
class Workflow:
    """A complete workflow definition."""

    id: str
    name: str
    description: str
    icon: str
    intent: str  # Maps to manifest intent
    asset_type: str  # spatial, tabular, joined
    steps: list[WorkflowStep]
    defaults: dict[str, Any] = field(default_factory=dict)


WORKFLOWS = {
    "ingest-vector": Workflow(
        id="ingest-vector",
        name="Ingest Vector Data",
        description="Import GeoJSON, Shapefile, or GeoPackage into the data lake",
        icon="map",
        intent="ingest_vector",
        asset_type="spatial",
        steps=[
            WorkflowStep(
                id="metadata",
                title="Dataset Information",
                description="Describe your dataset",
                fields=[
                    "title",
                    "description",
                    "keywords",
                    "source",
                    "license",
                    "attribution",
                ],
            ),
            WorkflowStep(
                id="files",
                title="Select Files",
                description="Choose files from the landing zone",
                fields=["file_path", "file_format", "file_type"],
            ),
            WorkflowStep(
                id="options",
                title="Processing Options",
                description="Configure processing settings",
                fields=["dataset_id", "project", "tags"],
            ),
        ],
    ),
    "ingest-csv": Workflow(
        id="ingest-csv",
        name="Ingest CSV Data",
        description="Import tabular CSV data for joining with spatial datasets",
        icon="table",
        intent="ingest_tabular",
        asset_type="tabular",
        steps=[
            WorkflowStep(
                id="metadata",
                title="Dataset Information",
                description="Describe your dataset",
                fields=[
                    "title",
                    "description",
                    "keywords",
                    "source",
                    "license",
                    "attribution",
                ],
            ),
            WorkflowStep(
                id="file",
                title="Select CSV File",
                description="Choose a single CSV file",
                fields=["file_path"],
            ),
            WorkflowStep(
                id="options",
                title="Processing Options",
                description="Configure dataset identity",
                fields=["dataset_id", "project"],
            ),
        ],
        defaults={
            "file_format": "CSV",
            "file_type": "tabular",
        },
    ),
    "join-datasets": Workflow(
        id="join-datasets",
        name="Join Spatial + Tabular",
        description="Combine a spatial dataset with tabular attributes",
        icon="link",
        intent="join_datasets",
        asset_type="joined",
        steps=[
            WorkflowStep(
                id="metadata",
                title="Output Dataset Information",
                description="Describe the joined output",
                fields=[
                    "title",
                    "description",
                    "keywords",
                    "source",
                    "license",
                    "attribution",
                ],
            ),
            WorkflowStep(
                id="spatial",
                title="Select Spatial Dataset",
                description="Choose the geometry source",
                fields=["spatial_dataset_id", "spatial_version"],
            ),
            WorkflowStep(
                id="tabular",
                title="Select Tabular Dataset",
                description="Choose the attribute source",
                fields=["tabular_dataset_id", "tabular_version"],
            ),
            WorkflowStep(
                id="join",
                title="Configure Join",
                description="Specify how to match records",
                fields=["left_key", "right_key", "how"],
            ),
            WorkflowStep(
                id="options",
                title="Processing Options",
                description="Configure dataset identity",
                fields=["dataset_id", "project"],
            ),
        ],
    ),
    "ingest-buildings": Workflow(
        id="ingest-buildings",
        name="Ingest Building Footprints",
        description="Import building footprints with heavy geometry simplification",
        icon="building",
        intent="ingest_building_footprints",
        asset_type="spatial",
        steps=[
            WorkflowStep(
                id="metadata",
                title="Dataset Information",
                description="Describe your building footprint dataset",
                fields=[
                    "title",
                    "description",
                    "keywords",
                    "source",
                    "license",
                    "attribution",
                ],
            ),
            WorkflowStep(
                id="file",
                title="Select File",
                description="Choose a building footprint file",
                fields=["file_path", "file_format"],
            ),
        ],
        defaults={
            "file_type": "vector",
            "keywords": ["buildings", "footprints"],
        },
    ),
}


def get_workflow(workflow_id: str) -> Workflow | None:
    """Get workflow by ID."""
    return WORKFLOWS.get(workflow_id)


def list_workflows() -> list[Workflow]:
    """List all available workflows."""
    return list(WORKFLOWS.values())
