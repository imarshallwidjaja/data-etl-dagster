# =============================================================================
# Assets Router Unit Tests
# =============================================================================
# Tests for enhanced asset display and lineage graph endpoints.
# =============================================================================

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from starlette.testclient import TestClient


@pytest.fixture
def mock_mongodb_service():
    with patch("app.routers.assets.get_mongodb_service") as mock_factory:
        mock_service = MagicMock()
        mock_factory.return_value = mock_service
        yield mock_service


@pytest.fixture
def auth_override():
    from app.auth.dependencies import get_current_user
    from app.auth.providers import AuthenticatedUser
    from app.main import app

    app.dependency_overrides[get_current_user] = lambda: AuthenticatedUser(
        username="testuser", display_name="Test User"
    )
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def client(auth_override):
    from app.main import app

    return TestClient(app)


def build_asset(kind: str = "joined") -> dict:
    metadata = {
        "title": "Test Joined Asset",
        "description": "Has **markdown** and <script>alert('xss')</script>",
        "keywords": ["test", "joined"],
        "source": "test",
        "license": "CC-BY-4.0",
        "attribution": "Test",
        "column_schema": {
            "id": {
                "title": "ID",
                "description": "",
                "type_name": "INTEGER",
                "logical_type": "int64",
                "nullable": False,
            },
            "geom": {
                "title": "Geometry",
                "description": "Feature geometry",
                "type_name": "GEOMETRY",
                "logical_type": "binary",
                "nullable": False,
            },
        },
    }

    if kind in {"spatial", "joined"}:
        metadata["geometry_type"] = "POLYGON"
        metadata["crs"] = "EPSG:9999"
        metadata["bounds"] = {
            "minx": -122.0,
            "miny": 37.0,
            "maxx": -121.0,
            "maxy": 38.0,
        }

    return {
        "_id": "507f1f77bcf86cd799439011",
        "dataset_id": "test_joined_asset",
        "version": 1,
        "kind": kind,
        "s3_key": "data-lake/test/asset_v1.geoparquet",
        "content_hash": "sha256:" + ("a" * 64),
        "created_at": datetime(2024, 1, 1, 12, 0, 0),
        "crs": "EPSG:4326" if kind in {"spatial", "joined"} else None,
        "bounds": {
            "minx": -122.5,
            "miny": 37.7,
            "maxx": -122.3,
            "maxy": 37.9,
        }
        if kind in {"spatial", "joined"}
        else None,
        "metadata": metadata,
    }


def build_versions() -> list[dict]:
    return [
        {
            "version": 1,
            "s3_key": "data-lake/test/asset_v1.geoparquet",
            "created_at": datetime(2024, 1, 1, 12, 0, 0),
            "content_hash": "sha256:" + ("a" * 64),
        }
    ]


def test_asset_detail_returns_html(client, mock_mongodb_service):
    mock_mongodb_service.get_asset.return_value = build_asset()
    mock_mongodb_service.get_asset_versions.return_value = build_versions()

    response = client.get("/assets/test_joined_asset")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_asset_detail_displays_title(client, mock_mongodb_service):
    mock_mongodb_service.get_asset.return_value = build_asset()
    mock_mongodb_service.get_asset_versions.return_value = build_versions()

    response = client.get("/assets/test_joined_asset")

    assert "Test Joined Asset" in response.text


def test_asset_detail_displays_keywords(client, mock_mongodb_service):
    mock_mongodb_service.get_asset.return_value = build_asset()
    mock_mongodb_service.get_asset_versions.return_value = build_versions()

    response = client.get("/assets/test_joined_asset")

    assert "<kbd>test</kbd>" in response.text
    assert "<kbd>joined</kbd>" in response.text


def test_asset_detail_crs_from_top_level(client, mock_mongodb_service):
    mock_mongodb_service.get_asset.return_value = build_asset()
    mock_mongodb_service.get_asset_versions.return_value = build_versions()

    response = client.get("/assets/test_joined_asset")

    assert "EPSG:4326" in response.text
    assert "EPSG:9999" not in response.text


def test_asset_detail_bounds_from_top_level(client, mock_mongodb_service):
    mock_mongodb_service.get_asset.return_value = build_asset()
    mock_mongodb_service.get_asset_versions.return_value = build_versions()

    response = client.get("/assets/test_joined_asset")

    assert "-122.5" in response.text
    assert "37.7" in response.text
    assert "-122.3" in response.text
    assert "37.9" in response.text


def test_asset_detail_markdown_sanitized(client, mock_mongodb_service):
    mock_mongodb_service.get_asset.return_value = build_asset()
    mock_mongodb_service.get_asset_versions.return_value = build_versions()

    response = client.get("/assets/test_joined_asset")

    assert "<strong>markdown</strong>" in response.text
    assert "<script>" not in response.text


def test_asset_detail_column_schema_table(client, mock_mongodb_service):
    mock_mongodb_service.get_asset.return_value = build_asset()
    mock_mongodb_service.get_asset_versions.return_value = build_versions()

    response = client.get("/assets/test_joined_asset")

    assert "Column Schema" in response.text
    assert "Logical Type" in response.text
    assert "nullable" in response.text.lower()
    assert "Geometry" in response.text


def test_asset_detail_tabular_hides_spatial(client, mock_mongodb_service):
    mock_mongodb_service.get_asset.return_value = build_asset(kind="tabular")
    mock_mongodb_service.get_asset_versions.return_value = build_versions()

    response = client.get("/assets/test_joined_asset")

    assert "Spatial Information" not in response.text


def test_lineage_graph_returns_json(client, mock_mongodb_service):
    asset = build_asset()
    mock_mongodb_service.get_asset.return_value = asset
    mock_mongodb_service.get_parent_assets.return_value = []
    mock_mongodb_service.get_child_assets.return_value = []

    response = client.get("/assets/test_joined_asset/v1/lineage/graph")

    assert response.status_code == 200
    payload = response.json()
    assert "elements" in payload
    assert "nodes" in payload["elements"]
    assert "edges" in payload["elements"]


def test_lineage_graph_focal_is_root(client, mock_mongodb_service):
    asset = build_asset()
    mock_mongodb_service.get_asset.return_value = asset
    mock_mongodb_service.get_parent_assets.return_value = []
    mock_mongodb_service.get_child_assets.return_value = []

    response = client.get("/assets/test_joined_asset/v1/lineage/graph")

    nodes = response.json()["elements"]["nodes"]
    root_nodes = [node for node in nodes if node["data"].get("is_root")]
    assert len(root_nodes) == 1


def test_lineage_graph_includes_parents(client, mock_mongodb_service):
    asset = build_asset()
    parent = {
        "_id": "parent-oid",
        "dataset_id": "parent_spatial",
        "version": 1,
        "kind": "spatial",
        "metadata": {"title": "Parent Spatial Asset"},
        "_lineage_transformation": "join_operation",
    }

    mock_mongodb_service.get_asset.return_value = asset
    mock_mongodb_service.get_parent_assets.return_value = [parent]
    mock_mongodb_service.get_child_assets.return_value = []

    response = client.get("/assets/test_joined_asset/v1/lineage/graph")

    node_ids = {node["data"]["id"] for node in response.json()["elements"]["nodes"]}
    assert "parent-oid" in node_ids


def test_lineage_graph_includes_children(client, mock_mongodb_service):
    asset = build_asset()
    child = {
        "_id": "child-oid",
        "dataset_id": "child_spatial",
        "version": 1,
        "kind": "spatial",
        "metadata": {"title": "Child Spatial Asset"},
        "_lineage_transformation": "derive_operation",
    }

    mock_mongodb_service.get_asset.return_value = asset
    mock_mongodb_service.get_parent_assets.return_value = []
    mock_mongodb_service.get_child_assets.return_value = [child]

    response = client.get("/assets/test_joined_asset/v1/lineage/graph")

    node_ids = {node["data"]["id"] for node in response.json()["elements"]["nodes"]}
    assert "child-oid" in node_ids


def test_lineage_graph_edge_direction(client, mock_mongodb_service):
    asset = build_asset()
    parent = {
        "_id": "parent-oid",
        "dataset_id": "parent_spatial",
        "version": 1,
        "kind": "spatial",
        "metadata": {"title": "Parent Spatial Asset"},
        "_lineage_transformation": "join_operation",
    }
    child = {
        "_id": "child-oid",
        "dataset_id": "child_spatial",
        "version": 1,
        "kind": "spatial",
        "metadata": {"title": "Child Spatial Asset"},
        "_lineage_transformation": "derive_operation",
    }

    mock_mongodb_service.get_asset.return_value = asset
    mock_mongodb_service.get_parent_assets.return_value = [parent]
    mock_mongodb_service.get_child_assets.return_value = [child]

    response = client.get("/assets/test_joined_asset/v1/lineage/graph")

    edges = response.json()["elements"]["edges"]
    edge_pairs = {(edge["data"]["source"], edge["data"]["target"]) for edge in edges}
    assert ("parent-oid", asset["_id"]) in edge_pairs
    assert (asset["_id"], "child-oid") in edge_pairs
