# =============================================================================
# Webapp Assets Integration Tests
# =============================================================================
# Tests asset browsing endpoints against running container with MongoDB.
# =============================================================================

from datetime import datetime

import pytest
import requests
from bson import ObjectId

WEBAPP_URL = "http://localhost:8080"
AUTH = ("admin", "admin")


@pytest.fixture
def seeded_asset(clean_mongodb):
    parent_oid = ObjectId()
    child_oid = ObjectId()
    grandchild_oid = ObjectId()
    run_oid = ObjectId()
    run_id_str = str(ObjectId())

    clean_mongodb["assets"].insert_one(
        {
            "_id": parent_oid,
            "s3_key": "data-lake/test/parent_v1.geoparquet",
            "dataset_id": "parent_spatial",
            "version": 1,
            "content_hash": "sha256:" + ("a" * 64),
            "run_id": run_id_str,
            "kind": "spatial",
            "format": "geoparquet",
            "crs": "EPSG:4326",
            "bounds": {"minx": -122.5, "miny": 37.7, "maxx": -122.3, "maxy": 37.9},
            "metadata": {
                "title": "Parent Spatial Asset",
                "description": "Test parent",
                "keywords": ["test"],
                "source": "test",
                "license": "MIT",
                "attribution": "Test",
                "geometry_type": "POLYGON",
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
            },
            "created_at": datetime.utcnow(),
        }
    )

    clean_mongodb["assets"].insert_one(
        {
            "_id": child_oid,
            "s3_key": "data-lake/test/child_v1.geoparquet",
            "dataset_id": "test_joined_asset",
            "version": 1,
            "content_hash": "sha256:" + ("b" * 64),
            "run_id": run_id_str,
            "kind": "joined",
            "format": "geoparquet",
            "crs": "EPSG:4326",
            "bounds": {"minx": -122.5, "miny": 37.7, "maxx": -122.3, "maxy": 37.9},
            "metadata": {
                "title": "Test Joined Asset",
                "description": "Has **markdown** and <script>alert('xss')</script>",
                "keywords": ["test", "joined"],
                "source": "test",
                "license": "CC-BY-4.0",
                "attribution": "Test",
                "geometry_type": "POLYGON",
                "column_schema": {
                    "id": {
                        "title": "ID",
                        "description": "",
                        "type_name": "INTEGER",
                        "logical_type": "int64",
                        "nullable": False,
                    },
                    "name": {
                        "title": "Name",
                        "description": "Feature name",
                        "type_name": "STRING",
                        "logical_type": "string",
                        "nullable": True,
                    },
                    "geom": {
                        "title": "Geometry",
                        "description": "",
                        "type_name": "GEOMETRY",
                        "logical_type": "binary",
                        "nullable": False,
                    },
                },
            },
            "created_at": datetime.utcnow(),
        }
    )

    clean_mongodb["assets"].insert_one(
        {
            "_id": grandchild_oid,
            "s3_key": "data-lake/test/grandchild_v1.geoparquet",
            "dataset_id": "grandchild_derived",
            "version": 1,
            "content_hash": "sha256:" + ("c" * 64),
            "run_id": run_id_str,
            "kind": "spatial",
            "format": "geoparquet",
            "crs": "EPSG:4326",
            "bounds": {"minx": -122.5, "miny": 37.7, "maxx": -122.3, "maxy": 37.9},
            "metadata": {
                "title": "Grandchild Derived Asset",
                "description": "Derived from joined asset",
                "keywords": ["derived"],
                "source": "test",
                "license": "MIT",
                "attribution": "Test",
                "geometry_type": "POLYGON",
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
                        "description": "",
                        "type_name": "GEOMETRY",
                        "logical_type": "binary",
                        "nullable": False,
                    },
                },
            },
            "created_at": datetime.utcnow(),
        }
    )

    clean_mongodb["lineage"].insert_one(
        {
            "source_asset_id": parent_oid,
            "target_asset_id": child_oid,
            "run_id": run_oid,
            "transformation": "join_operation",
            "created_at": datetime.utcnow(),
        }
    )

    clean_mongodb["lineage"].insert_one(
        {
            "source_asset_id": child_oid,
            "target_asset_id": grandchild_oid,
            "run_id": run_oid,
            "transformation": "derive_operation",
            "created_at": datetime.utcnow(),
        }
    )

    yield {"dataset_id": "test_joined_asset"}


@pytest.fixture
def seeded_tabular_asset(clean_mongodb):
    tabular_oid = ObjectId()
    run_id_str = str(ObjectId())

    clean_mongodb["assets"].insert_one(
        {
            "_id": tabular_oid,
            "s3_key": "data-lake/test/tabular_v1.parquet",
            "dataset_id": "tabular_asset",
            "version": 1,
            "content_hash": "sha256:" + ("d" * 64),
            "run_id": run_id_str,
            "kind": "tabular",
            "format": "parquet",
            "crs": None,
            "bounds": None,
            "metadata": {
                "title": "Tabular Asset",
                "description": "Tabular description",
                "keywords": ["tabular"],
                "source": "test",
                "license": "MIT",
                "attribution": "Test",
                "column_schema": {
                    "id": {
                        "title": "ID",
                        "description": "",
                        "type_name": "INTEGER",
                        "logical_type": "int64",
                        "nullable": False,
                    },
                    "name": {
                        "title": "Name",
                        "description": "",
                        "type_name": "STRING",
                        "logical_type": "string",
                        "nullable": True,
                    },
                },
            },
            "created_at": datetime.utcnow(),
        }
    )

    yield {"dataset_id": "tabular_asset"}


@pytest.mark.integration
class TestWebappAssets:
    """Integration tests for asset endpoints."""

    def test_list_assets_html(self):
        """Assets list should return HTML by default."""
        response = requests.get(
            f"{WEBAPP_URL}/assets/",
            auth=AUTH,
            timeout=10,
        )

        assert response.status_code == 200
        assert "text/html" in response.headers.get("content-type", "")
        assert "Assets" in response.text

    def test_list_assets_json(self):
        """Assets list should return JSON with format param."""
        response = requests.get(
            f"{WEBAPP_URL}/assets/",
            auth=AUTH,
            params={"format": "json"},
            timeout=10,
        )

        assert response.status_code == 200
        data = response.json()
        assert "assets" in data
        assert "count" in data
        assert isinstance(data["assets"], list)

    def test_filter_assets_by_kind(self):
        """Assets list should filter by kind."""
        response = requests.get(
            f"{WEBAPP_URL}/assets/",
            auth=AUTH,
            params={"format": "json", "kind": "spatial"},
            timeout=10,
        )

        assert response.status_code == 200
        data = response.json()
        # All returned assets should be spatial
        for asset in data["assets"]:
            assert asset["kind"] == "spatial"

    def test_assets_requires_auth(self):
        """Assets endpoint should require authentication."""
        response = requests.get(
            f"{WEBAPP_URL}/assets/",
            timeout=10,
        )

        assert response.status_code == 401

    def test_asset_detail_with_seeded_data(self, seeded_asset):
        response = requests.get(
            f"{WEBAPP_URL}/assets/{seeded_asset['dataset_id']}",
            auth=AUTH,
            timeout=10,
        )

        assert response.status_code == 200
        assert "Test Joined Asset" in response.text
        assert "EPSG:4326" in response.text
        assert "Column Schema" in response.text

    def test_asset_detail_markdown_xss_sanitized(self, seeded_asset):
        response = requests.get(
            f"{WEBAPP_URL}/assets/{seeded_asset['dataset_id']}",
            auth=AUTH,
            timeout=10,
        )

        assert response.status_code == 200
        assert "<script>" not in response.text
        assert "<strong>markdown</strong>" in response.text

    def test_lineage_graph_returns_three_nodes(self, seeded_asset):
        response = requests.get(
            f"{WEBAPP_URL}/assets/{seeded_asset['dataset_id']}/v1/lineage/graph",
            auth=AUTH,
            timeout=10,
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["elements"]["nodes"]) == 3

    def test_lineage_graph_returns_two_edges(self, seeded_asset):
        response = requests.get(
            f"{WEBAPP_URL}/assets/{seeded_asset['dataset_id']}/v1/lineage/graph",
            auth=AUTH,
            timeout=10,
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["elements"]["edges"]) == 2
        transformations = {
            edge["data"]["transformation"] for edge in data["elements"]["edges"]
        }
        assert "join_operation" in transformations
        assert "derive_operation" in transformations

    def test_lineage_graph_focal_marked_root(self, seeded_asset):
        response = requests.get(
            f"{WEBAPP_URL}/assets/{seeded_asset['dataset_id']}/v1/lineage/graph",
            auth=AUTH,
            timeout=10,
        )

        assert response.status_code == 200
        nodes = response.json()["elements"]["nodes"]
        root_nodes = [node for node in nodes if node["data"].get("is_root")]
        assert len(root_nodes) == 1

    def test_tabular_asset_hides_spatial_section(self, seeded_tabular_asset):
        response = requests.get(
            f"{WEBAPP_URL}/assets/{seeded_tabular_asset['dataset_id']}",
            auth=AUTH,
            timeout=10,
        )

        assert response.status_code == 200
        assert "Spatial Information" not in response.text
