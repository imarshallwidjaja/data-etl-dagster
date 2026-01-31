import pytest

from libs.models import Manifest
from services.dagster.etl_pipelines.sensors.tag_helpers import derive_testing_tag


def test_derive_testing_tag_handles_bool(valid_manifest_dict):
    manifest = Manifest(**valid_manifest_dict)
    manifest.metadata.tags["testing"] = True
    assert derive_testing_tag(manifest) == "true"

    manifest.metadata.tags["testing"] = False
    assert derive_testing_tag(manifest) == "false"


def test_derive_testing_tag_returns_none_when_missing(valid_manifest_dict):
    manifest = Manifest(**valid_manifest_dict)
    manifest.metadata.tags.pop("testing", None)
    assert derive_testing_tag(manifest) is None
