import pytest
from app.services.workflow_registry import list_workflows, get_workflow, Workflow, WorkflowStep

def test_list_workflows_returns_all():
    workflows = list_workflows()
    assert len(workflows) == 4
    ids = [w.id for w in workflows]
    assert "ingest-vector" in ids
    assert "ingest-csv" in ids
    assert "join-datasets" in ids
    assert "ingest-buildings" in ids

def test_get_workflow_success():
    workflow = get_workflow("ingest-vector")
    assert workflow is not None
    assert workflow.id == "ingest-vector"
    assert workflow.name == "Ingest Vector Data"
    assert workflow.asset_type == "spatial"
    assert len(workflow.steps) == 3

def test_get_workflow_nonexistent():
    assert get_workflow("nonexistent") is None

def test_workflow_intent_coherence():
    # Valid intents from libs/models/manifest.py
    # ingest_vector, ingest_tabular, join_datasets, ingest_building_footprints
    
    v = get_workflow("ingest-vector")
    assert v.intent == "ingest_vector"
    
    c = get_workflow("ingest-csv")
    assert c.intent == "ingest_tabular"
    
    j = get_workflow("join-datasets")
    assert j.intent == "join_datasets"
    
    b = get_workflow("ingest-buildings")
    assert b.intent == "ingest_building_footprints"

def test_workflow_steps_structure():
    workflow = get_workflow("ingest-vector")
    for step in workflow.steps:
        assert isinstance(step, WorkflowStep)
        assert step.id is not None
        assert step.title is not None
        assert isinstance(step.fields, list)
        for field in step.fields:
            assert isinstance(field, str)
