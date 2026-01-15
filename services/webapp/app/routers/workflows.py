import json
import logging
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.auth.dependencies import AuthenticatedUser, get_current_user
from app.services.workflow_registry import get_workflow, list_workflows
from app.services.mongodb_service import get_mongodb_service
from app.services.manifest_builder import build_manifest
from app.services.minio_service import get_minio_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/workflows", tags=["workflows"])

# Templates
BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def parse_wizard_state(form_data: dict) -> dict:
    """Parse accumulated wizard state from hidden field."""
    state_json = form_data.get("_wizard_state", "{}")
    try:
        return json.loads(state_json)
    except json.JSONDecodeError:
        logger.error(f"Failed to decode wizard state: {state_json}")
        return {}


def merge_step_data(
    accumulated: dict, current_step: dict, step_fields: list[str]
) -> dict:
    """Merge current step data into accumulated state."""
    result = accumulated.copy()
    for field in step_fields:
        if field in current_step:
            result[field] = current_step[field]
    return result


def normalize_field(key: str, value: Any) -> Any:
    """Normalize form values to manifest-compatible types."""
    EMPTY_TO_NONE = {"project", "dataset_id", "right_key"}
    if key in EMPTY_TO_NONE and value == "":
        return None

    if key == "how" and value == "":
        return "left"

    if key == "keywords":
        if isinstance(value, str):
            return [k.strip() for k in value.split(",") if k.strip()]
        return value if isinstance(value, list) else []

    return value


def form_to_manifest_dict(workflow_id: str, data: dict) -> dict:
    """Translate flat wizard state to manifest structure."""
    normalized = {k: normalize_field(k, v) for k, v in data.items()}

    manifest_dict = {
        "title": normalized.get("title", ""),
        "description": normalized.get("description", ""),
        "keywords": normalized.get("keywords", []),
        "source": normalized.get("source", ""),
        "license": normalized.get("license", ""),
        "attribution": normalized.get("attribution", ""),
        "project": normalized.get("project"),
        "dataset_id": normalized.get("dataset_id"),
        "tags": normalized.get("tags", {}),
    }

    workflow = get_workflow(workflow_id)
    if not workflow:
        return manifest_dict

    # Handle files
    if workflow.asset_type in ["spatial", "tabular"]:
        file_path = normalized.get("file_path")
        if file_path:
            # For ingest-csv, format is always CSV
            file_format = normalized.get("file_format", "CSV")
            file_type = "tabular" if workflow.asset_type == "tabular" else "vector"
            manifest_dict["files"] = [
                {"path": file_path, "format": file_format, "type": file_type}
            ]
        else:
            manifest_dict["files"] = []

    # Handle join_config
    if workflow_id == "join-datasets":
        join_config = {
            "spatial_dataset_id": normalized.get("spatial_dataset_id"),
            "tabular_dataset_id": normalized.get("tabular_dataset_id"),
            "left_key": normalized.get("left_key"),
        }

        spatial_version = normalized.get("spatial_version")
        if spatial_version:
            try:
                join_config["spatial_version"] = int(spatial_version)
            except (ValueError, TypeError):
                pass

        tabular_version = normalized.get("tabular_version")
        if tabular_version:
            try:
                join_config["tabular_version"] = int(tabular_version)
            except (ValueError, TypeError):
                pass

        if normalized.get("right_key"):
            join_config["right_key"] = normalized["right_key"]
        if normalized.get("how"):
            join_config["how"] = normalized["how"]

        manifest_dict["join_config"] = join_config
        manifest_dict["files"] = []  # join_datasets forbids files

    return manifest_dict


def validate_step(workflow_id: str, step_index: int, data: dict) -> dict[str, str]:
    """Basic presence validation for workflow steps."""
    errors = {}
    workflow = get_workflow(workflow_id)
    if not workflow or step_index >= len(workflow.steps):
        return errors

    step = workflow.steps[step_index]

    # Define required fields per step
    # This matches Spec §2
    REQUIRED_FIELDS = {
        "metadata": ["title"],
        "files": ["file_path", "file_format"],
        "file": ["file_path"],  # for ingest-csv and ingest-buildings
        "spatial": ["spatial_dataset_id"],
        "tabular": ["tabular_dataset_id"],
        "join": ["left_key"],
    }

    fields_to_check = REQUIRED_FIELDS.get(step.id, [])
    for field in fields_to_check:
        value = data.get(field)
        if not value or (isinstance(value, str) and not value.strip()):
            errors[field] = f"{field.replace('_', ' ').title()} is required"

    return errors


@router.get("/", response_class=HTMLResponse)
async def list_workflows_page(
    request: Request,
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    return templates.TemplateResponse(
        "workflows/list.html",
        {
            "request": request,
            "user": current_user,
            "workflows": list_workflows(),
        },
    )


def get_step_template_path(workflow, step_index):
    step = workflow.steps[step_index]
    TEMPLATE_MAP = {
        "metadata": "_step_metadata.html",
        "files": "_step_file_spatial.html",
        "file": "_step_file_tabular.html"
        if workflow.id == "ingest-csv"
        else "_step_file_spatial.html",
        "options": "_step_options.html",
        "spatial": "_step_spatial_select.html",
        "tabular": "_step_tabular_select.html",
        "join": "_step_join_config.html",
    }
    return f"workflows/{TEMPLATE_MAP.get(step.id, f'_{step.id}.html')}"


@router.get("/{workflow_id}", response_class=HTMLResponse)
async def start_workflow(
    workflow_id: str,
    request: Request,
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    workflow = get_workflow(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")

    # Initial state from defaults
    initial_state = workflow.defaults.copy()

    return templates.TemplateResponse(
        "workflows/wizard.html",
        {
            "request": request,
            "user": current_user,
            "workflow": workflow,
            "current_step_index": 0,
            "accumulated_data": initial_state,
            "step_template": get_step_template_path(workflow, 0),
            "errors": {},
        },
    )


@router.post("/{workflow_id}/step/{step_index}", response_class=HTMLResponse)
async def process_step(
    workflow_id: str,
    step_index: int,
    request: Request,
    current_user: AuthenticatedUser = Depends(get_current_user),
):
    workflow = get_workflow(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")

    form_data = await request.form()
    form_dict = dict(form_data)

    nav = form_dict.get("_nav", "next")
    accumulated_data = parse_wizard_state(form_dict)

    if nav == "back":
        # Render previous step
        prev_index = max(0, step_index - 1)
        return await render_step(
            workflow, prev_index, accumulated_data, request, current_user
        )

    # Merge current step fields into accumulated data
    current_step = workflow.steps[step_index]
    merged_data = merge_step_data(accumulated_data, form_dict, current_step.fields)

    # Validate
    errors = validate_step(workflow_id, step_index, form_dict)
    if errors:
        return await render_step(
            workflow, step_index, accumulated_data, request, current_user, errors=errors
        )

    if nav == "submit":
        # Final submission
        return await submit_workflow(workflow_id, merged_data, request, current_user)

    # Next step
    next_index = min(len(workflow.steps) - 1, step_index + 1)
    return await render_step(workflow, next_index, merged_data, request, current_user)


async def render_step(workflow, step_index, data, request, user, errors=None):
    step = workflow.steps[step_index]

    # Fetch assets for selection steps
    extra_context = {}
    if step.id == "spatial":
        mongo = get_mongodb_service()
        extra_context["spatial_assets"] = mongo.list_assets(kind="spatial")
    elif step.id == "tabular":
        mongo = get_mongodb_service()
        extra_context["tabular_assets"] = mongo.list_assets(kind="tabular")

    return templates.TemplateResponse(
        get_step_template_path(workflow, step_index),
        {
            "request": request,
            "user": user,
            "workflow": workflow,
            "current_step_index": step_index,
            "accumulated_data": data,
            "errors": errors or {},
            **extra_context,
        },
    )


async def submit_workflow(
    workflow_id: str, data: dict, request: Request, user: AuthenticatedUser
):
    workflow = get_workflow(workflow_id)
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")

    manifest_dict = form_to_manifest_dict(workflow_id, data)

    try:
        manifest = build_manifest(
            workflow.asset_type, manifest_dict, uploader=user.username
        )
        manifest_json = manifest.model_dump_json(indent=2)

        minio = get_minio_service()
        manifest_key = f"manifests/{manifest.batch_id}.json"

        from io import BytesIO

        minio.upload_to_landing(
            file=BytesIO(manifest_json.encode("utf-8")),
            key=manifest_key,
            content_type="application/json",
        )

        return templates.TemplateResponse(
            "workflows/_step_success.html",
            {
                "request": request,
                "user": user,
                "workflow": workflow,
                "batch_id": manifest.batch_id,
                "manifest_key": manifest_key,
            },
        )
    except Exception as e:
        logger.exception("Failed to submit workflow")
        # Re-render last step with global error
        return await render_step(
            workflow,
            len(workflow.steps) - 1,
            data,
            request,
            user,
            errors={"global": str(e)},
        )
