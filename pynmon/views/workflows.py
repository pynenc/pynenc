"""
Workflow monitoring views for Pynmon.

This module provides views for monitoring and managing workflows in the Pynenc system.
It includes listing all workflows, viewing workflow runs, and workflow details.
"""

import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynenc.identifiers.task_id import TaskId

from pynmon.app import get_pynenc_instance, templates
from pynmon.util.formatting import format_task_extra_info

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.workflow.workflow_identity import WorkflowIdentity


router = APIRouter(prefix="/workflows", tags=["workflows"])
logger = logging.getLogger("pynmon.views.workflows")


def _workflow_run_rows(
    app: "Pynenc", workflow_runs: list["WorkflowIdentity"]
) -> list[dict[str, Any]]:
    """Build workflow run rows with parent workflow details when available."""
    rows: list[dict[str, Any]] = []
    for run in workflow_runs:
        parent_workflow = None
        if run.parent_workflow_id:
            try:
                parent_invocation = app.state_backend.get_invocation(
                    run.parent_workflow_id
                )
                parent_workflow = parent_invocation.workflow
            except Exception:
                logger.debug(
                    "Could not load parent workflow invocation %s",
                    run.parent_workflow_id,
                    exc_info=True,
                )
        rows.append(
            {
                "workflow_id": run.workflow_id,
                "workflow_type": run.workflow_type,
                "parent_workflow_id": run.parent_workflow_id,
                "parent_workflow": parent_workflow,
            }
        )
    return rows


@router.get("/", response_class=HTMLResponse)
async def workflows_list(request: Request) -> HTMLResponse:
    """Display all workflows."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving workflows list for app: {app.app_id}")

    try:
        # Get all workflow types (convert iterator to list)
        workflow_types = list(app.state_backend.get_all_workflow_types())
        logger.info(f"Found {len(workflow_types)} workflow types")

        # Get workflow runs for each type
        workflows_with_runs = []
        for workflow_type in workflow_types:
            runs = list(app.state_backend.get_workflow_runs(workflow_type))
            workflows_with_runs.append(
                {
                    "workflow_type": workflow_type,
                    "run_count": len(runs),
                    "latest_run": runs[0] if runs else None,
                }
            )

        logger.info(f"Prepared {len(workflows_with_runs)} workflow entries for display")

    except Exception as e:
        logger.exception(f"Error retrieving workflows: {e}")
        workflows_with_runs = []

    return templates.TemplateResponse(
        request,
        "workflows/list.html",
        context={
            "title": "Workflows Monitor",
            "app_id": app.app_id,
            "workflows": workflows_with_runs,
        },
    )


@router.get("/refresh", response_class=HTMLResponse)
async def refresh_workflows_list(request: Request) -> HTMLResponse:
    """Refresh the workflows list for HTMX partial updates."""
    app = get_pynenc_instance()
    logger.info(f"Refreshing workflows list for app: {app.app_id}")

    try:
        # Get all workflow types (convert iterator to list)
        workflow_types = list(app.state_backend.get_all_workflow_types())
        logger.info(f"Found {len(workflow_types)} workflow types")

        # Get workflow runs for each type
        workflows_with_runs = []
        for workflow_type in workflow_types:
            runs = list(app.state_backend.get_workflow_runs(workflow_type))
            workflows_with_runs.append(
                {
                    "workflow_type": workflow_type,
                    "run_count": len(runs),
                    "latest_run": runs[0] if runs else None,
                }
            )

        logger.info(f"Prepared {len(workflows_with_runs)} workflow entries for display")

    except Exception as e:
        logger.exception(f"Error refreshing workflows: {e}")
        workflows_with_runs = []

    return templates.TemplateResponse(
        request,
        "workflows/partials/list_content.html",
        context={"workflows": workflows_with_runs},
    )


@router.get("/runs", response_class=HTMLResponse)
async def workflow_runs_list(request: Request) -> HTMLResponse:
    """Display all workflow runs."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving all workflow runs for app: {app.app_id}")

    try:
        # Get all workflow runs (convert iterator to list)
        all_runs = list(app.state_backend.get_all_workflow_runs())
        logger.info(f"Found {len(all_runs)} workflow runs")

        # Sort by creation time if available (newest first)
        sorted_runs = sorted(
            all_runs, key=lambda x: getattr(x, "created_at", ""), reverse=True
        )

    except Exception as e:
        logger.exception(f"Error retrieving workflow runs: {e}")
        sorted_runs = []

    return templates.TemplateResponse(
        request,
        "workflows/runs.html",
        context={
            "title": "Workflow Runs",
            "app_id": app.app_id,
            "workflow_runs": _workflow_run_rows(app, sorted_runs),
        },
    )


@router.get("/runs/refresh", response_class=HTMLResponse)
async def refresh_workflow_runs_list(request: Request) -> HTMLResponse:
    """Refresh the workflow runs list for HTMX partial updates."""
    app = get_pynenc_instance()
    logger.info(f"Refreshing all workflow runs for app: {app.app_id}")

    try:
        # Get all workflow runs (convert iterator to list)
        all_runs = list(app.state_backend.get_all_workflow_runs())
        logger.info(f"Found {len(all_runs)} workflow runs")

        # Sort by creation time if available (newest first)
        sorted_runs = sorted(
            all_runs, key=lambda x: getattr(x, "created_at", ""), reverse=True
        )

    except Exception as e:
        logger.exception(f"Error refreshing workflow runs: {e}")
        sorted_runs = []

    return templates.TemplateResponse(
        request,
        "workflows/partials/runs_content.html",
        context={"workflow_runs": _workflow_run_rows(app, sorted_runs)},
    )


# Important: This route must come AFTER all specific routes like '/refresh' and '/runs/refresh'
# because FastAPI matches routes in order and '/{workflow_type_key}' would match anything
@router.get("/{workflow_type_key}", response_class=HTMLResponse)
async def workflow_detail(request: Request, workflow_type_key: str) -> HTMLResponse:
    """Display details for a specific workflow type."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving workflow details for: {workflow_type_key}")

    try:
        # Get workflow type (TaskId) from workflow main invocation
        workflow_type = TaskId.from_key(workflow_type_key)
        # Get runs for this specific workflow type (convert iterator to list)
        workflow_runs = list(app.state_backend.get_workflow_runs(workflow_type))
        logger.info(f"Found {len(workflow_runs)} runs for workflow {workflow_type}")

        # Sort by creation time if available (newest first)
        sorted_runs = sorted(
            workflow_runs, key=lambda x: getattr(x, "created_at", ""), reverse=True
        )

        # Get the task if it exists
        task = app.tasks.get(workflow_type)

        # Create additional task information for template
        task_extra = format_task_extra_info(task) if task else None

        return templates.TemplateResponse(
            request,
            "workflows/detail.html",
            context={
                "title": "Workflow Details",
                "app_id": app.app_id,
                "workflow_type": workflow_type,
                "workflow_runs": _workflow_run_rows(app, sorted_runs),
                "task": task,
                "task_extra": task_extra,
            },
        )

    except Exception as e:
        logger.exception(
            f"Error retrieving workflow details for {workflow_type_key}: {e}"
        )

        return templates.TemplateResponse(
            request,
            "shared/error.html",
            context={
                "title": "Error",
                "app_id": app.app_id,
                "error_title": "Workflow Not Found",
                "error_message": f"Could not load workflow '{workflow_type_key}': {str(e)}",
            },
            status_code=404,
        )


@router.get("/{workflow_type_key}/refresh", response_class=HTMLResponse)
async def refresh_workflow_detail(
    request: Request, workflow_type_key: str
) -> HTMLResponse:
    """Refresh the workflow detail for HTMX partial updates."""
    app = get_pynenc_instance()
    logger.info(f"Refreshing workflow details for: {workflow_type_key}")

    try:
        # Get workflow type (TaskId) from workflow main invocation
        workflow_type = TaskId.from_key(workflow_type_key)
        # Get runs for this specific workflow type (convert iterator to list)
        workflow_runs = list(app.state_backend.get_workflow_runs(workflow_type))
        logger.info(f"Found {len(workflow_runs)} runs for workflow {workflow_type}")

        # Sort by creation time if available (newest first)
        sorted_runs = sorted(
            workflow_runs, key=lambda x: getattr(x, "created_at", ""), reverse=True
        )

        # Get the task if it exists
        task = app.tasks.get(workflow_type)

        # Create additional task information for template
        task_extra = format_task_extra_info(task) if task else None

        return templates.TemplateResponse(
            request,
            "workflows/partials/detail_content.html",
            context={
                "workflow_type": workflow_type,
                "workflow_runs": _workflow_run_rows(app, sorted_runs),
                "task": task,
                "task_extra": task_extra,
            },
        )

    except Exception as e:
        logger.exception(
            f"Error refreshing workflow details for {workflow_type_key}: {e}"
        )

        return HTMLResponse(
            f'<div class="alert alert-danger">Error loading workflow: {str(e)}</div>',
            status_code=500,
        )
