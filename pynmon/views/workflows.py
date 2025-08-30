"""
Workflow monitoring views for Pynmon.

This module provides views for monitoring and managing workflows in the Pynenc system.
It includes listing all workflows, viewing workflow runs, and workflow details.
"""

import logging
import traceback
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynmon.app import get_pynenc_instance, templates

if TYPE_CHECKING:
    from pynenc.task import Task

router = APIRouter(prefix="/workflows", tags=["workflows"])
logger = logging.getLogger("pynmon.views.workflows")


def _create_task_extra_info(task: "Task") -> dict[str, str | list[str]]:
    """
    Create additional task information for template display.

    :param task: The task to extract information from
    :return: Dictionary with extra task information
    """
    return {
        "module": task.func.__module__,
        "func_qualname": task.func.__qualname__,
        "retry_for": [e.__name__ for e in task.conf.retry_for],
    }


@router.get("/", response_class=HTMLResponse)
async def workflows_list(request: Request) -> HTMLResponse:
    """Display all workflows."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving workflows list for app: {app.app_id}")

    try:
        # Get all workflow types (convert iterator to list)
        workflow_types = list(app.state_backend.get_all_workflows())
        logger.info(f"Found {len(workflow_types)} workflow types")

        # Get workflow runs for each type
        workflows_with_runs = []
        for workflow_task_id in workflow_types:
            runs = list(app.state_backend.get_workflow_runs(workflow_task_id))
            workflows_with_runs.append(
                {
                    "workflow_task_id": workflow_task_id,
                    "run_count": len(runs),
                    "latest_run": runs[0] if runs else None,
                }
            )

        logger.info(f"Prepared {len(workflows_with_runs)} workflow entries for display")

    except Exception as e:
        logger.error(f"Error retrieving workflows: {e}")
        workflows_with_runs = []

    return templates.TemplateResponse(
        "workflows/list.html",
        {
            "request": request,
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
        workflow_types = list(app.state_backend.get_all_workflows())
        logger.info(f"Found {len(workflow_types)} workflow types")

        # Get workflow runs for each type
        workflows_with_runs = []
        for workflow_task_id in workflow_types:
            runs = list(app.state_backend.get_workflow_runs(workflow_task_id))
            workflows_with_runs.append(
                {
                    "workflow_task_id": workflow_task_id,
                    "run_count": len(runs),
                    "latest_run": runs[0] if runs else None,
                }
            )

        logger.info(f"Prepared {len(workflows_with_runs)} workflow entries for display")

    except Exception as e:
        logger.error(f"Error refreshing workflows: {e}")
        workflows_with_runs = []

    return templates.TemplateResponse(
        "workflows/partials/list_content.html",
        {
            "request": request,
            "workflows": workflows_with_runs,
        },
    )


@router.get("/runs", response_class=HTMLResponse)
async def workflow_runs_list(request: Request) -> HTMLResponse:
    """Display all workflow runs."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving all workflow runs for app: {app.app_id}")

    try:
        # Get all workflow runs (convert iterator to list)
        all_runs = list(app.state_backend.get_all_workflows_runs())
        logger.info(f"Found {len(all_runs)} workflow runs")

        # Sort by creation time if available (newest first)
        sorted_runs = sorted(
            all_runs, key=lambda x: getattr(x, "created_at", ""), reverse=True
        )

    except Exception as e:
        logger.error(f"Error retrieving workflow runs: {e}")
        sorted_runs = []

    return templates.TemplateResponse(
        "workflows/runs.html",
        {
            "request": request,
            "title": "Workflow Runs",
            "app_id": app.app_id,
            "workflow_runs": sorted_runs,
        },
    )


@router.get("/runs/refresh", response_class=HTMLResponse)
async def refresh_workflow_runs_list(request: Request) -> HTMLResponse:
    """Refresh the workflow runs list for HTMX partial updates."""
    app = get_pynenc_instance()
    logger.info(f"Refreshing all workflow runs for app: {app.app_id}")

    try:
        # Get all workflow runs (convert iterator to list)
        all_runs = list(app.state_backend.get_all_workflows_runs())
        logger.info(f"Found {len(all_runs)} workflow runs")

        # Sort by creation time if available (newest first)
        sorted_runs = sorted(
            all_runs, key=lambda x: getattr(x, "created_at", ""), reverse=True
        )

    except Exception as e:
        logger.error(f"Error refreshing workflow runs: {e}")
        sorted_runs = []

    return templates.TemplateResponse(
        "workflows/partials/runs_content.html",
        {
            "request": request,
            "workflow_runs": sorted_runs,
        },
    )


# Important: This route must come AFTER all specific routes like '/refresh' and '/runs/refresh'
# because FastAPI matches routes in order and '/{workflow_task_id}' would match anything
@router.get("/{workflow_task_id}", response_class=HTMLResponse)
async def workflow_detail(request: Request, workflow_task_id: str) -> HTMLResponse:
    """Display details for a specific workflow type."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving workflow details for: {workflow_task_id}")

    try:
        # Get runs for this specific workflow type (convert iterator to list)
        workflow_runs = list(app.state_backend.get_workflow_runs(workflow_task_id))
        logger.info(f"Found {len(workflow_runs)} runs for workflow {workflow_task_id}")

        # Sort by creation time if available (newest first)
        sorted_runs = sorted(
            workflow_runs, key=lambda x: getattr(x, "created_at", ""), reverse=True
        )

        # Get the task if it exists
        task = app.tasks.get(workflow_task_id)

        # Create additional task information for template
        task_extra = _create_task_extra_info(task) if task else None

    except Exception as e:
        logger.error(f"Error retrieving workflow details for {workflow_task_id}: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sorted_runs = []
        task = None
        task_extra = None

    return templates.TemplateResponse(
        "workflows/detail.html",
        {
            "request": request,
            "title": "Workflow Details",
            "app_id": app.app_id,
            "workflow_task_id": workflow_task_id,
            "workflow_runs": sorted_runs,
            "task": task,
            "task_extra": task_extra,
        },
    )


@router.get("/{workflow_task_id}/refresh", response_class=HTMLResponse)
async def refresh_workflow_detail(
    request: Request, workflow_task_id: str
) -> HTMLResponse:
    """Refresh the workflow detail for HTMX partial updates."""
    app = get_pynenc_instance()
    logger.info(f"Refreshing workflow details for: {workflow_task_id}")

    try:
        # Get runs for this specific workflow type (convert iterator to list)
        workflow_runs = list(app.state_backend.get_workflow_runs(workflow_task_id))
        logger.info(f"Found {len(workflow_runs)} runs for workflow {workflow_task_id}")

        # Sort by creation time if available (newest first)
        sorted_runs = sorted(
            workflow_runs, key=lambda x: getattr(x, "created_at", ""), reverse=True
        )

        # Get the task if it exists
        task = app.tasks.get(workflow_task_id)

        # Create additional task information for template
        task_extra = _create_task_extra_info(task) if task else None

    except Exception as e:
        logger.error(f"Error refreshing workflow details for {workflow_task_id}: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        sorted_runs = []
        task = None
        task_extra = None

    return templates.TemplateResponse(
        "workflows/partials/detail_content.html",
        {
            "request": request,
            "workflow_task_id": workflow_task_id,
            "workflow_runs": sorted_runs,
            "task": task,
            "task_extra": task_extra,
        },
    )


@router.get("/debug", response_class=HTMLResponse)
async def debug_info(request: Request) -> HTMLResponse:
    """Debug endpoint to test if the server is working and show basic info."""
    app = get_pynenc_instance()

    try:
        # Try to get some basic info
        info = {
            "app_id": app.app_id,
            "tasks_count": len(app.tasks),
            "task_names": list(app.tasks.keys()) if app.tasks else [],
        }

        # Try to get workflow runs count
        try:
            all_workflow_runs = list(app.state_backend.get_all_workflows_runs())
            info["total_workflow_runs"] = len(all_workflow_runs)
        except Exception as e:
            info["workflow_runs_error"] = str(e)

        logger.info(f"Debug info retrieved successfully: {info}")

        return HTMLResponse(
            f"""
            <html>
                <head><title>Pynmon Debug Info</title></head>
                <body>
                    <h1>Pynmon Debug Info</h1>
                    <pre>{info}</pre>
                    <p>Server is working! Check logs for detailed information.</p>
                </body>
            </html>
        """
        )

    except Exception as e:
        logger.error(f"Error in debug endpoint: {e}")
        logger.error(f"Full traceback: {traceback.format_exc()}")

        return HTMLResponse(
            f"""
            <html>
                <head><title>Pynmon Debug Error</title></head>
                <body>
                    <h1>Pynmon Debug Error</h1>
                    <pre>Error: {e}</pre>
                    <p>Check logs for full traceback.</p>
                </body>
            </html>
        """,
            status_code=500,
        )
