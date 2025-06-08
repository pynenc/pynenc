"""
Workflow monitoring views for Pynmon.

This module provides views for monitoring and managing workflows in the Pynenc system.
It includes listing all workflows, viewing workflow runs, and workflow details.
"""

import logging
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynmon.app import get_pynenc_instance, templates

if TYPE_CHECKING:
    pass

router = APIRouter(prefix="/workflows", tags=["workflows"])
logger = logging.getLogger("pynmon.views.workflows")


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

    except Exception as e:
        logger.error(f"Error retrieving workflow details for {workflow_task_id}: {e}")
        sorted_runs = []
        task = None

    return templates.TemplateResponse(
        "workflows/detail.html",
        {
            "request": request,
            "title": f"Workflow: {workflow_task_id}",
            "app_id": app.app_id,
            "workflow_task_id": workflow_task_id,
            "workflow_runs": sorted_runs,
            "task": task,
        },
    )
