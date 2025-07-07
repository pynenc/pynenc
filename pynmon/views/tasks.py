import logging
import time
import traceback
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynmon.app import get_pynenc_instance, templates

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.call import Call
    from pynenc.task import Task

router = APIRouter(prefix="/tasks", tags=["tasks"])
logger = logging.getLogger("pynmon.views.tasks")


def _get_config_differences(task: "Task") -> dict[str, dict[str, Any]]:
    """
    Get task configuration values that differ from defaults.

    :param task: The task to analyze
    :return: Dictionary with 'differences', 'defaults', and 'all_current' containing configuration info
    """

    from pynenc.conf.config_task import ConfigTask

    # Use task_options (only explicitly set values) for differences
    differences = task.conf.task_options.copy()

    # Get all available ConfigField options using introspection
    defaults = {}
    all_current = {}

    # Use task.conf.all_fields to get all configuration field names
    for attr_name in task.conf.all_fields:
        # Get the ConfigField from the class to access the default value
        defaults[attr_name] = getattr(ConfigTask, attr_name)
        # Get the current value from the task configuration instance
        all_current[attr_name] = getattr(task.conf, attr_name)

    return {
        "differences": differences,
        "defaults": defaults,
        "all_current": all_current,
    }


def _get_workflow_context_info(task: "Task") -> dict[str, Any] | None:
    """
    Get workflow context information for a task if it's associated with workflows.

    :param task: The task to check for workflow associations
    :return: Workflow context information or None
    """
    app = task.app

    try:
        # Check if this task has any workflow runs
        workflow_runs = list(app.state_backend.get_workflow_runs(task.task_id))

        if not workflow_runs:
            return None

        # Get basic workflow information
        total_runs = len(workflow_runs)
        latest_run = workflow_runs[0] if workflow_runs else None

        # Check if this task has the force_new_workflow flag
        has_force_new_workflow = getattr(task.conf, "force_new_workflow", False)

        return {
            "is_workflow_task": True,
            "force_new_workflow": has_force_new_workflow,
            "total_workflow_runs": total_runs,
            "latest_workflow_run": latest_run,
            "workflow_task_url": f"/workflows/{task.task_id}",
        }

    except Exception as e:
        logger.debug(f"Error getting workflow context for task {task.task_id}: {e}")
        return None


def _get_task_calls(
    app: "Pynenc", task: "Task", max_calls: int = 50, timeout: float = 10.0
) -> list["Call"]:
    """
    Get calls for a task with timeout protection.

    :param app: The Pynenc application instance
    :param task: The task to get calls for
    :param max_calls: Maximum number of calls to return
    :param timeout: Maximum time to spend collecting calls
    :return: List of unique calls for the task
    """
    calls = []
    start_time = time.time()
    invocation_count = 0

    try:
        logger.info("Starting to retrieve existing invocations")
        invocations_gen = app.orchestrator.get_existing_invocations(task=task)
        logger.info("Got invocations generator")

        for invocation in invocations_gen:
            if time.time() - start_time > timeout:
                logger.warning("Timeout reached when processing invocations")
                break

            invocation_count += 1
            logger.debug(
                f"Processing invocation {invocation_count}: {invocation.invocation_id}"
            )

            if invocation.call not in calls:
                calls.append(invocation.call)
                logger.debug(f"Added call {len(calls)}: {invocation.call.call_id}")

            if len(calls) >= max_calls:
                logger.info(f"Reached maximum of {max_calls} calls")
                break

        logger.info(
            f"Processed {invocation_count} invocations, found {len(calls)} unique calls"
        )

    except Exception as e:
        logger.error(f"Error processing invocations: {str(e)}")
        logger.error(traceback.format_exc())
        # Continue with whatever calls we found so far

    return calls


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
async def tasks_list(request: Request) -> HTMLResponse:
    """Display all registered tasks."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving tasks list for app: {app.app_id}")

    # Get all registered tasks
    tasks = list(app.tasks.values())
    logger.info(f"Found {len(tasks)} tasks")

    return templates.TemplateResponse(
        "tasks/list.html",
        {
            "request": request,
            "title": "Tasks Monitor",
            "app_id": app.app_id,
            "tasks": tasks,
        },
    )


@router.get("/refresh", response_class=HTMLResponse)
async def refresh_tasks_list(request: Request) -> HTMLResponse:
    """Refresh the tasks list for HTMX partial updates."""
    app = get_pynenc_instance()
    logger.info(f"Refreshing tasks list for app: {app.app_id}")

    # Get all registered tasks
    tasks = list(app.tasks.values())
    logger.info(f"Found {len(tasks)} tasks")

    return templates.TemplateResponse(
        "tasks/partials/list_content.html",
        {
            "request": request,
            "tasks": tasks,
        },
    )


@router.get("/{task_id}", response_class=HTMLResponse)
async def task_detail(request: Request, task_id: str) -> HTMLResponse:
    """Display detailed information about a specific task."""
    logger.info(f"Retrieving details for task: {task_id}")
    start_time = time.time()

    try:
        app = get_pynenc_instance()
        logger.info(f"Got app instance: {app.app_id}")

        # Try to find the task by ID
        try:
            task = app.get_task(task_id)
            logger.info(f"Task found: {task is not None}")
        except (ModuleNotFoundError, AttributeError, ValueError) as e:
            logger.warning(
                f"No task found with ID: {task_id} - {type(e).__name__}: {e}"
            )
            return templates.TemplateResponse(
                "shared/error.html",
                {
                    "request": request,
                    "title": "Task Not Found",
                    "message": f"No task found with ID: {task_id}",
                },
                status_code=404,
            )

        if not task:
            logger.warning(f"No task found with ID: {task_id}")
            return templates.TemplateResponse(
                "shared/error.html",
                {
                    "request": request,
                    "title": "Task Not Found",
                    "message": f"No task found with ID: {task_id}",
                },
                status_code=404,
            )

        # Get calls for this task with timeout protection
        logger.info(f"Fetching invocations for task: {task_id}")
        calls = _get_task_calls(app, task, max_calls=50, timeout=10.0)

        # Create additional task information for template
        task_extra = _create_task_extra_info(task)

        # Get configuration differences and workflow context info
        config_info = _get_config_differences(task)
        workflow_info = _get_workflow_context_info(task)

        logger.info(f"Rendering template with {len(calls)} calls")
        return templates.TemplateResponse(
            "tasks/detail.html",
            {
                "request": request,
                "title": "Task Details",
                "app_id": app.app_id,
                "task": task,
                "task_extra": task_extra,
                "calls": calls,
                "config_info": config_info,
                "workflow_info": workflow_info,
            },
        )
    except Exception as e:
        logger.error(f"Unexpected error in task_detail: {str(e)}")
        logger.error(traceback.format_exc())
        return templates.TemplateResponse(
            "shared/error.html",
            {
                "request": request,
                "title": "Error",
                "message": f"An error occurred: {str(e)}",
            },
            status_code=500,
        )
    finally:
        elapsed = time.time() - start_time
        logger.info(f"task_detail completed in {elapsed:.2f} seconds")
