import logging
import time
import traceback
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynmon.app import get_pynenc_instance, templates

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.call import Call
    from pynenc.task import Task

router = APIRouter(prefix="/tasks", tags=["tasks"])
logger = logging.getLogger("pynmon.views.tasks")


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
