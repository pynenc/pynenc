import logging
import time
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynenc.identifiers.task_id import TaskId

from pynmon.app import get_pynenc_instance, templates
from pynmon.util.formatting import format_task_extra_info

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.task import Task
    from pynenc.call import Call

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
    calls: list[Call] = []
    start_time = time.time()
    invocation_count = 0

    try:
        for invocation_id in app.orchestrator.get_existing_invocations(task=task):
            if time.time() - start_time > timeout:
                logger.warning("Timeout reached when processing invocations")
                break

            invocation_count += 1
            if invocation := app.state_backend.get_invocation(invocation_id):
                if invocation.call not in calls:
                    calls.append(invocation.call)

            if len(calls) >= max_calls:
                break

        logger.info(
            f"Processed {invocation_count} invocations, found {len(calls)} calls"
        )

    except Exception as e:
        logger.exception(f"Error processing invocations: {e}")

    return calls


@router.get("/", response_class=HTMLResponse)
async def tasks_list(request: Request) -> HTMLResponse:
    """Display all registered tasks."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving tasks list for app: {app.app_id}")

    # Get all registered tasks
    tasks = list(app.tasks.values())
    logger.info(f"Found {len(tasks)} tasks")

    return templates.TemplateResponse(
        request,
        "tasks/list.html",
        context={"title": "Tasks Monitor", "app_id": app.app_id, "tasks": tasks},
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
        request,
        "tasks/partials/list_content.html",
        context={"tasks": tasks},
    )


@router.get("/{task_id_key}", response_class=HTMLResponse)
async def task_detail(request: Request, task_id_key: str) -> HTMLResponse:
    """Display detailed information about a specific task."""
    if not task_id_key:
        return templates.TemplateResponse(
            request,
            "shared/error.html",
            context={
                "title": "Missing Task ID",
                "message": "No task_id was provided. Please check the URL and try again.",
            },
            status_code=400,
        )

    try:
        task_id = TaskId.from_key(task_id_key)
    except ValueError as e:
        logger.warning(f"Invalid task ID format: {task_id_key} - {str(e)}")
        return templates.TemplateResponse(
            request,
            "shared/error.html",
            context={
                "title": "Invalid Task ID Format",
                "message": f"The provided task ID is not properly formatted: {str(e)}",
            },
            status_code=400,
        )

    logger.info(f"Retrieving details for task: {task_id}")
    start_time = time.time()

    try:
        app = get_pynenc_instance()
        logger.info(f"Got app instance: {app.app_id}")
        try:
            task = app.get_task(task_id)
            logger.info(f"Task found: {task is not None}")
        except (ModuleNotFoundError, AttributeError, ValueError) as e:
            logger.warning(
                f"No task found with ID: {task_id} - {type(e).__name__}: {e}"
            )
            return templates.TemplateResponse(
                request,
                "shared/error.html",
                context={
                    "title": "Task Not Found",
                    "message": f"No task found with ID: {task_id}",
                },
                status_code=404,
            )

        if not task:
            logger.warning(f"No task found with ID: {task_id}")
            return templates.TemplateResponse(
                request,
                "shared/error.html",
                context={
                    "title": "Task Not Found",
                    "message": f"No task found with ID: {task_id}",
                },
                status_code=404,
            )

        # Get calls for this task with timeout protection
        logger.info(f"Fetching invocations for task: {task_id}")
        calls = _get_task_calls(app, task, max_calls=50, timeout=10.0)

        # Create additional task information for template
        task_extra = format_task_extra_info(task)

        logger.info(f"Rendering template with {len(calls)} calls")
        return templates.TemplateResponse(
            request,
            "tasks/detail.html",
            context={
                "title": "Task Details",
                "app_id": app.app_id,
                "task": task,
                "task_extra": task_extra,
                "calls": calls,
            },
        )
    except Exception as e:
        logger.exception(f"Unexpected error in task_detail: {str(e)}")
        return templates.TemplateResponse(
            request,
            "shared/error.html",
            context={"title": "Error", "message": f"An error occurred: {str(e)}"},
            status_code=500,
        )
    finally:
        elapsed = time.time() - start_time
        logger.info(f"task_detail completed in {elapsed:.2f} seconds")
