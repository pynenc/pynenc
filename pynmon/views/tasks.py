import logging
import time
import traceback

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynmon.app import get_pynenc_instance, templates

router = APIRouter(prefix="/tasks", tags=["tasks"])
logger = logging.getLogger("pynmon.views.tasks")


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


@router.get("/{task_id}", response_class=HTMLResponse)
async def task_detail(request: Request, task_id: str) -> HTMLResponse:
    """Display detailed information about a specific task."""
    logger.info(f"Retrieving details for task: {task_id}")
    start_time = time.time()

    try:
        app = get_pynenc_instance()
        logger.info(f"Got app instance: {app.app_id}")

        # Try to find the task by ID
        task = app.get_task(task_id)
        logger.info(f"Task found: {task is not None}")

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

        # Get active calls for this task (limit to 50)
        calls = []
        logger.info(f"Fetching invocations for task: {task_id}")

        # This is the suspected problematic part - add step-by-step logging
        try:
            logger.info("Starting to retrieve existing invocations")
            invocation_count = 0

            # Create a generator but don't consume it yet
            invocations_gen = app.orchestrator.get_existing_invocations(task=task)
            logger.info("Got invocations generator")

            # Adding a timeout mechanism
            max_time = 10  # seconds
            for invocation in invocations_gen:
                if time.time() - start_time > max_time:
                    logger.warning("Timeout reached when processing invocations")
                    break

                invocation_count += 1
                logger.debug(
                    f"Processing invocation {invocation_count}: {invocation.invocation_id}"
                )

                if invocation.call not in calls:
                    calls.append(invocation.call)
                    logger.debug(f"Added call {len(calls)}: {invocation.call.call_id}")

                if len(calls) >= 50:
                    logger.info("Reached maximum of 50 calls")
                    break

            logger.info(
                f"Processed {invocation_count} invocations, found {len(calls)} unique calls"
            )

        except Exception as e:
            logger.error(f"Error processing invocations: {str(e)}")
            logger.error(traceback.format_exc())
            # Continue with whatever calls we found so far

        # Add some computed/helper properties for the template
        task_extra = {
            "module": task.func.__module__,
            "func_qualname": task.func.__qualname__,
            "retry_for": [
                e.__name__ for e in task.conf.retry_for
            ],  # Format the exception names for display
        }

        logger.info(f"Rendering template with {len(calls)} calls")
        return templates.TemplateResponse(
            "tasks/detail.html",
            {
                "request": request,
                "title": f"Task {task_id}",
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
