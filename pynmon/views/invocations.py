import logging
import time
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynenc.invocation.status import InvocationStatus
from pynmon.app import get_pynenc_instance, templates

router = APIRouter(prefix="/invocations", tags=["invocations"])
logger = logging.getLogger("pynmon.views.invocations")


@router.get("/", response_class=HTMLResponse)
async def invocations_list(
    request: Request,
    status: Optional[list[str]] = None,
    task_id: Optional[str] = None,
    limit: int = 50,
) -> HTMLResponse:
    """Display invocations with optional filtering."""
    app = get_pynenc_instance()

    # Convert status strings to InvocationStatus enum values
    statuses = None
    if status:
        statuses = [
            InvocationStatus[s.upper()]
            for s in status
            if hasattr(InvocationStatus, s.upper())
        ]

    # Get all invocations across all tasks
    all_invocations = []

    # Find task by ID if specified
    if task_id:
        task = app.get_task(task_id)
        if task:
            # Get invocations for this specific task with filters
            all_invocations = list(
                app.orchestrator.get_existing_invocations(
                    task=task,
                    statuses=statuses,
                )
            )[:limit]
    else:
        # Get invocations from all tasks
        for task in app.tasks.values():
            invocations = list(
                app.orchestrator.get_existing_invocations(
                    task=task,
                    statuses=statuses,
                )
            )
            all_invocations.extend(invocations)
            if len(all_invocations) >= limit:
                all_invocations = all_invocations[:limit]
                break

    # Get all possible statuses for filter dropdown
    all_statuses = [status.name.lower() for status in InvocationStatus]

    # Get all available task IDs for the dropdown
    all_task_ids = list(app.tasks.keys())

    return templates.TemplateResponse(
        "invocations/list.html",
        {
            "request": request,
            "title": "Invocations Monitor",
            "app_id": app.app_id,
            "invocations": all_invocations,
            "all_statuses": all_statuses,
            "all_task_ids": all_task_ids,
            "current_filters": {
                "status": status or [],
                "task_id": task_id or "",
                "limit": limit,
            },
        },
    )


@router.get("/{invocation_id}", response_class=HTMLResponse)
async def invocation_detail(request: Request, invocation_id: str) -> HTMLResponse:
    """Display detailed information about a specific invocation."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving details for invocation: {invocation_id}")
    start_time = time.time()

    try:
        # Use the direct method to get the invocation
        invocation = app.orchestrator.get_invocation(invocation_id)

        if not invocation:
            logger.warning(f"No invocation found with ID: {invocation_id}")
            return templates.TemplateResponse(
                "shared/error.html",
                {
                    "request": request,
                    "title": "Invocation Not Found",
                    "message": f"No invocation found with ID: {invocation_id}",
                },
                status_code=404,
            )

        # Get the task from the invocation
        task = invocation.task

        # Get result or exception if available
        result = None
        exception = None
        formatted_result = None
        formatted_exception = None

        try:
            if invocation.status == InvocationStatus.SUCCESS:
                logger.info(f"Retrieving result for invocation {invocation_id}")
                result = app.state_backend.get_result(invocation)
                # Format the result for display
                if isinstance(result, (dict, list)):
                    import json

                    formatted_result = json.dumps(result, indent=2)
                else:
                    formatted_result = str(result)
            elif invocation.status == InvocationStatus.FAILED:
                logger.info(f"Retrieving exception for invocation {invocation_id}")
                exception = app.state_backend.get_exception(invocation)
                # Format the exception for display
                formatted_exception = str(exception)
                if hasattr(exception, "__traceback__"):
                    import traceback

                    formatted_exception = "".join(
                        traceback.format_exception(
                            type(exception), exception, exception.__traceback__
                        )
                    )
        except Exception as e:
            logger.error(f"Error retrieving result/exception: {str(e)}")
            formatted_exception = f"Error retrieving result/exception: {str(e)}"

        # Get history from state backend with timeout protection
        history = []
        try:
            logger.info(f"Retrieving history for invocation {invocation_id}")
            history_start = time.time()
            # Maximum time to spend retrieving history (3 seconds)
            max_history_time = 3

            # Get history with a timeout
            history = app.state_backend.get_history(invocation)

            # Convert history items to a more template-friendly format
            formatted_history = []
            for entry in history:
                if time.time() - history_start > max_history_time:
                    logger.warning("History retrieval timeout reached")
                    break

                formatted_history.append(
                    {
                        "timestamp": entry.timestamp.isoformat(),
                        "status": entry.status.name if entry.status else "UNKNOWN",
                        "execution_context": entry.execution_context,
                    }
                )

            # Sort history by timestamp using explicit string comparison
            # to satisfy mypy's type checking
            from functools import cmp_to_key

            def compare_timestamps(a: dict, b: dict) -> int:
                return -1 if a["timestamp"] < b["timestamp"] else 1

            formatted_history.sort(key=cmp_to_key(compare_timestamps))

            logger.info(
                f"Retrieved {len(formatted_history)} history entries in {time.time() - history_start:.2f}s"
            )
        except Exception as e:
            logger.error(f"Error retrieving history: {str(e)}")
            import traceback

            logger.error(traceback.format_exc())

        # Get call details
        call = invocation.call

        # Add additional info for the template
        task_extra = {
            "module": task.func.__module__,
            "func_qualname": task.func.__qualname__,
        }

        # Safely get timestamps
        created_at: str | None = "Unknown"
        completed_at = None

        if formatted_history:
            try:
                created_at = formatted_history[0]["timestamp"]

                # Check if the last entry is a terminal state
                if formatted_history[-1]["status"] in ["SUCCESS", "FAILED"]:
                    completed_at = formatted_history[-1]["timestamp"]
            except (IndexError, KeyError) as e:
                logger.warning(f"Error processing history timestamps: {str(e)}")

        duration_seconds = None
        if created_at and completed_at:
            try:
                dt_created = datetime.fromisoformat(created_at)
                dt_completed = datetime.fromisoformat(completed_at)
                duration_seconds = (dt_completed - dt_created).total_seconds()
            except (ValueError, TypeError) as e:
                logger.warning(f"Error calculating duration: {str(e)}")

        # Then in the template context:

        logger.info(
            f"Rendering invocation detail template in {time.time() - start_time:.2f}s"
        )
        return templates.TemplateResponse(
            "invocations/detail.html",
            {
                "request": request,
                "title": f"Invocation {invocation_id}",
                "app_id": app.app_id,
                "invocation": invocation,
                "call": call,
                "task": task,
                "task_extra": task_extra,
                "result": formatted_result,
                "exception": formatted_exception,
                "history": formatted_history,
                "serialized_arguments": call.serialized_arguments,
                "created_at": created_at,
                "completed_at": completed_at,
                "duration": f"{duration_seconds:.2f} seconds"
                if duration_seconds is not None
                else None,
            },
        )
    except Exception as e:
        logger.error(f"Unexpected error in invocation_detail: {str(e)}")
        import traceback

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
        logger.info(f"invocation_detail completed in {elapsed:.2f} seconds")
