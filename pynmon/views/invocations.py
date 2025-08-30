import json
import logging
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Optional

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from pynenc.invocation.status import InvocationStatus
from pynmon.app import get_pynenc_instance, templates

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.call import Call
    from pynenc.invocation.dist_invocation import DistributedInvocation

router = APIRouter(prefix="/invocations", tags=["invocations"])
logger = logging.getLogger("pynmon.views.invocations")


@router.get("/", response_class=HTMLResponse)
async def invocations_list(
    request: Request,
    status: Optional[str] = None,
    task_id: Optional[str] = None,
    limit: int = 50,
) -> HTMLResponse:
    """Display invocations with optional filtering."""
    app = get_pynenc_instance()

    # Convert status to list format for consistent processing
    status_list = None
    if status:
        status_list = [status]

    # Convert status strings to InvocationStatus enum values
    statuses = None
    if status_list:
        statuses = [
            InvocationStatus[s.upper()]
            for s in status_list
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
                "status": status_list or [],
                "task_id": task_id or "",
                "limit": limit,
            },
        },
    )


def _parse_timeline_date_range(
    time_range: str, start_date: Optional[str], end_date: Optional[str]
) -> tuple[datetime, datetime]:
    """Parse the time range parameters into start and end datetime objects."""
    now = datetime.now(timezone.utc)
    end_datetime = now

    if time_range == "custom" and start_date and end_date:
        try:
            # Parse custom dates and convert to UTC if they don't have timezone info
            start_datetime = datetime.fromisoformat(start_date)
            if start_datetime.tzinfo is None:
                start_datetime = start_datetime.replace(tzinfo=timezone.utc)

            end_datetime = datetime.fromisoformat(end_date)
            if end_datetime.tzinfo is None:
                end_datetime = end_datetime.replace(tzinfo=timezone.utc)
        except ValueError:
            logger.warning(f"Invalid date format: {start_date} - {end_date}")
            # Fall back to last hour
            start_datetime = now - timedelta(hours=1)
    else:
        # Parse time range - using UTC time
        if time_range == "15m":
            start_datetime = now - timedelta(minutes=15)
        elif time_range == "1h":
            start_datetime = now - timedelta(hours=1)
        elif time_range == "3h":
            start_datetime = now - timedelta(hours=3)
        elif time_range == "12h":
            start_datetime = now - timedelta(hours=12)
        elif time_range == "1d":
            start_datetime = now - timedelta(days=1)
        elif time_range == "3d":
            start_datetime = now - timedelta(days=3)
        elif time_range == "1w":
            start_datetime = now - timedelta(weeks=1)
        else:
            # Default to last hour
            start_datetime = now - timedelta(hours=1)

    return start_datetime, end_datetime


def _get_tasks_to_check(app: "Pynenc", task_id: Optional[str]) -> list:
    """Get the list of tasks to check based on task_id filter.

    :param app: The Pynenc application instance
    :param task_id: Optional task ID to filter by
    :return: List of tasks to check
    :raises ValueError: If task_id is provided but the task doesn't exist
    """
    if task_id:
        try:
            task = app.get_task(task_id)
            return [task] if task else []
        except (ModuleNotFoundError, AttributeError, ValueError) as e:
            # Re-raise with a more descriptive error message
            if isinstance(e, ModuleNotFoundError):
                raise ValueError(
                    f"Task not found: Module '{task_id.rsplit('.', 1)[0]}' could not be imported. {str(e)}"
                ) from e
            elif isinstance(e, AttributeError):
                module_name = task_id.rsplit(".", 1)[0]
                function_name = task_id.rsplit(".", 1)[1]
                raise ValueError(
                    f"Task not found: Function '{function_name}' not found in module '{module_name}'. {str(e)}"
                ) from e
            else:
                raise ValueError(
                    f"Task not found: Invalid task ID format '{task_id}'. {str(e)}"
                ) from e
    return list(app.tasks.values())


def _collect_invocations_from_tasks(
    app: "Pynenc", tasks_to_check: list, limit: int
) -> list["DistributedInvocation"]:
    """Collect invocations from the specified tasks up to the limit."""
    all_invocations = []
    logger.info(f"Fetching invocations from {len(tasks_to_check)} tasks")

    for task in tasks_to_check:
        invocations = list(app.orchestrator.get_existing_invocations(task=task))
        all_invocations.extend(invocations)

        # Limit the number to avoid overloading the browser
        if len(all_invocations) >= limit:
            all_invocations = all_invocations[:limit]
            break

    logger.info(f"Found {len(all_invocations)} invocations before time filtering")
    return all_invocations


def _filter_invocations_by_time(
    app: "Pynenc",
    all_invocations: list["DistributedInvocation"],
    start_datetime: datetime,
    end_datetime: datetime,
    show_relationships: bool,
) -> list[dict[str, str | None]]:
    """Filter invocations based on time range and format for timeline display."""
    filtered_invocations = []

    for invocation in all_invocations:
        # Try to get history for the invocation
        history = app.state_backend.get_history(invocation)

        if not history:
            logger.debug(
                f"Invocation {invocation.invocation_id} has no history, skipping"
            )
            continue

        # Sort history by timestamp
        sorted_history = sorted(history, key=lambda entry: entry.timestamp)

        # Get start time (first entry)
        invocation_start_time = sorted_history[0].timestamp

        # Skip if outside our time range
        if (
            invocation_start_time < start_datetime
            or invocation_start_time > end_datetime
        ):
            logger.debug(
                f"Invocation {invocation.invocation_id} outside time range, skipping"
            )
            continue

        # Get end time (last entry if in final state)
        invocation_end_time = None
        if sorted_history[-1].status and sorted_history[-1].status.is_final():
            invocation_end_time = sorted_history[-1].timestamp

        # Find parent invocation if it exists
        parent_id = None
        if show_relationships and invocation.parent_invocation:
            parent_id = invocation.parent_invocation.invocation_id

        # Add to filtered list
        filtered_invocations.append(
            {
                "invocation_id": invocation.invocation_id,
                "task_id": invocation.task.task_id,
                "status": invocation.status.name,
                "start_time": invocation_start_time.isoformat(),
                "end_time": invocation_end_time.isoformat()
                if invocation_end_time
                else None,
                "parent_id": parent_id,
            }
        )

    # Sort by start time
    def get_start_time(inv: dict) -> str:
        start_time = inv.get("start_time")
        return start_time if isinstance(start_time, str) else ""

    filtered_invocations.sort(key=get_start_time)
    return filtered_invocations


@router.get("/timeline", response_class=HTMLResponse)
async def invocations_timeline(
    request: Request,
    time_range: str = "1h",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    task_id: Optional[str] = None,
    show_relationships: bool = False,
    limit: int = 100,
) -> HTMLResponse:
    """Display a visual timeline of invocations with filters."""
    app = get_pynenc_instance()
    logger.info(f"Generating invocation timeline with time_range={time_range}")
    start_time = time.time()

    try:
        # Parse date range parameters
        start_datetime, end_datetime = _parse_timeline_date_range(
            time_range, start_date, end_date
        )
        logger.info(
            f"Using time range: {start_datetime.isoformat()} to {end_datetime.isoformat()}"
        )

        # Get tasks to check - this may raise ValueError for invalid task_id
        try:
            tasks_to_check = _get_tasks_to_check(app, task_id)
        except ValueError as e:
            logger.warning(f"Invalid task ID: {task_id} - {str(e)}")
            return templates.TemplateResponse(
                "shared/error.html",
                {
                    "request": request,
                    "title": "Task Not Found",
                    "message": str(e),
                },
                status_code=404,
            )

        # Collect invocations from tasks
        all_invocations = _collect_invocations_from_tasks(app, tasks_to_check, limit)

        # Filter invocations by time range
        filtered_invocations = _filter_invocations_by_time(
            app, all_invocations, start_datetime, end_datetime, show_relationships
        )

        logger.info(
            f"After time filtering: {len(filtered_invocations)} invocations match criteria"
        )

        # Prepare timeline data for JavaScript
        timeline_data = {
            "invocations": filtered_invocations,
            "start_time": start_datetime.isoformat(),
            "end_time": end_datetime.isoformat(),
        }

        # Get all available task IDs for the dropdown
        all_task_ids = list(app.tasks.keys())

        logger.info(f"Rendering timeline with {len(filtered_invocations)} invocations")
        logger.info(f"Timeline data structure: {timeline_data}")
        logger.info(
            f"Filtered invocations structure: {filtered_invocations[:2] if filtered_invocations else 'empty'}"
        )

        # Try to serialize timeline_data to catch any JSON issues
        try:
            serialized_timeline_data = json.dumps(timeline_data)
            logger.info(
                f"Successfully serialized timeline_data, length: {len(serialized_timeline_data)}"
            )
        except Exception as e:
            logger.error(f"Failed to serialize timeline_data: {e}")
            timeline_data = {
                "invocations": [],
                "start_time": start_datetime.isoformat(),
                "end_time": end_datetime.isoformat(),
            }

        return templates.TemplateResponse(
            "invocations/timeline.html",
            {
                "request": request,
                "title": "Invocations Timeline",
                "app_id": app.app_id,
                "invocations": filtered_invocations,
                "timeline_data": json.dumps(timeline_data),
                "all_task_ids": all_task_ids,
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
                "current_filters": {
                    "time_range": time_range,
                    "start_date": start_date or "",
                    "end_date": end_date or "",
                    "task_id": task_id or "",
                    "show_relationships": show_relationships,
                    "limit": limit,
                },
            },
        )
    except Exception as e:
        logger.error(f"Error in invocations_timeline: {str(e)}")
        logger.error(traceback.format_exc())
        return templates.TemplateResponse(
            "shared/error.html",
            {
                "request": request,
                "title": "Error",
                "message": f"An error occurred while generating the timeline: {str(e)}",
            },
            status_code=500,
        )
    finally:
        elapsed = time.time() - start_time
        logger.info(f"invocations_timeline completed in {elapsed:.2f} seconds")


def _get_invocation_result_and_exception(
    app: "Pynenc", invocation: "DistributedInvocation", invocation_id: str
) -> tuple[str | None, str | None]:
    """Get formatted result or exception for an invocation."""
    formatted_result = None
    formatted_exception = None

    try:
        if invocation.status == InvocationStatus.SUCCESS:
            logger.info(f"Retrieving result for invocation {invocation_id}")
            result = app.state_backend.get_result(invocation)
            # Format the result for display
            if isinstance(result, (dict, list)):
                formatted_result = json.dumps(result, indent=2)
            else:
                formatted_result = str(result)
        elif invocation.status == InvocationStatus.FAILED:
            logger.info(f"Retrieving exception for invocation {invocation_id}")
            exception = app.state_backend.get_exception(invocation)
            # Format the exception for display
            formatted_exception = str(exception)
            if hasattr(exception, "__traceback__"):
                formatted_exception = "".join(
                    traceback.format_exception(
                        type(exception), exception, exception.__traceback__
                    )
                )
    except Exception as e:
        logger.error(f"Error retrieving result/exception: {str(e)}")
        formatted_exception = f"Error retrieving result/exception: {str(e)}"

    return formatted_result, formatted_exception


def _get_formatted_invocation_history(
    app: "Pynenc", invocation: "DistributedInvocation", invocation_id: str
) -> list[dict[str, str | None]]:
    """Get formatted history for an invocation with timeout protection."""
    try:
        logger.info(f"Retrieving history for invocation {invocation_id}")
        history_start = time.time()
        max_history_time = 3  # Maximum time to spend retrieving history (3 seconds)

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
        from functools import cmp_to_key

        def compare_timestamps(a: dict, b: dict) -> int:
            return -1 if a["timestamp"] < b["timestamp"] else 1

        formatted_history.sort(key=cmp_to_key(compare_timestamps))

        logger.info(
            f"Retrieved {len(formatted_history)} history entries in {time.time() - history_start:.2f}s"
        )
        return formatted_history
    except Exception as e:
        logger.error(f"Error retrieving history: {str(e)}")
        logger.error(traceback.format_exc())
        return []


def _get_invocation_timestamps_and_duration(
    formatted_history: list[dict[str, str | None]]
) -> tuple[str | None, str | None, float | None]:
    """Extract timestamps and calculate duration from formatted history."""
    created_at: str | None = "Unknown"
    completed_at = None
    duration_seconds = None

    if formatted_history:
        try:
            created_at = formatted_history[0]["timestamp"]

            # Check if the last entry is a terminal state
            if formatted_history[-1]["status"] in ["SUCCESS", "FAILED"]:
                completed_at = formatted_history[-1]["timestamp"]
        except (IndexError, KeyError) as e:
            logger.warning(f"Error processing history timestamps: {str(e)}")

    if created_at and completed_at:
        try:
            dt_created = datetime.fromisoformat(created_at)
            dt_completed = datetime.fromisoformat(completed_at)
            duration_seconds = (dt_completed - dt_created).total_seconds()
        except (ValueError, TypeError) as e:
            logger.warning(f"Error calculating duration: {str(e)}")

    return created_at, completed_at, duration_seconds


def _format_invocation_arguments(call: "Call") -> dict[str, str]:
    """Format invocation arguments for display."""
    try:
        formatted_arguments = {}
        max_length = 500  # Limit display length for very large values

        for key, value in call.arguments.kwargs.items():
            str_value = str(value)
            if len(str_value) > max_length:
                formatted_arguments[key] = f"{str_value[:max_length]}... (truncated)"
            else:
                formatted_arguments[key] = str_value

        return formatted_arguments
    except Exception as e:
        logger.error(f"Error formatting arguments: {str(e)}")
        return {"Error": f"Could not format arguments: {str(e)}"}


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

        # Get basic details
        task = invocation.task
        call = invocation.call

        # Get result and exception data
        formatted_result, formatted_exception = _get_invocation_result_and_exception(
            app, invocation, invocation_id
        )

        # Get history data
        formatted_history = _get_formatted_invocation_history(
            app, invocation, invocation_id
        )

        # Get timestamps and duration
        (
            created_at,
            completed_at,
            duration_seconds,
        ) = _get_invocation_timestamps_and_duration(formatted_history)

        # Format arguments
        formatted_arguments = _format_invocation_arguments(call)

        # Add additional info for the template
        task_extra = {
            "module": task.func.__module__,
            "func_qualname": task.func.__qualname__,
        }

        logger.info(
            f"Rendering invocation detail template in {time.time() - start_time:.2f}s"
        )
        return templates.TemplateResponse(
            "invocations/detail.html",
            {
                "request": request,
                "title": "Invocation Details",
                "app_id": app.app_id,
                "invocation": invocation,
                "call": call,
                "task": task,
                "task_extra": task_extra,
                "result": formatted_result,
                "exception": formatted_exception,
                "history": formatted_history,
                "arguments": formatted_arguments,
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


@router.get("/{invocation_id}/history")
async def invocation_history(request: Request, invocation_id: str) -> JSONResponse:
    """Return invocation history as JSON for timeline visualization."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving history for invocation {invocation_id} (API)")

    try:
        # Get the invocation
        invocation = app.orchestrator.get_invocation(invocation_id)
        if not invocation:
            return JSONResponse(
                {"error": f"No invocation found with ID: {invocation_id}"}, 404
            )

        # Get history
        history = app.state_backend.get_history(invocation)

        # Convert to format needed for visualization
        formatted_history: list[dict] = []
        for entry in history:
            # Make sure timestamp is timezone-aware for comparison
            timestamp = entry.timestamp
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)

            formatted_history.append(
                {
                    "timestamp": timestamp.isoformat(),
                    "status": entry.status.name if entry.status else "UNKNOWN",
                    "execution_context": entry.execution_context,
                }
            )

        # Sort by timestamp
        formatted_history.sort(key=lambda x: x["timestamp"])

        return JSONResponse(formatted_history)
    except Exception as e:
        logger.error(
            f"Error retrieving history for invocation {invocation_id}: {str(e)}"
        )
        logger.error(traceback.format_exc())
        return JSONResponse({"error": str(e)}, 500)


@router.get("/{invocation_id}/api")
async def invocation_api(request: Request, invocation_id: str) -> JSONResponse:
    """Return invocation data as JSON for the timeline visualization."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving API data for invocation {invocation_id}")

    try:
        # Get the invocation
        invocation = app.orchestrator.get_invocation(invocation_id)
        if not invocation:
            return JSONResponse(
                {"error": f"No invocation found with ID: {invocation_id}"}, 404
            )

        # Create a simplified representation for the API
        invocation_data = {
            "invocation_id": invocation.invocation_id,
            "task_id": invocation.task.task_id,
            "status": invocation.status.name,
            "num_retries": invocation.num_retries,
            "parent_invocation_id": invocation.parent_invocation.invocation_id
            if invocation.parent_invocation
            else None,
        }

        return JSONResponse(invocation_data)
    except Exception as e:
        logger.error(
            f"Error retrieving API data for invocation {invocation_id}: {str(e)}"
        )
        logger.error(traceback.format_exc())
        return JSONResponse({"error": str(e)}, 500)


@router.get("/table", response_class=HTMLResponse)
async def invocations_table(
    request: Request,
    status: Optional[str] = None,
    task_id: Optional[str] = None,
    limit: int = 50,
) -> HTMLResponse:
    """Return just the invocations table for HTMX refresh."""
    # This is essentially the same logic as invocations_list but returns only the table partial
    app = get_pynenc_instance()

    # Parse status parameter
    status_list = None
    if status:
        status_list = [status]

    # Convert to InvocationStatus objects
    statuses = None
    if status_list:
        statuses = []
        for status_str in status_list:
            try:
                statuses.append(InvocationStatus[status_str.upper()])
            except KeyError:
                continue

    all_invocations = []

    if task_id:
        # Get invocations from specific task
        if task_id in app.tasks:
            task = app.tasks[task_id]
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

    return templates.TemplateResponse(
        "invocations/partials/table.html",
        {
            "request": request,
            "invocations": all_invocations,
        },
    )
