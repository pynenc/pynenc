import json
import logging
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

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
        # Calculate time range based on selection - USING UTC TIME
        now = datetime.now(timezone.utc)  # Use UTC time to match invocation timestamps
        start_datetime = None
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

        logger.info(
            f"Using time range: {start_datetime.isoformat()} to {end_datetime.isoformat()}"
        )

        # Get all invocations across all tasks or specific task
        all_invocations = []
        tasks_to_check = []

        if task_id:
            task = app.get_task(task_id)
            if task:
                tasks_to_check = [task]
        else:
            tasks_to_check = list(app.tasks.values())

        # Get invocations from selected tasks
        logger.info(f"Fetching invocations from {len(tasks_to_check)} tasks")
        for task in tasks_to_check:
            invocations = list(app.orchestrator.get_existing_invocations(task=task))
            all_invocations.extend(invocations)

            # Limit the number to avoid overloading the browser
            if len(all_invocations) >= limit:
                all_invocations = all_invocations[:limit]
                break

        logger.info(f"Found {len(all_invocations)} invocations before time filtering")

        # Filter invocations based on time
        filtered_invocations = []
        for invocation in all_invocations:
            # Try to get history for the invocation
            history = app.state_backend.get_history(invocation)

            if not history:
                # Skip invocations with no history
                logger.debug(
                    f"Invocation {invocation.invocation_id} has no history, skipping"
                )
                continue

            # Sort history by timestamp
            sorted_history = sorted(history, key=lambda entry: entry.timestamp)

            # Get start time (first entry)
            invocation_start_time = sorted_history[0].timestamp

            # Add debug logging
            logger.debug(
                f"Invocation {invocation.invocation_id} start time: {invocation_start_time.isoformat()}, "
                f"filter range: {start_datetime.isoformat()} to {end_datetime.isoformat()}"
            )

            # Skip if outside our time range - use proper timezone-aware comparison
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
            # Ensure we always return a comparable string
            start_time = inv.get("start_time")
            return start_time if isinstance(start_time, str) else ""

        # Sort by start time, safely handling None values
        filtered_invocations.sort(key=get_start_time)

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

        # Format the arguments for display
        formatted_arguments = {}
        try:
            # Use the original arguments instead of serialized arguments
            # This shows the actual values in a human-readable format
            for key, value in call.arguments.kwargs.items():
                # Convert values to string format with simple truncation for long values
                max_length = 500  # Limit display length for very large values
                str_value = str(value)
                if len(str_value) > max_length:
                    formatted_arguments[
                        key
                    ] = f"{str_value[:max_length]}... (truncated)"
                else:
                    formatted_arguments[key] = str_value
        except Exception as e:
            logger.error(f"Error formatting arguments: {str(e)}")
            formatted_arguments = {"Error": f"Could not format arguments: {str(e)}"}

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
