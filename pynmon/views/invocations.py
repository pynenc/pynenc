import json
import logging
import time
import traceback
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from pynenc.exceptions import InvocationNotFoundError
from pynenc.identifiers.invocation_id import InvocationId
from pynenc.identifiers.task_id import TaskId
from pynenc.invocation.status import InvocationStatus
from pynmon.app import get_pynenc_instance, templates
from pynmon.util.formatting import RunnerContextInfo
from pynmon.util.time_ranges import parse_time_range, parse_resolution
from pynmon.util.view_helpers import format_call_arguments
from pynmon.util.svg.builder import TimelineDataBuilder
from pynmon.util.svg.models import TimelineConfig
from pynmon.util.svg.renderer import TimelineSVGRenderer

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.runner.runner_context import RunnerContext

router = APIRouter(prefix="/invocations", tags=["invocations"])
logger = logging.getLogger("pynmon.views.invocations")


@router.get("/", response_class=HTMLResponse)
async def invocations_list(
    request: Request,
    status: str | None = None,
    task_id_key: str | None = None,
    limit: int = 50,
    page: int = 1,
) -> HTMLResponse:
    """Display invocations with optional filtering and pagination."""
    app = get_pynenc_instance()
    task_id = TaskId.from_key(task_id_key) if task_id_key else None

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

    # Calculate offset for pagination
    offset = (page - 1) * limit

    # Use paginated query for efficient data retrieval
    all_invocation_ids = app.orchestrator.get_invocation_ids_paginated(
        task_id=task_id,
        statuses=statuses,
        limit=limit,
        offset=offset,
    )

    # Get total count for pagination info
    total_count = app.orchestrator.count_invocations(
        task_id=task_id,
        statuses=statuses,
    )
    total_pages = (total_count + limit - 1) // limit if limit > 0 else 1

    # Parse actual invocation objects
    all_invocations = [
        app.state_backend.get_invocation(inv_id) for inv_id in all_invocation_ids
    ]

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
            "pagination": {
                "page": page,
                "limit": limit,
                "total_count": total_count,
                "total_pages": total_pages,
                "has_prev": page > 1,
                "has_next": page < total_pages,
            },
        },
    )


def _get_tasks_to_check(app: "Pynenc", task_id: "TaskId | None") -> list:
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
                    f"Task not found: Module '{task_id.module}' could not be imported. {str(e)}"
                ) from e
            elif isinstance(e, AttributeError):
                module_name = task_id.module
                function_name = task_id.func_name
                raise ValueError(
                    f"Task not found: Function '{function_name}' not found in module '{module_name}'. {str(e)}"
                ) from e
            else:
                raise ValueError(
                    f"Task not found: Invalid task ID format '{task_id}'. {str(e)}"
                ) from e
    return list(app.tasks.values())


def _build_svg_timeline(
    app: "Pynenc",
    start_datetime: datetime,
    end_datetime: datetime,
    limit: int = 100,
    resolution_seconds: int | None = None,
) -> str:
    """
    Build SVG timeline using iter_history_in_timerange.

    Uses the new efficient time-range iteration for history entries.

    :param Pynenc app: The Pynenc application instance
    :param datetime start_datetime: Start of the time range
    :param datetime end_datetime: End of the time range
    :param int limit: Maximum number of invocations to include
    :param int | None resolution_seconds: Tick interval in seconds (None for auto)
    :return: SVG string for the timeline
    """
    config = TimelineConfig(resolution_seconds=resolution_seconds)
    builder = TimelineDataBuilder(config=config)

    # Count invocations seen to enforce limit
    invocations_seen: set[str] = set()
    history_count = 0

    runner_contexts: dict[str, RunnerContext] = {}
    for batch in app.state_backend.iter_history_in_timerange(
        start_time=start_datetime,
        end_time=end_datetime,
        batch_size=100,
    ):
        # Filter batch by limit
        filtered_batch = []
        required_runner_context_ids: set[str] = set()
        for history in batch:
            if history.invocation_id not in invocations_seen:
                if len(invocations_seen) >= limit:
                    break
                invocations_seen.add(history.invocation_id)
            filtered_batch.append(history)
            if history.runner_context_id not in runner_contexts:
                required_runner_context_ids.add(history.runner_context_id)
            history_count += 1

        if required_runner_context_ids:
            for new_ctx in app.state_backend.get_runner_contexts(
                list(required_runner_context_ids)
            ):
                runner_contexts[new_ctx.runner_id] = new_ctx

        if filtered_batch:
            builder.add_history_batch(filtered_batch, runner_contexts)

        # Stop if we've hit the limit
        if len(invocations_seen) >= limit:
            break

    logger.info(
        f"Built timeline with {len(invocations_seen)} invocations, "
        f"{history_count} history entries"
    )

    # Build and render timeline using the full requested time range
    timeline_data = builder.build(start_time=start_datetime, end_time=end_datetime)
    renderer = TimelineSVGRenderer()
    return renderer.render(timeline_data)


@router.get("/timeline", response_class=HTMLResponse)
async def invocations_timeline(
    request: Request,
    time_range: str = "1h",
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 100,
    resolution: str = "auto",
) -> HTMLResponse:
    """Display a visual SVG timeline of invocations with filters."""
    app = get_pynenc_instance()
    logger.info(f"Generating invocation timeline with time_range={time_range}")
    start_time = time.time()

    try:
        # Parse date range and resolution parameters
        start_datetime, end_datetime = parse_time_range(
            time_range, start_date, end_date
        )
        resolution_seconds = parse_resolution(resolution)
        logger.info(
            f"Using time range: {start_datetime.isoformat()} to {end_datetime.isoformat()}"
        )

        # Build SVG timeline using efficient history iteration
        svg_content = _build_svg_timeline(
            app,
            start_datetime,
            end_datetime,
            limit=limit,
            resolution_seconds=resolution_seconds,
        )

        # Get all available task IDs for the dropdown
        all_task_ids = list(app.tasks.keys())

        return templates.TemplateResponse(
            "invocations/timeline.html",
            {
                "request": request,
                "title": "Invocations Timeline",
                "app_id": app.app_id,
                "svg_content": svg_content,
                "all_task_ids": all_task_ids,
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
                "current_filters": {
                    "time_range": time_range,
                    "start_date": start_date or "",
                    "end_date": end_date or "",
                    "limit": limit,
                    "resolution": resolution,
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
            result = app.state_backend.get_result(invocation.invocation_id)
            # Format the result for display
            if isinstance(result, dict | list):
                formatted_result = json.dumps(result, indent=2)
            else:
                formatted_result = str(result)
        elif invocation.status == InvocationStatus.FAILED:
            logger.info(f"Retrieving exception for invocation {invocation_id}")
            exception = app.state_backend.get_exception(invocation.invocation_id)
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
) -> list[dict]:
    """Get formatted history for an invocation with timeout protection."""
    try:
        logger.info(f"Retrieving history for invocation {invocation_id}")
        history_start = time.time()

        # Get history
        history = app.state_backend.get_history(invocation.invocation_id)

        # Collect all unique runner context IDs
        runner_context_ids = list({entry.runner_context_id for entry in history})

        # Batch-load all runner contexts
        runner_contexts_list = app.state_backend.get_runner_contexts(runner_context_ids)
        runner_contexts = {ctx.runner_id: ctx for ctx in runner_contexts_list if ctx}

        # Log any missing contexts
        missing_ids = set(runner_context_ids) - set(runner_contexts.keys())
        if missing_ids:
            logger.warning(
                f"Missing runner contexts for invocation {invocation_id}: "
                f"{', '.join(missing_ids)}"
            )

        # Convert history items to a more template-friendly format
        formatted_history = []
        for entry in history:
            # Get runner context, may be None if not found
            runner_context = runner_contexts.get(entry.runner_context_id)
            context_info = _format_owner_context(
                runner_context, entry.runner_context_id
            )

            formatted_history.append(
                {
                    "timestamp": entry.timestamp.isoformat(),
                    "status": entry.status_record.status.name,
                    "status_runner_id": entry.status_record.runner_id,
                    "runner_context_summary": context_info["summary"],
                    "runner_cls": context_info["runner_cls"],
                    "runner_id": context_info["runner_id"],
                    "hostname": context_info["hostname"],
                    "pid": context_info["pid"],
                    "thread_id": context_info["thread_id"],
                    "parent_runner_cls": context_info["parent_runner_cls"],
                    "parent_runner_id": context_info["parent_runner_id"],
                    "parent_hostname": context_info["parent_hostname"],
                    "parent_pid": context_info["parent_pid"],
                    "parent_thread_id": context_info["parent_thread_id"],
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


def _format_owner_context(
    owner_context: "RunnerContext | None", runner_context_id: str | None = None
) -> dict[str, str | None]:
    """Format owner context into display-friendly dict using RunnerContextInfo.

    Handles None contexts gracefully, logging a warning if context is missing.

    :param owner_context: The runner context to format, or None if not found
    :param runner_context_id: The ID that was attempted (for logging), or None
    :return: Formatted context dictionary with N/A values if context is missing
    """
    if owner_context is None and runner_context_id:
        logger.warning(
            f"Runner context not found for ID: {runner_context_id}. "
            f"Displaying N/A values."
        )
    return RunnerContextInfo.from_context(owner_context).to_dict()


def _get_invocation_timestamps_and_duration(
    formatted_history: list[dict[str, str | None]],
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


@router.get("/{invocation_id}", response_class=HTMLResponse)
async def invocation_detail(
    request: Request, invocation_id: "InvocationId"
) -> HTMLResponse:
    """Display detailed information about a specific invocation."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving details for invocation: {invocation_id}")
    start_time = time.time()

    try:
        # Use the direct method to get the invocation
        invocation = app.state_backend.get_invocation(invocation_id)

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

        formatted_arguments = format_call_arguments(call)

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
    except InvocationNotFoundError:
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
async def invocation_history(
    request: Request, invocation_id: "InvocationId"
) -> JSONResponse:
    """Return invocation history as JSON for timeline visualization."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving history for invocation {invocation_id} (API)")

    try:
        # Get the invocation
        invocation = app.state_backend.get_invocation(invocation_id)
        if not invocation:
            return JSONResponse(
                {"error": f"No invocation found with ID: {invocation_id}"}, 404
            )

        # Get history
        history = app.state_backend.get_history(invocation.invocation_id)

        # Convert to format needed for visualization
        formatted_history: list[dict] = []
        for entry in history:
            # Make sure timestamp is timezone-aware for comparison
            timestamp = entry.timestamp
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)

            # Get full context info from runner_context
            runner_context = app.state_backend.get_runner_context(
                entry.runner_context_id
            )
            context_info = _format_owner_context(runner_context)

            formatted_history.append(
                {
                    "timestamp": timestamp.isoformat(),
                    "status": entry.status_record.status.value,
                    "status_runner_id": entry.status_record.runner_id,
                    "runner_context_summary": context_info["summary"],
                    "runner_cls": context_info["runner_cls"],
                    "runner_id": context_info["runner_id"],
                    "hostname": context_info["hostname"],
                    "pid": context_info["pid"],
                    "thread_id": context_info["thread_id"],
                    "parent_runner_cls": context_info["parent_runner_cls"],
                    "parent_runner_id": context_info["parent_runner_id"],
                    "parent_hostname": context_info["parent_hostname"],
                    "parent_pid": context_info["parent_pid"],
                    "parent_thread_id": context_info["parent_thread_id"],
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
async def invocation_api(
    request: Request, invocation_id: "InvocationId"
) -> JSONResponse:
    """Return invocation data as JSON for the timeline visualization."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving API data for invocation {invocation_id}")

    try:
        # Get the invocation
        invocation = app.state_backend.get_invocation(invocation_id)
        if not invocation:
            return JSONResponse(
                {"error": f"No invocation found with ID: {invocation_id}"}, 404
            )

        # Create a simplified representation for the API
        invocation_data = {
            "invocation_id": invocation.invocation_id,
            "task_id_key": invocation.task.task_id.key,
            "status": invocation.status.name,
            "num_retries": invocation.num_retries,
            "parent_invocation_id": invocation.parent_invocation_id,
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
    status: str | None = None,
    task_id_key: str | None = None,
    limit: int = 50,
) -> HTMLResponse:
    """Return just the invocations table for HTMX refresh."""
    # This is essentially the same logic as invocations_list but returns only the table partial
    app = get_pynenc_instance()
    task_id = TaskId.from_key(task_id_key) if task_id_key else None

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
