import asyncio
import json
import logging
import time
import traceback
from dataclasses import dataclass, field
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


@dataclass
class TimelineRequest:
    """Grouped parameters for building an SVG timeline."""

    app: "Pynenc"
    start_time: datetime
    end_time: datetime
    config: "TimelineConfig"
    limit: int | None
    task_id: "TaskId | None" = None


@dataclass
class _IterState:
    """Mutable accumulation state for history iteration."""

    builder: "TimelineDataBuilder"
    runner_contexts: dict = field(default_factory=dict)
    invocations_seen: set = field(default_factory=set)
    history_count: int = 0
    allowed_invocation_ids: set | None = None
    skipped_by_task_filter: int = 0
    batch_number: int = 0


router = APIRouter(prefix="/invocations", tags=["invocations"])
logger = logging.getLogger("pynmon.views.invocations")


@router.get("/", response_class=HTMLResponse)
async def invocations_list(
    request: Request,
    status: str | None = None,
    task_id: str | None = None,
    limit: int = 50,
    page: int = 1,
) -> HTMLResponse:
    """Display invocations with optional filtering and pagination."""
    app = get_pynenc_instance()
    parsed_task_id = TaskId.from_key(task_id) if task_id else None

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

    # Offload heavy DB queries to a thread so the event loop stays free
    def _fetch_invocations() -> tuple[list["DistributedInvocation"], int]:
        ids = app.orchestrator.get_invocation_ids_paginated(
            task_id=parsed_task_id,
            statuses=statuses,
            limit=limit,
            offset=offset,
        )
        count = app.orchestrator.count_invocations(
            task_id=parsed_task_id,
            statuses=statuses,
        )
        invocations = [app.state_backend.get_invocation(inv_id) for inv_id in ids]
        return invocations, count

    all_invocations, total_count = await asyncio.to_thread(_fetch_invocations)
    total_pages = (total_count + limit - 1) // limit if limit > 0 else 1

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


def _build_svg_timeline(req: TimelineRequest) -> str:
    """
    Build SVG timeline from a TimelineRequest.

    :param TimelineRequest req: Grouped parameters
    :return: SVG markup as string
    """
    state = _IterState(
        builder=TimelineDataBuilder(config=req.config),
        allowed_invocation_ids=_load_task_invocation_ids(req),
    )
    _accumulate_history(req, state)
    logger.info(
        f"Built timeline with {len(state.invocations_seen)} invocations, "
        f"{state.history_count} history entries"
        + (
            f", {state.skipped_by_task_filter} skipped by task filter"
            if state.skipped_by_task_filter
            else ""
        )
    )
    data = state.builder.build(start_time=req.start_time, end_time=req.end_time)
    return TimelineSVGRenderer().render(data)


def _load_task_invocation_ids(req: TimelineRequest) -> set | None:
    """
    Pre-load invocation IDs for a task to enable client-side filtering.

    :param TimelineRequest req: Request with optional task_id
    :return: Set of invocation IDs if task_id is set, else None (no filter)
    """
    if not req.task_id:
        return None
    load_start = time.time()
    ids = set(req.app.orchestrator.get_task_invocation_ids(req.task_id))
    logger.info(
        f"Pre-loaded {len(ids)} invocation IDs for task {req.task_id} "
        f"in {time.time() - load_start:.2f}s"
    )
    return ids


def _accumulate_history(req: TimelineRequest, state: _IterState) -> None:
    """
    Iterate history in batches and accumulate into state.

    :param TimelineRequest req: Request parameters
    :param _IterState state: Mutable accumulation state
    """
    for batch in req.app.state_backend.iter_history_in_timerange(
        start_time=req.start_time,
        end_time=req.end_time,
        batch_size=2000,
    ):
        state.batch_number += 1
        if state.batch_number % 5 == 0:
            logger.debug(
                f"Processing batch {state.batch_number}: "
                f"{len(state.invocations_seen)} invocations, "
                f"{state.history_count} history entries so far"
            )
        if not _process_batch(batch, req, state):
            break


def _process_batch(batch: list, req: TimelineRequest, state: _IterState) -> bool:
    """
    Filter, fetch contexts, and add batch to builder.

    :return: False to signal early termination
    """
    filtered = _filter_batch(batch, req.limit, state)
    _fetch_new_runner_contexts(filtered, req.app, state)
    if filtered:
        state.builder.add_history_batch(filtered, state.runner_contexts)
    return not (req.limit and len(state.invocations_seen) >= req.limit)


def _filter_batch(batch: list, limit: int | None, state: _IterState) -> list:
    """
    Filter batch entries respecting invocation limit and task filter.

    :return: Filtered list of history entries
    """
    result = []
    for entry in batch:
        # Skip entries not matching task filter
        if state.allowed_invocation_ids is not None:
            if entry.invocation_id not in state.allowed_invocation_ids:
                state.skipped_by_task_filter += 1
                continue

        if entry.invocation_id not in state.invocations_seen:
            if limit and len(state.invocations_seen) >= limit:
                break
            state.invocations_seen.add(entry.invocation_id)
        result.append(entry)
        state.history_count += 1
    return result


def _fetch_new_runner_contexts(batch: list, app: "Pynenc", state: _IterState) -> None:
    """
    Fetch and cache any runner contexts not yet in state.

    :param batch: Filtered history entries
    :param app: Pynenc application instance
    :param state: Mutable accumulation state
    """
    new_ids = [
        h.runner_context_id
        for h in batch
        if h.runner_context_id not in state.runner_contexts
    ]
    if not new_ids:
        return
    for ctx in app.state_backend.get_runner_contexts(new_ids):
        state.runner_contexts[ctx.runner_id] = ctx


@router.get("/timeline", response_class=HTMLResponse)
async def invocations_timeline(
    request: Request,
    time_range: str = "5m",
    start_date: str | None = None,
    end_date: str | None = None,
    task_id: str | None = None,
    limit: str
    | None = "500",  # str so empty string ("No limit") doesn't cause int parse error
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
        # Empty string means "No limit" from the select; None or empty → unlimited
        limit_int: int | None = int(limit) if limit and limit.strip() else None

        parsed_task_id = TaskId.from_key(task_id) if task_id else None

        logger.info(
            f"Using time range: {start_datetime.isoformat()} to {end_datetime.isoformat()}"
            + (f", task_id={parsed_task_id}" if parsed_task_id else "")
            + f", limit={limit_int}"
        )

        # Build SVG timeline using efficient history iteration (offload to thread)
        config = TimelineConfig(resolution_seconds=resolution_seconds)
        req = TimelineRequest(
            app=app,
            start_time=start_datetime,
            end_time=end_datetime,
            config=config,
            limit=limit_int,
            task_id=parsed_task_id,
        )
        svg_content = await asyncio.to_thread(_build_svg_timeline, req)

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
                    "task_id": task_id or "",
                    "limit": limit_int,
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

        # Get workflow identity
        workflow = None
        try:
            workflow = invocation.workflow
        except Exception:
            logger.debug("No workflow identity for invocation %s", invocation_id)

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
                "workflow": workflow,
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
        invocation_data: dict = {
            "invocation_id": invocation.invocation_id,
            "task_id_key": invocation.task.task_id.key,
            "status": invocation.status.name,
            "num_retries": invocation.num_retries,
            "parent_invocation_id": invocation.parent_invocation_id,
        }

        # Add workflow information if available
        try:
            wf = invocation.workflow
            invocation_data["workflow"] = {
                "workflow_id": str(wf.workflow_id),
                "workflow_type": str(wf.workflow_type),
                "parent_workflow_id": str(wf.parent_workflow_id)
                if wf.parent_workflow_id
                else None,
                "is_subworkflow": wf.is_subworkflow,
            }
        except Exception:
            invocation_data["workflow"] = None

        return JSONResponse(invocation_data)
    except Exception as e:
        logger.error(
            f"Error retrieving API data for invocation {invocation_id}: {str(e)}"
        )
        logger.error(traceback.format_exc())
        return JSONResponse({"error": str(e)}, 500)


@router.post("/{invocation_id}/rerun")
async def rerun_invocation(invocation_id: "InvocationId") -> JSONResponse:
    """Re-run the same call as a brand-new invocation (unrelated to the original).

    Reads the original invocation's call (task + arguments) and routes a fresh
    call through the orchestrator, producing a new independent invocation.
    """
    app = get_pynenc_instance()
    logger.info(f"Re-running call for invocation {invocation_id}")

    try:
        invocation = app.state_backend.get_invocation(invocation_id)
        call = invocation.call
        task = invocation.task

        # Route a fresh call through the task (produces a new invocation)
        new_invocation = await asyncio.to_thread(task._call, call.arguments)

        return JSONResponse(
            {
                "success": True,
                "new_invocation_id": str(new_invocation.invocation_id),
                "message": "New invocation created successfully.",
            }
        )
    except InvocationNotFoundError:
        return JSONResponse(
            {"success": False, "message": f"Invocation {invocation_id} not found."},
            status_code=404,
        )
    except Exception as e:
        logger.error(f"Error re-running invocation {invocation_id}: {e}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            {"success": False, "message": f"Error: {str(e)}"},
            status_code=500,
        )


@router.get("/table", response_class=HTMLResponse)
async def invocations_table(
    request: Request,
    status: str | None = None,
    task_id: str | None = None,
    limit: int = 50,
) -> HTMLResponse:
    """Return just the invocations table for HTMX refresh."""
    # This is essentially the same logic as invocations_list but returns only the table partial
    app = get_pynenc_instance()
    parsed_task_id = TaskId.from_key(task_id) if task_id else None

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

    def _fetch_table_invocations() -> list[InvocationId]:
        result = []
        if parsed_task_id:
            if parsed_task_id in app.tasks:
                task = app.tasks[parsed_task_id]
                result = list(
                    app.orchestrator.get_existing_invocations(
                        task=task,
                        statuses=statuses,
                    )
                )[:limit]
        else:
            for task in app.tasks.values():
                invocations = list(
                    app.orchestrator.get_existing_invocations(
                        task=task,
                        statuses=statuses,
                    )
                )
                result.extend(invocations)
                if len(result) >= limit:
                    result = result[:limit]
                    break
        return result

    all_invocations = await asyncio.to_thread(_fetch_table_invocations)

    return templates.TemplateResponse(
        "invocations/partials/table.html",
        {
            "request": request,
            "invocations": all_invocations,
        },
    )
