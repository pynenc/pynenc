import logging
import time
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynenc.call import CallId

from pynmon.app import get_pynenc_instance, templates
from pynmon.util.view_helpers import (
    format_serialized_arguments,
    render_error_response,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.call import Call, CallId
    from pynenc.invocation import DistributedInvocation
    from pynenc.task import Task

router = APIRouter(prefix="/calls", tags=["calls"])
logger = logging.getLogger("pynmon.views.calls")


def _check_timeout_for_call_search(
    start_time: float, timeout: int, task_index: int, task_count: int
) -> bool:
    """Check if timeout has been reached during call search."""
    elapsed = time.time() - start_time
    if elapsed > timeout:
        logger.warning(
            f"Timeout reached after {elapsed:.2f}s when searching for call (checked {task_index}/{task_count} tasks)"
        )
        return True
    return False


def _search_invocations_in_task(
    app: "Pynenc", task: "Task", call_id: "CallId", task_timeout: float
) -> tuple[list["DistributedInvocation"], Any, bool]:
    """
    Search for invocations matching call_id in a specific task.

    :param app: The Pynenc application instance
    :param task: The task to search in
    :param call_id: The call ID to search for
    :param task_timeout: Maximum time to spend on this task
    :return: Tuple of (matching_invocations, target_call, timeout_reached)
    """
    invocations: list = []
    target_call = None
    invocation_count = 0
    task_start = time.time()

    for invocation_id in app.orchestrator.get_existing_invocations(task=task):
        invocation_count += 1

        # Check timeout for each invocation batch (every 10 invocations)
        if invocation_count % 10 == 0 and time.time() - task_start > task_timeout:
            logger.warning(
                f"Task timeout reached after checking {invocation_count} invocations for task {task.task_id}"
            )
            return invocations, target_call, True

        invocation = app.state_backend.get_invocation(invocation_id)

        # Check if this invocation has the call we're looking for
        if (
            hasattr(invocation, "call")
            and invocation.call
            and hasattr(invocation.call, "call_id")
            and invocation.call.call_id == call_id
        ):
            logger.debug(f"Found matching invocation: {invocation.invocation_id}")
            invocations.append(invocation)

            if target_call is None:
                target_call = invocation.call
                logger.info(f"Found call in task: {task.task_id}")

    # Log summary for this task
    logger.debug(
        f"Checked {invocation_count} invocations in task {task.task_id}, found {len(invocations)} matches"
    )
    return invocations, target_call, False


def find_call_and_invocations(
    app: "Pynenc", call_id: "CallId", timeout: int = 10
) -> tuple["Call | None", "Task | None", list["DistributedInvocation"]]:
    """
    Find a call by its ID along with related invocations and task.

    Uses the orchestrator's call-to-invocation index for direct lookup,
    falling back to a full task scan if no index is available.

    :param app: The Pynenc application instance
    :param call_id: The ID of the call to find
    :param timeout: Maximum search time in seconds
    :return: Tuple containing (target_call, target_task, invocations)
    """
    start_time = time.time()
    logger.info(f"Starting search for call ID: {call_id}")

    # Fast path: use the orchestrator index to look up invocations by call_id
    inv_ids = list(app.orchestrator.get_call_invocation_ids(call_id))
    if inv_ids:
        invocations = [app.state_backend.get_invocation(iid) for iid in inv_ids]
        target_call = invocations[0].call if invocations else None
        target_task = invocations[0].task if invocations else None
        elapsed = time.time() - start_time
        logger.info(
            f"Found call {call_id} via index in {elapsed:.2f}s with {len(invocations)} invocations"
        )
        return target_call, target_task, invocations

    # Slow path: scan all tasks (needed for backends without a call index)
    all_invocations: list[DistributedInvocation] = []
    target_call = None
    target_task = None
    task_count = len(app.tasks)
    logger.info(f"No index match; scanning {task_count} tasks for call {call_id}")

    for i, task in enumerate(app.tasks.values()):
        if _check_timeout_for_call_search(start_time, timeout, i, task_count):
            break

        try:
            elapsed = time.time() - start_time
            task_timeout = min(3.0, timeout - elapsed)

            task_invocations, task_call, timeout_reached = _search_invocations_in_task(
                app, task, call_id, task_timeout
            )
            all_invocations.extend(task_invocations)
            if task_call and target_call is None:
                target_call = task_call
                target_task = task
            if timeout_reached:
                break
        except Exception as e:
            logger.exception(f"Error searching in task {task.task_id}: {str(e)}")
            continue

    elapsed = time.time() - start_time
    if target_call:
        logger.info(
            f"Found call {call_id} in {elapsed:.2f}s with {len(all_invocations)} invocations"
        )
    else:
        logger.warning(f"Failed to find call {call_id} after {elapsed:.2f}s")

    return target_call, target_task, all_invocations


async def process_call_detail(
    request: Request, call_id: "CallId", request_source: str
) -> HTMLResponse:
    """
    Process a call detail request regardless of how the call_id was provided.

    :param request: The FastAPI request object
    :param call_id: The ID of the call to display
    :param request_source: A string indicating the source of the request (for logging)
    :return: HTML response with call details or error page
    """
    app = get_pynenc_instance()

    logger.info(f"Looking for call with ID ({request_source}): {call_id}")
    start_time = time.time()

    try:
        # Find the call, task and invocations
        target_call, target_task, invocations = find_call_and_invocations(app, call_id)

        if not target_call or not target_task:
            logger.warning(f"No call found with ID: {call_id}")
            return render_error_response(
                templates,
                request,
                "Call Not Found",
                f"No call found with ID: {call_id}",
                404,
            )

        # Create template context
        context = _create_call_detail_context(
            request, app, target_call, target_task, invocations
        )

        logger.info(
            f"Rendering call detail template with {len(invocations)} invocations"
        )
        return templates.TemplateResponse(request, "calls/detail.html", context=context)

    except Exception as e:
        logger.exception(
            f"Unexpected error in call_detail ({request_source}): {str(e)}"
        )
        return render_error_response(
            templates, request, "Error", f"An error occurred: {str(e)}", 500
        )
    finally:
        elapsed = time.time() - start_time
        logger.info(
            f"call_detail ({request_source}) completed in {elapsed:.2f} seconds"
        )


def _create_call_detail_context(
    request: Request,
    app: "Pynenc",
    target_call: "Call",
    target_task: "Task",
    invocations: list["DistributedInvocation"],
) -> dict[str, Any]:
    """Create the template context for call detail page."""
    serialized = getattr(target_call, "serialized_arguments", None)
    formatted_args, raw_args = format_serialized_arguments(serialized)

    return {
        "app_id": app.app_id,
        "call": target_call,
        "task": target_task,
        "arguments": getattr(target_call, "arguments", None),
        "serialized_arguments": raw_args if raw_args is not None else formatted_args,
        "formatted_args": formatted_args,
        "invocations": invocations,
    }


@router.get("/", response_class=HTMLResponse)
async def call_detail_by_query(request: Request) -> HTMLResponse:
    """Display detailed information about a specific call via query parameter."""
    try:
        call_id_key = request.query_params.get("call_id_key")
        if not call_id_key:
            return render_error_response(
                templates,
                request,
                "Missing Call ID",
                "No call_id was provided. Please check the URL and try again.",
                400,
            )
        try:
            call_id = CallId.from_key(call_id_key)
        except ValueError as e:
            logger.warning(f"Invalid call ID format: {call_id_key} - {str(e)}")
            return render_error_response(
                templates,
                request,
                "Invalid Call ID Format",
                f"The provided call ID is not properly formatted: {str(e)}",
                400,
            )
        logger.info(f"Call detail by query requested for: {call_id}")
        logger.debug(f"Query params: {dict(request.query_params)}")

        return await process_call_detail(request, call_id, "query param")
    except Exception as e:
        logger.exception(f"Unhandled exception in call_detail_by_query: {str(e)}")
        return templates.TemplateResponse(
            request,
            "shared/error.html",
            context={
                "title": "Unexpected Error",
                "message": f"An unexpected error occurred: {str(e)}",
            },
            status_code=500,
        )


@router.get("/{call_id_key:path}", response_class=HTMLResponse)
async def call_detail(request: Request, call_id_key: str) -> HTMLResponse:
    """Display detailed information about a specific call via path parameter."""
    # Check if this is accidentally a query param request
    if call_id_key == "" and "call_id_key" in request.query_params:
        # Redirect to the query param handler
        return await call_detail_by_query(request)

    try:
        call_id = CallId.from_key(call_id_key)
    except ValueError as e:
        logger.warning(f"Invalid call ID format: {call_id_key} - {str(e)}")
        return render_error_response(
            templates,
            request,
            "Invalid Call ID Format",
            f"The provided call ID is not properly formatted: {str(e)}",
            400,
        )
    return await process_call_detail(request, call_id, "path param")
