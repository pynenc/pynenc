import json
import logging
import time
import traceback
from typing import TYPE_CHECKING, Any, Optional, TypedDict

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynmon.app import get_pynenc_instance, templates

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.call import Call
    from pynenc.invocation import DistributedInvocation
    from pynenc.task import Task

router = APIRouter(prefix="/calls", tags=["calls"])
logger = logging.getLogger("pynmon.views.calls")


class TaskInfo(TypedDict):
    """Type for task information dictionary."""

    task_id: str
    module: str
    func_qualname: str


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
    app: "Pynenc", task: "Task", call_id: str, task_timeout: float
) -> tuple[list["DistributedInvocation"], Optional[Any], bool]:
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

    for invocation in app.orchestrator.get_existing_invocations(task=task):
        invocation_count += 1

        # Check timeout for each invocation batch (every 10 invocations)
        if invocation_count % 10 == 0 and time.time() - task_start > task_timeout:
            logger.warning(
                f"Task timeout reached after checking {invocation_count} invocations for task {task.task_id}"
            )
            return invocations, target_call, True

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
    app: "Pynenc", call_id: str, timeout: int = 10
) -> tuple[Optional[Any], Optional["Task"], list["DistributedInvocation"]]:
    """
    Find a call by its ID along with related invocations and task.

    :param app: The Pynenc application instance
    :param call_id: The ID of the call to find
    :param timeout: Maximum search time in seconds
    :return: Tuple containing (target_call, target_task, invocations)
    """
    start_time = time.time()
    all_invocations = []
    target_call = None
    target_task = None

    # Log startup info
    task_count = len(app.tasks)
    logger.info(f"Starting search for call ID: {call_id} across {task_count} tasks")

    # Search through all tasks to find a matching call
    for i, task in enumerate(app.tasks.values()):
        # Check for timeout at the beginning of each task iteration
        if _check_timeout_for_call_search(start_time, timeout, i, task_count):
            break

        logger.debug(f"Searching in task [{i+1}/{task_count}]: {task.task_id}")
        try:
            # Use a separate timeout for each task to prevent one slow task from consuming all the time
            elapsed = time.time() - start_time
            task_timeout = min(3.0, timeout - elapsed)  # Max 3 seconds per task

            task_invocations, task_call, timeout_reached = _search_invocations_in_task(
                app, task, call_id, task_timeout
            )

            # Add any found invocations to our collection
            all_invocations.extend(task_invocations)

            # Set target_call and target_task if we found them
            if task_call and target_call is None:
                target_call = task_call
                target_task = task

            if timeout_reached:
                break

        except Exception as e:
            logger.error(f"Error searching in task {task.task_id}: {str(e)}")
            logger.error(traceback.format_exc())
            continue

    # Log summary information
    elapsed = time.time() - start_time
    if target_call:
        logger.info(
            f"Successfully found call {call_id} in {elapsed:.2f}s with {len(all_invocations)} related invocations"
        )
    else:
        logger.warning(f"Failed to find call {call_id} after {elapsed:.2f}s")

    return target_call, target_task, all_invocations


def format_call_arguments(call: "Call") -> tuple[dict[str, str], Optional[dict]]:
    """
    Format the arguments of a call for display.

    :param call: The call object containing arguments
    :return: Tuple of (formatted_args, raw_args)
    """
    formatted_args: dict[str, str] = {}
    raw_args = None

    if not hasattr(call, "serialized_arguments"):
        return formatted_args, raw_args

    try:
        # If it's already a dictionary, use it directly
        if isinstance(call.serialized_arguments, dict):
            raw_args = call.serialized_arguments
        # Otherwise try to parse it as JSON
        else:
            raw_args = json.loads(call.serialized_arguments)

        # Format each argument for better display
        for key, value in raw_args.items():
            if isinstance(value, (dict, list)):
                formatted_args[key] = json.dumps(value, indent=2)
            else:
                formatted_args[key] = str(value)
    except (json.JSONDecodeError, TypeError, AttributeError):
        # If it can't be parsed as JSON, use it as-is
        formatted_args = {"raw": str(call.serialized_arguments)}

    return formatted_args, raw_args


def create_task_info(task: "Task") -> TaskInfo:
    """
    Create a dictionary with task information.

    :param task: The task object
    :return: Dictionary with task information
    """
    return {
        "task_id": task.task_id,
        "module": task.func.__module__,
        "func_qualname": task.func.__qualname__,
    }


async def process_call_detail(
    request: Request, call_id: str, request_source: str
) -> HTMLResponse:
    """
    Process a call detail request regardless of how the call_id was provided.

    :param request: The FastAPI request object
    :param call_id: The ID of the call to display
    :param request_source: A string indicating the source of the request (for logging)
    :return: HTML response with call details or error page
    """
    app = get_pynenc_instance()

    if not call_id or call_id.strip() == "":
        return _create_error_response(
            request,
            "Missing Call ID",
            "No call_id was provided. Please check the URL and try again.",
            400,
        )

    logger.info(f"Looking for call with ID ({request_source}): {call_id}")
    start_time = time.time()

    try:
        # Find the call, task and invocations
        target_call, target_task, invocations = find_call_and_invocations(app, call_id)

        if not target_call or not target_task:
            logger.warning(f"No call found with ID: {call_id}")
            return _create_error_response(
                request, "Call Not Found", f"No call found with ID: {call_id}", 404
            )

        # Create template context
        context = _create_call_detail_context(
            request, app, target_call, target_task, invocations, call_id
        )

        logger.info(
            f"Rendering call detail template with {len(invocations)} invocations"
        )
        return templates.TemplateResponse("calls/detail.html", context)

    except Exception as e:
        logger.error(f"Unexpected error in call_detail ({request_source}): {str(e)}")
        logger.error(traceback.format_exc())
        return _create_error_response(
            request, "Error", f"An error occurred: {str(e)}", 500
        )
    finally:
        elapsed = time.time() - start_time
        logger.info(
            f"call_detail ({request_source}) completed in {elapsed:.2f} seconds"
        )


def _create_error_response(
    request: Request, title: str, message: str, status_code: int
) -> HTMLResponse:
    """Create a standardized error response."""
    logger.warning(f"{title}: {message}")
    return templates.TemplateResponse(
        "shared/error.html",
        {
            "request": request,
            "title": title,
            "message": message,
        },
        status_code=status_code,
    )


def _create_call_detail_context(
    request: Request,
    app: "Pynenc",
    target_call: "Call",
    target_task: "Task",
    invocations: list["DistributedInvocation"],
    call_id: str,
) -> dict[str, Any]:
    """Create the template context for call detail page."""
    # Format the arguments
    formatted_args, raw_args = format_call_arguments(target_call)

    # Get task information
    task_info = create_task_info(target_task)

    return {
        "request": request,
        "title": f"Call {call_id}",
        "app_id": app.app_id,
        "call": target_call,
        "task": task_info,
        "arguments": getattr(target_call, "arguments", None),
        "serialized_arguments": raw_args if raw_args is not None else formatted_args,
        "formatted_args": formatted_args,
        "invocations": invocations,
    }


@router.get("/", response_class=HTMLResponse)
async def call_detail_by_query(request: Request) -> HTMLResponse:
    """Display detailed information about a specific call via query parameter."""
    try:
        call_id = request.query_params.get("call_id", "")
        logger.info(f"Call detail by query requested for: {call_id}")
        logger.debug(f"Query params: {dict(request.query_params)}")

        return await process_call_detail(request, call_id, "query param")
    except Exception as e:
        logger.error(f"Unhandled exception in call_detail_by_query: {str(e)}")
        logger.error(traceback.format_exc())
        return templates.TemplateResponse(
            "shared/error.html",
            {
                "request": request,
                "title": "Unexpected Error",
                "message": f"An unexpected error occurred: {str(e)}",
            },
            status_code=500,
        )


@router.get("/{call_id:path}", response_class=HTMLResponse)
async def call_detail(request: Request, call_id: str) -> HTMLResponse:
    """Display detailed information about a specific call via path parameter."""
    # Check if this is accidentally a query param request
    if call_id == "" and "call_id" in request.query_params:
        # Redirect to the query param handler
        return await call_detail_by_query(request)

    return await process_call_detail(request, call_id, "path param")
