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


def find_call_and_invocations(
    app: "Pynenc", call_id: str, timeout: int = 10
) -> tuple[Optional[Any], Optional["Task"], list["DistributedInvocation"]]:
    """
    Find a call by its ID along with related invocations and task.

    Args:
        app: The Pynenc application instance
        call_id: The ID of the call to find
        timeout: Maximum search time in seconds

    Returns:
        tuple containing (target_call, target_task, invocations)
    """
    start_time = time.time()
    invocations = []
    target_call = None
    target_task = None

    # Search through all tasks to find a matching call
    for task in app.tasks.values():
        if time.time() - start_time > timeout:
            logger.warning("Timeout reached when searching for call")
            break

        logger.debug(f"Searching in task: {task.task_id}")
        try:
            # Compare the call.call_id (not invocation.call_id)
            for invocation in app.orchestrator.get_existing_invocations(task=task):
                if (
                    hasattr(invocation, "call")
                    and invocation.call
                    and hasattr(invocation.call, "call_id")
                    and invocation.call.call_id == call_id
                ):
                    logger.debug(
                        f"Found matching invocation: {invocation.invocation_id}"
                    )
                    invocations.append(invocation)

                    if target_call is None:
                        target_call = invocation.call
                        target_task = task
                        logger.info(f"Found call in task: {task.task_id}")
        except Exception as e:
            logger.error(f"Error searching in task {task.task_id}: {str(e)}")
            continue

    return target_call, target_task, invocations


def format_call_arguments(call: "Call") -> tuple[dict[str, str], Optional[dict]]:
    """
    Format the arguments of a call for display.

    Args:
        call: The call object containing arguments

    Returns:
        tuple of (formatted_args, raw_args)
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

    Args:
        task: The task object

    Returns:
        dictionary with task information
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

    Args:
        request: The FastAPI request object
        call_id: The ID of the call to display
        request_source: A string indicating the source of the request (for logging)

    Returns:
        HTML response with call details or error page
    """
    app = get_pynenc_instance()

    if not call_id or call_id.strip() == "":
        logger.warning(f"Missing call_id in {request_source} request")
        return templates.TemplateResponse(
            "shared/error.html",
            {
                "request": request,
                "title": "Missing Call ID",
                "message": "No call_id was provided. Please check the URL and try again.",
            },
            status_code=400,
        )

    logger.info(f"Looking for call with ID ({request_source}): {call_id}")
    start_time = time.time()

    try:
        # Find the call, task and invocations
        target_call, target_task, invocations = find_call_and_invocations(app, call_id)

        if not target_call or not target_task:
            logger.warning(f"No call found with ID: {call_id}")
            return templates.TemplateResponse(
                "shared/error.html",
                {
                    "request": request,
                    "title": "Call Not Found",
                    "message": f"No call found with ID: {call_id}",
                },
                status_code=404,
            )

        # Format the arguments
        formatted_args, raw_args = format_call_arguments(target_call)

        # Get task information
        task_info = create_task_info(target_task)

        logger.info(
            f"Rendering call detail template with {len(invocations)} invocations"
        )
        return templates.TemplateResponse(
            "calls/detail.html",
            {
                "request": request,
                "title": f"Call {call_id}",
                "app_id": app.app_id,
                "call": target_call,
                "task": task_info,
                "arguments": getattr(target_call, "arguments", None),
                "serialized_arguments": raw_args
                if raw_args is not None
                else formatted_args,
                "formatted_args": formatted_args,
                "invocations": invocations,
            },
        )
    except Exception as e:
        logger.error(f"Unexpected error in call_detail ({request_source}): {str(e)}")
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
        logger.info(
            f"call_detail ({request_source}) completed in {elapsed:.2f} seconds"
        )


@router.get("/{call_id:path}", response_class=HTMLResponse)
async def call_detail(request: Request, call_id: str) -> HTMLResponse:
    """Display detailed information about a specific call via path parameter."""
    return await process_call_detail(request, call_id, "path param")


@router.get("/", response_class=HTMLResponse)
async def call_detail_by_query(request: Request) -> HTMLResponse:
    """Display detailed information about a specific call via query parameter."""
    call_id = request.query_params.get("call_id", "")
    return await process_call_detail(request, call_id, "query param")
