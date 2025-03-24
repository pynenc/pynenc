from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynenc.invocation.status import InvocationStatus
from pynmon.app import get_pynenc_instance, templates

router = APIRouter(prefix="/invocations", tags=["invocations"])


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

    # Try to find the invocation by ID
    invocation = None
    for task in app.tasks.values():
        for inv in app.orchestrator.get_existing_invocations(task=task):
            if inv.invocation_id == invocation_id:
                invocation = inv
                break
        if invocation:
            break

    if not invocation:
        return templates.TemplateResponse(
            "shared/error.html",
            {
                "request": request,
                "title": "Invocation Not Found",
                "message": f"No invocation found with ID: {invocation_id}",
            },
            status_code=404,
        )

    # Get result or exception if available
    result = None
    exception = None
    try:
        if invocation.status == InvocationStatus.SUCCESS:
            result = app.state_backend.get_result(invocation)
        elif invocation.status == InvocationStatus.FAILED:
            exception = app.state_backend.get_exception(invocation)
    except Exception as e:
        exception = Exception(f"Error retrieving result/exception: {str(e)}")

    # Get history from state backend
    history = app.state_backend.get_history(invocation)

    # Get call details
    call = invocation.call

    return templates.TemplateResponse(
        "invocations/detail.html",
        {
            "request": request,
            "title": f"Invocation {invocation_id}",
            "invocation": invocation,
            "call": call,
            "task": call.task,
            "result": result,
            "exception": exception,
            "history": history,
            "arguments": call.arguments,
            "serialized_arguments": call.serialized_arguments,
        },
    )
