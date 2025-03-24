from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynmon.app import get_pynenc_instance, templates

router = APIRouter(prefix="/calls", tags=["calls"])


@router.get("/{call_id}", response_class=HTMLResponse)
async def call_detail(request: Request, call_id: str) -> HTMLResponse:
    """Display detailed information about a specific call and its invocations."""
    app = get_pynenc_instance()

    # Try to find invocations with this call_id
    invocations = []
    target_call = None

    # Search through all tasks to find a matching call
    for task in app.tasks.values():
        for invocation in app.orchestrator.get_existing_invocations(task=task):
            if invocation.call_id == call_id:
                invocations.append(invocation)
                if target_call is None:
                    target_call = invocation.call

    if not target_call:
        return templates.TemplateResponse(
            "shared/error.html",
            {
                "request": request,
                "title": "Call Not Found",
                "message": f"No call found with ID: {call_id}",
            },
            status_code=404,
        )

    return templates.TemplateResponse(
        "calls/detail.html",
        {
            "request": request,
            "title": f"Call {call_id}",
            "app_id": app.app_id,
            "call": target_call,
            "task": target_call.task,
            "arguments": target_call.arguments,
            "serialized_arguments": target_call.serialized_arguments,
            "invocations": invocations,
        },
    )
