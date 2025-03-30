from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from pynenc.invocation.status import InvocationStatus
from pynmon.app import get_pynenc_instance, templates

router = APIRouter(prefix="/orchestrator", tags=["orchestrator"])


@router.get("/", response_class=HTMLResponse)
async def orchestrator_view(request: Request) -> HTMLResponse:
    """Display orchestrator information and configuration."""
    app = get_pynenc_instance()

    # Get basic orchestrator info
    orchestrator_info = {
        "type": app.orchestrator.__class__.__name__,
        "cycle_control_enabled": app.orchestrator.conf.cycle_control,
        "blocking_control_enabled": app.orchestrator.conf.blocking_control,
        "auto_purge_hours": app.orchestrator.conf.auto_final_invocation_purge_hours,
    }

    # Get statistics on invocations by status
    status_counts: dict[str, int] = {}

    # Initialize all status counts to 0
    for status in InvocationStatus:
        status_counts[status.name] = 0

    # Iterate through all tasks and count invocations by status
    for task in app.tasks.values():
        for status in InvocationStatus:
            count = sum(
                1
                for _ in app.orchestrator.get_existing_invocations(
                    task=task, statuses=[status]
                )
            )
            status_counts[status.name] += count

    # Get any blocking invocations (limit to 10 for display)
    blocking_invocations = list(app.orchestrator.get_blocking_invocations(10))

    return templates.TemplateResponse(
        "orchestrator/overview.html",
        {
            "request": request,
            "title": "Orchestrator Monitor",
            "app_id": app.app_id,
            "orchestrator_info": orchestrator_info,
            "blocking_invocations": blocking_invocations,
            "status_counts": status_counts,  # Add status_counts here
        },
    )


@router.get("/refresh", response_class=HTMLResponse)
async def refresh_orchestrator(request: Request) -> HTMLResponse:
    """Refresh the orchestrator data for HTMX partial updates."""
    app = get_pynenc_instance()

    # Get statistics on invocations by status
    status_counts: dict[str, int] = {}

    # Initialize all status counts to 0
    for status in InvocationStatus:
        status_counts[status.name] = 0

    # Iterate through all tasks and count invocations by status
    for task in app.tasks.values():
        for status in InvocationStatus:
            count = sum(
                1
                for _ in app.orchestrator.get_existing_invocations(
                    task=task, statuses=[status]
                )
            )
            status_counts[status.name] += count

    # Get any blocking invocations (limit to 10 for display)
    blocking_invocations = list(app.orchestrator.get_blocking_invocations(10))

    return templates.TemplateResponse(
        "orchestrator/partials/stats.html",
        {
            "request": request,
            "status_counts": status_counts,
            "blocking_invocations": blocking_invocations,
        },
    )


@router.post("/auto-purge", response_class=JSONResponse)
async def auto_purge_orchestrator() -> JSONResponse:
    """Run auto-purge on the orchestrator."""
    app = get_pynenc_instance()

    try:
        # Perform purge operation
        app.orchestrator.purge()
        return JSONResponse(
            {"success": True, "message": "Auto-purge completed successfully."}
        )
    except Exception as e:
        return JSONResponse(
            {"success": False, "message": f"Error during auto-purge: {str(e)}"},
            status_code=500,
        )
