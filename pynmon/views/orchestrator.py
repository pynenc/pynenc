import asyncio

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from pynenc.invocation.status import InvocationStatus
from pynmon.app import get_pynenc_instance, templates

router = APIRouter(prefix="/orchestrator", tags=["orchestrator"])


def _collect_status_counts(app) -> dict[str, int]:  # type: ignore[no-untyped-def]
    """Synchronous helper that counts invocations per status (CPU-bound)."""
    status_counts: dict[str, int] = {}
    for status in InvocationStatus:
        status_counts[status.name] = 0
    for task in app.tasks.values():
        for status in InvocationStatus:
            count = sum(
                1
                for _ in app.orchestrator.get_existing_invocations(
                    task=task, statuses=[status]
                )
            )
            status_counts[status.name] += count
    return status_counts


@router.get("/", response_class=HTMLResponse)
async def orchestrator_view(request: Request) -> HTMLResponse:
    """Display orchestrator information and configuration."""
    app = get_pynenc_instance()

    # Get basic orchestrator info
    orchestrator_info = {
        "type": app.orchestrator.__class__.__name__,
        "blocking_control_enabled": app.orchestrator.conf.blocking_control,
        "auto_purge_hours": app.orchestrator.conf.auto_final_invocation_purge_hours,
    }

    # Offload heavy iteration to a thread so the event loop stays free
    status_counts = await asyncio.to_thread(_collect_status_counts, app)

    # Get any blocking invocations (limit to 10 for display)
    blocking_invocations = await asyncio.to_thread(
        lambda: list(app.orchestrator.get_blocking_invocations(10))
    )

    # Get active runners
    active_runners = await asyncio.to_thread(app.orchestrator.get_active_runners)

    return templates.TemplateResponse(
        "orchestrator/overview.html",
        {
            "request": request,
            "title": "Orchestrator Monitor",
            "app_id": app.app_id,
            "orchestrator_info": orchestrator_info,
            "blocking_invocations": blocking_invocations,
            "status_counts": status_counts,
            "active_runners": active_runners,
            "total_runners": len(active_runners),
        },
    )


@router.get("/refresh", response_class=HTMLResponse)
async def refresh_orchestrator(request: Request) -> HTMLResponse:
    """Refresh the orchestrator data for HTMX partial updates."""
    app = get_pynenc_instance()

    status_counts = await asyncio.to_thread(_collect_status_counts, app)

    blocking_invocations = await asyncio.to_thread(
        lambda: list(app.orchestrator.get_blocking_invocations(10))
    )

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
