import asyncio
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from pynenc.invocation.status import InvocationStatus
from pynmon.app import get_pynenc_instance, templates
from pynmon.util.formatting import cron_to_human

if TYPE_CHECKING:
    from pynenc.app import Pynenc

router = APIRouter(prefix="/orchestrator", tags=["orchestrator"])


def _collect_status_counts(app: "Pynenc") -> dict[str, int]:
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

    # Get app-level recovery / atomic service config (lightweight reads)
    recovery_info = {
        "atomic_service_interval": app.conf.atomic_service_interval_minutes,
        "atomic_service_spread_margin": app.conf.atomic_service_spread_margin_minutes,
        "atomic_service_check_interval": app.conf.atomic_service_check_interval_minutes,
        "recover_pending_cron": app.conf.recover_pending_invocations_cron,
        "recover_pending_human": cron_to_human(
            app.conf.recover_pending_invocations_cron
        ),
        "max_pending_seconds": app.conf.max_pending_seconds,
        "recover_running_cron": app.conf.recover_running_invocations_cron,
        "recover_running_human": cron_to_human(
            app.conf.recover_running_invocations_cron
        ),
        "runner_dead_after_minutes": app.conf.runner_considered_dead_after_minutes,
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
        request,
        "orchestrator/overview.html",
        context={
            "title": "Orchestrator Monitor",
            "app_id": app.app_id,
            "orchestrator_info": orchestrator_info,
            "recovery_info": recovery_info,
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
        request,
        "orchestrator/partials/stats.html",
        context={
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
