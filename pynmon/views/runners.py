"""
Runners monitoring views.

Provides monitoring interfaces for runner heartbeat tracking,
active runner status, and recovery service coordination.
"""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynmon.app import get_pynenc_instance, templates

router = APIRouter(prefix="/runners", tags=["runners"])


@router.get("/", response_class=HTMLResponse)
async def runners_view(request: Request) -> HTMLResponse:
    """Display active runners and heartbeat information."""
    app = get_pynenc_instance()

    active_runners = app.orchestrator.get_active_runners()

    # Calculate some statistics
    total_runners = len(active_runners)

    return templates.TemplateResponse(
        "runners/overview.html",
        {
            "request": request,
            "title": "Active Runners",
            "app_id": app.app_id,
            "active_runners": active_runners,
            "total_runners": total_runners,
            "heartbeat_timeout_minutes": app.conf.atomic_service_runner_considered_dead_after_minutes,
            "atomic_service_check_interval_minutes": app.conf.atomic_service_check_interval_minutes,
        },
    )


@router.get("/refresh", response_class=HTMLResponse)
async def refresh_runners(request: Request) -> HTMLResponse:
    """Refresh runner data for HTMX partial updates."""
    app = get_pynenc_instance()

    active_runners = app.orchestrator.get_active_runners()

    return templates.TemplateResponse(
        "runners/partials/runners_table.html",
        {
            "request": request,
            "active_runners": active_runners,
        },
    )
