import asyncio
import logging
import pathlib
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from pynenc.invocation.status import InvocationStatus
from pynmon.app import get_active_app, get_all_apps

if TYPE_CHECKING:
    from pynenc.app import Pynenc

logger = logging.getLogger("pynmon.views.home")
router = APIRouter()

# Set up templates
base_dir = pathlib.Path(__file__).parent.parent
templates = Jinja2Templates(directory=str(base_dir / "templates"))


def _collect_component_info(app: "Pynenc") -> list[dict[str, str]]:
    """Build a list of component cards for the dashboard."""
    return [
        {
            "label": "Broker",
            "type": app.broker.__class__.__name__,
            "icon": "queue",
            "href": "/broker",
            "color": "info",
        },
        {
            "label": "Orchestrator",
            "type": app.orchestrator.__class__.__name__,
            "icon": "settings",
            "href": "/orchestrator",
            "color": "primary",
        },
        {
            "label": "State Backend",
            "type": app.state_backend.__class__.__name__,
            "icon": "storage",
            "href": "/state-backend",
            "color": "secondary",
        },
        {
            "label": "Runner",
            "type": app.runner.__class__.__name__,
            "icon": "devices",
            "href": "/runners",
            "color": "success",
        },
        {
            "label": "Serializer",
            "type": app.serializer.__class__.__name__,
            "icon": "code",
            "href": "#",
            "color": "dark",
        },
        {
            "label": "Data Store",
            "type": app.client_data_store.__class__.__name__,
            "icon": "cached",
            "href": "/client-data-store",
            "color": "warning",
        },
        {
            "label": "Trigger",
            "type": app.trigger.__class__.__name__,
            "icon": "alarm",
            "href": "#",
            "color": "danger",
        },
    ]


def _collect_config_summary(app: "Pynenc") -> dict[str, str | bool | float]:
    """Collect key configuration values from ConfigPynenc."""
    conf = app.conf
    return {
        "dev_mode": conf.dev_mode_force_sync_tasks,
        "logging_level": conf.logging_level,
        "log_format": str(conf.log_format),
        "argument_print_mode": str(conf.argument_print_mode),
        "compact_log_context": conf.compact_log_context,
        "atomic_service_interval": conf.atomic_service_interval_minutes,
        "runner_dead_after": conf.runner_considered_dead_after_minutes,
        "max_pending_seconds": conf.max_pending_seconds,
        "blocking_control": app.orchestrator.conf.blocking_control,
        "auto_purge_hours": app.orchestrator.conf.auto_final_invocation_purge_hours,
    }


def _collect_invocation_summary(app: "Pynenc") -> dict[str, int]:
    """Collect invocation counts grouped by meaningful categories.

    Uses orchestrator.count_invocations for lightweight indexed lookups.
    """
    running = app.orchestrator.count_invocations(statuses=[InvocationStatus.RUNNING])
    pending = app.orchestrator.count_invocations(
        statuses=[
            InvocationStatus.PENDING,
            InvocationStatus.REGISTERED,
            InvocationStatus.REROUTED,
        ]
    )
    paused = app.orchestrator.count_invocations(statuses=[InvocationStatus.PAUSED])
    failed = app.orchestrator.count_invocations(statuses=[InvocationStatus.FAILED])
    succeeded = app.orchestrator.count_invocations(statuses=[InvocationStatus.SUCCESS])
    recovering = app.orchestrator.count_invocations(
        statuses=[InvocationStatus.PENDING_RECOVERY, InvocationStatus.RUNNING_RECOVERY]
    )
    total = sum([running, pending, paused, failed, succeeded, recovering])
    return {
        "running": running,
        "pending": pending,
        "paused": paused,
        "failed": failed,
        "succeeded": succeeded,
        "recovering": recovering,
        "total": total,
    }


@router.get("/", response_class=HTMLResponse, tags=["Dashboard"])
async def index(request: Request) -> HTMLResponse:
    """Dashboard home page."""
    active_app = get_active_app()
    all_apps = get_all_apps()

    if not all_apps or not active_app:
        return templates.TemplateResponse(
            request,
            "critical_error.html",
            context={
                "title": "No App Configured",
                "message": "No Pynenc application is configured for monitoring.",
            },
        )

    components = _collect_component_info(active_app)
    config_summary = _collect_config_summary(active_app)

    # Lightweight counts: broker pending + active runners
    broker_pending = active_app.broker.count_invocations()
    active_runners = active_app.orchestrator.get_active_runners()

    # Invocation status summary (offload to thread since it hits the orchestrator)
    invocation_summary = await asyncio.to_thread(
        _collect_invocation_summary, active_app
    )

    # Registered task count
    registered_tasks = len(active_app.tasks)

    return templates.TemplateResponse(
        request,
        "index.html",
        context={
            "app_id": active_app.app_id,
            "all_apps": list(all_apps.keys()),
            "components": components,
            "config_summary": config_summary,
            "broker_pending": broker_pending,
            "active_runner_count": len(active_runners),
            "invocation_summary": invocation_summary,
            "registered_tasks": registered_tasks,
        },
    )
