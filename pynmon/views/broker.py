from typing import Any, Dict

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from pynmon.app import get_pynenc_instance, templates

router = APIRouter(prefix="/broker", tags=["broker"])


@router.get("/", response_class=HTMLResponse)
async def broker_view(request: Request) -> HTMLResponse:
    """Display broker information and configuration."""
    app = get_pynenc_instance()

    # Get broker type and count of pending invocations
    broker_info = {
        "type": app.broker.__class__.__name__,
        "pending_count": app.broker.count_invocations(),
    }

    return templates.TemplateResponse(
        "broker/overview.html",
        {
            "request": request,
            "title": "Broker Monitor",
            "app_id": app.app_id,
            "broker_info": broker_info,
        },
    )


@router.get("/queue", response_class=HTMLResponse)
async def queue_view(
    request: Request,
    limit: int = Query(20, description="Maximum number of pending invocations to show"),
) -> HTMLResponse:
    """Display queue contents with pending invocations."""
    app = get_pynenc_instance()

    # Get pending invocations
    pending_invocations = []
    queue_size = app.broker.count_invocations()

    # Warning: This operation has overhead as we retrieve and re-queue messages
    for _ in range(min(limit, queue_size)):
        invocation = app.broker.retrieve_invocation()
        if invocation:
            pending_invocations.append(invocation)
            # Re-queue the invocation after retrieving it for display
            app.broker.route_invocation(invocation)

    return templates.TemplateResponse(
        "broker/queue.html",
        {
            "request": request,
            "title": "Broker Queue",
            "app_id": app.app_id,
            "pending_invocations": pending_invocations,
            "total_count": queue_size,
            "displayed_count": len(pending_invocations),
        },
    )


@router.get("/refresh", response_class=HTMLResponse)
async def refresh_broker(request: Request) -> HTMLResponse:
    """Refresh the broker data for HTMX partial updates."""
    app = get_pynenc_instance()

    pending_count = app.broker.count_invocations()

    return templates.TemplateResponse(
        "broker/partials/info.html",
        {
            "request": request,
            "broker_info": {
                "type": app.broker.__class__.__name__,
                "pending_count": pending_count,
            },
        },
    )


@router.post("/purge", response_class=JSONResponse)
async def purge_broker(request: Request) -> Dict[str, Any]:
    """Purge all pending invocations from the broker."""
    app = get_pynenc_instance()

    # Purge the broker
    app.broker.purge()

    return {"success": True, "message": "Broker queue purged successfully"}
