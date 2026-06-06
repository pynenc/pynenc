"""Detail views for trigger definitions."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynmon.app import get_pynenc_instance, templates
from pynmon.views.trigger_runs import _condition_view_from_object, _shorten_id

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.trigger.trigger_definitions import TriggerDefinition

router = APIRouter(prefix="/triggers", tags=["triggers"])
logger = logging.getLogger(__name__)


def _condition_view_unavailable(condition_id: str) -> dict[str, Any]:
    """Return a compact placeholder for a missing trigger condition."""
    return {
        "available": False,
        "kind": "TriggerCondition",
        "type": "Unknown condition",
        "id": condition_id,
        "id_short": _shorten_id(condition_id),
        "detail_url": "",
        "items": [],
    }


def _trigger_view_from_object(
    app: Pynenc, trigger: TriggerDefinition
) -> dict[str, Any]:
    """Build a template view model for a trigger definition."""
    try:
        conditions = app.trigger.get_conditions_batch(trigger.condition_ids)
    except Exception:  # pragma: no cover - defensive backend failure
        logger.debug("condition batch lookup failed for trigger %s", trigger.trigger_id)
        conditions = {}

    condition_views = []
    for condition_id in trigger.condition_ids:
        condition = conditions.get(condition_id)
        if condition is None:
            condition_views.append(_condition_view_unavailable(condition_id))
        else:
            condition_views.append(_condition_view_from_object(condition, app))

    return {
        "id": trigger.trigger_id,
        "id_short": _shorten_id(trigger.trigger_id),
        "task_id": str(trigger.task_id),
        "logic": trigger.logic.value,
        "condition_count": len(trigger.condition_ids),
        "argument_provider": (
            type(trigger.argument_provider).__name__
            if trigger.argument_provider is not None
            else "None"
        ),
        "conditions": condition_views,
    }


@router.get("/{trigger_id}", response_class=HTMLResponse)
async def trigger_detail(request: Request, trigger_id: str) -> HTMLResponse:
    """Render details for a registered trigger definition."""
    app = get_pynenc_instance()
    trigger = await asyncio.to_thread(app.trigger.get_trigger, trigger_id)
    if trigger is None:
        return templates.TemplateResponse(
            request,
            "triggers/not_found.html",
            context={
                "request": request,
                "title": "Trigger not found",
                "trigger_id": trigger_id,
            },
            status_code=404,
        )
    return templates.TemplateResponse(
        request,
        "triggers/detail.html",
        context={
            "request": request,
            "title": f"Trigger {_shorten_id(trigger.trigger_id)}",
            "trigger": await asyncio.to_thread(_trigger_view_from_object, app, trigger),
        },
    )
