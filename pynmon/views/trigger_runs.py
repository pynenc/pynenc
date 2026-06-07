"""Detail view for a single trigger run."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from pynmon.app import get_pynenc_instance, templates
from pynmon.util.trigger_monitoring import (
    event_to_dict,
    trigger_run_to_dict,
)
from pynmon.views.invocations import _fetch_event_summary, _fetch_inv_summary
from pynmon.util.trigger_timeline import timeline_url_for_trigger_run

router = APIRouter(prefix="/trigger-runs", tags=["trigger-runs"])

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.trigger.monitoring import TriggerRunRecord


def _shorten_id(value: str, *, head: int = 22, tail: int = 12) -> str:
    """Shorten a long monitoring identifier while keeping both ends visible."""
    if len(value) <= head + tail + 1:
        return value
    return f"{value[:head]}...{value[-tail:]}"


def _json_safe(value: Any) -> Any:
    """Return a display-safe JSON-ish value for Pynmon detail rendering."""
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _display_value(value: Any) -> tuple[str, bool]:
    """Format a value for compact key/value rendering."""
    safe = _json_safe(value)
    if isinstance(safe, str):
        stripped = safe.strip()
        if (stripped.startswith("{") and stripped.endswith("}")) or (
            stripped.startswith("[") and stripped.endswith("]")
        ):
            try:
                safe = json.loads(stripped)
            except json.JSONDecodeError:
                pass
    if isinstance(safe, (dict, list)):
        return json.dumps(safe, indent=2, sort_keys=True), True
    if safe is True:
        return "true", False
    if safe is False:
        return "false", False
    if safe is None:
        return "null", False
    return str(safe), False


def _detail_items(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Turn raw serialized condition/context data into template items."""
    items: list[dict[str, Any]] = []
    for key, value in data.items():
        if key == "statuses":
            safe_statuses = _json_safe(value)
            statuses = (
                safe_statuses if isinstance(safe_statuses, list) else [safe_statuses]
            )
            items.append(
                {
                    "key": key,
                    "label": key.replace("_", " ").title(),
                    "render": "status_badges",
                    "statuses": [str(status).upper() for status in statuses],
                }
            )
            continue
        display, multiline = _display_value(value)
        items.append(
            {
                "key": key,
                "label": key.replace("_", " ").title(),
                "value": display,
                "multiline": multiline,
                "render": "value",
            }
        )
    return items


def _condition_url(condition_id: str) -> str:
    """Return the independent condition-detail URL for a condition id."""
    return "/trigger-runs/condition?" + urlencode({"condition_id": condition_id})


def _valid_condition_url(valid_condition_id: str) -> str:
    """Return the independent valid-condition-detail URL for a valid condition id."""
    return "/trigger-runs/valid-condition?" + urlencode(
        {"valid_condition_id": valid_condition_id}
    )


def _condition_view_from_object(condition: Any, app: Pynenc) -> dict[str, Any]:
    """Build a Pynmon view model from a TriggerCondition object."""
    data = _json_safe(condition._to_json(app))
    return {
        "available": True,
        "kind": "TriggerCondition",
        "type": type(condition).__name__,
        "id": condition.condition_id,
        "id_short": _shorten_id(condition.condition_id),
        "detail_url": _condition_url(condition.condition_id),
        "items": _detail_items(data),
    }


def _context_view_from_object(context: Any, app: Pynenc) -> dict[str, Any]:
    """Build a Pynmon view model from a ConditionContext object."""
    data = _json_safe(context._to_json(app))
    return {
        "available": True,
        "kind": "ConditionContext",
        "type": type(context).__name__,
        "id": context.context_id,
        "id_short": _shorten_id(context.context_id),
        "timestamp": context.timestamp.isoformat(),
        "summary": "",
        "items": _detail_items(data),
    }


def _condition_view_from_participant(
    app: Pynenc, participant: dict[str, Any]
) -> dict[str, Any]:
    """Resolve condition details for one participant via the live registry."""
    condition_id = participant.get("condition_id") or ""
    if condition_id:
        try:
            condition = app.trigger.get_condition(condition_id)
        except Exception:  # pragma: no cover - defensive backend failure
            logger.debug("condition lookup failed for %s", condition_id)
            condition = None
        if condition is not None:
            return _condition_view_from_object(condition, app)
    return {
        "available": False,
        "kind": "TriggerCondition",
        "type": "Unknown condition",
        "id": condition_id,
        "id_short": _shorten_id(condition_id) if condition_id else "",
        "detail_url": "",
        "items": [],
    }


def _context_view_from_participant(
    app: Pynenc, participant: dict[str, Any]
) -> dict[str, Any]:
    """Build a lightweight context view from a participant snapshot.

    The participant only carries reference ids and labels; payload-level
    detail is re-fetched on demand from the event or invocation it points
    to. When the source is no longer resolvable we degrade to the
    summary/timestamp the participant already carries.
    """
    context_type = participant.get("context_type") or "ConditionContext"
    timestamp = participant.get("context_timestamp") or ""
    summary = participant.get("context_summary") or ""
    event_id = participant.get("event_id")
    source_inv = participant.get("source_invocation_id")
    items: list[dict[str, Any]] = []
    available = bool(event_id or source_inv or summary)
    target_id = event_id or source_inv or ""
    if event_id:
        try:
            event = app.trigger.get_event(event_id)
        except Exception:  # pragma: no cover - defensive backend failure
            logger.debug("event lookup failed for %s", event_id)
            event = None
        if event is not None:
            items = _detail_items(
                _json_safe(
                    {
                        "event_id": event.event_id,
                        "event_code": event.event_code,
                        "payload": event.payload,
                        "emitted_by_invocation_id": event.emitted_by_invocation_id,
                    }
                )
            )
    return {
        "available": available,
        "kind": "ConditionContext",
        "type": context_type,
        "id": target_id,
        "id_short": _shorten_id(target_id) if target_id else "",
        "timestamp": timestamp,
        "summary": summary,
        "items": items,
    }


def _valid_condition_snapshot_from_run(
    app: Pynenc, valid_condition_id: str, run: TriggerRunRecord
) -> dict[str, Any] | None:
    """Build a valid-condition detail model from a stored trigger-run snapshot."""
    for participant in run.participants or []:
        if participant.valid_condition_id != valid_condition_id:
            continue
        participant_dict = participant.to_dict()
        return {
            "valid_condition_id": valid_condition_id,
            "valid_condition_id_short": _shorten_id(valid_condition_id),
            "condition": _condition_view_from_participant(app, participant_dict),
            "context": _context_view_from_participant(app, participant_dict),
            "source": "snapshot",
            "trigger_run_id": run.trigger_run_id,
            "trigger_run_id_short": _shorten_id(run.trigger_run_id),
            "trigger_id": run.trigger_id,
            "task_id_key": run.task_id_key,
        }
    return None


def _valid_condition_snapshot(
    app: Pynenc, valid_condition_id: str
) -> dict[str, Any] | None:
    """Find a stored trigger-run participant for a transient valid condition."""
    try:
        runs = app.trigger.get_trigger_runs_for_valid_condition(valid_condition_id)
    except Exception:  # pragma: no cover - defensive backend failure
        logger.debug(
            "valid-condition snapshot lookup failed for %s", valid_condition_id
        )
        return None
    for run in runs:
        snapshot = _valid_condition_snapshot_from_run(app, valid_condition_id, run)
        if snapshot is not None:
            return snapshot
    return None


def _enrich_trigger_run_details(
    app: Pynenc, run_dict: dict[str, Any]
) -> dict[str, Any]:
    """Attach condition/context view models to a trigger-run projection."""
    try:
        valid_conditions = app.trigger.get_valid_conditions()
    except Exception:  # pragma: no cover - defensive backend failure
        logger.debug("valid-condition lookup failed")
        valid_conditions = {}

    condition_views_by_id: dict[str, dict[str, Any]] = {}
    participants = run_dict.get("participants") or []
    participant_event_ids = {
        participant.get("event_id")
        for participant in participants
        if participant.get("event_id")
    }
    participant_source_ids = {
        participant.get("source_invocation_id")
        for participant in participants
        if participant.get("source_invocation_id")
    }
    run_dict["show_participant_event_column"] = bool(participant_event_ids)
    run_dict["show_participant_source_column"] = len(participant_source_ids) > 1
    for participant in participants:
        valid_condition_id = participant.get("valid_condition_id")
        valid_condition = valid_conditions.get(valid_condition_id)
        if valid_condition is not None:
            condition_view = _condition_view_from_object(valid_condition.condition, app)
            context_view = _context_view_from_object(valid_condition.context, app)
            valid_source = "live"
        else:
            condition_view = _condition_view_from_participant(app, participant)
            context_view = _context_view_from_participant(app, participant)
            valid_source = "snapshot"

        participant["condition_view"] = condition_view
        participant["context_view"] = context_view
        participant["valid_condition_view"] = {
            "id": valid_condition_id or "",
            "id_short": _shorten_id(valid_condition_id) if valid_condition_id else "",
            "available": bool(valid_condition_id),
            "source": valid_source,
            "detail_url": (
                _valid_condition_url(valid_condition_id) if valid_condition_id else ""
            ),
        }
        condition_id = participant.get("condition_id")
        if condition_id:
            condition_views_by_id[condition_id] = condition_view

    condition_details: list[dict[str, Any]] = []
    for condition_id in run_dict.get("condition_ids") or []:
        if condition_id not in condition_views_by_id:
            condition_views_by_id[condition_id] = _condition_view_from_participant(
                app, {"condition_id": condition_id}
            )
        condition_details.append(condition_views_by_id[condition_id])
    run_dict["condition_details"] = condition_details
    return run_dict


@router.get("/condition", response_class=HTMLResponse)
async def trigger_condition_detail(request: Request, condition_id: str) -> HTMLResponse:
    """Render details for a registered trigger condition."""
    app = get_pynenc_instance()
    condition = await asyncio.to_thread(app.trigger.get_condition, condition_id)
    if condition is None:
        return templates.TemplateResponse(
            request,
            "trigger_runs/condition_not_found.html",
            context={
                "request": request,
                "title": "Trigger condition not found",
                "condition_id": condition_id,
            },
            status_code=404,
        )
    return templates.TemplateResponse(
        request,
        "trigger_runs/condition_detail.html",
        context={
            "request": request,
            "title": f"Trigger Condition {_shorten_id(condition.condition_id)}",
            "condition": _condition_view_from_object(condition, app),
        },
    )


@router.get("/valid-condition", response_class=HTMLResponse)
async def valid_condition_detail(
    request: Request, valid_condition_id: str
) -> HTMLResponse:
    """Render details for a live or historical valid condition."""
    app = get_pynenc_instance()
    valid_conditions = await asyncio.to_thread(app.trigger.get_valid_conditions)
    valid_condition = valid_conditions.get(valid_condition_id)
    if valid_condition is not None:
        detail: dict[str, Any] | None = {
            "valid_condition_id": valid_condition_id,
            "valid_condition_id_short": _shorten_id(valid_condition_id),
            "condition": _condition_view_from_object(valid_condition.condition, app),
            "context": _context_view_from_object(valid_condition.context, app),
            "source": "live",
        }
    else:
        detail = await asyncio.to_thread(
            _valid_condition_snapshot, app, valid_condition_id
        )

    if detail is None:
        return templates.TemplateResponse(
            request,
            "trigger_runs/valid_condition_not_found.html",
            context={
                "request": request,
                "title": "Valid condition not found",
                "valid_condition_id": valid_condition_id,
            },
            status_code=404,
        )
    return templates.TemplateResponse(
        request,
        "trigger_runs/valid_condition_detail.html",
        context={
            "request": request,
            "title": f"Valid Condition {_shorten_id(valid_condition_id)}",
            **detail,
        },
    )


def _timeline_url_for_run(
    run: TriggerRunRecord,
    event_timestamps: list[datetime],
    app: Pynenc | None = None,
) -> str:
    """Return a timeline URL scoped to the invocations of *run*.

    The window uses the same timestamps as the timeline's "Zoom to Invocation"
    button: triggered invocation history plus trigger context timestamps. This
    keeps the detail-page link and the in-timeline zoom visually consistent.

    Also passes ``inv_ids`` so the timeline scopes the lane data to the
    triggered invocation plus any source invocations involved in this run.
    """
    return timeline_url_for_trigger_run(run, event_timestamps, app)


@router.get("/{trigger_run_id}/api", response_class=JSONResponse)
async def trigger_run_api(trigger_run_id: str) -> JSONResponse:
    """Return a trigger run, its participants, and related event metadata as JSON."""
    app = get_pynenc_instance()
    run = await asyncio.to_thread(app.trigger.get_trigger_run, trigger_run_id)
    if run is None:
        return JSONResponse({"error": "Trigger run not found"}, status_code=404)

    payload = trigger_run_to_dict(run)
    payload = await asyncio.to_thread(_enrich_trigger_run_details, app, payload)
    invocation_ids = {
        invocation_id
        for invocation_id in [
            run.triggered_invocation_id,
            *run.source_invocation_ids,
            *(participant.source_invocation_id for participant in run.participants),
        ]
        if invocation_id
    }
    payload["invocation_summaries"] = {
        invocation_id: await asyncio.to_thread(_fetch_inv_summary, app, invocation_id)
        for invocation_id in invocation_ids
    }
    events: list[dict] = []
    event_timestamps: list[datetime] = []
    for event_id in run.event_ids or []:
        try:
            event = await asyncio.to_thread(app.trigger.get_event, event_id)
        except Exception:  # pragma: no cover - defensive: backend lookup failure
            logger.debug("event lookup failed for %s", event_id)
            event = None
        if event is not None:
            event_timestamps.append(event.timestamp)
            events.append(event_to_dict(event))
    payload["events"] = events
    payload["timeline_url"] = _timeline_url_for_run(run, event_timestamps, app)
    return JSONResponse(payload)


@router.get("/{trigger_run_id}", response_class=HTMLResponse)
async def trigger_run_detail(request: Request, trigger_run_id: str) -> HTMLResponse:
    """Render the trigger run detail page.

    A trigger run is the record produced when one trigger's set of conditions
    matched and registered a new invocation. It links the upstream causes
    (events, source invocations) to the downstream effect (the new invocation).
    """
    app = get_pynenc_instance()
    run = await asyncio.to_thread(app.trigger.get_trigger_run, trigger_run_id)
    if run is None:
        return templates.TemplateResponse(
            request,
            "trigger_runs/not_found.html",
            context={
                "request": request,
                "title": "Trigger run not found",
                "trigger_run_id": trigger_run_id,
            },
            status_code=404,
        )

    run_dict = trigger_run_to_dict(run)
    run_dict = await asyncio.to_thread(_enrich_trigger_run_details, app, run_dict)

    related_events: list[dict] = []
    event_timestamps: list[datetime] = []
    for event_id in run.event_ids or []:
        try:
            event = await asyncio.to_thread(app.trigger.get_event, event_id)
        except Exception:  # pragma: no cover - defensive
            logger.debug("event lookup failed for %s", event_id)
            event = None
        if event is not None:
            event_timestamps.append(event.timestamp)
            related_events.append(event_to_dict(event))

    # Collect all invocation IDs referenced in this trigger run so we can
    # show task name, status, and duration next to each link.
    src_ids: set[str] = set()
    for sid in run.source_invocation_ids or []:
        src_ids.add(sid)
    if run.triggered_invocation_id:
        src_ids.add(run.triggered_invocation_id)
    for p in run.participants or []:
        participant_source_id = getattr(p, "source_invocation_id", None)
        if participant_source_id:
            src_ids.add(participant_source_id)
    source_inv_summaries = {
        sid: await asyncio.to_thread(_fetch_inv_summary, app, sid) for sid in src_ids
    }

    # Collect rich summaries for every event referenced on the run so the
    # Outcome panel can render pills with code + timestamp + outcome state.
    event_ids: set[str] = set()
    for eid in run.event_ids or []:
        event_ids.add(eid)
    for p in run.participants or []:
        participant_event_id = getattr(p, "event_id", None)
        if participant_event_id:
            event_ids.add(participant_event_id)
    source_event_summaries = {
        eid: await asyncio.to_thread(_fetch_event_summary, app, eid)
        for eid in event_ids
    }

    return templates.TemplateResponse(
        request,
        "trigger_runs/detail.html",
        context={
            "request": request,
            "title": f"Trigger Run {trigger_run_id}",
            "run": run_dict,
            "related_events": related_events,
            "timeline_url": _timeline_url_for_run(run, event_timestamps, app),
            "source_inv_summaries": source_inv_summaries,
            "source_event_summaries": source_event_summaries,
        },
    )
