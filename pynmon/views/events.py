"""Trigger event monitoring views for pynmon."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from pynmon.app import get_pynenc_instance, templates
from pynmon.util.trigger_monitoring import (
    event_to_dict,
    marker_page_to_dict,
    trigger_run_to_dict,
)
from pynmon.util.timeline_zoom import (
    calculate_timeline_zoom_window,
    invocation_trigger_run,
    invocation_zoom_window,
    trigger_run_zoom_timestamps,
)
from pynmon.util.trigger_timeline import timeline_url_for_trigger_run

router = APIRouter(prefix="/events", tags=["events"])

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.trigger.monitoring import EventRecord
    from pynenc.trigger.monitoring import TriggerRunRecord

DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 200


def _parse_datetime(value: str | None) -> datetime | None:
    """Parse an ISO-8601 datetime, returning ``None`` for empty input."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=f"Invalid datetime: {value}"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def _parse_matched(value: str | None) -> bool | None:
    """Parse the ``matched`` filter (``"yes"``/``"no"``/empty)."""
    if value in (None, "", "all"):
        return None
    if value == "yes":
        return True
    if value == "no":
        return False
    raise HTTPException(status_code=400, detail=f"Invalid matched filter: {value}")


def _parse_triggered(value: str | None) -> bool | None:
    """Parse the ``triggered`` filter (``"yes"``/``"no"``/empty)."""
    if value in (None, "", "all"):
        return None
    if value == "yes":
        return True
    if value == "no":
        return False
    raise HTTPException(status_code=400, detail=f"Invalid triggered filter: {value}")


def _timeline_url_for_event(
    event: EventRecord,
    app: Pynenc | None = None,
    trigger_runs: list[TriggerRunRecord] | None = None,
) -> str:
    """Return a timeline URL focused on the useful context for *event*.

    Recorded-only events (matched nothing, triggered nothing) are useful in the
    Event Monitor, but they add noise in the timeline. For those events we zoom
    to the emitter and its trigger origin without forcing an event marker.
    """
    runs = trigger_runs or []
    scoped_ids: list[str] = []

    def add_invocation(invocation_id: str | None) -> None:
        if invocation_id and invocation_id not in scoped_ids:
            scoped_ids.append(invocation_id)

    def add_run_scope(run: TriggerRunRecord) -> None:
        add_invocation(run.triggered_invocation_id)
        for source_id in run.source_invocation_ids or []:
            add_invocation(source_id)
        for participant in run.participants:
            add_invocation(participant.source_invocation_id)

    if event.emitted_by_invocation_id:
        add_invocation(event.emitted_by_invocation_id)

    for invocation_id in event.triggered_invocation_ids:
        add_invocation(invocation_id)
    for run in runs:
        add_run_scope(run)

    origin_run = None
    if app is not None and event.emitted_by_invocation_id:
        origin_run = invocation_trigger_run(app, event.emitted_by_invocation_id)
        if origin_run is not None:
            add_run_scope(origin_run)

    window = None
    if app is not None and event.emitted_by_invocation_id:
        window = invocation_zoom_window(
            app,
            event.emitted_by_invocation_id,
            origin_run,
            [event.timestamp],
        )
    if window is None:
        fallback_times: list[datetime | None] = [event.timestamp]
        if origin_run is not None:
            fallback_times.extend(trigger_run_zoom_timestamps(origin_run))
        for run in runs:
            fallback_times.extend(trigger_run_zoom_timestamps(run))
        window = calculate_timeline_zoom_window(fallback_times)
    if window is None:
        return "/invocations/timeline"

    start_time, end_time = window

    params: dict[str, str] = {
        "time_range": "custom",
        # Strip timezone (+00:00) — form submission URL-encodes '+' as space which
        # breaks datetime.fromisoformat.  The server always treats naked timestamps as UTC.
        "start_date": start_time.replace(tzinfo=None).isoformat(
            timespec="microseconds"
        ),
        "end_date": end_time.replace(tzinfo=None).isoformat(timespec="microseconds"),
        "resolution": "100ms",
    }
    if event.triggered or event.matched:
        params["focus_event"] = event.event_id
    if event.emitted_by_invocation_id:
        params["selected"] = event.emitted_by_invocation_id
    if scoped_ids:
        params["inv_ids"] = ",".join(scoped_ids)
    return "/invocations/timeline?" + urlencode(params)


@router.get("/", response_class=HTMLResponse, response_model=None)
async def events_list(
    request: Request,
    event_code: str | None = Query(None),
    start: str | None = Query(None),
    end: str | None = Query(None),
    matched: str | None = Query(None),
    triggered: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
) -> HTMLResponse | RedirectResponse:
    """Render the paginated event list with filters."""
    app = get_pynenc_instance()
    event_code_filter = event_code.strip() if event_code else None
    start_time = _parse_datetime(start)
    end_time = _parse_datetime(end)
    matched_filter = _parse_matched(matched)
    triggered_filter = _parse_triggered(triggered)

    # First, get the total to clamp the page so we never serve a phantom
    # "next page" past the end.
    total = await asyncio.to_thread(
        app.trigger.count_events,
        event_code=event_code_filter,
        start_time=start_time,
        end_time=end_time,
        matched=matched_filter,
        triggered=triggered_filter,
    )
    total_pages = max(1, (total + page_size - 1) // page_size)
    requested_page = page
    page = min(page, total_pages)
    if requested_page != page:
        return RedirectResponse(
            str(request.url.include_query_params(page=page)), status_code=303
        )
    offset = (page - 1) * page_size

    events = await asyncio.to_thread(
        app.trigger.get_events,
        event_code=event_code_filter,
        start_time=start_time,
        end_time=end_time,
        matched=matched_filter,
        triggered=triggered_filter,
        limit=page_size,
        offset=offset,
    )
    event_codes = await asyncio.to_thread(app.trigger.list_event_codes)

    return templates.TemplateResponse(
        request,
        "events/list.html",
        context={
            "title": "Event Monitor",
            "app_id": app.app_id,
            "events": events,
            "total": total,
            "page": page,
            "total_pages": total_pages,
            "page_size": page_size,
            "event_codes": event_codes,
            "filters": {
                "event_code": event_code_filter or "",
                "start": start or "",
                "end": end or "",
                "matched": matched or "all",
                "triggered": triggered or "all",
            },
        },
    )


@router.get("/{event_id}/api", response_class=JSONResponse)
async def event_detail_api(event_id: str) -> JSONResponse:
    """Return the event detail as JSON for timeline/detail-panel consumers."""
    app = get_pynenc_instance()
    event = await asyncio.to_thread(app.trigger.get_event, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    runs = await asyncio.to_thread(app.trigger.get_trigger_runs_for_event, event_id)
    return JSONResponse(
        {
            "event": event_to_dict(event),
            "trigger_runs": [trigger_run_to_dict(r) for r in runs],
        }
    )


@router.get("/{event_id}/trigger-runs", response_class=JSONResponse)
async def event_trigger_runs_api(event_id: str) -> JSONResponse:
    """Return only the trigger runs that reference ``event_id``."""
    app = get_pynenc_instance()
    event = await asyncio.to_thread(app.trigger.get_event, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    runs = await asyncio.to_thread(app.trigger.get_trigger_runs_for_event, event_id)
    return JSONResponse({"trigger_runs": [trigger_run_to_dict(r) for r in runs]})


@router.get("/{event_id}/trace", response_class=JSONResponse)
async def event_trace_api(event_id: str) -> JSONResponse:
    """Return the focused relation graph rooted at ``event_id``.

    Useful to render the "what did this event trigger" sub-graph that the
    dashboard's relation overlay needs without issuing N+1 calls.
    """
    app = get_pynenc_instance()
    event = await asyncio.to_thread(app.trigger.get_event, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    runs = await asyncio.to_thread(app.trigger.get_trigger_runs_for_event, event_id)
    sources: list[str] = []
    generated: list[str] = []
    for run in runs:
        for src in run.source_invocation_ids:
            if src not in sources:
                sources.append(src)
        if run.triggered_invocation_id and run.triggered_invocation_id not in generated:
            generated.append(run.triggered_invocation_id)
    return JSONResponse(
        {
            "focus_kind": "event",
            "focus_id": event_id,
            "trigger_runs": [trigger_run_to_dict(r) for r in runs],
            "events": [event_to_dict(event)],
            "source_invocation_ids": sources,
            "generated_invocation_ids": generated,
        }
    )


@router.get("/api/markers", response_class=JSONResponse)
async def event_markers_api(
    start: str = Query(...),
    end: str = Query(...),
    event_code: str | None = Query(None),
    state: str = Query("all"),
    limit: int = Query(1000, ge=1, le=5000),
    offset: int = Query(0, ge=0),
) -> JSONResponse:
    """Return the timeline-marker projection for the given time window.

    Backed by ``BaseTrigger.get_event_markers_in_timerange``; surfaces a
    ``truncated`` flag so the UI can show "more events outside window".
    """
    app = get_pynenc_instance()
    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)
    if start_dt is None or end_dt is None:
        raise HTTPException(status_code=400, detail="start and end are required")
    page = await asyncio.to_thread(
        app.trigger.get_event_markers_in_timerange,
        start_dt,
        end_dt,
        event_code=event_code,
        state=state,
        limit=limit,
        offset=offset,
    )
    return JSONResponse(marker_page_to_dict(page))


@router.get("/{event_id}", response_class=HTMLResponse)
async def event_detail(request: Request, event_id: str) -> HTMLResponse:
    """Render the event detail page."""
    app = get_pynenc_instance()
    event = await asyncio.to_thread(app.trigger.get_event, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    runs = await asyncio.to_thread(app.trigger.get_trigger_runs_for_event, event_id)

    def _resolve_conditions() -> list[dict]:
        # Unique union of valid + matched condition IDs, with matched flag.
        matched_ids = set(event.matched_condition_ids or [])
        all_ids = list(
            dict.fromkeys(list(event.valid_condition_ids or []) + list(matched_ids))
        )
        items: list[dict] = []
        for cid in all_ids:
            cond = app.trigger.get_condition(cid)
            if cond is None:
                items.append(
                    {
                        "kind": "Unknown",
                        "condition_id": cid,
                        "matched": cid in matched_ids,
                        "summary": [],
                        "raw": None,
                    }
                )
                continue
            try:
                raw = cond._to_json(app)
            except Exception:  # pragma: no cover - defensive
                raw = {}
            kind = type(cond).__name__
            summary: list[tuple[str, str]] = []
            for k in ("event_code", "cron_expression", "task_id_key", "task_key"):
                if k in raw and raw[k]:
                    summary.append((k, str(raw[k])))
            if "statuses" in raw and raw["statuses"]:
                summary.append(("statuses", ", ".join(map(str, raw["statuses"]))))
            if "exception_types" in raw and raw["exception_types"]:
                summary.append(
                    ("exception_types", ", ".join(map(str, raw["exception_types"])))
                )
            if "logic" in raw and raw["logic"]:
                summary.append(("logic", str(raw["logic"])))
            if "child_condition_ids" in raw and raw["child_condition_ids"]:
                summary.append(
                    (
                        "children",
                        ", ".join(c[:8] + "…" for c in raw["child_condition_ids"]),
                    )
                )
            for k in ("payload_filter", "result_filter", "arguments_filter", "filter"):
                if k in raw and raw[k]:
                    summary.append((k, "(callable)"))
            items.append(
                {
                    "kind": kind,
                    "condition_id": cid,
                    "matched": cid in matched_ids,
                    "summary": summary,
                    "raw": raw,
                }
            )
        return items

    conditions = await asyncio.to_thread(_resolve_conditions)
    trigger_run_timeline_urls = {
        run.trigger_run_id: timeline_url_for_trigger_run(run, [event.timestamp], app)
        for run in runs
    }

    return templates.TemplateResponse(
        request,
        "events/detail.html",
        context={
            "title": f"Event {event_id[:8]}",
            "app_id": app.app_id,
            "event": event,
            "trigger_runs": runs,
            "trigger_run_timeline_urls": trigger_run_timeline_urls,
            "conditions": conditions,
            "timeline_url": _timeline_url_for_event(event, app, runs),
        },
    )


@router.post("/auto-purge", response_class=JSONResponse)
async def events_auto_purge() -> JSONResponse:
    """Run ``BaseTrigger.auto_purge_events`` and report the deleted count."""
    app = get_pynenc_instance()
    try:
        deleted = await asyncio.to_thread(app.trigger.auto_purge_events)
        return JSONResponse(
            {"success": True, "deleted": deleted, "message": "Auto-purge completed."}
        )
    except Exception as exc:  # pragma: no cover - defensive
        return JSONResponse(
            {"success": False, "message": f"Error during auto-purge: {exc}"},
            status_code=500,
        )
