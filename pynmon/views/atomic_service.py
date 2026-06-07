"""Atomic-service scheduling visibility for Pynmon.

The page surfaces what the audit (`13_atomic_service_audit.md`) calls the
"runtime event model": for every active runner we show the scheduling
decision (assigned vs not-assigned-slot, why), the active-runner table the
decision was made from, the latest recorded execution windows, and the
recent skip-event feed produced by the orchestrator backend.

This is a read-only debugging panel. It performs the same calculation the
orchestrator does on each cycle without claiming a run, so refreshing it has
no side effects on scheduling.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from time import time
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynenc.orchestrator.atomic_service import (
    decide_atomic_service_claim,
)
from pynmon.app import get_pynenc_instance, templates
from pynmon.util.atomic_service_timeline import (
    ATOMIC_TIMELINE_MIN_DURATION_CHOICES,
    ATOMIC_TIMELINE_PAGE_SIZE_CHOICES,
    ATOMIC_TIMELINE_STATUS_CHOICES,
    add_atomic_service_period_offsets,
    atomic_service_execution_data,
    filter_atomic_service_timeline_executions,
    find_atomic_service_execution,
    load_atomic_service_timeline_executions,
    paginate_atomic_service_timeline,
    parse_atomic_timeline_filters,
    selected_atomic_service_run_id as _selected_atomic_service_run_id,
)
from pynmon.util.trigger_timeline import timeline_url_for_trigger_run

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.orchestrator.atomic_service import (
        AtomicServiceClaim,
        AtomicServiceExecution,
    )
    from pynenc.trigger.monitoring import TriggerRunRecord

router = APIRouter(
    prefix="/runners/atomic-service",
    tags=["atomic-service"],
)


def _collect_context(app: Pynenc) -> dict[str, Any]:
    """Build the full panel context. Runs in a worker thread."""
    active_runners = app.orchestrator.get_active_runners(can_run_atomic_service=True)
    now_ts = time()
    interval_minutes = float(app.conf.atomic_service_interval_minutes)
    spread_margin_minutes = float(app.conf.atomic_service_spread_margin_minutes)

    decisions_by_runner: dict[str, AtomicServiceClaim] = {}
    now_dt = datetime.fromtimestamp(now_ts, tz=UTC)
    stabilization_seconds = (
        float(app.conf.atomic_service_membership_stabilization_minutes) * 60.0
    )
    max_start_slot_fraction = float(app.conf.atomic_service_max_start_slot_fraction)
    for runner in active_runners:
        decisions_by_runner[runner.runner_id] = decide_atomic_service_claim(
            runner_id=runner.runner_id,
            active_runners=active_runners,
            current_time=now_ts,
            service_interval_minutes=interval_minutes,
            spread_margin_minutes=spread_margin_minutes,
            membership_stabilization_seconds=stabilization_seconds,
            max_start_slot_fraction=max_start_slot_fraction,
        )

    # Slot diagram for the cycle currently in progress: derive from the
    # first decision (they all share the same cycle_start / membership shape).
    slot_rows: list[dict[str, Any]] = []
    cycle_start_iso: str | None = None
    cycle_end_iso: str | None = None
    if decisions_by_runner:
        any_decision = next(iter(decisions_by_runner.values()))
        cycle_start = any_decision.cycle_start
        cycle_end = cycle_start + timedelta(minutes=interval_minutes)
        cycle_start_iso = cycle_start.isoformat()
        cycle_end_iso = cycle_end.isoformat()
        for runner in active_runners:
            decision = decisions_by_runner[runner.runner_id]
            slot_rows.append(
                {
                    "runner_id": runner.runner_id,
                    "position": decision.runner_position,
                    "slot_start": (
                        decision.slot_start.isoformat() if decision.slot_start else None
                    ),
                    "slot_end": (
                        decision.slot_end.isoformat() if decision.slot_end else None
                    ),
                    "is_current_owner": decision.is_slot_owner_now,
                    "assigned_runner_id": decision.assigned_runner_id,
                    "reason": str(decision.reason),
                    "stable_runner_count": decision.stable_runner_count,
                }
            )

    runner_rows = []
    last_service_by_runner: dict[str, tuple[datetime | None, datetime | None]] = {}
    for runner in active_runners:
        try:
            executions = app.orchestrator.get_atomic_service_executions_in_timerange(
                datetime.fromtimestamp(0, tz=UTC),
                now_dt,
                limit=1,
                runner_id=runner.runner_id,
            )
        except Exception:
            executions = []
        if executions:
            last_service_by_runner[runner.runner_id] = (
                executions[0].start_time,
                executions[0].end_time,
            )
        else:
            last_service_by_runner[runner.runner_id] = (None, None)
    for runner in active_runners:
        decision = decisions_by_runner.get(runner.runner_id)  # type: ignore[assignment]
        eligible_since = runner.creation_time
        eligible_age_seconds = now_ts - eligible_since.timestamp()
        in_grace = (
            stabilization_seconds > 0.0 and eligible_age_seconds < stabilization_seconds
        )
        last_start, last_end = last_service_by_runner[runner.runner_id]
        runner_rows.append(
            {
                "runner_id": runner.runner_id,
                "creation_time": runner.creation_time.isoformat(),
                "last_heartbeat": runner.last_heartbeat.isoformat(),
                "allow_to_run_atomic_service": (runner.allow_to_run_atomic_service),
                "atomic_service_eligible_since": eligible_since.isoformat(),
                "eligible_age_seconds": eligible_age_seconds,
                "in_membership_grace": in_grace,
                "in_runnable_set": (
                    runner.allow_to_run_atomic_service and not in_grace
                ),
                "last_service_start": (last_start.isoformat() if last_start else None),
                "last_service_end": (last_end.isoformat() if last_end else None),
                "decision": {
                    "should_try_start": (
                        decision.should_try_start if decision else None
                    ),
                    "reason": (str(decision.reason) if decision else "unknown"),
                    "assigned_runner_id": (
                        decision.assigned_runner_id if decision else None
                    ),
                }
                if decision
                else None,
            }
        )

    try:
        blocked_executions = (
            app.orchestrator.get_atomic_service_executions_in_timerange(
                datetime.fromtimestamp(0, tz=UTC),
                now_dt,
                limit=200,
            )
        )
    except (AttributeError, NotImplementedError):
        blocked_executions = []
    event_rows = []
    for execution in blocked_executions:
        from pynenc.orchestrator.atomic_service import AtomicServiceExecutionStatus

        if execution.status != AtomicServiceExecutionStatus.BLOCKED:
            continue
        raw_reason = execution.reason or ""
        if ":" in raw_reason:
            reason_part, _, message_part = raw_reason.partition(":")
        else:
            reason_part, message_part = raw_reason, ""
        run = execution.atomic_service_run
        event_rows.append(
            {
                "created_at": execution.start_time.isoformat(),
                "cycle_start": (
                    run.cycle_start.isoformat() if run.cycle_start else None
                ),
                "slot_start": (run.slot_start.isoformat() if run.slot_start else None),
                "slot_end": (run.slot_end.isoformat() if run.slot_end else None),
                "runner_id": execution.runner_id,
                "reason": reason_part,
                "message": message_part,
            }
        )
        if len(event_rows) >= 50:
            break

    return {
        "title": "Atomic Service Scheduler",
        "app_id": app.app_id,
        "config": {
            "interval_minutes": interval_minutes,
            "spread_margin_minutes": spread_margin_minutes,
            "check_interval_minutes": (app.conf.atomic_service_check_interval_minutes),
            "runner_dead_after_minutes": (
                app.conf.runner_considered_dead_after_minutes
            ),
            "membership_stabilization_minutes": (
                app.conf.atomic_service_membership_stabilization_minutes
            ),
            "max_start_slot_fraction": (
                app.conf.atomic_service_max_start_slot_fraction
            ),
        },
        "cycle": {
            "now": datetime.fromtimestamp(now_ts, tz=UTC).isoformat(),
            "cycle_start": cycle_start_iso,
            "cycle_end": cycle_end_iso,
        },
        "slot_rows": slot_rows,
        "runner_rows": runner_rows,
        "events": event_rows,
        "total_runners": len(active_runners),
    }


@router.get("/scheduling", response_class=HTMLResponse)
async def atomic_service_view(request: Request) -> HTMLResponse:
    app = get_pynenc_instance()
    context = await asyncio.to_thread(_collect_context, app)
    return templates.TemplateResponse(
        request, "atomic_service/overview.html", context=context
    )


@router.get("/scheduling/refresh", response_class=HTMLResponse)
async def refresh_atomic_service(request: Request) -> HTMLResponse:
    app = get_pynenc_instance()
    context = await asyncio.to_thread(_collect_context, app)
    return templates.TemplateResponse(
        request, "atomic_service/_panel.html", context=context
    )


def _get_running_atomic_service_runner(app: Pynenc) -> dict[str, Any] | None:
    """Return a small dict describing the runner currently holding the atomic
    -service slot, if any.

    This is only a pynmon visibility helper. If stale or inconsistent data
    exposes more than one RUNNING execution, pick the most recently started
    one because it is the best clue for the current in-progress claim.
    """
    try:
        active_executions = app.orchestrator.get_active_atomic_service_executions()
    except Exception:
        return None
    if not active_executions:
        return None
    most_recent = max(active_executions, key=lambda e: e.start_time)
    return {
        "runner_id": most_recent.runner_id,
        "last_service_start": most_recent.start_time,
    }


def _load_trigger_runs_for_atomic_service(
    app: Pynenc, atomic_service_run_id: str, *, limit: int = 200
) -> list[TriggerRunRecord]:
    """Return retained trigger runs produced by one atomic-service run."""
    runs = app.trigger.get_trigger_runs_in_timerange(
        start_time=datetime.min.replace(tzinfo=UTC),
        end_time=datetime.now(UTC),
    )
    matches = [
        run for run in runs if run.atomic_service_run_id == atomic_service_run_id
    ]
    matches.sort(
        key=lambda run: run.executed_at
        or run.claimed_at
        or datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )
    return matches[:limit]


def _atomic_service_runner_id_from_refs(
    execution: AtomicServiceExecution | None,
    trigger_runs: list[TriggerRunRecord],
) -> str:
    """Return the best-known runner id for an atomic-service run."""
    if execution is not None:
        return execution.runner_id
    for run in trigger_runs:
        if run.atomic_service_runner_id:
            return run.atomic_service_runner_id
    return ""


def _trigger_run_row(run: TriggerRunRecord, app: Pynenc) -> dict:
    """Build template data for a trigger run that references an AS run."""
    return {
        "trigger_run_id": run.trigger_run_id,
        "trigger_id": run.trigger_id,
        "task_id_key": run.task_id_key,
        "logic_value": run.logic_value,
        "claimed_at": run.claimed_at,
        "executed_at": run.executed_at,
        "triggered_invocation_id": run.triggered_invocation_id,
        "detail_url": f"/trigger-runs/{quote(run.trigger_run_id, safe='')}",
        "timeline_url": timeline_url_for_trigger_run(run, app=app),
    }


@router.get("/runs/{atomic_service_run_id}", response_class=HTMLResponse)
async def atomic_service_run_detail(
    request: Request, atomic_service_run_id: str
) -> HTMLResponse:
    """Display a retained or trigger-referenced atomic-service run."""
    app = get_pynenc_instance()
    execution, trigger_runs = await asyncio.gather(
        asyncio.to_thread(find_atomic_service_execution, app, atomic_service_run_id),
        asyncio.to_thread(
            _load_trigger_runs_for_atomic_service, app, atomic_service_run_id
        ),
    )
    runner_id = _atomic_service_runner_id_from_refs(execution, trigger_runs)
    runner_context = None
    if runner_id:
        runner_context = await asyncio.to_thread(
            app.state_backend.get_runner_context, runner_id
        )
    active_runners = await asyncio.to_thread(app.orchestrator.get_active_runners)
    active_runner = next(
        (runner for runner in active_runners if runner.runner_id == runner_id), None
    )
    execution_data = (
        atomic_service_execution_data(
            execution.runner_id,
            execution.start_time,
            execution.end_time,
            execution.duration_seconds,
            execution.atomic_service_run_id,
            status=execution.status.value,
            reason=execution.reason,
            runner_alive=any(
                r.runner_id == execution.runner_id for r in active_runners
            ),
        )
        if execution is not None
        else None
    )
    trigger_run_rows = [_trigger_run_row(run, app) for run in trigger_runs]

    return templates.TemplateResponse(
        request,
        "runners/atomic_service_run_detail.html",
        context={
            "title": "Atomic Service Run",
            "app_id": app.app_id,
            "atomic_service_run_id": atomic_service_run_id,
            "runner_id": runner_id,
            "runner_context": runner_context,
            "active_runner": active_runner,
            "execution": execution_data,
            "trigger_runs": trigger_run_rows,
            "has_monitoring_reference": bool(trigger_run_rows),
        },
    )


@router.get("/timeline", response_class=HTMLResponse)
async def atomic_service_timeline(request: Request) -> HTMLResponse:
    """Display atomic service execution timeline."""
    app = get_pynenc_instance()
    filters = parse_atomic_timeline_filters(request)

    active_runners = await asyncio.to_thread(app.orchestrator.get_active_runners)
    executions = await asyncio.to_thread(
        load_atomic_service_timeline_executions, app, filters
    )
    filtered_executions = filter_atomic_service_timeline_executions(executions, filters)
    selected_run_id = _selected_atomic_service_run_id(filtered_executions, filters)
    selected_atomic_service_found = any(
        execution.atomic_service_run_id == selected_run_id
        for execution in filtered_executions
    )
    page_executions, pagination = paginate_atomic_service_timeline(
        filtered_executions, filters, selected_run_id
    )
    used_fallback = False
    active_runner_ids = {runner.runner_id for runner in active_runners}
    timeline_data = [
        atomic_service_execution_data(
            execution.runner_id,
            execution.start_time,
            execution.end_time,
            execution.duration_seconds,
            execution.atomic_service_run_id,
            status=execution.status.value,
            reason=execution.reason,
            runner_alive=execution.runner_id in active_runner_ids,
        )
        for execution in page_executions
    ]
    for row in timeline_data:
        row["is_selected"] = (
            bool(selected_run_id) and row["atomic_service_run_id"] == selected_run_id
        )
    add_atomic_service_period_offsets(timeline_data)

    runner_ids = sorted(
        {runner.runner_id for runner in active_runners}
        | {execution.runner_id for execution in executions}
    )
    running_atomic_service = _get_running_atomic_service_runner(app)

    return templates.TemplateResponse(
        request,
        "runners/atomic_service_timeline.html",
        context={
            "title": "Atomic Service Timeline",
            "app_id": app.app_id,
            "timeline_data": timeline_data,
            "service_interval_minutes": app.conf.atomic_service_interval_minutes,
            "retention_minutes": app.conf.atomic_service_execution_retention_minutes,
            "max_records": app.conf.atomic_service_execution_max_records,
            "filter_limit": filters.page_size,
            "filter_page_size": filters.page_size,
            "filter_runner_id": filters.runner_id or "",
            "filter_atomic_service_run_id": filters.atomic_service_run_id,
            "filter_selected_atomic_service_run_id": selected_run_id,
            "filter_status": filters.status,
            "filter_min_duration_seconds": filters.min_duration_seconds,
            "limit_choices": ATOMIC_TIMELINE_PAGE_SIZE_CHOICES,
            "page_size_choices": ATOMIC_TIMELINE_PAGE_SIZE_CHOICES,
            "min_duration_choices": ATOMIC_TIMELINE_MIN_DURATION_CHOICES,
            "status_choices": ATOMIC_TIMELINE_STATUS_CHOICES,
            "runner_id_choices": runner_ids,
            "used_fallback": used_fallback,
            "running_atomic_service": running_atomic_service,
            "pagination": pagination,
            "selected_atomic_service_found": selected_atomic_service_found,
        },
    )


# Re-export internal helper aliases for tests that previously imported them
# from the runners module.
_atomic_service_execution_data = atomic_service_execution_data
_find_atomic_service_execution = find_atomic_service_execution
_load_atomic_service_timeline_executions = load_atomic_service_timeline_executions
_filter_atomic_service_timeline_executions = filter_atomic_service_timeline_executions
_paginate_atomic_service_timeline = paginate_atomic_service_timeline
_parse_atomic_timeline_filters = parse_atomic_timeline_filters
_add_atomic_service_period_offsets = add_atomic_service_period_offsets
