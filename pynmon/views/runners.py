"""
Runners monitoring views.

Provides monitoring interfaces for runner heartbeat tracking,
active runner status, and recovery service coordination.
All heavy data queries are offloaded to threads so the event loop stays free.
"""

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from pynmon.app import get_pynenc_instance, templates
from pynmon.util.atomic_service_timeline import load_atomic_service_timeline_data

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.orchestrator.atomic_service import ActiveRunnerInfo
    from pynenc.runner.runner_context import RunnerContext

router = APIRouter(prefix="/runners", tags=["runners"])


def _get_runner_contexts(
    app: "Pynenc", runner_ids: list[str]
) -> dict[str, "RunnerContext"]:
    """Fetch runner contexts for a list of runner IDs."""
    contexts = app.state_backend.get_runner_contexts(runner_ids)
    return {ctx.runner_id: ctx for ctx in contexts}


def _calculate_runner_age(runner_info: "ActiveRunnerInfo") -> dict[str, int | float]:
    """Calculate runner age in various units."""
    age_seconds = (
        runner_info.last_heartbeat - runner_info.creation_time
    ).total_seconds()
    return {
        "seconds": int(age_seconds),
        "minutes": int(age_seconds / 60),
        "hours": age_seconds / 3600,
        "total_seconds": age_seconds,
    }


def _calculate_last_heartbeat_age(runner_info: "ActiveRunnerInfo") -> dict[str, int]:
    """Calculate time since last heartbeat."""
    age_seconds = (
        datetime.now(runner_info.last_heartbeat.tzinfo) - runner_info.last_heartbeat
    ).total_seconds()
    return {
        "seconds": int(age_seconds),
        "minutes": int(age_seconds / 60),
        "is_stale": age_seconds > 60,
    }


def _enrich_runner_info(
    runner_info: "ActiveRunnerInfo",
    context: "RunnerContext | None",
    last_service: tuple[datetime | None, datetime | None] = (None, None),
) -> dict:
    """Enrich runner info with context and calculated fields."""
    last_service_start, last_service_end = last_service
    return {
        "runner_info": runner_info,
        "context": context,
        "age": _calculate_runner_age(runner_info),
        "heartbeat_age": _calculate_last_heartbeat_age(runner_info),
        "has_context": context is not None,
        "last_service_start": last_service_start,
        "last_service_end": last_service_end,
    }


def _load_last_service_by_runner(
    app: "Pynenc", runner_ids: list[str]
) -> dict[str, tuple[datetime | None, datetime | None]]:
    """Return per-runner (start, end) of the most recent execution row.

    Empty (None, None) entries for runners with no recorded execution.
    """
    result: dict[str, tuple[datetime | None, datetime | None]] = {}
    epoch = datetime.fromtimestamp(0, tz=UTC)
    now = datetime.now(UTC)
    for runner_id in runner_ids:
        try:
            executions = app.orchestrator.get_atomic_service_executions_in_timerange(
                epoch, now, limit=1, runner_id=runner_id
            )
        except Exception:
            executions = []
        if executions:
            result[runner_id] = (executions[0].start_time, executions[0].end_time)
        else:
            result[runner_id] = (None, None)
    return result


def _collect_runner_config(app: "Pynenc") -> dict[str, str | int | float]:
    """Collect runner configuration for display."""
    runner = app.runner
    conf = runner.conf
    runner_type = type(runner).__name__

    info: dict[str, str | int | float] = {
        "runner_type": runner_type,
        "wait_results_sleep_sec": conf.invocation_wait_results_sleep_time_sec,
        "loop_sleep_sec": conf.runner_loop_sleep_time_sec,
        "min_parallel_slots": conf.min_parallel_slots,
    }

    # Thread-runner-specific fields
    if hasattr(conf, "min_threads"):
        info["min_threads"] = conf.min_threads
        info["max_threads"] = conf.max_threads or "cpu_count"

    # Multi-thread-runner-specific fields
    if hasattr(conf, "min_processes"):
        info["min_processes"] = conf.min_processes
        info["max_processes"] = conf.max_processes or "cpu_count"
        info["idle_timeout_sec"] = conf.idle_timeout_process_sec
        info["enforce_max"] = conf.enforce_max_processes

    # Persistent-process-runner-specific fields
    if hasattr(conf, "num_processes"):
        info["num_processes"] = conf.num_processes or "cpu_count"

    return info


@router.get("/", response_class=HTMLResponse)
async def runners_view(request: Request) -> HTMLResponse:
    """Display active runners and heartbeat information."""
    app = get_pynenc_instance()

    active_runners = await asyncio.to_thread(app.orchestrator.get_active_runners)
    runner_ids = [r.runner_id for r in active_runners]
    contexts = await asyncio.to_thread(_get_runner_contexts, app, runner_ids)
    last_service = await asyncio.to_thread(
        _load_last_service_by_runner, app, runner_ids
    )

    # Enrich runners with context
    enriched_runners = [
        _enrich_runner_info(
            runner,
            contexts.get(runner.runner_id),
            last_service.get(runner.runner_id, (None, None)),
        )
        for runner in active_runners
    ]

    # Statistics
    total_runners = len(active_runners)
    atomic_eligible = sum(1 for r in active_runners if r.allow_to_run_atomic_service)
    runners_with_history = sum(
        1 for start, _ in last_service.values() if start is not None
    )

    runner_config = _collect_runner_config(app)

    return templates.TemplateResponse(
        request,
        "runners/overview.html",
        context={
            "title": "Active Runners",
            "app_id": app.app_id,
            "enriched_runners": enriched_runners,
            "total_runners": total_runners,
            "atomic_eligible": atomic_eligible,
            "runners_with_history": runners_with_history,
            "heartbeat_timeout_minutes": app.conf.runner_considered_dead_after_minutes,
            "atomic_service_check_interval_minutes": app.conf.atomic_service_check_interval_minutes,
            "runner_config": runner_config,
        },
    )


@router.get("/refresh", response_class=HTMLResponse)
async def refresh_runners(request: Request) -> HTMLResponse:
    """Refresh runner data for HTMX partial updates."""
    app = get_pynenc_instance()

    active_runners = await asyncio.to_thread(app.orchestrator.get_active_runners)
    runner_ids = [r.runner_id for r in active_runners]
    contexts = await asyncio.to_thread(_get_runner_contexts, app, runner_ids)
    last_service = await asyncio.to_thread(
        _load_last_service_by_runner, app, runner_ids
    )

    enriched_runners = [
        _enrich_runner_info(
            runner,
            contexts.get(runner.runner_id),
            last_service.get(runner.runner_id, (None, None)),
        )
        for runner in active_runners
    ]

    return templates.TemplateResponse(
        request,
        "runners/partials/runners_table.html",
        context={"enriched_runners": enriched_runners},
    )


@router.get("/{runner_id}", response_class=HTMLResponse)
async def runner_detail(request: Request, runner_id: str) -> HTMLResponse:
    """Display detailed information for a specific runner."""
    app = get_pynenc_instance()

    # Find runner in active runners
    active_runners = await asyncio.to_thread(app.orchestrator.get_active_runners)
    runner_info = next((r for r in active_runners if r.runner_id == runner_id), None)

    if not runner_info:
        raise HTTPException(status_code=404, detail="Runner not found")

    # Get runner context and parent context
    context = app.state_backend.get_runner_context(runner_id)
    parent_context = None
    if context and context.parent_ctx:
        parent_context = context.parent_ctx

    # Calculate execution stats if available
    execution_stats = None
    last_service = await asyncio.to_thread(
        _load_last_service_by_runner, app, [runner_id]
    )
    last_start, last_end = last_service.get(runner_id, (None, None))
    if last_start and last_end:
        execution_stats = {
            "last_start": last_start,
            "last_end": last_end,
            "duration_seconds": (last_end - last_start).total_seconds(),
        }

    # Recent atomic-service executions for this runner (most recent first).
    recent_executions = await asyncio.to_thread(
        load_atomic_service_timeline_data,
        app,
        limit=20,
        runner_id=runner_id,
        min_duration_seconds=0.0,
    )

    return templates.TemplateResponse(
        request,
        "runners/detail.html",
        context={
            "title": f"Runner {runner_id[:8]}",
            "app_id": app.app_id,
            "runner_info": runner_info,
            "context": context,
            "parent_context": parent_context,
            "age": _calculate_runner_age(runner_info),
            "heartbeat_age": _calculate_last_heartbeat_age(runner_info),
            "execution_stats": execution_stats,
            "atomic_service_history": recent_executions,
        },
    )
