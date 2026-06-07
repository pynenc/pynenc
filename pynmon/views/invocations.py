import asyncio
import json
import logging
import time
import traceback
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from pynenc.exceptions import InvocationNotFoundError
from pynenc.identifiers.invocation_id import InvocationId
from pynenc.identifiers.task_id import TaskId
from pynenc.invocation.status import InvocationStatus
from pynmon.app import get_pynenc_instance, templates
from pynmon.util.formatting import RunnerContextInfo
from pynmon.util.time_ranges import parse_time_range, parse_resolution
from pynmon.util.view_helpers import format_call_arguments
from pynmon.util.svg.atomic_service import (
    AtomicServiceWindow,
    assign_atomic_service_sub_lanes,
)
from pynmon.util.svg.builder import TimelineDataBuilder
from pynmon.util.svg.models import TimelineConfig
from pynmon.util.svg.renderer import TimelineSVGRenderer
from pynmon.util.svg.runner_info import RunnerInfo
from pynmon.util.trigger_timeline import timeline_url_for_trigger_run
from pynmon.util.trigger_monitoring import (
    relevant_trigger_run_participants,
    trigger_run_to_dict,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.runner.runner_context import RunnerContext
    from pynenc.state_backend.base_state_backend import InvocationHistory
    from pynenc.trigger.monitoring import TriggerRunRecord
    from pynmon.util.svg.timeline_data import TimelineData


@dataclass
class TimelineRequest:
    """Grouped parameters for building an SVG timeline."""

    app: "Pynenc"
    start_time: datetime
    end_time: datetime
    config: "TimelineConfig"
    limit: int | None
    task_id: "TaskId | None" = None
    workflow_inv_ids: set[str] | None = None
    inv_ids_filter: set[str] | None = None
    focus_event: str | None = None
    collapse_external: bool = True
    show_system_tasks: bool = True
    show_atomic_service: bool = True


@dataclass
class _IterState:
    """Mutable accumulation state for history iteration."""

    builder: "TimelineDataBuilder"
    runner_contexts: dict = field(default_factory=dict)
    invocations_seen: set = field(default_factory=set)
    history_count: int = 0
    allowed_invocation_ids: set | None = None
    excluded_invocation_ids: set[str] = field(default_factory=set)
    skipped_by_task_filter: int = 0
    skipped_system_tasks: int = 0
    batch_number: int = 0


router = APIRouter(prefix="/invocations", tags=["invocations"])
logger = logging.getLogger("pynmon.views.invocations")

_SYSTEM_TASK_PREFIX = "pynenc.core_tasks."


@router.get("/", response_class=HTMLResponse)
async def invocations_list(
    request: Request,
    status: str | None = None,
    task_id: str | None = None,
    workflow_id: str | None = None,
    workflow_type: str | None = None,
    limit: int = 50,
    page: int = 1,
) -> HTMLResponse:
    """Display invocations with optional filtering and pagination."""
    limit = max(1, min(limit, 1000))
    page = max(1, page)
    app = get_pynenc_instance()
    parsed_task_id = TaskId.from_key(task_id) if task_id else None

    # Convert status to list format for consistent processing
    status_list = None
    if status:
        status_list = [status]

    # Convert status strings to InvocationStatus enum values
    statuses = None
    if status_list:
        statuses = [
            InvocationStatus[s.upper()]
            for s in status_list
            if hasattr(InvocationStatus, s.upper())
        ]

    # Pre-load workflow-filtered invocation IDs if workflow filters active
    workflow_inv_ids = _load_workflow_invocation_ids(app, workflow_id, workflow_type)

    total_count = await asyncio.to_thread(
        app.orchestrator.count_invocations,
        task_id=parsed_task_id,
        statuses=statuses,
    )
    total_pages = max(1, (total_count + limit - 1) // limit)
    page = min(page, total_pages)
    offset = (page - 1) * limit

    # Offload heavy DB queries to a thread so the event loop stays free
    def _fetch_invocations() -> tuple[list["DistributedInvocation"], int]:
        ids = app.orchestrator.get_invocation_ids_paginated(
            task_id=parsed_task_id,
            statuses=statuses,
            limit=limit,
            offset=offset,
        )
        # Apply workflow filter if active
        if workflow_inv_ids is not None:
            ids = [i for i in ids if str(i) in workflow_inv_ids]
        invocations = [app.state_backend.get_invocation(inv_id) for inv_id in ids]
        return invocations, total_count

    all_invocations, total_count = await asyncio.to_thread(_fetch_invocations)

    # Get all possible statuses for filter dropdown
    all_statuses = [status.name.lower() for status in InvocationStatus]

    # Get all available task IDs for the dropdown
    all_task_ids = list(app.tasks.keys())

    # Get all workflow types for the dropdown
    all_workflow_types = await asyncio.to_thread(
        lambda: [str(wt) for wt in app.state_backend.get_all_workflow_types()]
    )

    return templates.TemplateResponse(
        request,
        "invocations/list.html",
        context={
            "title": "Invocations Monitor",
            "app_id": app.app_id,
            "invocations": all_invocations,
            "all_statuses": all_statuses,
            "all_task_ids": all_task_ids,
            "all_workflow_types": all_workflow_types,
            "current_filters": {
                "status": status_list or [],
                "task_id": task_id or "",
                "workflow_id": workflow_id or "",
                "workflow_type": workflow_type or "",
                "limit": limit,
            },
            "pagination": {
                "page": page,
                "limit": limit,
                "total_count": total_count,
                "total_pages": total_pages,
                "has_prev": page > 1,
                "has_next": page < total_pages,
            },
        },
    )


def _get_tasks_to_check(app: "Pynenc", task_id: "TaskId | None") -> list:
    """Get the list of tasks to check based on task_id filter.

    :param app: The Pynenc application instance
    :param task_id: Optional task ID to filter by
    :return: List of tasks to check
    :raises ValueError: If task_id is provided but the task doesn't exist
    """
    if task_id:
        try:
            task = app.get_task(task_id)
            return [task] if task else []
        except (ModuleNotFoundError, AttributeError, ValueError) as e:
            # Re-raise with a more descriptive error message
            if isinstance(e, ModuleNotFoundError):
                raise ValueError(
                    f"Task not found: Module '{task_id.module}' could not be imported. {str(e)}"
                ) from e
            elif isinstance(e, AttributeError):
                module_name = task_id.module
                function_name = task_id.func_name
                raise ValueError(
                    f"Task not found: Function '{function_name}' not found in module '{module_name}'. {str(e)}"
                ) from e
            else:
                raise ValueError(
                    f"Task not found: Invalid task ID format '{task_id}'. {str(e)}"
                ) from e
    return list(app.tasks.values())


def _load_workflow_invocation_ids(
    app: "Pynenc",
    workflow_id: str | None,
    workflow_type: str | None,
) -> set[str] | None:
    """Pre-load invocation IDs matching workflow filters.

    :param Pynenc app: The Pynenc application instance
    :param str | None workflow_id: Workflow ID to filter by
    :param str | None workflow_type: Workflow type key to filter by
    :return: Set of matching invocation ID strings, or None if no filter
    """
    if not workflow_id and not workflow_type:
        return None
    ids = app.state_backend.get_invocation_ids_by_workflow(
        workflow_id=workflow_id,
        workflow_type_key=workflow_type,
    )
    return {str(i) for i in ids}


def _merge_allowed_ids(
    task_ids: set | None,
    workflow_ids: set[str] | None,
) -> set | None:
    """Intersect task and workflow ID sets (None means "all").

    :param set | None task_ids: IDs from task filter, or None for all
    :param set[str] | None workflow_ids: IDs from workflow filter, or None for all
    :return: Intersection of non-None sets, or None if both are None
    """
    if task_ids is None:
        return workflow_ids
    if workflow_ids is None:
        return task_ids
    return task_ids & workflow_ids


def _load_event_markers(req: TimelineRequest, trigger_runs: list | None = None) -> list:
    """Fetch trigger event markers in ``[start_time, end_time]``.

    Uses the indexed :meth:`BaseTrigger.get_event_markers_in_timerange`
    projection rather than full event loads, so the timeline stays cheap
    even when the window contains many events. The returned list is the
    backend-side projection; truncation is exposed through
    :attr:`TimelineData.events_truncated` so the UI can surface it.
    """
    from pynmon.util.svg.event_markers import EventMarker as RenderEventMarker
    from pynmon.util.svg.event_markers import cron_event_markers_from_trigger_runs

    markers: list[RenderEventMarker] = []
    seen_event_ids: set[str] = set()
    condition_types_by_event = _condition_types_by_event(trigger_runs or [])
    try:
        page = req.app.trigger.get_event_markers_in_timerange(
            req.start_time,
            req.end_time,
            # Default: only show events that actually triggered at least one
            # invocation. Events with no triggered invocations render as orphan
            # vertical lines with no connection to anything, which is noise.
            state="triggered",
            limit=1000,
        )
    except Exception:
        logger.debug(
            "trigger backend get_event_markers_in_timerange failed; "
            "skipping event markers"
        )
        return markers
    for marker in page.markers:
        try:
            full_event = req.app.trigger.get_event(marker.event_id)
        except Exception:
            full_event = None
        seen_event_ids.add(marker.event_id)
        markers.append(
            RenderEventMarker(
                event_id=marker.event_id,
                event_code=marker.event_code,
                timestamp=marker.timestamp,
                triggered=marker.triggered,
                matched=marker.matched,
                payload_excerpt=_event_payload_excerpt(full_event),
                triggered_invocation_ids=list(
                    getattr(full_event, "triggered_invocation_ids", []) or []
                ),
                emitted_by_invocation_id=getattr(
                    full_event, "emitted_by_invocation_id", None
                ),
                emitted_by_runner_context_id=getattr(
                    full_event,
                    "emitted_by_runner_context_id",
                    marker.emitted_by_runner_context_id,
                ),
                condition_types=_condition_types_for_event(
                    marker.event_id,
                    marker.event_code,
                    condition_types_by_event,
                ),
            )
        )
    if req.focus_event and req.focus_event not in seen_event_ids:
        try:
            focused_event = req.app.trigger.get_event(req.focus_event)
        except Exception:
            focused_event = None
        if (
            focused_event
            and req.start_time <= focused_event.timestamp <= req.end_time
            and (focused_event.triggered or focused_event.matched)
        ):
            markers.append(
                RenderEventMarker(
                    event_id=focused_event.event_id,
                    event_code=focused_event.event_code,
                    timestamp=focused_event.timestamp,
                    triggered=focused_event.triggered,
                    matched=focused_event.matched,
                    payload_excerpt=_event_payload_excerpt(focused_event),
                    triggered_invocation_ids=list(
                        focused_event.triggered_invocation_ids or []
                    ),
                    emitted_by_invocation_id=focused_event.emitted_by_invocation_id,
                    emitted_by_runner_context_id=(
                        focused_event.emitted_by_runner_context_id
                    ),
                    condition_types=_condition_types_for_event(
                        focused_event.event_id,
                        focused_event.event_code,
                        condition_types_by_event,
                    ),
                )
            )
    if page.truncated:
        logger.info(
            "Event marker query returned %d/%d markers (truncated)",
            len(page.markers),
            page.total,
        )
    markers.extend(cron_event_markers_from_trigger_runs(trigger_runs or []))
    markers.sort(key=lambda marker: marker.timestamp)
    return markers


def _event_payload_excerpt(event: Any) -> str:
    """Return a short marker tooltip excerpt from a resolved event payload."""
    payload = getattr(event, "payload", None)
    if not payload:
        return ""
    try:
        keys = list(payload.keys())[:3]
    except AttributeError:
        return ""
    return f"payload keys: {', '.join(keys)}"


def _condition_types_by_event(trigger_runs: list) -> dict[str, list[str]]:
    """Return event id -> participant context types for marker overlays."""
    result: dict[str, list[str]] = {}
    for run in trigger_runs:
        for participant in run.participants or []:
            if not participant.event_id or not participant.context_type:
                continue
            types = result.setdefault(participant.event_id, [])
            if participant.context_type not in types:
                types.append(participant.context_type)
    return result


def _condition_types_for_event(
    event_id: str,
    event_code: str,
    condition_types_by_event: dict[str, list[str]],
) -> list[str]:
    """Return condition context types for an event marker.

    Trigger-run participants are the authoritative source. When a focused
    emitted event has no trigger run (for example a result-capture event that
    only records payload), infer a useful type from the event-code prefix so
    the timeline still communicates whether it is result/status/exception-like.
    """
    if types := condition_types_by_event.get(event_id):
        return types
    prefix = event_code.split(".", 1)[0].lower()
    return {
        "status": ["StatusContext"],
        "result": ["ResultContext"],
        "exception": ["ExceptionContext"],
        "cron": ["CronContext"],
    }.get(prefix, [])


def _load_trigger_runs(req: TimelineRequest) -> "list":
    """Fetch trigger-run records for the timeline window.

    The records drive the new participant relation lines added in Phase
    5 (status / result / exception / cron edges). Backend failures or
    backends without monitoring data degrade silently — only direct-call
    and event-trigger lines render.
    """
    from pynenc.trigger.monitoring import TriggerRunRecord

    runs: list[TriggerRunRecord] = []
    try:
        runs = list(
            req.app.trigger.get_trigger_runs_in_timerange(
                req.start_time,
                req.end_time,
                limit=2000,
            )
        )
    except Exception:
        logger.debug(
            "trigger backend get_trigger_runs_in_timerange failed; "
            "skipping trigger-run relations"
        )
    return runs


def _build_svg_timeline(req: TimelineRequest) -> str:
    """
    Build SVG timeline from a TimelineRequest.

    :param TimelineRequest req: Grouped parameters
    :return: SVG markup as string
    """
    t0 = time.time()
    # Load trigger context first so the scope can be expanded with the
    # invocations that emitted the markers' source events. Otherwise an
    # event-triggered invocation (e.g. release_approval) would render its
    # ``approval.requested`` marker without an anchor because the emitting
    # invocation (``request_approval``) is neither a listed source nor the
    # focus, leaving the marker visually floating between unrelated bars.
    trigger_runs = _load_trigger_runs(req)
    markers = _load_event_markers(req, trigger_runs)
    # Collect every invocation that the rendered events / trigger runs
    # already reference (emitter, triggered, source). These are the
    # invocations that *must* appear on the timeline so the markers and
    # relation lines have something to anchor to. They are folded into the
    # scope below and ghost-loaded later if their status history does not
    # intersect the window.
    referenced_ids = _collect_referenced_invocation_ids(markers, trigger_runs)
    task_ids = _load_task_invocation_ids(req)
    allowed = _merge_allowed_ids(task_ids, req.workflow_inv_ids)
    # When an explicit invocation-id scope was requested (e.g. from a
    # trigger run "Show in Timeline" link) or any of the task / workflow
    # filters narrow the view, expand the scope with the invocations that
    # the visible events and trigger runs reference. Without this, an
    # event-triggered invocation can render its event marker while the
    # emitting invocation is silently filtered out, leaving the marker
    # visually floating between unrelated bars.
    if allowed is not None or req.inv_ids_filter is not None:
        scope = set(req.inv_ids_filter or set()) | referenced_ids
        if scope:
            allowed = _merge_allowed_ids(allowed, scope)
    excluded = _load_system_task_invocation_ids(req)
    state = _IterState(
        builder=TimelineDataBuilder(
            config=req.config,
            collapse_external=req.collapse_external,
        ),
        allowed_invocation_ids=allowed,
        excluded_invocation_ids=excluded,
    )
    _register_event_runner_contexts(req, state, markers)
    _accumulate_history(req, state)
    # Complete the left boundary for invocations that already have at least
    # one visible status point. ``iter_history_in_timerange`` only returns
    # in-window records, so a SUCCESS/FAILED point can appear without the
    # RUNNING/PENDING segment that began before ``start_time``. Add a
    # synthetic segment entry at the left edge so partially visible
    # invocations render as bars instead of floating status dots.
    _load_visible_invocation_boundary_history(req, state)
    # Backfill clipped "ghost" history for invocations referenced by visible
    # markers / trigger runs whose own status records fall entirely outside
    # the time window. Without this, the strict event-marker anchoring drops
    # markers that point at off-screen emitters, leaving floating dots above
    # the lanes (e.g. ``stock.reserved`` over ThreadRunner when the emitter
    # invocation finished before window.start).
    _load_ghost_invocation_history(req, state, markers, trigger_runs)
    t_hist = time.time()
    logger.info(
        f"History accumulated: {len(state.invocations_seen)} invocations, "
        f"{state.history_count} entries in {t_hist - t0:.2f}s"
        + (
            f", {state.skipped_by_task_filter} skipped by task filter"
            if state.skipped_by_task_filter
            else ""
        )
        + (
            f", {state.skipped_system_tasks} skipped system-task entries"
            if state.skipped_system_tasks
            else ""
        )
    )
    data = state.builder.build(start_time=req.start_time, end_time=req.end_time)
    if req.show_atomic_service:
        data.atomic_service_windows = _load_atomic_service_windows(
            req, data, trigger_runs
        )
    data.trigger_runs = trigger_runs
    data.event_markers = markers
    t_build = time.time()
    logger.info(f"SVG data built in {t_build - t_hist:.2f}s")
    svg = TimelineSVGRenderer().render(data)
    logger.info(f"SVG rendered in {time.time() - t_build:.2f}s ({len(svg):,} chars)")
    return svg


def _register_event_runner_contexts(
    req: TimelineRequest, state: _IterState, markers: list
) -> None:
    """Add event emitters as timeline lanes and apply external-runner collapsing."""
    context_ids = {
        marker.emitted_by_runner_context_id
        for marker in markers
        if marker.emitted_by_runner_context_id
    }
    if not context_ids:
        return
    contexts = {
        context.runner_id: context
        for context in req.app.state_backend.get_runner_contexts(list(context_ids))
    }
    for marker in markers:
        runner_context_id = marker.emitted_by_runner_context_id
        if runner_context_id is None:
            continue
        context = contexts.get(runner_context_id)
        if context is not None:
            marker.emitted_by_runner_context_id = state.builder.add_runner_context(
                context
            )


def _load_task_invocation_ids(req: TimelineRequest) -> set | None:
    """
    Pre-load invocation IDs for a task to enable client-side filtering.

    :param TimelineRequest req: Request with optional task_id
    :return: Set of invocation IDs if task_id is set, else None (no filter)
    """
    if not req.task_id:
        return None
    load_start = time.time()
    ids = set(req.app.orchestrator.get_task_invocation_ids(req.task_id))
    logger.info(
        f"Pre-loaded {len(ids)} invocation IDs for task {req.task_id} "
        f"in {time.time() - load_start:.2f}s"
    )
    return ids


def _load_system_task_invocation_ids(req: TimelineRequest) -> set[str]:
    """Return invocation IDs for Pynenc core/system tasks when hidden."""
    if req.show_system_tasks:
        return set()
    invocation_ids: set[str] = set()
    for task_id in req.app.tasks.keys():
        task_key = str(task_id)
        if not task_key.startswith(_SYSTEM_TASK_PREFIX):
            continue
        try:
            ids = req.app.orchestrator.get_task_invocation_ids(
                TaskId.from_key(task_key)
            )
        except Exception:
            logger.debug("system-task invocation lookup failed for %s", task_key)
            continue
        invocation_ids.update(str(invocation_id) for invocation_id in ids)
    return invocation_ids


def _load_atomic_service_windows(
    req: TimelineRequest,
    data: "TimelineData",
    trigger_runs: "list[TriggerRunRecord]",
) -> list[AtomicServiceWindow]:
    """Load atomic-service execution windows for the selected timeline range."""
    try:
        executions = req.app.orchestrator.get_atomic_service_executions_in_timerange(
            req.start_time,
            req.end_time,
            limit=2000,
        )
    except Exception:
        logger.debug("atomic-service history lookup failed; skipping history windows")
        executions = []

    windows = []
    for execution in executions:
        if execution.end_time is None:
            continue
        _ensure_atomic_service_lane(req, data, execution.runner_id)
        windows.append(
            AtomicServiceWindow(
                runner_id=execution.runner_id,
                start_time=execution.start_time,
                end_time=execution.end_time,
                atomic_service_run_id=execution.atomic_service_run_id,
                duration_seconds=execution.duration_seconds,
            )
        )
    return assign_atomic_service_sub_lanes(data, windows, trigger_runs)


def _ensure_atomic_service_lane(
    req: TimelineRequest, data: "TimelineData", runner_id: str
) -> None:
    """Ensure a runner with only atomic-service activity still has a lane."""
    if runner_id in data.lanes:
        return
    try:
        runner_context = req.app.state_backend.get_runner_context(runner_id)
    except Exception:
        runner_context = None
    runner_info = RunnerInfo.from_context(runner_context)
    if runner_info.runner_id == "unknown":
        runner_info = RunnerInfo("Runner", runner_id, "unknown", 0)
    group_id = runner_info.group_id if runner_info.has_parent else ""
    if group_id:
        data.get_or_create_group(
            group_id=group_id,
            hostname=runner_info.hostname,
            runner_cls=runner_info.parent_runner_cls or runner_info.runner_cls,
            runner_id=group_id,
            pid=runner_info.pid,
            thread_id=runner_info.thread_id,
        )
    data.get_or_create_lane(
        runner_id=runner_info.runner_id,
        runner_info=runner_info,
        color="#94a3b8",
        group_id=group_id,
    )


def _accumulate_history(req: TimelineRequest, state: _IterState) -> None:
    """
    Iterate history in batches and accumulate into state.

    :param TimelineRequest req: Request parameters
    :param _IterState state: Mutable accumulation state
    """
    for batch in req.app.state_backend.iter_history_in_timerange(
        start_time=req.start_time,
        end_time=req.end_time,
        batch_size=2000,
    ):
        state.batch_number += 1
        if state.batch_number % 5 == 0:
            logger.debug(
                f"Processing batch {state.batch_number}: "
                f"{len(state.invocations_seen)} invocations, "
                f"{state.history_count} history entries so far"
            )
        if not _process_batch(batch, req, state):
            break


def _process_batch(batch: list, req: TimelineRequest, state: _IterState) -> bool:
    """
    Filter, fetch contexts, and add batch to builder.

    :return: False to signal early termination
    """
    filtered = _filter_batch(batch, req.limit, state)
    _fetch_new_runner_contexts(filtered, req.app, state)
    if filtered:
        state.builder.add_history_batch(filtered, state.runner_contexts)
    return not (req.limit and len(state.invocations_seen) >= req.limit)


def _filter_batch(batch: list, limit: int | None, state: _IterState) -> list:
    """
    Filter batch entries respecting invocation limit and task filter.

    :return: Filtered list of history entries
    """
    result = []
    for entry in batch:
        # Skip entries not matching task filter
        if state.allowed_invocation_ids is not None:
            if entry.invocation_id not in state.allowed_invocation_ids:
                state.skipped_by_task_filter += 1
                continue
        if str(entry.invocation_id) in state.excluded_invocation_ids:
            state.skipped_system_tasks += 1
            continue

        if entry.invocation_id not in state.invocations_seen:
            if limit and len(state.invocations_seen) >= limit:
                break
            state.invocations_seen.add(entry.invocation_id)
        result.append(entry)
        state.history_count += 1
    return result


def _fetch_new_runner_contexts(batch: list, app: "Pynenc", state: _IterState) -> None:
    """
    Fetch and cache any runner contexts not yet in state.

    :param batch: Filtered history entries
    :param app: Pynenc application instance
    :param state: Mutable accumulation state
    """
    new_ids = [
        h.runner_context_id
        for h in batch
        if h.runner_context_id not in state.runner_contexts
    ]
    if not new_ids:
        return
    for ctx in app.state_backend.get_runner_contexts(new_ids):
        state.runner_contexts[ctx.runner_id] = ctx


def _collect_referenced_invocation_ids(markers: list, trigger_runs: list) -> set[str]:
    """Collect invocation IDs referenced by visible markers and trigger runs.

    These are invocations a user can already see *some* trace of on the
    timeline (an event dot, a trigger relation line, …) but whose status
    history may live outside the rendered time window.
    """
    refs: set[str] = set()
    for marker in markers:
        emitter = getattr(marker, "emitted_by_invocation_id", None)
        if emitter:
            refs.add(str(emitter))
        for tid in getattr(marker, "triggered_invocation_ids", None) or []:
            refs.add(str(tid))
    for run in trigger_runs:
        for sid in getattr(run, "source_invocation_ids", None) or []:
            refs.add(str(sid))
        triggered = getattr(run, "triggered_invocation_id", None)
        if triggered:
            refs.add(str(triggered))
    return refs


def _clip_history_to_window(history: list, start: datetime, end: datetime) -> list:
    """Clip a per-invocation history list to a visible time window.

    Returns synthetic ``InvocationHistory`` entries representing the portion
    of the invocation's lifecycle that intersects ``[start, end]``:

    - If the invocation was already in a segment status at ``start``, a
      synthetic entry is prepended at ``start`` carrying that status so the
      builder produces a partial bar clipped to the window's left edge.
    - In-window entries are kept as-is.
    - Entries past ``end`` are dropped; any trailing in-window segment
      status becomes "ongoing" and the builder extends it to ``end``.
    """
    from pynenc.state_backend.base_state_backend import InvocationHistory
    from pynmon.util.status_colors import is_segment_status

    sorted_hist = sorted(history, key=lambda h: h.timestamp)
    pre_window = [h for h in sorted_hist if h.timestamp < start]
    in_window = [h for h in sorted_hist if start <= h.timestamp <= end]

    result: list = []
    if pre_window:
        last_before = pre_window[-1]
        status_value = last_before.status_record.status.value
        if is_segment_status(status_value.upper()):
            synthetic = InvocationHistory(
                invocation_id=last_before.invocation_id,
                status_record=last_before.status_record,
                runner_context_id=last_before.runner_context_id,
                registered_by_inv_id=last_before.registered_by_inv_id,
            )
            synthetic._timestamp = start
            result.append(synthetic)
    result.extend(in_window)
    return result


def _left_boundary_segment_entry(
    history: list["InvocationHistory"],
    start: datetime,
    end: datetime,
) -> "InvocationHistory | None":
    """Return a synthetic left-edge segment entry for a visible invocation.

    The main history iterator has already yielded at least one in-window
    status for these invocations. If the previous status before the window
    was a segment status, we need a synthetic copy at ``start`` so the
    renderer paints the clipped part of the invocation up to the first
    visible status.
    """
    from pynenc.state_backend.base_state_backend import InvocationHistory
    from pynmon.util.status_colors import is_segment_status

    sorted_hist = sorted(history, key=lambda h: h.timestamp)
    pre_window = [h for h in sorted_hist if h.timestamp < start]
    in_window = [h for h in sorted_hist if start <= h.timestamp <= end]
    if not pre_window or not in_window:
        return None
    last_before = pre_window[-1]
    status_value = last_before.status_record.status.value
    if not is_segment_status(status_value.upper()):
        return None
    synthetic = InvocationHistory(
        invocation_id=last_before.invocation_id,
        status_record=last_before.status_record,
        runner_context_id=last_before.runner_context_id,
        registered_by_inv_id=last_before.registered_by_inv_id,
    )
    synthetic._timestamp = start
    return synthetic


def _load_visible_invocation_boundary_history(
    req: TimelineRequest,
    state: _IterState,
) -> None:
    """Backfill left-edge segments for invocations with visible status points."""
    visible_inv_ids = {
        str(invocation_id)
        for invocation_id in state.invocations_seen
        if str(invocation_id) not in state.excluded_invocation_ids
    }
    if not visible_inv_ids:
        return
    synthetic_batch: list = []
    for inv_id_str in sorted(visible_inv_ids):
        try:
            history = req.app.state_backend.get_history(InvocationId(inv_id_str))
        except Exception:
            logger.debug(
                "boundary-history lookup failed for %s", inv_id_str, exc_info=True
            )
            continue
        synthetic = _left_boundary_segment_entry(history, req.start_time, req.end_time)
        if synthetic is not None:
            synthetic_batch.append(synthetic)
    if not synthetic_batch:
        return
    _fetch_new_runner_contexts(synthetic_batch, req.app, state)
    state.builder.add_history_batch(synthetic_batch, state.runner_contexts)
    logger.info(
        "Visible invocation boundary backfilled: %d clipped segment entries",
        len(synthetic_batch),
    )


def _load_ghost_invocation_history(
    req: TimelineRequest,
    state: _IterState,
    markers: list,
    trigger_runs: list,
) -> None:
    """Backfill clipped history for referenced-but-off-window invocations.

    For every invocation that a visible marker or trigger run points at but
    whose status history did not enter the main batch loop, fetch the full
    history, clip it to the window, and feed it back through the builder.
    The existing renderer then emits a clipped status segment ("ghost bar")
    that the strict event-marker anchor can latch onto, and the existing
    JS click/hover handlers automatically pick it up via
    ``data-invocation-id``.
    """
    referenced = _collect_referenced_invocation_ids(markers, trigger_runs)
    seen_str = {str(inv_id) for inv_id in state.invocations_seen}
    missing = referenced - seen_str - state.excluded_invocation_ids
    if not missing:
        return
    ghost_batch: list = []
    for inv_id_str in missing:
        try:
            history = req.app.state_backend.get_history(InvocationId(inv_id_str))
        except Exception:
            logger.debug(
                "ghost-history lookup failed for %s", inv_id_str, exc_info=True
            )
            continue
        clipped = _clip_history_to_window(history, req.start_time, req.end_time)
        if not clipped:
            continue
        ghost_batch.extend(clipped)
        state.invocations_seen.add(inv_id_str)
    if not ghost_batch:
        return
    _fetch_new_runner_contexts(ghost_batch, req.app, state)
    state.builder.add_history_batch(ghost_batch, state.runner_contexts)
    logger.info(
        "Ghost history backfilled: %d entries across %d invocations",
        len(ghost_batch),
        len({h.invocation_id for h in ghost_batch}),
    )


@router.get("/timeline", response_class=HTMLResponse)
async def invocations_timeline(
    request: Request,
    time_range: str = "5m",
    start_date: str | None = None,
    end_date: str | None = None,
    task_id: str | None = None,
    workflow_id: str | None = None,
    workflow_type: str | None = None,
    limit: (
        str | None
    ) = "500",  # str so empty string ("No limit") doesn't cause int parse error
    resolution: str = "auto",
    collapse_external: str = "1",  # "0" disables collapsing; default on
    show_system: str = "1",
    show_atomic_service: str = "1",
    focus_event: str | None = None,
    selected: str | None = None,
    inv_ids: str | None = None,  # comma-separated invocation IDs to scope the view
) -> HTMLResponse:
    """Display a visual SVG timeline of invocations with filters."""
    app = get_pynenc_instance()
    logger.info(f"Generating invocation timeline with time_range={time_range}")
    start_time = time.time()

    try:
        # Parse date range and resolution parameters
        start_datetime, end_datetime = parse_time_range(
            time_range, start_date, end_date
        )
        resolution_seconds = parse_resolution(resolution)
        # Empty string means "No limit" from the select; None or empty → unlimited
        limit_int: int | None = int(limit) if limit and limit.strip() else None
        if limit_int is not None:
            limit_int = max(1, min(limit_int, 5000))

        parsed_task_id = TaskId.from_key(task_id) if task_id else None

        logger.info(
            f"Using time range: {start_datetime.isoformat()} to {end_datetime.isoformat()}"
            + (f", task_id={parsed_task_id}" if parsed_task_id else "")
            + f", limit={limit_int}"
        )

        # Pre-load workflow-filtered invocation IDs
        workflow_inv_ids = _load_workflow_invocation_ids(
            app, workflow_id, workflow_type
        )

        # Parse explicit invocation-id scope (e.g. from "Show in Timeline"
        # on a trigger run detail page). Empty/blank -> no filter.
        inv_ids_filter: set[str] | None = None
        if inv_ids:
            ids = {part.strip() for part in inv_ids.split(",") if part.strip()}
            if ids:
                inv_ids_filter = ids

        # Build SVG timeline using efficient history iteration (offload to thread)
        config = TimelineConfig(resolution_seconds=resolution_seconds)
        req = TimelineRequest(
            app=app,
            start_time=start_datetime,
            end_time=end_datetime,
            config=config,
            limit=limit_int,
            task_id=parsed_task_id,
            workflow_inv_ids=workflow_inv_ids,
            inv_ids_filter=inv_ids_filter,
            focus_event=focus_event,
            collapse_external=collapse_external != "0",
            show_system_tasks=show_system != "0",
            show_atomic_service=show_atomic_service != "0",
        )
        svg_content = await asyncio.to_thread(_build_svg_timeline, req)

        # Get all available task IDs for the dropdown
        all_task_ids = list(app.tasks.keys())

        # Get all workflow types for the dropdown
        all_workflow_types = await asyncio.to_thread(
            lambda: [str(wt) for wt in app.state_backend.get_all_workflow_types()]
        )

        return templates.TemplateResponse(
            request,
            "invocations/timeline.html",
            context={
                "title": "Invocations Timeline",
                "app_id": app.app_id,
                "svg_content": svg_content,
                "all_task_ids": all_task_ids,
                "all_workflow_types": all_workflow_types,
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
                "current_filters": {
                    "time_range": time_range,
                    "start_date": start_date or "",
                    "end_date": end_date or "",
                    "task_id": task_id or "",
                    "workflow_id": workflow_id or "",
                    "workflow_type": workflow_type or "",
                    "limit": limit_int,
                    "resolution": resolution,
                    "collapse_external": collapse_external != "0",
                    "show_system": show_system != "0",
                    "show_atomic_service": show_atomic_service != "0",
                    "focus_event": focus_event or "",
                    "selected": selected or "",
                    "inv_ids": inv_ids or "",
                    "scope_clear_url": _timeline_scope_clear_url(request),
                },
                "focus_event": focus_event or "",
            },
        )
    except Exception as e:
        logger.exception(f"Error in invocations_timeline: {str(e)}")
        return templates.TemplateResponse(
            request,
            "shared/error.html",
            context={
                "title": "Error",
                "message": f"An error occurred while generating the timeline: {str(e)}",
            },
            status_code=500,
        )
    finally:
        elapsed = time.time() - start_time
        logger.info(f"invocations_timeline completed in {elapsed:.2f} seconds")


def _get_invocation_result_and_exception(
    app: "Pynenc", invocation: "DistributedInvocation", invocation_id: str
) -> tuple[str | None, str | None]:
    """Get formatted result or exception for an invocation."""
    formatted_result = None
    formatted_exception = None

    try:
        if invocation.status == InvocationStatus.SUCCESS:
            logger.info(f"Retrieving result for invocation {invocation_id}")
            result = app.state_backend.get_result(invocation.invocation_id)
            # Format the result for display
            if isinstance(result, dict | list):
                formatted_result = json.dumps(result, indent=2)
            else:
                formatted_result = str(result)
        elif invocation.status == InvocationStatus.FAILED:
            logger.info(f"Retrieving exception for invocation {invocation_id}")
            exception = app.state_backend.get_exception(invocation.invocation_id)
            # Format the exception for display
            formatted_exception = str(exception)
            if hasattr(exception, "__traceback__"):
                formatted_exception = "".join(
                    traceback.format_exception(
                        type(exception), exception, exception.__traceback__
                    )
                )
    except Exception as e:
        logger.exception(f"Error retrieving result/exception: {str(e)}")
        formatted_exception = f"Error retrieving result/exception: {str(e)}"

    return formatted_result, formatted_exception


def _get_formatted_invocation_history(
    app: "Pynenc", invocation: "DistributedInvocation", invocation_id: str
) -> list[dict]:
    """Get formatted history for an invocation with timeout protection."""
    try:
        logger.info(f"Retrieving history for invocation {invocation_id}")
        history_start = time.time()

        # Get history
        history = app.state_backend.get_history(invocation.invocation_id)

        # Collect all unique runner context IDs
        runner_context_ids = list({entry.runner_context_id for entry in history})

        # Batch-load all runner contexts
        runner_contexts_list = app.state_backend.get_runner_contexts(runner_context_ids)
        runner_contexts = {ctx.runner_id: ctx for ctx in runner_contexts_list if ctx}

        # Log any missing contexts
        missing_ids = set(runner_context_ids) - set(runner_contexts.keys())
        if missing_ids:
            logger.warning(
                f"Missing runner contexts for invocation {invocation_id}: "
                f"{', '.join(missing_ids)}"
            )

        # Convert history items to a more template-friendly format
        formatted_history = []
        for entry in history:
            # Get runner context, may be None if not found
            runner_context = runner_contexts.get(entry.runner_context_id)
            context_info = _format_owner_context(
                runner_context, entry.runner_context_id
            )

            formatted_history.append(
                {
                    "timestamp": entry.timestamp.isoformat(),
                    "status": entry.status_record.status.name,
                    "status_runner_id": entry.status_record.runner_id,
                    "runner_context_summary": context_info["summary"],
                    "runner_cls": context_info["runner_cls"],
                    "runner_id": context_info["runner_id"],
                    "hostname": context_info["hostname"],
                    "pid": context_info["pid"],
                    "thread_id": context_info["thread_id"],
                    "parent_runner_cls": context_info["parent_runner_cls"],
                    "parent_runner_id": context_info["parent_runner_id"],
                    "parent_hostname": context_info["parent_hostname"],
                    "parent_pid": context_info["parent_pid"],
                    "parent_thread_id": context_info["parent_thread_id"],
                }
            )

        # Sort history by timestamp using explicit string comparison
        from functools import cmp_to_key

        def compare_timestamps(a: dict, b: dict) -> int:
            return -1 if a["timestamp"] < b["timestamp"] else 1

        formatted_history.sort(key=cmp_to_key(compare_timestamps))

        logger.info(
            f"Retrieved {len(formatted_history)} history entries in {time.time() - history_start:.2f}s"
        )
        return formatted_history
    except Exception as e:
        logger.exception(f"Error retrieving history: {str(e)}")
        return []


def _format_owner_context(
    owner_context: "RunnerContext | None", runner_context_id: str | None = None
) -> dict[str, str | None]:
    """Format owner context into display-friendly dict using RunnerContextInfo.

    Handles None contexts gracefully, logging a warning if context is missing.

    :param owner_context: The runner context to format, or None if not found
    :param runner_context_id: The ID that was attempted (for logging), or None
    :return: Formatted context dictionary with N/A values if context is missing
    """
    if owner_context is None and runner_context_id:
        logger.warning(
            f"Runner context not found for ID: {runner_context_id}. "
            f"Displaying N/A values."
        )
    return RunnerContextInfo.from_context(owner_context).to_dict()


def _get_invocation_timestamps_and_duration(
    formatted_history: list[dict[str, str | None]],
) -> tuple[str | None, str | None, float | None]:
    """Extract timestamps and calculate duration from formatted history."""
    created_at: str | None = "Unknown"
    completed_at = None
    duration_seconds = None

    if formatted_history:
        try:
            created_at = formatted_history[0]["timestamp"]

            # Check if the last entry is a terminal state
            if formatted_history[-1]["status"] in ["SUCCESS", "FAILED"]:
                completed_at = formatted_history[-1]["timestamp"]
        except (IndexError, KeyError) as e:
            logger.warning(f"Error processing history timestamps: {str(e)}")

    if created_at and completed_at:
        try:
            dt_created = datetime.fromisoformat(created_at)
            dt_completed = datetime.fromisoformat(completed_at)
            duration_seconds = (dt_completed - dt_created).total_seconds()
        except (ValueError, TypeError) as e:
            logger.warning(f"Error calculating duration: {str(e)}")

    return created_at, completed_at, duration_seconds


def _fetch_inv_summary(app: "Pynenc", inv_id_str: str) -> dict:
    """Return task, status and duration for a source invocation.

    Used to enrich source invocation references in the Related-events panels
    with more than just a truncated ID badge.  Returns an empty dict on any
    backend failure so callers can degrade gracefully.
    """
    try:
        from pynenc.identifiers.invocation_id import InvocationId as _InvId

        inv = app.state_backend.get_invocation(_InvId(inv_id_str))
        if not inv:
            return {}
        history = app.state_backend.get_history(inv.invocation_id)
        duration_seconds: float | None = None
        if len(history) >= 2:
            delta = history[-1].timestamp - history[0].timestamp
            duration_seconds = delta.total_seconds()
        task_id = inv.task.task_id
        return {
            "invocation_id": str(inv.invocation_id),
            "task_id_key": task_id.key,
            "func_name": task_id.func_name,
            "status": inv.status.name,
            "duration_seconds": duration_seconds,
        }
    except Exception:
        logger.debug("_fetch_inv_summary failed for %s", inv_id_str)
        return {}


def _collect_source_inv_summaries(
    app: "Pynenc",
    triggered_by: object | None,
    triggered_runs_caused: list,
) -> dict[str, dict]:
    """Build a {id: summary} dict for all source invocations in context.

    Covers both the trigger run that spawned this invocation and any trigger
    runs caused by this invocation.
    """
    ids: set[str] = set()
    if triggered_by is not None:
        for sid in getattr(triggered_by, "source_invocation_ids", None) or []:
            ids.add(sid)
        for p in getattr(triggered_by, "participants", None) or []:
            sid = getattr(p, "source_invocation_id", None)
            if sid:
                ids.add(sid)
    for run in triggered_runs_caused:
        tid = getattr(run, "triggered_invocation_id", None)
        if tid:
            ids.add(tid)
        for participant in relevant_trigger_run_participants(run):
            sid = participant.source_invocation_id
            if sid:
                ids.add(sid)
    return {inv_id: _fetch_inv_summary(app, inv_id) for inv_id in ids}


def _fetch_event_summary(app: "Pynenc", event_id: str) -> dict:
    """Return a compact summary for one event id (code, timestamp, state)."""
    try:
        event = app.trigger.get_event(event_id)
    except Exception:
        logger.debug("_fetch_event_summary failed for %s", event_id)
        return {"event_id": event_id}
    if event is None:
        return {"event_id": event_id}
    if event.triggered:
        state = "triggered"
    elif event.matched:
        state = "matched"
    else:
        state = "unmatched"
    return {
        "event_id": event.event_id,
        "event_code": event.event_code,
        "timestamp": event.timestamp.isoformat(),
        "state": state,
        "matched": event.matched,
        "triggered": event.triggered,
        "triggered_count": len(event.triggered_invocation_ids or []),
        "emitted_by_invocation_id": event.emitted_by_invocation_id,
    }


def _collect_source_event_summaries(
    app: "Pynenc",
    triggered_by: object | None,
    triggered_runs_caused: list,
) -> dict[str, dict]:
    """Return ``{event_id: summary}`` for every event referenced in context.

    Covers events from the trigger run that produced this invocation and
    from trigger runs caused by this invocation. Used by templates to
    render rich source-event pills with code, timestamp and outcome state.
    """
    ids: set[str] = set()
    if triggered_by is not None:
        for eid in getattr(triggered_by, "event_ids", None) or []:
            ids.add(eid)
        for p in getattr(triggered_by, "participants", None) or []:
            eid = getattr(p, "event_id", None)
            if eid:
                ids.add(eid)
    for run in triggered_runs_caused:
        for eid in getattr(run, "event_ids", None) or []:
            ids.add(eid)
        for p in getattr(run, "participants", None) or []:
            eid = getattr(p, "event_id", None)
            if eid:
                ids.add(eid)
    return {eid: _fetch_event_summary(app, eid) for eid in ids}


def _timeline_scope_clear_url(request: Request) -> str:
    """Return the current timeline URL without the invocation scope filter."""
    params = [
        (key, value)
        for key, value in request.query_params.multi_items()
        if key != "inv_ids"
    ]
    query = urlencode(params)
    return "/invocations/timeline" + (f"?{query}" if query else "")


@router.get("/{invocation_id}", response_class=HTMLResponse)
async def invocation_detail(
    request: Request, invocation_id: "InvocationId"
) -> HTMLResponse:
    """Display detailed information about a specific invocation."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving details for invocation: {invocation_id}")
    start_time = time.time()

    try:
        # Use the direct method to get the invocation
        invocation = app.state_backend.get_invocation(invocation_id)

        # Get basic details
        task = invocation.task
        call = invocation.call

        # Get result and exception data
        formatted_result, formatted_exception = _get_invocation_result_and_exception(
            app, invocation, invocation_id
        )

        # Get history data
        formatted_history = _get_formatted_invocation_history(
            app, invocation, invocation_id
        )

        # Get timestamps and duration
        (
            created_at,
            completed_at,
            duration_seconds,
        ) = _get_invocation_timestamps_and_duration(formatted_history)

        formatted_arguments = format_call_arguments(call)

        # Get workflow identity
        workflow = None
        try:
            workflow = invocation.workflow
        except Exception:
            logger.debug("No workflow identity for invocation %s", invocation_id)

        # Look up trigger run that produced this invocation, if any.
        triggered_by = None
        try:
            triggered_by_runs = await asyncio.to_thread(
                app.trigger.get_trigger_runs_for_invocation, str(invocation_id)
            )
            if triggered_by_runs:
                triggered_by = triggered_by_runs[0]
        except Exception:
            logger.debug(
                "trigger backend lookup failed for invocation %s", invocation_id
            )

        # Look up events emitted by this invocation, if any.
        emitted_events: list = []
        try:
            emitted_events = await asyncio.to_thread(
                app.trigger.get_events_emitted_by_invocation, str(invocation_id)
            )
        except Exception:
            logger.debug(
                "trigger backend emitted-events lookup failed for invocation %s",
                invocation_id,
            )

        # Look up trigger runs sourced by this invocation (reverse trigger
        # effects: status/result/exception conditions that this invocation
        # satisfied). Drives the "Triggers caused" panel and the trigger
        # children edges in the family tree.
        triggered_runs_caused: list = []
        try:
            triggered_runs_caused = await asyncio.to_thread(
                app.trigger.get_trigger_runs_sourced_by_invocation,
                str(invocation_id),
            )
        except Exception:
            logger.debug(
                "trigger backend sourced-runs lookup failed for invocation %s",
                invocation_id,
            )

        logger.info(
            f"Rendering invocation detail template in {time.time() - start_time:.2f}s"
        )
        source_inv_summaries = await asyncio.to_thread(
            _collect_source_inv_summaries, app, triggered_by, triggered_runs_caused
        )
        source_event_summaries = await asyncio.to_thread(
            _collect_source_event_summaries,
            app,
            triggered_by,
            triggered_runs_caused,
        )
        emitted_event_inv_summaries: dict[str, dict] = {}
        for ev in emitted_events:
            for inv_id in getattr(ev, "triggered_invocation_ids", None) or []:
                if inv_id and inv_id not in emitted_event_inv_summaries:
                    emitted_event_inv_summaries[inv_id] = await asyncio.to_thread(
                        _fetch_inv_summary, app, inv_id
                    )
        trigger_run_timeline_urls = {
            run.trigger_run_id: timeline_url_for_trigger_run(run, [], app)
            for run in ([triggered_by] if triggered_by else []) + triggered_runs_caused
        }
        triggered_by_participants = (
            relevant_trigger_run_participants(triggered_by) if triggered_by else []
        )
        triggered_runs_caused_participants = {
            run.trigger_run_id: relevant_trigger_run_participants(run)
            for run in triggered_runs_caused
        }
        triggered_by_source_invocation_ids = [
            participant.source_invocation_id
            for participant in triggered_by_participants
            if participant.source_invocation_id
        ]
        return templates.TemplateResponse(
            request,
            "invocations/detail.html",
            context={
                "title": "Invocation Details",
                "app_id": app.app_id,
                "invocation": invocation,
                "call": call,
                "task": task,
                "result": formatted_result,
                "exception": formatted_exception,
                "history": formatted_history,
                "arguments": formatted_arguments,
                "created_at": created_at,
                "completed_at": completed_at,
                "duration": (
                    f"{duration_seconds:.2f} seconds"
                    if duration_seconds is not None
                    else None
                ),
                "workflow": workflow,
                "triggered_by": triggered_by,
                "triggered_by_participants": triggered_by_participants,
                "triggered_by_source_invocation_ids": (
                    triggered_by_source_invocation_ids
                ),
                "emitted_events": emitted_events,
                "triggered_runs_caused": triggered_runs_caused,
                "triggered_runs_caused_participants": (
                    triggered_runs_caused_participants
                ),
                "source_inv_summaries": source_inv_summaries,
                "source_event_summaries": source_event_summaries,
                "emitted_event_inv_summaries": emitted_event_inv_summaries,
                "trigger_run_timeline_urls": trigger_run_timeline_urls,
            },
        )
    except InvocationNotFoundError:
        logger.warning(f"No invocation found with ID: {invocation_id}")
        return templates.TemplateResponse(
            request,
            "shared/error.html",
            context={
                "title": "Invocation Not Found",
                "message": f"No invocation found with ID: {invocation_id}",
            },
            status_code=404,
        )
    except Exception as e:
        logger.exception(f"Unexpected error in invocation_detail: {str(e)}")
        return templates.TemplateResponse(
            request,
            "shared/error.html",
            context={"title": "Error", "message": f"An error occurred: {str(e)}"},
            status_code=500,
        )
    finally:
        elapsed = time.time() - start_time
        logger.info(f"invocation_detail completed in {elapsed:.2f} seconds")


@router.get("/{invocation_id}/history")
async def invocation_history(
    request: Request, invocation_id: "InvocationId"
) -> JSONResponse:
    """Return invocation history as JSON for timeline visualization."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving history for invocation {invocation_id} (API)")

    try:
        # Get the invocation
        invocation = app.state_backend.get_invocation(invocation_id)
        if not invocation:
            return JSONResponse(
                {"error": f"No invocation found with ID: {invocation_id}"}, 404
            )

        # Get history
        history = app.state_backend.get_history(invocation.invocation_id)

        # Convert to format needed for visualization
        formatted_history: list[dict] = []
        for entry in history:
            # Make sure timestamp is timezone-aware for comparison
            timestamp = entry.timestamp
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)

            # Get full context info from runner_context
            runner_context = app.state_backend.get_runner_context(
                entry.runner_context_id
            )
            context_info = _format_owner_context(runner_context)

            formatted_history.append(
                {
                    "timestamp": timestamp.isoformat(),
                    "status": entry.status_record.status.value,
                    "status_runner_id": entry.status_record.runner_id,
                    "runner_context_summary": context_info["summary"],
                    "runner_cls": context_info["runner_cls"],
                    "runner_id": context_info["runner_id"],
                    "hostname": context_info["hostname"],
                    "pid": context_info["pid"],
                    "thread_id": context_info["thread_id"],
                    "parent_runner_cls": context_info["parent_runner_cls"],
                    "parent_runner_id": context_info["parent_runner_id"],
                    "parent_hostname": context_info["parent_hostname"],
                    "parent_pid": context_info["parent_pid"],
                    "parent_thread_id": context_info["parent_thread_id"],
                }
            )

        # Sort by timestamp
        formatted_history.sort(key=lambda x: x["timestamp"])

        return JSONResponse(formatted_history)
    except Exception as e:
        logger.exception(
            f"Error retrieving history for invocation {invocation_id}: {str(e)}"
        )
        return JSONResponse({"error": str(e)}, 500)


@router.get("/{invocation_id}/api")
async def invocation_api(
    request: Request, invocation_id: "InvocationId"
) -> JSONResponse:
    """Return invocation data as JSON for the timeline visualization."""
    app = get_pynenc_instance()
    logger.info(f"Retrieving API data for invocation {invocation_id}")

    try:
        # Get the invocation
        invocation = app.state_backend.get_invocation(invocation_id)
        if not invocation:
            return JSONResponse(
                {"error": f"No invocation found with ID: {invocation_id}"}, 404
            )

        # Create a simplified representation for the API
        invocation_data: dict = {
            "invocation_id": invocation.invocation_id,
            "task_id_key": invocation.task.task_id.key,
            "status": invocation.status.name,
            "num_retries": invocation.num_retries,
            "parent_invocation_id": invocation.parent_invocation_id,
            "parent_event_id": invocation.parent_event_id,
        }

        # Add workflow information if available
        try:
            wf = invocation.workflow
            invocation_data["workflow"] = {
                "workflow_id": str(wf.workflow_id),
                "workflow_type": str(wf.workflow_type),
                "parent_workflow_id": (
                    str(wf.parent_workflow_id) if wf.parent_workflow_id else None
                ),
                "is_subworkflow": wf.is_subworkflow,
            }
        except Exception:
            invocation_data["workflow"] = None

        # Trigger origin (None if not produced by a trigger run).
        invocation_data["triggered_by"] = None
        try:
            triggered_by_runs = await asyncio.to_thread(
                app.trigger.get_trigger_runs_for_invocation, str(invocation_id)
            )
            if triggered_by_runs:
                triggered_by = triggered_by_runs[0]
                invocation_data["triggered_by"] = trigger_run_to_dict(triggered_by)
                invocation_data["triggered_by"]["timeline_url"] = (
                    timeline_url_for_trigger_run(triggered_by, [], app)
                )
        except Exception:
            logger.debug(
                "trigger backend lookup failed for invocation %s", invocation_id
            )

        # Events emitted by this invocation (empty list if none).
        invocation_data["emitted_events"] = []
        try:
            emitted_events = await asyncio.to_thread(
                app.trigger.get_events_emitted_by_invocation, str(invocation_id)
            )
            invocation_data["emitted_events"] = [
                {
                    "event_id": e.event_id,
                    "event_code": e.event_code,
                    "timestamp": e.timestamp.isoformat(),
                    "matched": e.matched,
                    "triggered": e.triggered,
                    "triggered_invocation_ids": e.triggered_invocation_ids,
                }
                for e in emitted_events
            ]
        except Exception:
            logger.debug(
                "trigger backend emitted-events lookup failed for invocation %s",
                invocation_id,
            )

        # Trigger runs caused by this invocation as a source participant. These
        # cover status/result/exception trigger edges where no event exists.
        triggered_runs_caused_view: list[dict[str, Any]] = []
        invocation_data["triggered_runs_caused"] = triggered_runs_caused_view
        try:
            triggered_runs_caused = await asyncio.to_thread(
                app.trigger.get_trigger_runs_sourced_by_invocation,
                str(invocation_id),
            )
            for run in triggered_runs_caused:
                item = trigger_run_to_dict(run)
                item["timeline_url"] = timeline_url_for_trigger_run(run, [], app)
                triggered_runs_caused_view.append(item)
        except Exception:
            logger.debug(
                "trigger backend sourced-runs lookup failed for invocation %s",
                invocation_id,
            )

        # Source invocation summaries (task, status, duration) for every ID
        # referenced in triggering relations. Keyed by invocation-ID string so
        # JS can do a fast dict look-up when rendering invocation cards.
        src_ids: set[str] = set()
        tb = invocation_data.get("triggered_by") or {}
        for sid in tb.get("source_invocation_ids") or []:
            src_ids.add(sid)
        for p in tb.get("participants") or []:
            sid = p.get("source_invocation_id")
            if sid:
                src_ids.add(sid)
        for run_view in triggered_runs_caused_view:
            triggered_invocation_id = run_view.get("triggered_invocation_id")
            if triggered_invocation_id:
                src_ids.add(triggered_invocation_id)
            for sid in run_view.get("source_invocation_ids") or []:
                src_ids.add(sid)
            for participant in run_view.get("participants") or []:
                sid = participant.get("source_invocation_id")
                if sid:
                    src_ids.add(sid)
        for ev in invocation_data.get("emitted_events") or []:
            for sid in ev.get("triggered_invocation_ids") or []:
                if sid:
                    src_ids.add(sid)
        invocation_data["source_inv_summaries"] = {
            sid: await asyncio.to_thread(_fetch_inv_summary, app, sid)
            for sid in src_ids
        }

        # Source event summaries keyed by event_id: surface code, timestamp
        # and state so the JS panel can render rich pills instead of bare ids.
        event_ids: set[str] = set()
        for eid in tb.get("event_ids") or []:
            event_ids.add(eid)
        for p in tb.get("participants") or []:
            eid = p.get("event_id")
            if eid:
                event_ids.add(eid)
        for run_view in triggered_runs_caused_view:
            for eid in run_view.get("event_ids") or []:
                event_ids.add(eid)
            for participant in run_view.get("participants") or []:
                eid = participant.get("event_id")
                if eid:
                    event_ids.add(eid)
        invocation_data["source_event_summaries"] = {
            eid: await asyncio.to_thread(_fetch_event_summary, app, eid)
            for eid in event_ids
        }

        return JSONResponse(invocation_data)
    except Exception as e:
        logger.exception(
            f"Error retrieving API data for invocation {invocation_id}: {str(e)}"
        )
        return JSONResponse({"error": str(e)}, 500)


@router.post("/{invocation_id}/rerun")
async def rerun_invocation(invocation_id: "InvocationId") -> JSONResponse:
    """Re-run the same call as a brand-new invocation (unrelated to the original).

    Reads the original invocation's call (task + arguments) and routes a fresh
    call through the orchestrator, producing a new independent invocation.
    """
    app = get_pynenc_instance()
    logger.info(f"Re-running call for invocation {invocation_id}")

    try:
        invocation = app.state_backend.get_invocation(invocation_id)
        call = invocation.call
        task = invocation.task

        # Route a fresh call through the task (produces a new invocation)
        new_invocation = await asyncio.to_thread(task._call, call.arguments)

        return JSONResponse(
            {
                "success": True,
                "new_invocation_id": str(new_invocation.invocation_id),
                "message": "New invocation created successfully.",
            }
        )
    except InvocationNotFoundError:
        return JSONResponse(
            {"success": False, "message": f"Invocation {invocation_id} not found."},
            status_code=404,
        )
    except Exception as e:
        logger.exception(f"Error re-running invocation {invocation_id}: {e}")
        return JSONResponse(
            {"success": False, "message": f"Error: {str(e)}"},
            status_code=500,
        )


@router.get("/table", response_class=HTMLResponse)
async def invocations_table(
    request: Request,
    status: str | None = None,
    task_id: str | None = None,
    limit: int = 50,
) -> HTMLResponse:
    """Return just the invocations table for HTMX refresh."""
    limit = max(1, min(limit, 1000))
    # This is essentially the same logic as invocations_list but returns only the table partial
    app = get_pynenc_instance()
    parsed_task_id = TaskId.from_key(task_id) if task_id else None

    # Parse status parameter
    status_list = None
    if status:
        status_list = [status]

    # Convert to InvocationStatus objects
    statuses = None
    if status_list:
        statuses = []
        for status_str in status_list:
            try:
                statuses.append(InvocationStatus[status_str.upper()])
            except KeyError:
                continue

    def _fetch_table_invocations() -> list[InvocationId]:
        result = []
        if parsed_task_id:
            if parsed_task_id in app.tasks:
                task = app.tasks[parsed_task_id]
                result = list(
                    app.orchestrator.get_existing_invocations(
                        task=task,
                        statuses=statuses,
                    )
                )[:limit]
        else:
            for task in app.tasks.values():
                invocations = list(
                    app.orchestrator.get_existing_invocations(
                        task=task,
                        statuses=statuses,
                    )
                )
                result.extend(invocations)
                if len(result) >= limit:
                    result = result[:limit]
                    break
        return result

    all_invocations = await asyncio.to_thread(_fetch_table_invocations)

    return templates.TemplateResponse(
        request,
        "invocations/partials/table.html",
        context={"invocations": all_invocations},
    )
