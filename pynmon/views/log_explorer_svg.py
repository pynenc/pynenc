"""SVG mini-timeline generation for the Log Explorer.

Builds a compact SVG timeline showing only the runners and invocations
mentioned in the parsed log lines. Uses the existing SVG infrastructure
(TimelineDataBuilder, TimelineSVGRenderer) with a filtered data set.

Key components:
- build_log_svg: async entry point producing an SVG string
- LogSvgParams: typed bundle for build parameters
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from pynenc.identifiers.invocation_id import InvocationId
from pynmon.util.log_parser import EntityRef
from pynmon.util.status_colors import is_segment_status
from pynmon.util.svg.atomic_service import AtomicServiceWindow
from pynmon.util.svg.builder import TimelineDataBuilder
from pynmon.util.svg.event_markers import (
    EventMarker as RenderEventMarker,
    cron_event_markers_from_trigger_runs,
)
from pynmon.util.svg.models import TimelineConfig
from pynmon.util.svg.renderer import TimelineSVGRenderer
from pynmon.util.svg.runner_info import RunnerInfo

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.runner.runner_context import RunnerContext
    from pynenc.state_backend.base_state_backend import InvocationHistory
    from pynenc.trigger.monitoring import EventRecord, TriggerRunRecord
    from pynmon.util.svg.timeline_data import TimelineData

logger = logging.getLogger("pynmon.views.log_explorer_svg")


# Use the same config as the main Timeline tab so lane proportions,
# font sizes, and bar heights are identical. The SVG is rendered at its
# native pixel width (not scaled to 100%) so labels never overflow.
_MINI_CONFIG = TimelineConfig()

_INV_KINDS = frozenset(
    {
        "invocation",
        "parent-invocation",
        "child-invocation",
        "new-invocation",
        # Trigger-related invocation roles emitted by BaseTrigger logs;
        # rendered on the same mini-timeline as plain invocations.
        "source-invocation",
        "triggered-invocation",
    }
)


@dataclass(frozen=True)
class LogSvgParams:
    """Parameters for building the log-explorer mini-timeline SVG.

    :param Pynenc app: Application instance for state backend access
    :param list[EntityRef] all_refs: Deduplicated entity refs from all log lines
    :param list[datetime] utc_timestamps: UTC-converted timestamps from log lines
    :param tuple time_range: Optional precomputed mini-timeline bounds
    """

    app: "Pynenc"
    all_refs: list[EntityRef]
    utc_timestamps: list[datetime]
    time_range: tuple[datetime, datetime] | None = None


@dataclass(frozen=True)
class _ResolvedLogSvgScope:
    """Resolved references and bounds shared by SVG and timeline links."""

    inv_ids: set[str]
    atomic_service_run_ids: set[str]
    events: list["EventRecord"]
    trigger_runs: list["TriggerRunRecord"]
    start: datetime
    end: datetime


async def build_log_svg(params: LogSvgParams) -> str:
    """Build a compact SVG timeline from log-referenced invocations.

    :param LogSvgParams params: Typed parameters bundle
    :return: SVG markup string, or empty string if no data
    """
    inv_ids, event_ids, trigger_run_ids, trigger_ids, atomic_service_run_ids = (
        _extract_reference_sets(params.all_refs)
    )
    if (
        not inv_ids
        and not event_ids
        and not trigger_run_ids
        and not trigger_ids
        and not atomic_service_run_ids
    ):
        return ""
    start, end = params.time_range or _compute_time_range(params.utc_timestamps)
    return await asyncio.to_thread(
        _build_sync,
        params.app,
        inv_ids,
        event_ids,
        trigger_run_ids,
        trigger_ids,
        atomic_service_run_ids,
        start,
        end,
    )


async def compute_log_svg_time_range(
    params: LogSvgParams,
) -> tuple[datetime, datetime]:
    """Return the exact time bounds the Log Explorer mini-timeline will use."""
    inv_ids, event_ids, trigger_run_ids, trigger_ids, atomic_service_run_ids = (
        _extract_reference_sets(params.all_refs)
    )
    start, end = params.time_range or _compute_time_range(params.utc_timestamps)
    if (
        not inv_ids
        and not event_ids
        and not trigger_run_ids
        and not trigger_ids
        and not atomic_service_run_ids
    ):
        return start, end
    scope = await asyncio.to_thread(
        _resolve_log_svg_scope,
        params.app,
        inv_ids,
        event_ids,
        trigger_run_ids,
        trigger_ids,
        atomic_service_run_ids,
        start,
        end,
    )
    return scope.start, scope.end


def _extract_reference_sets(
    refs: list[EntityRef],
) -> tuple[set[str], set[str], set[str], set[str], set[str]]:
    """Return invocation/event/trigger/AS reference sets from parsed refs."""
    return (
        _extract_invocation_ids(refs),
        _extract_event_ids(refs),
        _extract_trigger_run_ids(refs),
        _extract_trigger_ids(refs),
        _extract_atomic_service_run_ids(refs),
    )


def _extract_invocation_ids(refs: list[EntityRef]) -> set[str]:
    """Collect unique invocation IDs from entity refs."""
    return {r.value for r in refs if r.kind in _INV_KINDS}


def _extract_event_ids(refs: list[EntityRef]) -> set[str]:
    """Collect unique event IDs from entity refs."""
    return {r.value for r in refs if r.kind == "event"}


def _extract_trigger_run_ids(refs: list[EntityRef]) -> set[str]:
    """Collect unique trigger-run IDs from entity refs."""
    return {r.value for r in refs if r.kind == "trigger-run"}


def _extract_trigger_ids(refs: list[EntityRef]) -> set[str]:
    """Collect unique trigger definition IDs from entity refs."""
    return {r.value for r in refs if r.kind == "trigger"}


def _extract_atomic_service_run_ids(refs: list[EntityRef]) -> set[str]:
    """Collect unique atomic-service run IDs from entity refs."""
    return {r.value for r in refs if r.kind == "atomic-service-run"}


def _compute_time_range(
    utc_timestamps: list[datetime],
) -> tuple[datetime, datetime]:
    """Determine start/end from timestamps with tight 10% duration padding.

    :param list[datetime] utc_timestamps: UTC timestamps from logs
    :return: (start, end) datetime pair
    """
    if not utc_timestamps:
        now = datetime.now(UTC)
        return now - timedelta(seconds=5), now
    duration_s = (max(utc_timestamps) - min(utc_timestamps)).total_seconds()
    pad = timedelta(seconds=max(duration_s * 0.1, 0.25))
    return min(utc_timestamps) - pad, max(utc_timestamps) + pad


def _expand_time_range_for_references(
    start: datetime,
    end: datetime,
    events: list["EventRecord"],
    trigger_runs: list["TriggerRunRecord"],
) -> tuple[datetime, datetime]:
    """Include referenced event/source timestamps in the mini-timeline bounds."""
    timestamps: list[datetime] = []
    timestamps.extend(event.timestamp for event in events)
    for run in trigger_runs:
        timestamps.extend(
            timestamp for timestamp in (run.claimed_at, run.executed_at) if timestamp
        )
        timestamps.extend(
            participant.context_timestamp
            for participant in run.participants or []
            if participant.context_timestamp
        )
    if not timestamps:
        return start, end
    pad = timedelta(milliseconds=250)
    ref_start = min(timestamps)
    ref_end = max(timestamps)
    if start <= ref_start and ref_end <= end:
        return start, end
    return min(start, ref_start - pad), max(end, ref_end + pad)


def _build_sync(
    app: "Pynenc",
    inv_ids: set[str],
    event_ids: set[str],
    trigger_run_ids: set[str],
    trigger_ids: set[str],
    atomic_service_run_ids: set[str],
    start: datetime,
    end: datetime,
) -> str:
    """Synchronous SVG build — runs in a thread.

    Iterates history, filters to only relevant invocations, and renders SVG.
    """
    scope = _resolve_log_svg_scope(
        app,
        inv_ids,
        event_ids,
        trigger_run_ids,
        trigger_ids,
        atomic_service_run_ids,
        start,
        end,
    )
    start, end = scope.start, scope.end

    builder = TimelineDataBuilder(config=_MINI_CONFIG, collapse_external=True)
    contexts: dict[str, RunnerContext] = {}
    seen_invocation_ids: set[str] = set()
    for batch in app.state_backend.iter_history_in_timerange(start, end):
        filtered = [h for h in batch if str(h.invocation_id) in scope.inv_ids]
        if not filtered:
            continue
        seen_invocation_ids.update(str(h.invocation_id) for h in filtered)
        _fetch_new_contexts(filtered, app, contexts)
        builder.add_history_batch(filtered, contexts)
    _backfill_visible_boundary_history(
        app, builder, contexts, seen_invocation_ids, start, end
    )
    _backfill_referenced_history(
        app, builder, contexts, scope.inv_ids - seen_invocation_ids, start, end
    )
    data = builder.build(start_time=start, end_time=end)
    data.atomic_service_windows = _load_atomic_service_windows(
        app, data, scope.trigger_runs, scope.atomic_service_run_ids, start, end
    )
    data.trigger_runs = scope.trigger_runs
    data.event_markers = _render_markers_for_events(scope.events, scope.trigger_runs)
    if not data.lanes and not data.event_markers and not data.atomic_service_windows:
        return ""
    svg = TimelineSVGRenderer().render(data)
    # Render at native pixel width (like the main timeline) so it scrolls
    # horizontally instead of scaling down and making labels unreadable.
    return svg.replace('width="100%"', f'width="{_MINI_CONFIG.width}"', 1)


def _resolve_log_svg_scope(
    app: "Pynenc",
    inv_ids: set[str],
    event_ids: set[str],
    trigger_run_ids: set[str],
    trigger_ids: set[str],
    atomic_service_run_ids: set[str],
    start: datetime,
    end: datetime,
) -> _ResolvedLogSvgScope:
    """Resolve refs, related entities, and final mini-timeline bounds."""
    inv_ids = set(inv_ids)
    event_ids = set(event_ids)
    atomic_service_run_ids = set(atomic_service_run_ids)
    trigger_runs = _load_referenced_trigger_runs(
        app,
        start,
        end,
        inv_ids,
        event_ids,
        trigger_run_ids,
        trigger_ids,
        atomic_service_run_ids,
    )
    for run in trigger_runs:
        if run.atomic_service_run_id:
            atomic_service_run_ids.add(run.atomic_service_run_id)
        event_ids.update(run.event_ids or [])
        if run.triggered_invocation_id:
            inv_ids.add(str(run.triggered_invocation_id))
        inv_ids.update(str(item) for item in run.source_invocation_ids or [])
        for participant in run.participants or []:
            if participant.event_id:
                event_ids.add(participant.event_id)
            if participant.source_invocation_id:
                inv_ids.add(str(participant.source_invocation_id))

    events = _load_referenced_events(app, event_ids)
    for event in events:
        if event.emitted_by_invocation_id:
            inv_ids.add(str(event.emitted_by_invocation_id))
        inv_ids.update(str(item) for item in event.triggered_invocation_ids or [])
    start, end = _expand_time_range_for_references(start, end, events, trigger_runs)
    return _ResolvedLogSvgScope(
        inv_ids=inv_ids,
        atomic_service_run_ids=atomic_service_run_ids,
        events=events,
        trigger_runs=trigger_runs,
        start=start,
        end=end,
    )


def _backfill_referenced_history(
    app: "Pynenc",
    builder: TimelineDataBuilder,
    contexts: dict[str, "RunnerContext"],
    missing_inv_ids: set[str],
    start: datetime,
    end: datetime,
) -> None:
    """Add clipped history for referenced invocations outside the log window."""
    if not missing_inv_ids:
        return
    ghost_batch: list[InvocationHistory] = []
    for inv_id in missing_inv_ids:
        history = _get_invocation_history(app, inv_id)
        if not history:
            continue
        ghost_batch.extend(_clip_history_to_window(history, start, end))
    if not ghost_batch:
        return
    _fetch_new_contexts(ghost_batch, app, contexts)
    builder.add_history_batch(ghost_batch, contexts)


def _backfill_visible_boundary_history(
    app: "Pynenc",
    builder: TimelineDataBuilder,
    contexts: dict[str, "RunnerContext"],
    seen_invocation_ids: set[str],
    start: datetime,
    end: datetime,
) -> None:
    """Add clipped left-edge segments for invocations with visible points."""
    if not seen_invocation_ids:
        return
    synthetic_batch: list[InvocationHistory] = []
    for inv_id in seen_invocation_ids:
        history = _get_invocation_history(app, inv_id)
        synthetic = _left_boundary_segment_entry(history, start, end)
        if synthetic is not None:
            synthetic_batch.append(synthetic)
    if not synthetic_batch:
        return
    _fetch_new_contexts(synthetic_batch, app, contexts)
    builder.add_history_batch(synthetic_batch, contexts)


def _left_boundary_segment_entry(
    history: list["InvocationHistory"], start: datetime, end: datetime
) -> "InvocationHistory | None":
    """Return a synthetic segment entry when a visible point needs its bar."""
    from pynenc.state_backend.base_state_backend import InvocationHistory

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


def _get_invocation_history(app: "Pynenc", inv_id: str) -> list["InvocationHistory"]:
    """Fetch invocation history while tolerating stale or non-UUID refs."""
    invocation_id: InvocationId | str
    try:
        invocation_id = InvocationId(inv_id)
    except Exception:
        invocation_id = inv_id
    try:
        return app.state_backend.get_history(invocation_id)  # type: ignore[arg-type]
    except Exception:
        logger.debug("history lookup failed for log ref %s", inv_id, exc_info=True)
        return []


def _clip_history_to_window(
    history: list["InvocationHistory"], start: datetime, end: datetime
) -> list["InvocationHistory"]:
    """Clip referenced invocation history to the log-explorer SVG bounds."""
    from pynenc.state_backend.base_state_backend import InvocationHistory

    sorted_hist = sorted(history, key=lambda h: h.timestamp)
    pre_window = [h for h in sorted_hist if h.timestamp < start]
    in_window = [h for h in sorted_hist if start <= h.timestamp <= end]

    result: list[InvocationHistory] = []
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


def _load_atomic_service_windows(
    app: "Pynenc",
    data: "TimelineData",
    trigger_runs: list["TriggerRunRecord"],
    atomic_service_run_ids: set[str],
    start: datetime,
    end: datetime,
) -> list[AtomicServiceWindow]:
    """Load recorded AS execution windows relevant to the pasted logs."""
    target_run_ids = set(atomic_service_run_ids)
    for run in trigger_runs:
        if run.atomic_service_run_id:
            target_run_ids.add(run.atomic_service_run_id)
    if not target_run_ids:
        return []

    windows: list[AtomicServiceWindow] = []
    try:
        executions = app.orchestrator.get_atomic_service_executions_in_timerange(
            start,
            end,
            limit=2000,
        )
    except Exception:
        logger.debug("atomic-service history lookup failed for log SVG")
        executions = []

    for execution in executions:
        if execution.atomic_service_run_id not in target_run_ids:
            continue
        if execution.end_time is None:
            continue
        _ensure_runner_lane(app, data, execution.runner_id)
        windows.append(
            AtomicServiceWindow(
                runner_id=execution.runner_id,
                start_time=execution.start_time,
                end_time=execution.end_time,
                duration_seconds=execution.duration_seconds,
                atomic_service_run_id=execution.atomic_service_run_id,
            )
        )
    return windows


def _ensure_runner_lane(app: "Pynenc", data: "TimelineData", runner_id: str) -> None:
    """Ensure a runner with only AS activity still appears in the mini-timeline."""
    if runner_id in data.lanes:
        return
    try:
        runner_context = app.state_backend.get_runner_context(runner_id)
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


def _load_referenced_events(app: "Pynenc", event_ids: set[str]) -> list["EventRecord"]:
    """Load referenced events, ignoring IDs no longer present in storage."""
    events: list[EventRecord] = []
    for event_id in sorted(event_ids):
        try:
            event = app.trigger.get_event(event_id)
        except Exception:
            logger.debug("trigger backend get_event failed for %s", event_id)
            continue
        if event is not None:
            events.append(event)
    return events


def _load_referenced_trigger_runs(
    app: "Pynenc",
    start: datetime,
    end: datetime,
    inv_ids: set[str],
    event_ids: set[str],
    trigger_run_ids: set[str],
    trigger_ids: set[str],
    atomic_service_run_ids: set[str],
) -> list["TriggerRunRecord"]:
    """Load trigger runs that intersect the log references or time window."""
    runs_by_id: dict[str, TriggerRunRecord] = {}

    def add(run: "TriggerRunRecord | None") -> None:
        if run is not None:
            runs_by_id[run.trigger_run_id] = run

    for trigger_run_id in trigger_run_ids:
        try:
            add(app.trigger.get_trigger_run(trigger_run_id))
        except Exception:
            logger.debug(
                "trigger backend get_trigger_run failed for %s", trigger_run_id
            )
    for event_id in event_ids:
        try:
            for run in app.trigger.get_trigger_runs_for_event(event_id):
                add(run)
        except Exception:
            logger.debug(
                "trigger backend get_trigger_runs_for_event failed for %s", event_id
            )
    try:
        runs = app.trigger.get_trigger_runs_in_timerange(start, end, limit=2000)
    except Exception:
        logger.debug("trigger backend get_trigger_runs_in_timerange failed")
        runs = []
    for run in runs:
        run_inv_ids = {str(item) for item in run.source_invocation_ids or []}
        if run.triggered_invocation_id:
            run_inv_ids.add(str(run.triggered_invocation_id))
        run_event_ids = set(run.event_ids or [])
        run_atomic_service_run_id = run.atomic_service_run_id
        if (
            run_inv_ids & inv_ids
            or run_event_ids & event_ids
            or run.trigger_id in trigger_ids
            or (
                run_atomic_service_run_id
                and run_atomic_service_run_id in atomic_service_run_ids
            )
        ):
            add(run)
    return sorted(runs_by_id.values(), key=lambda run: run.claimed_at or start)


def _render_markers_for_events(
    events: list["EventRecord"], trigger_runs: list["TriggerRunRecord"]
) -> list[RenderEventMarker]:
    """Build timeline markers for referenced events plus synthetic cron ticks."""
    condition_types_by_event = _condition_types_by_event(trigger_runs)
    atomic_service_by_event = _atomic_service_by_event(trigger_runs)
    trigger_by_event = _trigger_by_event(trigger_runs)
    markers: list[RenderEventMarker] = []
    for event in events:
        if not event.triggered and not event.matched:
            continue
        atomic_service_run_id, atomic_service_runner_id = atomic_service_by_event.get(
            event.event_id,
            (None, None),
        )
        trigger_run_id, trigger_id = trigger_by_event.get(event.event_id, (None, None))
        markers.append(
            RenderEventMarker(
                event_id=event.event_id,
                event_code=event.event_code,
                timestamp=event.timestamp,
                triggered=event.triggered,
                matched=event.matched,
                triggered_invocation_ids=list(event.triggered_invocation_ids or []),
                emitted_by_invocation_id=event.emitted_by_invocation_id,
                condition_types=_condition_types_for_event(
                    event.event_id,
                    event.event_code,
                    condition_types_by_event,
                ),
                trigger_run_id=trigger_run_id,
                trigger_id=trigger_id,
                atomic_service_run_id=atomic_service_run_id,
                atomic_service_runner_id=atomic_service_runner_id,
            )
        )
    markers.extend(cron_event_markers_from_trigger_runs(trigger_runs))
    markers.sort(key=lambda marker: marker.timestamp)
    return markers


def _condition_types_by_event(
    trigger_runs: list["TriggerRunRecord"],
) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for run in trigger_runs:
        for participant in run.participants or []:
            if not participant.event_id or not participant.context_type:
                continue
            types = result.setdefault(participant.event_id, [])
            if participant.context_type not in types:
                types.append(participant.context_type)
    return result


def _atomic_service_by_event(
    trigger_runs: list["TriggerRunRecord"],
) -> dict[str, tuple[str | None, str | None]]:
    """Map event ids to the AS run that evaluated their trigger run."""
    result: dict[str, tuple[str | None, str | None]] = {}
    for run in trigger_runs:
        if not run.atomic_service_run_id and not run.atomic_service_runner_id:
            continue
        event_ids = set(run.event_ids or [])
        for participant in run.participants or []:
            if participant.event_id:
                event_ids.add(participant.event_id)
        for event_id in event_ids:
            result.setdefault(
                event_id,
                (run.atomic_service_run_id, run.atomic_service_runner_id),
            )
    return result


def _trigger_by_event(
    trigger_runs: list["TriggerRunRecord"],
) -> dict[str, tuple[str | None, str | None]]:
    """Map event ids to the trigger run and trigger definition that used them."""
    result: dict[str, tuple[str | None, str | None]] = {}
    for run in trigger_runs:
        event_ids = set(run.event_ids or [])
        for participant in run.participants or []:
            if participant.event_id:
                event_ids.add(participant.event_id)
        for event_id in event_ids:
            result.setdefault(event_id, (run.trigger_run_id, run.trigger_id))
    return result


def _condition_types_for_event(
    event_id: str,
    event_code: str,
    condition_types_by_event: dict[str, list[str]],
) -> list[str]:
    if types := condition_types_by_event.get(event_id):
        return types
    prefix = event_code.split(".", 1)[0].lower()
    return {
        "status": ["StatusContext"],
        "result": ["ResultContext"],
        "exception": ["ExceptionContext"],
        "cron": ["CronContext"],
    }.get(prefix, [])


def _fetch_new_contexts(
    batch: list["InvocationHistory"],
    app: "Pynenc",
    cache: dict[str, "RunnerContext"],
) -> None:
    """Fetch and cache runner contexts not yet in the cache."""
    new_ids = [h.runner_context_id for h in batch if h.runner_context_id not in cache]
    if not new_ids:
        return
    for ctx in app.state_backend.get_runner_contexts(new_ids):
        cache[ctx.runner_id] = ctx
