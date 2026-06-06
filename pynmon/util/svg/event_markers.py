"""
Event markers for the SVG timeline.

Each ``EventMarker`` represents a trigger event that occurred within the
timeline window. Markers are rendered as a subtle vertical guide plus a small
square in the dedicated Events row below the invocation lanes, coloured by
whether the event triggered any invocation. They link to the event detail page.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from html import escape
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from pynmon.util.svg.renderer import SVGStyle
    from pynmon.util.svg.timeline_data import TimelineData
    from pynenc.trigger.monitoring import TriggerRunRecord

from pynmon.util.svg.atomic_service import AtomicServiceWindow


EVENT_MARKER_COLORS: dict[str, str] = {
    "triggered": "#28a745",
    "matched": "#0d6efd",
    "unmatched": "#6c757d",
}

RELATION_LINE_COLORS: dict[str, str] = {
    "direct_call": "#78909c",
    "event_origin": "#0d9488",
    "event_trigger": "#7c3aed",
    "status_trigger": "#2563eb",
    "result_trigger": "#15803d",
    "exception_trigger": "#dc2626",
    "cron_trigger": "#f59e0b",
}

CONDITION_TYPE_COLORS: dict[str, str] = {
    "EventContext": RELATION_LINE_COLORS["event_trigger"],
    "StatusContext": RELATION_LINE_COLORS["status_trigger"],
    "ResultContext": RELATION_LINE_COLORS["result_trigger"],
    "ExceptionContext": RELATION_LINE_COLORS["exception_trigger"],
    "CronContext": RELATION_LINE_COLORS["cron_trigger"],
}

# Map participant ``context_type`` (set by ``_build_run_participants``) to
# the relation-kind suffix used by ``RELATION_LINE_COLORS`` and the
# ``data-context-type`` attribute consumed by the Pynmon JS overlay.
_CONTEXT_TYPE_TO_KIND: dict[str, str] = {
    "EventContext": "event_trigger",
    "StatusContext": "status_trigger",
    "ResultContext": "result_trigger",
    "ExceptionContext": "exception_trigger",
    "CronContext": "cron_trigger",
}

_MARKER_SIZE = 9
_EVENT_DOT_RADIUS = 4
_EVENT_LABEL_MAX_CHARS = 26


@dataclass
class EventMarker:
    """Minimal data needed to render one event on the timeline."""

    event_id: str
    event_code: str
    timestamp: datetime
    triggered: bool
    matched: bool
    payload_excerpt: str = ""
    triggered_invocation_ids: list[str] | None = None
    emitted_by_invocation_id: str | None = None
    href: str | None = None
    trigger_run_id: str | None = None
    trigger_id: str | None = None
    context_type: str | None = None
    condition_types: list[str] = field(default_factory=list)
    # Id of the atomic-service execution cycle that produced this marker, when
    # known. Set by ``cron_event_markers_from_trigger_runs`` from the
    # originating ``TriggerRunRecord``. Pynmon uses it as a primary anchor key
    # so a cron tick always lands inside the exact atomic-service bar that
    # produced it, even when the recorded windows of two runners overlap on
    # the timeline.
    atomic_service_run_id: str | None = None
    atomic_service_runner_id: str | None = None


class _TimelinePoint(Protocol):
    @property
    def invocation_id(self) -> str: ...

    @property
    def timestamp(self) -> datetime: ...

    @property
    def status(self) -> str: ...

    @property
    def sub_lane(self) -> int: ...

    @property
    def registered_by_inv_id(self) -> str | None: ...


@dataclass(frozen=True)
class _PointRef:
    """Rendered position for a status point used by relation overlays."""

    point: _TimelinePoint
    x: float
    y: float


@dataclass(frozen=True)
class _EventMarkerPlacement:
    marker: EventMarker
    x: float
    y: float
    label: str
    label_x: float
    label_y: float
    text_anchor: str


def _marker_state(marker: EventMarker) -> str:
    if marker.triggered:
        return "triggered"
    if marker.matched:
        return "matched"
    return "unmatched"


def _marker_color(marker: EventMarker) -> str:
    """Return the marker colour for the timeline overlay.

    Colour semantics align with the dashboard's condition-type legend so
    that filtered overlays share the same palette as relation lines:

    * green ``#28a745`` — event triggered at least one invocation
    * blue  ``#0d6efd`` — event matched conditions but did not trigger
    * gray  ``#6c757d`` — event recorded but unmatched
    """
    return EVENT_MARKER_COLORS[_marker_state(marker)]


def cron_event_markers_from_trigger_runs(
    trigger_runs: Iterable[TriggerRunRecord],
) -> list[EventMarker]:
    """Build synthetic timeline markers for cron-triggered runs.

    Cron triggers do not originate from a durable ``EventRecord``, but users
    still need the same event row anchor that event-based triggers get. These
    markers are display-only and link to the trigger-run detail page.
    """
    markers: list[EventMarker] = []
    seen: set[tuple[str, str, datetime]] = set()
    for run in trigger_runs:
        triggered_invocation_ids: list[str] = (
            [run.triggered_invocation_id] if run.triggered_invocation_id else []
        )
        for participant in run.participants or []:
            if participant.context_type != "CronContext":
                continue
            timestamp = (
                participant.context_timestamp or run.claimed_at or run.executed_at
            )
            if timestamp is None:
                continue
            condition_id = participant.condition_id or "cron"
            key = (run.trigger_run_id, condition_id, timestamp)
            if key in seen:
                continue
            seen.add(key)
            summary = participant.context_summary or "cron"
            payload = f"{summary}\ncondition:{condition_id}"
            markers.append(
                EventMarker(
                    event_id=f"cron:{run.trigger_run_id}:{condition_id}",
                    event_code="cron.tick",
                    timestamp=timestamp,
                    triggered=bool(triggered_invocation_ids),
                    matched=True,
                    payload_excerpt=payload,
                    triggered_invocation_ids=triggered_invocation_ids or None,
                    href=f"/trigger-runs/{run.trigger_run_id}",
                    trigger_run_id=run.trigger_run_id,
                    trigger_id=run.trigger_id,
                    context_type="CronContext",
                    atomic_service_run_id=run.atomic_service_run_id,
                    atomic_service_runner_id=run.atomic_service_runner_id,
                )
            )
    return markers


def _plot_bottom(data: TimelineData) -> float:
    """Return the bottom of the plotted lane area, excluding the legend."""
    lanes = data.get_sorted_lanes()
    if not lanes:
        return float(data.config.top_margin + data.config.lane_height)
    last_lane = lanes[-1]
    return float(data.lane_y_position(last_lane) + last_lane.lane_height(data.config))


def _segment_top_for_invocation(
    data: TimelineData,
    invocation_id: str,
    near_timestamp: datetime | None = None,
) -> float | None:
    """Return the y-position of the top edge of ``invocation_id``'s bar.

    Iterates all rendered lanes, looking for a segment whose ``invocation_id``
    matches. When ``near_timestamp`` is given, segments whose time range
    contains the timestamp (with a small slack) win over arbitrary matches.
    Returns ``None`` when no matching segment exists in the current view.
    """
    config = data.config
    slack = timedelta(milliseconds=50)
    overlap_top: float | None = None
    any_top: float | None = None
    for lane in data.get_sorted_lanes():
        lane_y = data.lane_y_position(lane) + config.bar_y_offset
        for seg in lane.segments:
            if seg.invocation_id != invocation_id:
                continue
            seg_top = lane_y + seg.sub_lane * (config.bar_height + 2)
            if any_top is None:
                any_top = seg_top
            if near_timestamp is None:
                continue
            seg_end = seg.start_time + timedelta(
                seconds=max(seg.duration_seconds or 0.0, 0.0)
            )
            if seg.start_time - slack <= near_timestamp <= seg_end + slack:
                overlap_top = seg_top
                break
        if overlap_top is not None:
            break
    return overlap_top if overlap_top is not None else any_top


def _atomic_service_top_for_marker(
    data: TimelineData, marker: EventMarker
) -> float | None:
    """Return the y-top of the atomic-service bar that owns ``marker``.

    Resolution order:

     0. ``marker.atomic_service_run_id`` matches a window's
         ``atomic_service_run_id`` (primary, unambiguous anchor — added so that
       cron ticks land in the exact runner bar that produced them, even
       when multiple runners' windows overlap on the timeline due to a
       startup race).
    1. A service window whose ``[start_time, end_time]`` contains the
       marker's own timestamp.
    2. For markers carrying ``trigger_run_id`` (cron ticks and other
       trigger-service markers), the timestamps of the originating
       trigger run (``claimed_at`` / ``executed_at``) — the cron tick
       may have been scheduled outside the polling window but the run
       itself definitely happened inside one.

    Returns ``None`` when no service window can be associated with the
    marker (the caller is responsible for the fallback).
    """
    config = data.config

    def _window_top(window: object) -> float | None:
        runner_lane = data.lanes.get(window.runner_id)  # type: ignore[attr-defined]
        if runner_lane is None:
            return None
        return float(data.lane_y_position(runner_lane) + config.bar_y_offset)

    if marker.atomic_service_run_id:
        for window in data.atomic_service_windows:
            if (
                getattr(window, "atomic_service_run_id", None)
                == marker.atomic_service_run_id
            ):
                top = _window_top(window)
                if top is not None:
                    return top

    for window in data.atomic_service_windows:
        if window.start_time <= marker.timestamp <= window.end_time:
            top = _window_top(window)
            if top is not None:
                return top

    if marker.trigger_run_id and data.trigger_runs:
        run = next(
            (r for r in data.trigger_runs if r.trigger_run_id == marker.trigger_run_id),
            None,
        )
        if run is not None:
            candidates = [
                ts for ts in (run.claimed_at, run.executed_at) if ts is not None
            ]
            for ts in candidates:
                for window in data.atomic_service_windows:
                    if window.start_time <= ts <= window.end_time:
                        top = _window_top(window)
                        if top is not None:
                            return top

    # Final fallback: snap to the temporally nearest atomic-service window.
    # Cron ticks are always produced by SOMEONE's trigger loop, so even if
    # the exact owning window isn't present (historical data without
    # atomic_service_run_id, or the window was purged), we always pick the closest
    # one rather than letting the marker float off-bar. This keeps the
    # invariant "events never float orphan" stated in
    # ``_resolve_marker_anchor``.
    if data.atomic_service_windows:

        def _distance(window: AtomicServiceWindow) -> float:
            if window.start_time <= marker.timestamp <= window.end_time:
                return 0.0
            return min(
                abs((window.start_time - marker.timestamp).total_seconds()),
                abs((window.end_time - marker.timestamp).total_seconds()),
            )

        nearest = min(data.atomic_service_windows, key=_distance)
        top = _window_top(nearest)
        if top is not None:
            return top

    return None


def _resolve_marker_anchor(
    data: TimelineData, marker: EventMarker
) -> tuple[float, float, float] | None:
    """Return ``(x, dot_y, label_y)`` for the marker, or ``None`` to skip.

    Events do not pop up from nowhere: they are always produced either by a
    task invocation (``emitted_by_invocation_id`` set), by the atomic
    trigger service running inside one of the runners (cron ticks,
    periodic checks), or — failing both — they at least caused a
    triggered invocation that IS visible in the timeline. The marker dot
    is drawn **on top of** the corresponding bar at its vertical centre
    and the label is placed inline so the marker never adds extra
    vertical space above the bar.

    Resolution priority:

    1. Segment matching ``emitted_by_invocation_id`` (real events).
    2. Atomic-service window containing the marker's timestamp, or the
       originating trigger run's ``claimed_at`` / ``executed_at``
       (cron / system events).
    3. First triggered-invocation segment visible in the timeline
       (universal fallback — for cron, this is the invocation the tick
       registered).
    4. ``None`` — caller skips the marker (we never float orphan dots).
    """
    config = data.config
    x = _clamp_marker_x(data, data.bounds.time_to_x(marker.timestamp))
    bar_h = config.bar_height

    def _anchor(top: float) -> tuple[float, float, float]:
        dot_y = top + bar_h / 2
        # Label sits at the same y as the dot so events align with the
        # status circles and invocation bars on the same row.
        return x, dot_y, dot_y

    inv_id = marker.emitted_by_invocation_id
    if inv_id:
        top = _segment_top_for_invocation(data, str(inv_id), marker.timestamp)
        if top is not None:
            return _anchor(top)

    atomic_top = _atomic_service_top_for_marker(data, marker)
    if atomic_top is not None:
        return _anchor(atomic_top)

    for triggered_id in marker.triggered_invocation_ids or []:
        top = _segment_top_for_invocation(data, str(triggered_id), marker.timestamp)
        if top is not None:
            return _anchor(top)

    return None


def event_row_y(data: TimelineData) -> float:
    """Legacy hook: y coordinate used when no emitter anchor is known."""
    return _plot_bottom(data) - _EVENT_DOT_RADIUS - 2


def event_row_height(data: TimelineData) -> int:
    """Event markers anchor on their emitter bars and consume no extra height."""
    del data
    return 0


def _clamp_marker_x(data: TimelineData, x: float) -> float:
    """Keep markers fully inside the viewBox."""
    return min(
        max(x, data.config.left_margin + _EVENT_DOT_RADIUS),
        data.config.width - _EVENT_DOT_RADIUS,
    )


def _event_marker_layout(data: TimelineData) -> list[_EventMarkerPlacement]:
    """Place each marker on top of the bar that emitted it.

    The dot sits at the vertical centre of the emitter bar and the label
    is rendered inline (same row as the dot) so markers never add
    vertical space above the bar. Markers that cannot be anchored to any
    visible bar are dropped — we never float orphan dots at the bottom
    of the plot.
    """
    placements: list[_EventMarkerPlacement] = []
    for marker in sorted(data.event_markers, key=lambda item: item.timestamp):
        anchor = _resolve_marker_anchor(data, marker)
        if anchor is None:
            continue
        x, dot_y, label_y = anchor
        label = _event_label(marker)
        text_width = _estimate_label_width(label)
        _left, _right, label_x, text_anchor = _label_bounds(data, x, text_width)
        placements.append(
            _EventMarkerPlacement(
                marker=marker,
                x=x,
                y=dot_y,
                label=label,
                label_x=label_x,
                label_y=label_y,
                text_anchor=text_anchor,
            )
        )
    return placements


def _event_label(marker: EventMarker) -> str:
    label = marker.event_code
    if len(label) > _EVENT_LABEL_MAX_CHARS:
        return label[: _EVENT_LABEL_MAX_CHARS - 1] + "…"
    return label


def _estimate_label_width(label: str) -> float:
    return (len(label) * 5.5) + 16


def _label_bounds(
    data: TimelineData, x: float, text_width: float
) -> tuple[float, float, float, str]:
    label_x = x + _EVENT_DOT_RADIUS + 4
    left = x - _EVENT_DOT_RADIUS
    right = label_x + text_width
    if right <= data.config.width - 4:
        return left, right, label_x, "start"
    label_x = x - _EVENT_DOT_RADIUS - 4
    left = max(data.config.left_margin, label_x - text_width)
    right = x + _EVENT_DOT_RADIUS
    return left, right, label_x, "end"


def render_event_markers(data: TimelineData, style: SVGStyle | None) -> str:
    """Render event markers as a dedicated bottom row of labelled dots.

    Each marker is rendered as:

    * A short, very faint vertical guide tick across the lane area so the
      eye can correlate the event with the invocations above it.
    * A coloured dot in the dedicated event row below the lanes.
    * The event code label next to the dot (truncated when many markers
      crowd together).

    The whole marker is wrapped in an anchor with ``data-event-id`` so the
    overlay layer can attach focused relation connectors and the click
    navigates to the event detail page.
    """
    del style
    if not data.event_markers:
        return ""
    placements = _event_marker_layout(data)

    parts: list[str] = ['  <g class="event-markers">']
    for placement in placements:
        marker = placement.marker
        x = placement.x
        y = placement.y
        colour = _marker_color(marker)
        state = _marker_state(marker)
        related_invocation_ids = [
            *(marker.triggered_invocation_ids or []),
            *(
                [marker.emitted_by_invocation_id]
                if marker.emitted_by_invocation_id
                else []
            ),
        ]
        related_ids = " ".join(
            escape(str(invocation_id)) for invocation_id in related_invocation_ids
        )
        tooltip = f"{marker.event_code} @ {marker.timestamp.isoformat(timespec='microseconds')}"
        if marker.payload_excerpt:
            tooltip = f"{tooltip}\n{marker.payload_excerpt}"
        if marker.condition_types:
            tooltip = f"{tooltip}\nconditions:{', '.join(marker.condition_types)}"
        href = marker.href or f"/events/{escape(marker.event_id)}"
        trigger_attrs = ""
        if marker.trigger_run_id:
            trigger_attrs += f' data-trigger-run-id="{escape(marker.trigger_run_id)}"'
        if marker.trigger_id:
            trigger_attrs += f' data-trigger-id="{escape(marker.trigger_id)}"'
        if marker.context_type:
            trigger_attrs += f' data-context-type="{escape(marker.context_type)}"'
        if marker.atomic_service_run_id:
            trigger_attrs += (
                f' data-atomic-service-run-id="{escape(marker.atomic_service_run_id)}"'
            )
        if marker.atomic_service_runner_id:
            trigger_attrs += f' data-atomic-service-runner-id="{escape(marker.atomic_service_runner_id)}"'
        parts.append(
            f'    <a href="{href}" class="event-marker-link" '
            f'data-event-id="{escape(marker.event_id)}" '
            f'data-event-code="{escape(marker.event_code)}" '
            f'data-event-state="{state}" '
            f'data-event-timestamp="{escape(marker.timestamp.isoformat(timespec="microseconds"))}" '
            f'data-related-invocation-ids="{related_ids}"{trigger_attrs}>'
            f"<title>{escape(tooltip)}</title>"
            # Marker dot drawn ON TOP of the emitter bar (centred vertically).
            f'<rect x="{x - _EVENT_DOT_RADIUS:.1f}" y="{y - _EVENT_DOT_RADIUS:.1f}" '
            f'width="{_EVENT_DOT_RADIUS * 2}" height="{_EVENT_DOT_RADIUS * 2}" rx="1.5" '
            f'fill="{colour}" stroke="white" stroke-width="1.5"/>'
            f"{_render_condition_type_squares(marker, x, y)}"
            # Label centered vertically with the dot so the event aligns
            # with the status circles / bar on the same row.
            f'<text x="{placement.label_x:.1f}" y="{placement.label_y:.1f}" '
            f'text-anchor="{placement.text_anchor}" dominant-baseline="middle" '
            f'fill="#1f2937" font-size="9px" font-weight="600" '
            f'paint-order="stroke" stroke="white" stroke-width="2.5" '
            f'stroke-linejoin="round" pointer-events="none">'
            f"{escape(placement.label)}</text>"
            "</a>"
        )
    parts.append("  </g>")
    return "\n".join(parts)


def _render_condition_type_squares(marker: EventMarker, x: float, dot_y: float) -> str:
    """Render tiny condition-type swatches inline to the left of the dot.

    Swatches sit on the SAME row as the event dot so the marker stays
    aligned with the status circles and invocation bars on its lane.
    """
    types = list(dict.fromkeys(marker.condition_types or []))[:4]
    if not types:
        return ""
    size = 5
    gap = 1
    # Right-anchored row sitting just left of the dot.
    right_edge = x - _EVENT_DOT_RADIUS - 3
    y_top = dot_y - size / 2
    parts: list[str] = []
    for idx, condition_type in enumerate(reversed(types)):
        colour = CONDITION_TYPE_COLORS.get(condition_type, "#64748b")
        square_x = right_edge - ((idx + 1) * size) - (idx * gap)
        parts.append(
            f'<rect class="condition-type-square" '
            f'data-condition-type="{escape(condition_type)}" '
            f'x="{square_x:.1f}" y="{y_top:.1f}" width="{size}" height="{size}" '
            f'rx="1" fill="{colour}" stroke="white" stroke-width="0.6"/>'
        )
    return "".join(parts)


def render_timeline_relations(data: TimelineData, style: SVGStyle | None) -> str:
    """Render direct-call and event-trigger relation lines.

    Direct-call lines connect a parent's nearest status point to the child's
    REGISTERED point. Event-trigger lines connect the square event marker to
    the first point of each invocation created by that event.
    """
    del style
    point_refs = _collect_point_refs(data)
    if not point_refs:
        return ""
    by_inv = _points_by_invocation(point_refs)
    parts: list[str] = ['  <g class="timeline-relations">']
    parts.extend(_render_trigger_condition_markers(data, by_inv))
    parts.extend(_render_direct_call_relations(by_inv))
    parts.extend(_render_event_origin_relations(data, by_inv))
    parts.extend(_render_event_trigger_relations(data, by_inv))
    parts.extend(_render_trigger_run_relations(data, by_inv))
    parts.append("  </g>")
    return "\n".join(parts)


def _collect_point_refs(data: TimelineData) -> list[_PointRef]:
    refs: list[_PointRef] = []
    bounds = data.bounds
    config = data.config
    sub_h = config.bar_height + 2
    for lane in data.get_sorted_lanes():
        base_y = (
            data.lane_y_position(lane) + config.bar_y_offset + config.bar_height // 2
        )
        for point in lane.points:
            refs.append(
                _PointRef(
                    point=point,
                    x=bounds.time_to_x(point.timestamp),
                    y=base_y + point.sub_lane * sub_h,
                )
            )
    return refs


def _points_by_invocation(refs: list[_PointRef]) -> dict[str, list[_PointRef]]:
    by_inv: dict[str, list[_PointRef]] = {}
    for ref in refs:
        by_inv.setdefault(ref.point.invocation_id, []).append(ref)
    for inv_refs in by_inv.values():
        inv_refs.sort(key=lambda ref: ref.point.timestamp)
    return by_inv


def _render_direct_call_relations(
    by_inv: dict[str, list[_PointRef]],
) -> list[str]:
    parts: list[str] = []
    for child_refs in by_inv.values():
        child_ref = child_refs[0]
        if child_ref.point.status.upper() != "REGISTERED":
            continue
        parent_id = child_ref.point.registered_by_inv_id
        if not parent_id:
            continue
        source_ref = _nearest_parent_ref(by_inv.get(str(parent_id), []), child_ref)
        if source_ref is None:
            continue
        parts.append(
            _relation_path(
                source_ref,
                child_ref,
                RELATION_LINE_COLORS["direct_call"],
                "direct-call",
                attrs=(
                    f'data-parent-invocation-id="{escape(str(parent_id))}" '
                    f'data-child-invocation-id="{escape(child_ref.point.invocation_id)}"'
                ),
            )
        )
    return parts


def _nearest_parent_ref(
    parent_refs: list[_PointRef], child_ref: _PointRef
) -> _PointRef | None:
    if not parent_refs:
        return None
    before = [
        ref for ref in parent_refs if ref.point.timestamp <= child_ref.point.timestamp
    ]
    return before[-1] if before else parent_refs[0]


def _render_event_trigger_relations(
    data: TimelineData, by_inv: dict[str, list[_PointRef]]
) -> list[str]:
    parts: list[str] = []
    marker_y = {
        placement.marker.event_id: placement.y
        for placement in _event_marker_layout(data)
    }
    for marker in data.event_markers:
        if marker.context_type == "CronContext":
            continue
        triggered_ids = marker.triggered_invocation_ids or []
        if not triggered_ids:
            continue
        source_ref = _PointRef(
            point=_SyntheticPoint(
                invocation_id=f"event:{marker.event_id}",
                timestamp=marker.timestamp,
            ),
            x=_clamp_marker_x(data, data.bounds.time_to_x(marker.timestamp)),
            y=marker_y.get(marker.event_id, event_row_y(data)),
        )
        for invocation_id in triggered_ids:
            target_refs = by_inv.get(str(invocation_id))
            if not target_refs:
                continue
            target_ref = target_refs[0]
            attrs = (
                f'data-event-id="{escape(marker.event_id)}" '
                f'data-child-invocation-id="{escape(str(invocation_id))}"'
            )
            if marker.trigger_run_id:
                attrs += f' data-trigger-run-id="{escape(marker.trigger_run_id)}"'
            if marker.trigger_id:
                attrs += f' data-trigger-id="{escape(marker.trigger_id)}"'
            parts.append(
                _relation_path(
                    source_ref,
                    target_ref,
                    RELATION_LINE_COLORS["event_trigger"],
                    "event-trigger",
                    attrs=attrs,
                )
            )
    return parts


def _render_event_origin_relations(
    data: TimelineData, by_inv: dict[str, list[_PointRef]]
) -> list[str]:
    """Render source invocation -> emitted event marker lines.

    A line is only drawn when the marker is anchored somewhere OTHER than
    its emitter invocation's own bar — i.e. when the emitter has no
    visible segment covering the marker's timestamp and the marker was
    placed on a different lane (atomic-service window or triggered
    invocation). When the marker already sits on top of the emitter's
    bar (the common case) the line would point from a status circle on
    that same bar to the marker right next to it, which is just visual
    noise. The bar placement is itself the origin.
    """
    parts: list[str] = []
    marker_y = {
        placement.marker.event_id: placement.y
        for placement in _event_marker_layout(data)
    }
    for marker in data.event_markers:
        source_invocation_id = marker.emitted_by_invocation_id
        if not source_invocation_id:
            continue
        # Skip when the marker is already on the emitter bar (case 1 of
        # _resolve_marker_anchor). The marker placement IS the origin
        # link, no relation line needed.
        if (
            _segment_top_for_invocation(
                data, str(source_invocation_id), marker.timestamp
            )
            is not None
        ):
            continue
        event_ref = _PointRef(
            point=_SyntheticPoint(
                invocation_id=f"event:{marker.event_id}",
                timestamp=marker.timestamp,
            ),
            x=_clamp_marker_x(data, data.bounds.time_to_x(marker.timestamp)),
            y=marker_y.get(marker.event_id, event_row_y(data)),
        )
        source_refs = by_inv.get(str(source_invocation_id))
        if not source_refs:
            continue
        source_ref = _nearest_point_by_timestamp(source_refs, marker.timestamp)
        parts.append(
            _relation_path(
                source_ref,
                event_ref,
                RELATION_LINE_COLORS["event_origin"],
                "event-origin",
                attrs=(
                    f'data-event-id="{escape(marker.event_id)}" '
                    f'data-source-invocation-id="{escape(str(source_invocation_id))}"'
                ),
            )
        )
    return parts


@dataclass(frozen=True)
class _SyntheticPoint:
    invocation_id: str
    timestamp: datetime
    status: str = "EVENT"
    sub_lane: int = 0
    registered_by_inv_id: str | None = None


def _render_trigger_run_relations(
    data: TimelineData, by_inv: dict[str, list[_PointRef]]
) -> list[str]:
    """Render trigger-run participant edges for status/result/exception/cron.

    Each :class:`TriggerRunRecord` carries one ``TriggerRunParticipant``
    per matched condition. For status/result/exception participants we
    draw a line from the closest existing status point on the *source*
    invocation's lane (anchored by ``context_timestamp``) to the first
    point of the *triggered* invocation. Event participants are already
    covered by :func:`_render_event_trigger_relations` via the event
    marker, so they are skipped here.

    Every line carries ``data-trigger-run-id``, ``data-context-type``,
    and source/target invocation attributes so the JS cross-highlight
    layer can fan out from any participant.
    """
    parts: list[str] = []
    if not data.trigger_runs:
        return parts
    marker_y = {
        placement.marker.event_id: placement.y
        for placement in _event_marker_layout(data)
    }
    for run in data.trigger_runs:
        target_refs = by_inv.get(str(run.triggered_invocation_id or ""))
        if not target_refs:
            continue
        target_ref = target_refs[0]
        if not run.participants:
            continue
        for participant in run.participants:
            kind = _CONTEXT_TYPE_TO_KIND.get(participant.context_type)
            if kind is None:
                continue
            if kind == "event_trigger":
                # Already drawn via event marker → invocation lines.
                continue
            source_ref = _trigger_run_source_ref(
                data,
                by_inv,
                marker_y,
                participant,
                target_ref,
                run.trigger_run_id,
            )
            if source_ref is None:
                continue
            colour = RELATION_LINE_COLORS[kind]
            attrs = (
                f'data-trigger-run-id="{escape(run.trigger_run_id)}" '
                f'data-trigger-id="{escape(run.trigger_id)}" '
                f'data-context-type="{escape(participant.context_type)}" '
                f'data-child-invocation-id="{escape(str(run.triggered_invocation_id))}"'
            )
            if participant.condition_id:
                attrs += f' data-condition-id="{escape(participant.condition_id)}"'
            if participant.source_invocation_id:
                attrs += (
                    f' data-source-invocation-id="'
                    f'{escape(participant.source_invocation_id)}"'
                )
            parts.append(
                _relation_path(
                    source_ref,
                    target_ref,
                    colour,
                    kind.replace("_", "-"),
                    attrs=attrs,
                )
            )
    return parts


def _render_trigger_condition_markers(
    data: TimelineData, by_inv: dict[str, list[_PointRef]]
) -> list[str]:
    """Render small condition-type squares under source status points.

    Status/result/exception trigger participants are anchored to an invocation
    status point. The relation line alone shows the connection, but the source
    point also needs a local condition marker so users can see *which* source
    status/result/exception satisfied the trigger without following the line.
    Event and cron participants are already represented in the event row.
    """
    parts: list[str] = []
    seen: set[tuple[str, str, str | None, float, float]] = set()
    for run in data.trigger_runs or []:
        for participant in run.participants or []:
            context_type = participant.context_type
            if context_type in {"EventContext", "CronContext"}:
                continue
            source_invocation_id = participant.source_invocation_id
            if not source_invocation_id:
                continue
            source_refs = by_inv.get(str(source_invocation_id))
            if not source_refs:
                continue
            source_ref = _nearest_point_by_timestamp(
                source_refs, participant.context_timestamp
            )
            condition_id = participant.condition_id
            key = (
                str(source_invocation_id),
                context_type,
                condition_id,
                round(source_ref.x, 1),
                round(source_ref.y, 1),
            )
            if key in seen:
                continue
            seen.add(key)
            colour = CONDITION_TYPE_COLORS.get(context_type, "#64748b")
            attrs = (
                f'data-trigger-run-id="{escape(run.trigger_run_id)}" '
                f'data-trigger-id="{escape(run.trigger_id)}" '
                f'data-context-type="{escape(context_type)}" '
                f'data-source-invocation-id="{escape(str(source_invocation_id))}" '
                f'data-child-invocation-id="{escape(str(run.triggered_invocation_id))}"'
            )
            if condition_id:
                attrs += f' data-condition-id="{escape(condition_id)}"'
            summary = participant.context_summary or context_type
            size = 15
            half = size / 2
            parts.append(
                f'    <rect class="trigger-condition-marker" {attrs} '
                f'x="{source_ref.x - half:.1f}" y="{source_ref.y - half:.1f}" '
                f'width="{size}" height="{size}" rx="2" fill="{colour}" '
                f'stroke="white" stroke-width="0.9" opacity="0.8">'
                f"<title>{escape(summary)}</title></rect>"
            )
    return parts


def _trigger_run_source_ref(
    data: TimelineData,
    by_inv: dict[str, list[_PointRef]],
    marker_y: dict[str, float],
    participant: object,
    target_ref: _PointRef,
    trigger_run_id: str,
) -> _PointRef | None:
    """Locate the SVG anchor for a participant's source context."""
    src_inv = getattr(participant, "source_invocation_id", None)
    ctx_ts = getattr(participant, "context_timestamp", None)
    if src_inv:
        candidates = by_inv.get(str(src_inv))
        if candidates:
            return _nearest_point_by_timestamp(candidates, ctx_ts)
        return None
    # No invocation source → cron-style tick anchored on the event row.
    if (
        getattr(participant, "context_type", None) == "CronContext"
        and ctx_ts is not None
    ):
        condition_id = getattr(participant, "condition_id", None) or "cron"
        marker_id = f"cron:{trigger_run_id}:{condition_id}"
        x = _clamp_marker_x(data, data.bounds.time_to_x(ctx_ts))
        y = marker_y.get(marker_id, event_row_y(data))
        return _PointRef(
            point=_SyntheticPoint(
                invocation_id=f"trigger-source:{target_ref.point.invocation_id}",
                timestamp=ctx_ts,
            ),
            x=x,
            y=y,
        )
    return None


def _nearest_point_by_timestamp(
    refs: list[_PointRef], ts: datetime | None
) -> _PointRef:
    """Pick the point closest to ``ts`` (or the last one if ``ts`` is None)."""
    if ts is None:
        return refs[-1]
    return min(refs, key=lambda r: abs((r.point.timestamp - ts).total_seconds()))


def _relation_path(
    source: _PointRef,
    target: _PointRef,
    colour: str,
    kind: str,
    attrs: str,
) -> str:
    """Render a relation with a white halo for contrast.

    Direct-call relations always use a straight polyline (vertical drop
    + horizontal travel) so parent/child links stay visually distinct
    from the curvy event-trigger / event-origin lines. Event relations
    use a bezier; when source and target share a lane the bezier bows
    upward so the arc clears the bar fill underneath.
    """
    dash = "2,3" if kind == "direct-call" else "6,3"
    dx = target.x - source.x
    dy = target.y - source.y
    if kind == "direct-call":
        # Straight-line direct-call: vertical leg + horizontal leg.
        # If aligned on the same x (within 0.5px) just draw a single line.
        if abs(dx) < 0.5:
            path = f"M{source.x:.1f},{source.y:.1f} L{target.x:.1f},{target.y:.1f}"
        else:
            path = (
                f"M{source.x:.1f},{source.y:.1f} "
                f"L{source.x:.1f},{target.y:.1f} "
                f"L{target.x:.1f},{target.y:.1f}"
            )
    elif abs(dy) < 4:
        # Vertical bow proportional to span, clamped to keep the arc on screen.
        bow = max(18.0, min(abs(dx) * 0.4, 56.0))
        c1y = source.y - bow
        c2y = target.y - bow
        c1x = source.x + dx * 0.25
        c2x = source.x + dx * 0.75
        path = (
            f"M{source.x:.1f},{source.y:.1f} "
            f"C{c1x:.1f},{c1y:.1f} {c2x:.1f},{c2y:.1f} "
            f"{target.x:.1f},{target.y:.1f}"
        )
    else:
        mid_y = (source.y + target.y) / 2
        path = (
            f"M{source.x:.1f},{source.y:.1f} "
            f"C{source.x:.1f},{mid_y:.1f} {target.x:.1f},{mid_y:.1f} "
            f"{target.x:.1f},{target.y:.1f}"
        )
    halo = (
        f'    <path class="timeline-relation-halo {kind}-relation-halo" '
        f'd="{path}" fill="none" stroke="white" stroke-width="4" '
        f'opacity="0.85" pointer-events="none"/>'
    )
    line = (
        f'    <path class="timeline-relation-line {kind}-relation-line" '
        f'data-relation-kind="{kind}" {attrs} '
        f'd="{path}" '
        f'fill="none" stroke="{colour}" stroke-width="1.8" '
        f'stroke-dasharray="{dash}" opacity="0.9"/>'
    )
    return halo + "\n" + line
