"""Atomic-service overlays for the invocation timeline SVG."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from html import escape
from typing import TYPE_CHECKING

from pynmon.util.svg.lane_assignment import TimeInterval

if TYPE_CHECKING:
    from pynenc.trigger.monitoring import TriggerRunRecord
    from pynmon.util.svg.renderer import SVGStyle
    from pynmon.util.svg.status_elements import StatusPoint
    from pynmon.util.svg.timeline_data import TimelineData


ATOMIC_SERVICE_FILL = "#f97316"
ATOMIC_SERVICE_STROKE = "#c2410c"
ATOMIC_SERVICE_LABEL = "atomic service run"
ATOMIC_SERVICE_MIN_WIDTH_PX = 6.0


@dataclass(frozen=True)
class AtomicServiceWindow:
    """One atomic-service execution window recorded for a runner."""

    runner_id: str
    start_time: datetime
    end_time: datetime
    atomic_service_run_id: str
    duration_seconds: float | None = None
    sub_lane: int = 0

    @property
    def window_id(self) -> str:
        """Stable DOM id for the currently recorded runner service window."""
        start = self.start_time.isoformat(timespec="microseconds")
        return f"atomic-service:{self.runner_id}:{start}"


def render_atomic_service_windows(data: TimelineData, style: SVGStyle) -> str:
    """Render latest atomic-service windows on their runner lanes."""
    del style
    if not data.atomic_service_windows:
        return ""
    parts: list[str] = ['  <g class="atomic-service-windows">']
    for window in data.atomic_service_windows:
        lane = data.lanes.get(window.runner_id)
        if lane is None:
            continue
        start_time = max(window.start_time, data.bounds.start_time)
        end_time = min(window.end_time, data.bounds.end_time)
        if end_time < data.bounds.start_time or start_time > data.bounds.end_time:
            continue
        x1 = data.bounds.time_to_x(start_time)
        x2 = data.bounds.time_to_x(end_time)
        raw_width = x2 - x1
        # Always render a visible sliver so short atomic-service runs can be
        # selected and correlated with cron/event markers.
        width = max(raw_width, ATOMIC_SERVICE_MIN_WIDTH_PX)
        rect_x = x1
        right_edge = float(data.config.width)
        if rect_x + width > right_edge:
            rect_x = max(float(data.config.left_margin), right_edge - width)
        if raw_width >= 4.0:
            opacity = 0.85
        else:
            # raw_width in [0, 4) -> opacity in [1.0, 0.85]
            opacity = 1.0 - (max(raw_width, 0.0) / 4.0) * 0.15
        config = data.config
        lane_y = data.lane_y_position(lane) + config.bar_y_offset
        y = lane_y + window.sub_lane * (config.bar_height + 2)
        height = config.bar_height
        duration = (
            f"{window.duration_seconds:.3f}s"
            if window.duration_seconds is not None
            else "unknown duration"
        )
        title = (
            f"Atomic service on {window.runner_id}\n"
            f"{window.start_time.isoformat()} - {window.end_time.isoformat()}\n"
            f"duration: {duration}\n"
            f"run: {window.atomic_service_run_id}"
        )
        start_iso = window.start_time.isoformat(timespec="microseconds")
        end_iso = window.end_time.isoformat(timespec="microseconds")
        duration_seconds = (
            "" if window.duration_seconds is None else str(window.duration_seconds)
        )
        label = "atomic service" if width >= 72 else "atomic"
        service_run_attr = (
            f' data-atomic-service-run-id="{escape(window.atomic_service_run_id)}"'
        )
        detail_url_attr = (
            f' data-detail-url="/runners/atomic-service/runs/'
            f'{escape(window.atomic_service_run_id)}"'
        )
        parts.append(
            f'    <g class="atomic-service-window" '
            f'data-atomic-service-id="{escape(window.window_id)}"'
            f"{service_run_attr}"
            f"{detail_url_attr} "
            f'data-runner-id="{escape(window.runner_id)}" '
            f'data-atomic-service="1" '
            f'data-start-time="{escape(start_iso)}" '
            f'data-end-time="{escape(end_iso)}" '
            f'data-duration-seconds="{escape(duration_seconds)}">'
            f"<title>{escape(title)}</title>"
            f'<rect x="{rect_x:.1f}" y="{y:.1f}" width="{width:.1f}" '
            f'height="{height}" rx="3" fill="{ATOMIC_SERVICE_FILL}" '
            f'opacity="{opacity:.2f}" stroke="{ATOMIC_SERVICE_STROKE}" '
            f'stroke-width="1" filter="url(#shadow)"/>'
            f"{_window_label(label, rect_x, y, width, height)}"
            "</g>"
        )
    parts.append("  </g>")
    return "\n".join(parts)


def assign_atomic_service_sub_lanes(
    data: TimelineData,
    windows: list[AtomicServiceWindow],
    trigger_runs: list[TriggerRunRecord] | None = None,
) -> list[AtomicServiceWindow]:
    """Place service windows without covering invocation activity."""
    anchored_point_keys = _atomic_service_registration_point_keys(
        data, windows, trigger_runs or []
    )
    occupancies: dict[str, list[list[TimeInterval]]] = {}
    for runner_id, lane in data.lanes.items():
        lane_occupancies: list[list[TimeInterval]] = [
            [] for _ in range(lane.max_sub_lane + 1)
        ]
        for segment in lane.segments:
            lane_occupancies[segment.sub_lane].append(
                TimeInterval(segment.start_time, segment.end_time)
            )
        for point in lane.points:
            if _point_key(runner_id, point) in anchored_point_keys:
                continue
            lane_occupancies[point.sub_lane].append(
                TimeInterval(point.timestamp, point.timestamp)
            )
        for bar in lane.bars:
            lane_occupancies[0].append(TimeInterval(bar.start_time, bar.end_time))
        occupancies[runner_id] = lane_occupancies

    assigned_by_id: dict[str, AtomicServiceWindow] = {}
    for window in sorted(windows, key=lambda item: item.start_time):
        window_occupancies = occupancies.get(window.runner_id)
        if window_occupancies is None:
            assigned_by_id[window.window_id] = window
            continue
        interval = _visible_window_interval(data, window)
        sub_lane = _first_available_sub_lane(window_occupancies, interval)
        data.reserve_auxiliary_sub_lane(window.runner_id, sub_lane)
        assigned_by_id[window.window_id] = replace(window, sub_lane=sub_lane)

    assigned = [assigned_by_id[window.window_id] for window in windows]
    _anchor_atomic_service_registration_points(data, assigned, trigger_runs or [])
    return assigned


def _atomic_service_registration_point_keys(
    data: TimelineData,
    windows: list[AtomicServiceWindow],
    trigger_runs: list[TriggerRunRecord],
) -> set[tuple[str, str, datetime]]:
    """Identify REGISTERED points created by the recorded service runs."""
    windows_by_run_id = {window.atomic_service_run_id: window for window in windows}
    keys: set[tuple[str, str, datetime]] = set()
    for trigger_run in trigger_runs:
        window = windows_by_run_id.get(trigger_run.atomic_service_run_id or "")
        invocation_id = trigger_run.triggered_invocation_id
        if window is None or invocation_id is None:
            continue
        lane = data.lanes.get(window.runner_id)
        if lane is None:
            continue
        for point in lane.points:
            if point.invocation_id == invocation_id and point.status == "REGISTERED":
                keys.add(_point_key(window.runner_id, point))
    return keys


def _point_key(
    runner_id: str,
    point: StatusPoint,
) -> tuple[str, str, datetime]:
    """Return the stable identity used while reallocating a status point."""
    return runner_id, point.invocation_id, point.timestamp


def _anchor_atomic_service_registration_points(
    data: TimelineData,
    windows: list[AtomicServiceWindow],
    trigger_runs: list[TriggerRunRecord],
) -> None:
    """Move trigger-created REGISTERED points onto their service window."""
    windows_by_run_id = {window.atomic_service_run_id: window for window in windows}
    invocation_anchors: dict[tuple[str, str], int] = {}
    for trigger_run in trigger_runs:
        window = windows_by_run_id.get(trigger_run.atomic_service_run_id or "")
        invocation_id = trigger_run.triggered_invocation_id
        if window is None or invocation_id is None:
            continue
        lane = data.lanes.get(window.runner_id)
        if lane is None:
            continue
        lane.points = [
            replace(point, sub_lane=window.sub_lane)
            if point.invocation_id == invocation_id and point.status == "REGISTERED"
            else point
            for point in lane.points
        ]
        invocation_anchors[(window.runner_id, invocation_id)] = window.sub_lane
        data.reserve_auxiliary_sub_lane(window.runner_id, window.sub_lane)

    if not invocation_anchors:
        return
    data.global_lines = [
        replace(
            line,
            from_sub_lane=invocation_anchors.get(
                (line.from_runner_id, line.invocation_id),
                line.from_sub_lane,
            ),
        )
        if line.from_status == "REGISTERED"
        else line
        for line in data.global_lines
    ]


def _first_available_sub_lane(
    occupancies: list[list[TimeInterval]],
    interval: TimeInterval,
) -> int:
    """Reserve and return the first sub-lane that fits an interval."""
    for sub_lane, occupied_intervals in enumerate(occupancies):
        if not any(occupied.overlaps(interval) for occupied in occupied_intervals):
            occupied_intervals.append(interval)
            return sub_lane
    occupancies.append([interval])
    return len(occupancies) - 1


def _visible_window_interval(
    data: TimelineData,
    window: AtomicServiceWindow,
) -> TimeInterval:
    """Return the time occupied by the service window's rendered width."""
    minimum_seconds = (
        data.bounds.duration_seconds
        * ATOMIC_SERVICE_MIN_WIDTH_PX
        / data.config.content_width
    )
    visible_end = max(
        window.end_time,
        window.start_time + timedelta(seconds=minimum_seconds),
    )
    return TimeInterval(window.start_time, visible_end)


def _window_label(label: str, x: float, y: float, width: float, height: int) -> str:
    """Return a compact label when the service window has enough width."""
    if width < 38:
        return ""
    return (
        f'<text x="{x + 4:.1f}" y="{y + height - 4:.1f}" '
        f'fill="#7c2d12" font-size="9px" font-weight="700">{escape(label)}</text>'
    )
