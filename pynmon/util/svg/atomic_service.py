"""Atomic-service overlays for the invocation timeline SVG."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html import escape
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynmon.util.svg.renderer import SVGStyle
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
        y = lane_y
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


def _window_label(label: str, x: float, y: float, width: float, height: int) -> str:
    """Return a compact label when the service window has enough width."""
    if width < 38:
        return ""
    return (
        f'<text x="{x + 4:.1f}" y="{y + height - 4:.1f}" '
        f'fill="#7c2d12" font-size="9px" font-weight="700">{escape(label)}</text>'
    )
