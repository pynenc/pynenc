"""
Axis, grid, and legend rendering functions for the SVG timeline.

All functions are pure: they receive TimelineData + SVGStyle and return
an SVG string fragment. No side effects or instance state.
"""

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from xml.sax.saxutils import escape

from pynmon.util.status_colors import STATUS_COLORS
from pynmon.util.svg.atomic_service import (
    ATOMIC_SERVICE_FILL,
    ATOMIC_SERVICE_LABEL,
    ATOMIC_SERVICE_STROKE,
)
from pynmon.util.svg.event_markers import (
    CONDITION_TYPE_COLORS,
    EVENT_MARKER_COLORS,
    RELATION_LINE_COLORS,
    event_row_height,
)

if TYPE_CHECKING:
    from pynmon.util.svg.bounds import TimelineBounds
    from pynmon.util.svg.config import TimelineConfig
    from pynmon.util.svg.renderer import SVGStyle
    from pynmon.util.svg.timeline_data import TimelineData


_MIN_TICK_SPACING_PX = 110
_LEGEND_TOP_GAP = 9
_LEGEND_ROW_GAP = 17
_LEGEND_BOTTOM_PADDING = 60
_STATUS_LEGEND_WRAP_GAP = 18
_STATUS_LEGEND_RIGHT_GUARD = 120
_LEGEND_RIGHT_GUARD = 8
_NICE_INTERVAL_SECONDS = (
    0.1,
    0.25,
    0.5,
    1,
    2,
    5,
    10,
    15,
    30,
    60,
    120,
    300,
    600,
    900,
    1800,
    3600,
)

_EVENT_LEGEND_ITEMS = (
    ("triggered", EVENT_MARKER_COLORS["triggered"]),
    ("matched only", EVENT_MARKER_COLORS["matched"]),
    ("unmatched", EVENT_MARKER_COLORS["unmatched"]),
)
_CONDITION_LEGEND_ITEMS = (
    ("event", CONDITION_TYPE_COLORS["EventContext"]),
    ("status", CONDITION_TYPE_COLORS["StatusContext"]),
    ("result", CONDITION_TYPE_COLORS["ResultContext"]),
    ("exception", CONDITION_TYPE_COLORS["ExceptionContext"]),
    ("cron", CONDITION_TYPE_COLORS["CronContext"]),
)
_RELATION_LEGEND_ITEMS = (
    ("direct task relation", RELATION_LINE_COLORS["direct_call"], "2,3"),
    ("event origin", RELATION_LINE_COLORS["event_origin"], "6,3"),
    ("event trigger", RELATION_LINE_COLORS["event_trigger"], "6,3"),
    ("status trigger", RELATION_LINE_COLORS["status_trigger"], "6,3"),
    ("result trigger", RELATION_LINE_COLORS["result_trigger"], "6,3"),
    ("exception trigger", RELATION_LINE_COLORS["exception_trigger"], "6,3"),
    ("cron trigger", RELATION_LINE_COLORS["cron_trigger"], "6,3"),
)


def legend_strip_height(config: "TimelineConfig") -> int:
    """Height reserved below the plot for the timeline legend."""
    y = _LEGEND_TOP_GAP
    y = _status_legend_end_y(config, y)
    y += _LEGEND_ROW_GAP  # Runner
    y += _LEGEND_ROW_GAP  # Events
    y = _wrapped_legend_end_y(
        config,
        y + _LEGEND_ROW_GAP,
        [len(label) * 7 + 36 for label, _color in _CONDITION_LEGEND_ITEMS],
    )
    y = _wrapped_legend_end_y(
        config,
        y + _LEGEND_ROW_GAP,
        [len(label) * 7 + 62 for label, _color, _dash in _RELATION_LEGEND_ITEMS],
    )
    return y + _LEGEND_BOTTOM_PADDING


def _status_legend_end_y(config: "TimelineConfig", y: int) -> int:
    x = config.left_margin
    for status in STATUS_COLORS:
        x += len(status) * 7 + 28
        if x > config.width - _STATUS_LEGEND_RIGHT_GUARD:
            x = config.left_margin
            y += _STATUS_LEGEND_WRAP_GAP
    return y


def _wrapped_legend_end_y(
    config: "TimelineConfig", y: int, item_widths: list[int]
) -> int:
    x = config.left_margin
    for item_width in item_widths:
        if (
            x > config.left_margin
            and x + item_width > config.width - _LEGEND_RIGHT_GUARD
        ):
            x = config.left_margin
            y += _LEGEND_ROW_GAP
        x += item_width
    return y


def render_grid(data: "TimelineData", style: "SVGStyle") -> str:
    """Vertical dashed grid lines at tick positions."""
    bounds = data.bounds
    ticks = _tick_positions(bounds)
    top, bottom = data.config.top_margin, data.total_height
    lines = [
        f'  <line x1="{bounds.time_to_x(t):.1f}" y1="{top}" '
        f'x2="{bounds.time_to_x(t):.1f}" y2="{bottom}" '
        f'stroke="{style.grid_color}" stroke-width="1" stroke-dasharray="4,4"/>'
        for t in ticks
    ]
    return "\n".join(lines)


def render_time_axis(data: "TimelineData", style: "SVGStyle") -> str:
    """Horizontal axis line with tick marks and time labels."""
    bounds, config = data.bounds, data.config
    axis_y = config.top_margin - 5
    parts = [
        f'  <line x1="{config.left_margin}" y1="{axis_y}" '
        f'x2="{config.width}" y2="{axis_y}" '
        f'stroke="{style.axis_color}" stroke-width="1"/>',
    ]
    for tick in _tick_positions(bounds):
        x = bounds.time_to_x(tick)
        label = _format_tick_label(tick, bounds)
        parts += [
            f'  <line x1="{x:.1f}" y1="{axis_y}" x2="{x:.1f}" y2="{axis_y - 5}" '
            f'stroke="{style.axis_color}" stroke-width="1"/>',
            f'  <text x="{x:.1f}" y="{axis_y - 10}" text-anchor="middle" '
            f'fill="{style.text_color}" font-size="{style.font_size - 1}px">{escape(label)}</text>',
        ]
    return "\n".join(parts)


def render_legend(data: "TimelineData", style: "SVGStyle") -> str:
    """Status, event marker, and relation color legend at the bottom.

    Status markers are rendered as circles to match the shape they take on
    the timeline itself. The legend is laid out beneath the dedicated
    event-markers row (if any).
    """
    config = data.config
    y = _legend_top_y(data)
    parts: list[str] = []
    y = _render_status_legend(parts, config, style, y)
    y = _render_runner_overlay_legend(parts, config, style, y + _LEGEND_ROW_GAP)
    y = _render_events_legend(parts, config, style, y + _LEGEND_ROW_GAP)
    y = _render_conditions_legend(parts, config, style, y + _LEGEND_ROW_GAP)
    _render_relations_legend(parts, config, style, y + _LEGEND_ROW_GAP)
    return "\n".join(parts)


def _legend_top_y(data: "TimelineData") -> int:
    """First baseline y for the legend, below the lanes and event-marker row."""
    config = data.config
    lanes = data.get_sorted_lanes()
    if lanes:
        last_lane = lanes[-1]
        plot_bottom = data.lane_y_position(last_lane) + last_lane.lane_height(config)
    else:
        plot_bottom = config.top_margin + config.lane_height
    return plot_bottom + event_row_height(data) + _LEGEND_TOP_GAP


def _legend_section_label(x: int, y: int, text: str, style: "SVGStyle") -> str:
    return (
        f'  <text x="{x - 8}" y="{y + 10}" text-anchor="end" '
        f'fill="{style.label_color}" font-size="{style.font_size - 2}px" '
        f'font-weight="600">{escape(text)}</text>'
    )


def _legend_item_text(x: int, y: int, text: str, style: "SVGStyle") -> str:
    return (
        f'  <text x="{x + 16}" y="{y + 10}" fill="{style.text_color}" '
        f'font-size="{style.font_size - 2}px">{escape(text)}</text>'
    )


def _render_status_legend(
    parts: list[str], config: "TimelineConfig", style: "SVGStyle", y: int
) -> int:
    """Render the Status row; may wrap to additional rows. Returns new y."""
    x = config.left_margin
    parts.append(_legend_section_label(x, y, "Status", style))
    for status, color in STATUS_COLORS.items():
        parts += [
            f'  <circle cx="{x + 6}" cy="{y + 6}" r="6" fill="{color}" '
            f'stroke="white" stroke-width="1"/>',
            _legend_item_text(x, y, status, style),
        ]
        x += len(status) * 7 + 28
        if x > config.width - _STATUS_LEGEND_RIGHT_GUARD:
            x = config.left_margin
            y += _STATUS_LEGEND_WRAP_GAP
    return y


def _render_events_legend(
    parts: list[str], config: "TimelineConfig", style: "SVGStyle", y: int
) -> int:
    """Render the Events row (single line, no wrap). Returns new y."""
    x = config.left_margin
    parts.append(_legend_section_label(x, y, "Events", style))
    for label, color in _EVENT_LEGEND_ITEMS:
        parts += [
            f'  <rect x="{x}" y="{y + 1}" width="10" height="10" fill="{color}" '
            f'rx="1.5" stroke="white" stroke-width="1"/>',
            _legend_item_text(x, y, label, style),
        ]
        x += len(label) * 7 + 34
    return y


def _render_runner_overlay_legend(
    parts: list[str], config: "TimelineConfig", style: "SVGStyle", y: int
) -> int:
    """Render the Runner overlay row (atomic service swatch).

    The atomic-service bars on the timeline are not events; they are the
    runner's periodic trigger loop. They live on their own legend row so
    the Events row stays focused on event markers proper.
    """
    x = config.left_margin
    parts.append(_legend_section_label(x, y, "Runner", style))
    label = ATOMIC_SERVICE_LABEL
    parts += [
        f'  <rect x="{x}" y="{y + 1}" width="10" height="10" '
        f'fill="{ATOMIC_SERVICE_FILL}" rx="1.5" stroke="{ATOMIC_SERVICE_STROKE}" '
        f'stroke-width="1"/>',
        _legend_item_text(x, y, label, style),
    ]
    return y


def _render_conditions_legend(
    parts: list[str], config: "TimelineConfig", style: "SVGStyle", y: int
) -> int:
    """Render the Condition row with auto-wrap. Returns new y."""
    x = config.left_margin
    parts.append(_legend_section_label(x, y, "Condition", style))
    for label, color in _CONDITION_LEGEND_ITEMS:
        item_width = len(label) * 7 + 36
        if (
            x > config.left_margin
            and x + item_width > config.width - _LEGEND_RIGHT_GUARD
        ):
            x = config.left_margin
            y += _LEGEND_ROW_GAP
        parts += [
            f'  <rect x="{x}" y="{y + 1}" width="10" height="10" fill="{color}" '
            f'rx="1.5" stroke="white" stroke-width="1"/>',
            _legend_item_text(x, y, label, style),
        ]
        x += item_width
    return y


def _render_relations_legend(
    parts: list[str], config: "TimelineConfig", style: "SVGStyle", y: int
) -> int:
    """Render the Relations row (dashed lines) with auto-wrap. Returns new y."""
    x = config.left_margin
    parts.append(_legend_section_label(x, y, "Relations", style))
    for label, color, dash in _RELATION_LEGEND_ITEMS:
        item_width = len(label) * 7 + 62
        if (
            x > config.left_margin
            and x + item_width > config.width - _LEGEND_RIGHT_GUARD
        ):
            x = config.left_margin
            y += _LEGEND_ROW_GAP
        parts += [
            f'  <line x1="{x}" y1="{y + 6}" x2="{x + 24}" y2="{y + 6}" '
            f'stroke="{color}" stroke-width="2" stroke-dasharray="{dash}"/>',
            f'  <text x="{x + 30}" y="{y + 10}" fill="{style.text_color}" '
            f'font-size="{style.font_size - 2}px">{escape(label)}</text>',
        ]
        x += item_width
    return y


def _tick_positions(bounds: "TimelineBounds") -> list[datetime]:
    """Calculate evenly-spaced tick positions for the given time range."""
    dur = bounds.duration_seconds
    cfg = bounds.config
    if cfg.resolution_seconds is not None:
        interval = _nice_interval_at_least(
            max(cfg.resolution_seconds, _minimum_readable_interval(bounds))
        )
    elif dur <= 5:
        interval = 1
    elif dur <= 30:
        interval = 5
    elif dur <= 120:
        interval = 15
    elif dur <= 600:
        interval = 60
    elif dur <= 3600:
        interval = 300
    elif dur <= 21600:
        interval = 1800
    else:
        interval = 3600
    if interval <= 0:
        return [bounds.start_time]
    ticks: list[datetime] = []
    current = bounds.start_time
    while current <= bounds.end_time:
        ticks.append(current)
        current += timedelta(seconds=interval)
    return ticks


def _minimum_readable_interval(bounds: "TimelineBounds") -> float:
    """Return the smallest tick interval that keeps labels visually separated."""
    dur = bounds.duration_seconds
    if dur <= 0:
        return 0
    content_width = max(bounds.config.width - bounds.config.left_margin, 1)
    max_ticks = max(content_width / _MIN_TICK_SPACING_PX, 1)
    return dur / max_ticks


def _nice_interval_at_least(seconds: float) -> float:
    """Round an interval up to a familiar timeline tick value."""
    for interval in _NICE_INTERVAL_SECONDS:
        if interval >= seconds:
            return interval
    return seconds


def _format_tick_label(t: datetime, bounds: "TimelineBounds") -> str:
    """Format a tick timestamp based on the total visible duration."""
    dur = bounds.duration_seconds
    utc = t.astimezone(UTC) if t.tzinfo else t
    if dur <= 10 or (
        bounds.config.resolution_seconds is not None
        and bounds.config.resolution_seconds < 1
        and dur <= 120
    ):
        ms = utc.microsecond // 1000
        return f"{utc.strftime('%H:%M:%S')}.{ms:03d}"
    if dur <= 3600:
        return utc.strftime("%H:%M:%S")
    if dur <= 86400:
        return utc.strftime("%H:%M")
    return utc.strftime("%m/%d %H:%M")
