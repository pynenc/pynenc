"""
Axis, grid, and legend rendering functions for the SVG timeline.

All functions are pure: they receive TimelineData + SVGStyle and return
an SVG string fragment. No side effects or instance state.
"""

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from xml.sax.saxutils import escape

from pynmon.util.status_colors import STATUS_COLORS

if TYPE_CHECKING:
    from pynmon.util.svg.renderer import SVGStyle
    from pynmon.util.svg.timeline_data import TimelineData
    from pynmon.util.svg.bounds import TimelineBounds


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
    """Status color legend at the bottom of the timeline."""
    config = data.config
    lanes = data.get_sorted_lanes()
    y = (
        config.top_margin
        + sum(lane.lane_height(config) + config.lane_padding for lane in lanes)
        + 15
    )
    x = config.left_margin
    parts: list[str] = []
    for status, color in STATUS_COLORS.items():
        parts += [
            f'  <rect x="{x}" y="{y}" width="12" height="12" fill="{color}" rx="2"/>',
            f'  <text x="{x + 16}" y="{y + 10}" fill="{style.text_color}" '
            f'font-size="{style.font_size - 2}px">{escape(status)}</text>',
        ]
        x += len(status) * 7 + 30
        if x > config.width - 100:
            x = config.left_margin
            y += 18
    return "\n".join(parts)


def _tick_positions(bounds: "TimelineBounds") -> list[datetime]:
    """Calculate evenly-spaced tick positions for the given time range."""
    dur = bounds.duration_seconds
    cfg = bounds.config
    if cfg.resolution_seconds is not None:
        interval = cfg.resolution_seconds
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
    ticks: list[datetime] = []
    current = bounds.start_time
    while current <= bounds.end_time:
        ticks.append(current)
        current += timedelta(seconds=interval)
    return ticks


def _format_tick_label(t: datetime, bounds: "TimelineBounds") -> str:
    """Format a tick timestamp based on the total visible duration."""
    dur = bounds.duration_seconds
    utc = t.astimezone(UTC) if t.tzinfo else t
    if dur <= 10:
        ms = utc.microsecond // 1000
        return f"{utc.strftime('%H:%M:%S')}.{ms:03d}"
    if dur <= 3600:
        return utc.strftime("%H:%M:%S")
    if dur <= 86400:
        return utc.strftime("%H:%M")
    return utc.strftime("%m/%d %H:%M")
