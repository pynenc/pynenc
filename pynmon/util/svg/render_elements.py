"""
Bar, segment, point, and line rendering for the SVG timeline.

Pure functions: receive TimelineData + SVGStyle, return SVG string fragments.
"""

from typing import TYPE_CHECKING
from xml.sax.saxutils import escape

if TYPE_CHECKING:
    from pynmon.util.svg.bounds import TimelineBounds
    from pynmon.util.svg.config import TimelineConfig
    from pynmon.util.svg.renderer import SVGStyle
    from pynmon.util.svg.status_elements import (
        InvocationBar,
        StatusLine,
        StatusPoint,
        StatusSegment,
    )
    from pynmon.util.svg.timeline_data import TimelineData


def render_bars(data: "TimelineData", style: "SVGStyle") -> str:
    """Render legacy InvocationBar elements."""
    config, bounds = data.config, data.bounds
    parts: list[str] = []
    for lane in data.get_sorted_lanes():
        lane_y = data.lane_y_position(lane) + config.bar_y_offset
        for bar in lane.bars:
            parts.append(_bar_svg(bar, bounds, lane_y, config, style))
    return "\n".join(parts)


def render_segments(data: "TimelineData", style: "SVGStyle") -> str:
    """Render StatusSegment bars (worker-occupying statuses)."""
    config, bounds = data.config, data.bounds
    parts: list[str] = []
    for lane in data.get_sorted_lanes():
        lane_y = data.lane_y_position(lane) + config.bar_y_offset
        for seg in lane.segments:
            parts.append(_segment_svg(seg, bounds, lane_y, config, style))
    return "\n".join(parts)


def render_points(data: "TimelineData", style: "SVGStyle") -> str:
    """Render StatusPoint circles for all status changes."""
    config, bounds = data.config, data.bounds
    parts: list[str] = []
    for lane in data.get_sorted_lanes():
        base_y = (
            data.lane_y_position(lane) + config.bar_y_offset + config.bar_height // 2
        )
        for pt in lane.points:
            parts.append(_point_svg(pt, bounds, base_y, config, style))
    return "\n".join(parts)


def render_lines(data: "TimelineData", style: "SVGStyle") -> str:
    """Render StatusLine connectors between status points."""
    bounds = data.bounds
    parts: list[str] = []
    for line in data.global_lines:
        svg = _line_svg(line, bounds, data, style)
        if svg:
            parts.append(svg)
    return "\n".join(parts)


def _bar_svg(
    bar: "InvocationBar",
    bounds: "TimelineBounds",
    lane_y: int,
    config: "TimelineConfig",
    style: "SVGStyle",
) -> str:
    x = bounds.time_to_x(bar.start_time)
    w = bounds.duration_to_width(bar.duration_seconds)
    return (
        f'  <g class="invocation-bar">'
        f'<rect x="{x:.1f}" y="{lane_y}" width="{w:.1f}" height="{config.bar_height}" '
        f'fill="{bar.color}" rx="{style.bar_rx}" opacity="{style.bar_opacity}" filter="url(#shadow)" '
        f'data-invocation-id="{escape(bar.invocation_id)}">'
        f"<title>{escape(bar.tooltip)}</title></rect></g>"
    )


def _segment_svg(
    seg: "StatusSegment",
    bounds: "TimelineBounds",
    lane_y: int,
    config: "TimelineConfig",
    style: "SVGStyle",
) -> str:
    x = bounds.time_to_x(seg.start_time)
    w = bounds.duration_to_width(seg.duration_seconds)
    y = lane_y + seg.sub_lane * (config.bar_height + 2)
    base = (
        f'  <g class="status-segment">'
        f'<rect x="{x:.1f}" y="{y}" width="{w:.1f}" height="{config.bar_height}" '
        f'fill="{seg.color}" rx="{style.bar_rx}" opacity="{style.bar_opacity}" filter="url(#shadow)" '
        f'data-invocation-id="{escape(seg.invocation_id)}" data-status="{escape(seg.status)}">'
        f"<title>{escape(seg.tooltip)}</title></rect>"
    )
    if seg.is_ongoing:
        base += (
            f'<rect x="{x:.1f}" y="{y}" width="{w:.1f}" height="{config.bar_height}" '
            f'fill="url(#ongoing-stripes)" rx="{style.bar_rx}"/>'
        )
    return base + "</g>"


def _point_svg(
    pt: "StatusPoint",
    bounds: "TimelineBounds",
    base_y: int,
    config: "TimelineConfig",
    style: "SVGStyle",
) -> str:
    x = bounds.time_to_x(pt.timestamp)
    y = base_y + pt.sub_lane * (config.bar_height + 2)
    return (
        f'  <g class="status-point">'
        f'<circle cx="{x:.1f}" cy="{y}" r="{style.point_radius}" '
        f'fill="{pt.color}" stroke="#ffffff" stroke-width="1.5" '
        f'data-invocation-id="{escape(pt.invocation_id)}" data-status="{escape(pt.status)}">'
        f"<title>{escape(pt.tooltip)}</title></circle></g>"
    )


def _line_svg(
    line: "StatusLine",
    bounds: "TimelineBounds",
    data: "TimelineData",
    style: "SVGStyle",
) -> str:
    from_lane = data.lanes.get(line.from_runner_id)
    to_lane = data.lanes.get(line.to_runner_id)
    if not from_lane or not to_lane:
        return ""
    config = data.config
    sub_h = config.bar_height + 2
    from_base = (
        data.lane_y_position(from_lane) + config.bar_y_offset + config.bar_height // 2
    )
    to_base = (
        data.lane_y_position(to_lane) + config.bar_y_offset + config.bar_height // 2
    )
    x1, x2 = bounds.time_to_x(line.start_time), bounds.time_to_x(line.end_time)
    y1 = from_base + line.from_sub_lane * sub_h
    y2 = to_base + line.to_sub_lane * sub_h
    return (
        f'  <line x1="{x1:.1f}" y1="{y1}" x2="{x2:.1f}" y2="{y2}" '
        f'stroke="{line.color}" stroke-width="{style.line_width}" '
        f'opacity="{style.line_opacity}" stroke-dasharray="4,2" '
        f'data-invocation-id="{escape(line.invocation_id)}"/>'
    )
