"""
Lane background, header, and label rendering for the SVG timeline.

Pure functions: receive TimelineData + SVGStyle, return SVG string fragments.
"""

from typing import TYPE_CHECKING
from xml.sax.saxutils import escape

if TYPE_CHECKING:
    from pynmon.util.svg.renderer import SVGStyle
    from pynmon.util.svg.timeline_data import TimelineData


def render_group_containers(data: "TimelineData", style: "SVGStyle") -> str:
    """Placeholder — group visual containers are handled via lane backgrounds."""
    return ""


def render_group_headers(data: "TimelineData", style: "SVGStyle") -> str:
    """Group header labels for hierarchical runner groups."""
    parts: list[str] = []
    for group in data.get_sorted_groups():
        lanes = data.get_lanes_for_group(group.group_id)
        if len(lanes) <= 1:
            continue
        y_start, _, _ = data.get_group_bounds(group.group_id)
        color = lanes[0].color if lanes else "#6c757d"
        header_y = y_start + 12
        parts += [
            f'  <text x="14" y="{header_y}" fill="{color}" font-weight="600" '
            f'font-size="{style.font_size}px">{escape(group.header_label)}</text>',
            f'  <text x="14" y="{header_y + 12}" fill="{style.text_color}" '
            f'font-size="{style.font_size - 3}px">{escape(group.details_line)}</text>',
        ]
    return "\n".join(parts)


def render_lane_backgrounds(data: "TimelineData", style: "SVGStyle") -> str:
    """Alternating tinted lane background stripes."""
    config = data.config
    parts: list[str] = []
    for lane in data.get_sorted_lanes():
        y = data.lane_y_position(lane)
        h = lane.lane_height(config)
        opacity = 0.08 if lane.lane_index % 2 == 0 else 0.15
        parts += [
            f'  <rect x="0" y="{y}" width="{config.width}" height="{h}" fill="#ffffff"/>',
            f'  <rect x="0" y="{y}" width="{config.width}" height="{h}" '
            f'fill="{lane.color}" opacity="{opacity}"/>',
        ]
    return "\n".join(parts)


def render_lane_labels(data: "TimelineData", style: "SVGStyle") -> str:
    """Lane labels on the left side."""
    config = data.config
    parts: list[str] = []
    for lane in data.get_sorted_lanes():
        h = lane.lane_height(config)
        y = data.lane_y_position(lane) + h // 2
        group = data.groups.get(lane.group_id) if lane.group_id else None
        if (
            lane.is_group_header
            and group
            and len(data.get_lanes_for_group(lane.group_id)) > 1
        ):
            continue  # header already rendered by render_group_headers
        parts.extend(_lane_label_parts(lane, y, group, data, style))
    return "\n".join(parts)


def _lane_label_parts(lane, y, group, data, style) -> list[str]:  # type: ignore[no-untyped-def]
    """Return SVG text elements for one lane label."""
    fs = style.font_size
    if lane.is_group_header:
        return [
            f'  <text x="8" y="{y - 4}" fill="{style.label_color}" font-weight="600" '
            f'font-size="{fs}px"><tspan>{escape(lane.label)}</tspan></text>',
            f'  <text x="8" y="{y + 8}" fill="{style.text_color}" opacity="0.7" '
            f'font-size="{fs - 2}px">{escape(lane.details_line)}</text>',
        ]
    if lane.is_child_lane and group:
        child_label = f"{lane.runner_cls}({lane.display_runner_id})"
        diff = lane.format_child_details(group.hostname, group.pid, group.thread_id)
        parts = [
            f'  <text x="20" y="{y - 4}" fill="{style.label_color}" font-weight="400" '
            f'font-size="{fs - 1}px"><tspan>{escape(child_label)}</tspan></text>',
        ]
        if diff:
            parts.append(
                f'  <text x="24" y="{y + 8}" fill="{style.text_color}" opacity="0.7" '
                f'font-size="{fs - 2}px">{escape(diff.strip(" ()"))}</text>'
            )
        return parts
    return [
        f'  <text x="8" y="{y - 4}" fill="{style.label_color}" font-weight="500" '
        f'font-size="{fs}px"><tspan>{escape(lane.label)}</tspan></text>',
        f'  <text x="8" y="{y + 8}" fill="{style.text_color}" opacity="0.7" '
        f'font-size="{fs - 2}px">{escape(lane.details_line)}</text>',
    ]
