"""
SVG renderer for timeline visualization.

Converts TimelineData into SVG markup. Responsible for generating the visual
representation including lanes, points, segments, lines, time axis, and legend.

Key components:
- TimelineSVGRenderer: Main renderer class
- SVG generation for status points (circles)
- SVG generation for status segments (bars for RUNNING, PENDING, etc.)
- SVG generation for connecting lines between status changes
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from xml.sax.saxutils import escape

from pynmon.util.svg.models import (
    InvocationBar,
    StatusLine,
    StatusPoint,
    StatusSegment,
    TimelineBounds,
    TimelineConfig,
    TimelineData,
)
from pynmon.util.status_colors import STATUS_COLORS


@dataclass(frozen=True)
class SVGStyle:
    """
    Style configuration for SVG elements.

    :param str font_family: Font family for text elements
    :param int font_size: Base font size in pixels
    :param str background_color: SVG background color
    :param str lane_background: Lane background color
    :param str axis_color: Time axis line color
    :param str grid_color: Grid line color
    :param str text_color: Text color
    :param str label_color: Lane label color
    :param float bar_opacity: Opacity for invocation bars
    :param int bar_rx: Border radius for bars
    :param int point_radius: Radius for status point circles
    :param float line_opacity: Opacity for connecting lines
    :param int line_width: Stroke width for connecting lines
    """

    font_family: str = "system-ui, -apple-system, sans-serif"
    font_size: int = 12
    background_color: str = "#ffffff"
    lane_background: str = "#f8f9fa"
    lane_alt_background: str = "#ffffff"
    axis_color: str = "#343a40"
    grid_color: str = "#dee2e6"
    text_color: str = "#212529"
    label_color: str = "#495057"
    bar_opacity: float = 0.9
    bar_rx: int = 3
    point_radius: int = 5
    line_opacity: float = 0.6
    line_width: int = 2


class TimelineSVGRenderer:
    """
    Renders TimelineData as SVG markup.

    Produces a complete SVG document with:
    - Background and lane stripes
    - Time axis with tick marks
    - Runner lane labels
    - Invocation bars with tooltips
    - Status legend

    Usage:
        renderer = TimelineSVGRenderer()
        svg_string = renderer.render(timeline_data)
    """

    def __init__(self, style: SVGStyle | None = None) -> None:
        """
        Initialize the renderer.

        :param SVGStyle | None style: Style configuration for rendering
        """
        self.style = style or SVGStyle()

    def render(self, data: TimelineData) -> str:
        """
        Render TimelineData to SVG string.

        :param TimelineData data: Complete timeline data
        :return: SVG markup as string
        """
        config = data.config
        width = config.width
        height = data.total_height

        parts: list[str] = []

        # SVG header
        parts.append(self._render_header(width, height))

        # Defs (for filters, gradients, patterns)
        parts.append(self._render_defs(data))

        # Background
        parts.append(self._render_background(width, height))

        # Group containers (parent wrapper rectangles) - drawn first as background
        parts.append(self._render_group_containers(data))

        # Lane backgrounds (alternating stripes)
        parts.append(self._render_lane_backgrounds(data))

        # Grid lines
        parts.append(self._render_grid(data))

        # Time axis
        parts.append(self._render_time_axis(data))

        # Group headers (parent labels on left side)
        parts.append(self._render_group_headers(data))

        # Lane labels
        parts.append(self._render_lane_labels(data))

        # Status segments (worker-occupying status bars) - drawn first
        parts.append(self._render_segments(data))

        # Legacy bars (for backwards compatibility)
        parts.append(self._render_bars(data))

        # Connecting lines (drawn on top of segments for visibility)
        parts.append(self._render_lines(data))

        # Status points (circles for all status changes) - drawn last
        parts.append(self._render_points(data))

        # Legend
        parts.append(self._render_legend(data))

        # Close SVG
        parts.append("</svg>")

        return "\n".join(parts)

    def _render_header(self, width: int, height: int) -> str:
        """Render SVG header with viewBox for responsive scaling."""
        # Use 100% width without explicit height so the browser derives height
        # from the viewBox aspect ratio. Setting a fixed height in pixels while
        # width="100%" causes the content to scale down (preserveAspectRatio meet)
        # but the SVG element stays the full pixel height, creating blank space
        # proportional to (1 - container_width / viewBox_width) * height.
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" '
            f'width="100%" '
            f'viewBox="0 0 {width} {height}" '
            f'preserveAspectRatio="xMinYMin meet" '
            f'style="font-family: {self.style.font_family}; font-size: {self.style.font_size}px;">'
        )

    def _render_defs(self, data: TimelineData) -> str:
        """Render SVG defs section with reusable elements."""
        left_margin = data.config.left_margin
        return f"""  <defs>
    <filter id="shadow" x="-10%" y="-10%" width="120%" height="120%">
      <feDropShadow dx="0" dy="1" stdDeviation="1" flood-opacity="0.15"/>
    </filter>
    <clipPath id="label-clip">
      <rect x="0" y="0" width="{left_margin - 10}" height="100%"/>
    </clipPath>
    <pattern id="ongoing-stripes" patternUnits="userSpaceOnUse" width="8" height="8" patternTransform="rotate(45)">
      <line x1="0" y1="0" x2="0" y2="8" stroke="rgba(255,255,255,0.3)" stroke-width="4"/>
    </pattern>
  </defs>"""

    def _render_background(self, width: int, height: int) -> str:
        """Render SVG background rectangle."""
        return (
            f'  <rect x="0" y="0" width="{width}" height="{height}" '
            f'fill="{self.style.background_color}"/>'
        )

    def _render_group_containers(self, data: TimelineData) -> str:
        """
        Render group container rectangles that wrap child lanes.

        Groups with multiple lanes (parent + children) get a visual container
        showing the hierarchical relationship.
        """
        lines: list[str] = []

        for group in data.get_sorted_groups():
            group_lanes = data.get_lanes_for_group(group.group_id)
            if len(group_lanes) <= 1:
                # Skip groups with only one lane (no hierarchy to show)
                continue

            y_start, y_end, total_height = data.get_group_bounds(group.group_id)
            if total_height <= 0:
                continue

            # No additional container rendering needed - lane backgrounds provide the color

        return "\n".join(lines)

    def _render_group_headers(self, data: TimelineData) -> str:
        """
        Render group header labels for hierarchical groups.

        Shows parent runner info (hostname, pid, runner class) at the top of the group.
        """
        lines: list[str] = []

        for group in data.get_sorted_groups():
            group_lanes = data.get_lanes_for_group(group.group_id)
            if len(group_lanes) <= 1:
                # Skip groups with only one lane - header shown inline with lane label
                continue

            y_start, _, total_height = data.get_group_bounds(group.group_id)
            if total_height <= 0:
                continue

            color = group_lanes[0].color if group_lanes else "#6c757d"

            # Render group header at top-left of the container
            header_y = y_start + 12

            # Parent runner info - compact header
            lines.append(
                f'  <text x="14" y="{header_y}" '
                f'fill="{color}" font-weight="600" '
                f'font-size="{self.style.font_size}px">'
                f"{escape(group.header_label)}</text>"
            )

            # Details line (hostname, pid) - smaller, below header
            lines.append(
                f'  <text x="14" y="{header_y + 12}" '
                f'fill="{self.style.text_color}" '
                f'font-size="{self.style.font_size - 3}px">'
                f"{escape(group.details_line)}</text>"
            )

        return "\n".join(lines)

    def _render_lane_backgrounds(self, data: TimelineData) -> str:
        """Render alternating lane background stripes with subtle runner colors."""
        lines: list[str] = []
        config = data.config

        for lane in data.get_sorted_lanes():
            y = data.lane_y_position(lane)
            lane_h = lane.lane_height(config)
            # Use subtle tinted background based on runner color
            # Even lanes are lighter (0.08 opacity), odd lanes are slightly darker (0.15 opacity)
            opacity = 0.08 if lane.lane_index % 2 == 0 else 0.15
            # Render full-width background (including label area) with white base
            lines.append(
                f'  <rect x="0" y="{y}" '
                f'width="{config.width}" height="{lane_h}" '
                f'fill="#ffffff"/>'
            )
            # Tinted overlay across full width
            lines.append(
                f'  <rect x="0" y="{y}" '
                f'width="{config.width}" height="{lane_h}" '
                f'fill="{lane.color}" opacity="{opacity}"/>'
            )

        return "\n".join(lines)

    def _render_grid(self, data: TimelineData) -> str:
        """Render vertical grid lines at time intervals."""
        lines: list[str] = []
        config = data.config
        bounds = data.bounds

        # Calculate appropriate tick interval based on duration
        tick_positions = self._calculate_tick_positions(bounds)

        content_top = config.top_margin
        content_bottom = data.total_height

        for tick_time in tick_positions:
            x = bounds.time_to_x(tick_time)
            lines.append(
                f'  <line x1="{x:.1f}" y1="{content_top}" '
                f'x2="{x:.1f}" y2="{content_bottom}" '
                f'stroke="{self.style.grid_color}" stroke-width="1" stroke-dasharray="4,4"/>'
            )

        return "\n".join(lines)

    def _render_time_axis(self, data: TimelineData) -> str:
        """Render time axis with labels."""
        lines: list[str] = []
        config = data.config
        bounds = data.bounds

        # Axis line
        axis_y = config.top_margin - 5
        lines.append(
            f'  <line x1="{config.left_margin}" y1="{axis_y}" '
            f'x2="{config.width}" y2="{axis_y}" '
            f'stroke="{self.style.axis_color}" stroke-width="1"/>'
        )

        # Tick marks and labels
        tick_positions = self._calculate_tick_positions(bounds)
        for tick_time in tick_positions:
            x = bounds.time_to_x(tick_time)
            # Tick mark
            lines.append(
                f'  <line x1="{x:.1f}" y1="{axis_y}" '
                f'x2="{x:.1f}" y2="{axis_y - 5}" '
                f'stroke="{self.style.axis_color}" stroke-width="1"/>'
            )
            # Label
            label = self._format_time_label(tick_time, bounds)
            lines.append(
                f'  <text x="{x:.1f}" y="{axis_y - 10}" '
                f'text-anchor="middle" fill="{self.style.text_color}" '
                f'font-size="{self.style.font_size - 1}px">{escape(label)}</text>'
            )

        return "\n".join(lines)

    def _render_lane_labels(self, data: TimelineData) -> str:
        """Render lane labels on the left side with hierarchical runner display."""
        lines: list[str] = []
        config = data.config

        for lane in data.get_sorted_lanes():
            lane_h = lane.lane_height(config)
            y = data.lane_y_position(lane) + (lane_h // 2)

            # Get group info for parent comparison
            group = data.groups.get(lane.group_id) if lane.group_id else None

            # Skip lane label for group headers in multi-lane groups (already shown in group header)
            if lane.is_group_header and group:
                group_lanes = data.get_lanes_for_group(lane.group_id)
                if len(group_lanes) > 1:
                    # This is a multi-lane group - header already rendered, skip lane label
                    continue

            if lane.is_group_header:
                # Group header lane (parent level): always show full info
                # Line 1: Runner class and truncated ID (main label)
                lines.append(
                    f'  <text x="8" y="{y - 4}" '
                    f'fill="{self.style.label_color}" font-weight="600" '
                    f'font-size="{self.style.font_size}px">'
                    f"<tspan>{escape(lane.label)}</tspan></text>"
                )

                # Line 2: Hostname, PID, and thread - more visible
                lines.append(
                    f'  <text x="8" y="{y + 8}" '
                    f'fill="{self.style.text_color}" opacity="0.7" '
                    f'font-size="{self.style.font_size - 2}px">'
                    f"<title>{escape(lane.hostname)} (pid:{lane.pid} thread:{lane.thread_id})</title>"
                    f"{escape(lane.details_line)}</text>"
                )
            elif lane.is_child_lane and group:
                # Child lane: show indented
                # Line 1: Runner class and ID (indented)
                child_label = f"{lane.runner_cls}({lane.display_runner_id})"
                lines.append(
                    f'  <text x="20" y="{y - 4}" '
                    f'fill="{self.style.label_color}" font-weight="400" '
                    f'font-size="{self.style.font_size - 1}px">'
                    f"<tspan>{escape(child_label)}</tspan></text>"
                )

                # Line 2: Show host/pid/thread that differ from parent - more visible
                child_details = lane.format_child_details(
                    group.hostname, group.pid, group.thread_id
                )
                if child_details:
                    lines.append(
                        f'  <text x="24" y="{y + 8}" '
                        f'fill="{self.style.text_color}" opacity="0.7" '
                        f'font-size="{self.style.font_size - 2}px">'
                        f"{escape(child_details.strip(' ()'))}</text>"
                    )
            else:
                # Standalone lane (no group): show full info
                lines.append(
                    f'  <text x="8" y="{y - 4}" '
                    f'fill="{self.style.label_color}" font-weight="500" '
                    f'font-size="{self.style.font_size}px">'
                    f"<tspan>{escape(lane.label)}</tspan></text>"
                )

                # Line 2: Hostname, PID, and thread - more visible
                lines.append(
                    f'  <text x="8" y="{y + 8}" '
                    f'fill="{self.style.text_color}" opacity="0.7" '
                    f'font-size="{self.style.font_size - 2}px">'
                    f"<title>{escape(lane.hostname)} (pid:{lane.pid} thread:{lane.thread_id})</title>"
                    f"{escape(lane.details_line)}</text>"
                )

        return "\n".join(lines)

    def _render_bars(self, data: TimelineData) -> str:
        """Render invocation bars for all lanes."""
        lines: list[str] = []
        config = data.config
        bounds = data.bounds

        for lane in data.get_sorted_lanes():
            lane_y = data.lane_y_position(lane) + config.bar_y_offset

            for bar in lane.bars:
                bar_svg = self._render_single_bar(bar, bounds, lane_y, config)
                lines.append(bar_svg)

        return "\n".join(lines)

    def _render_single_bar(
        self,
        bar: InvocationBar,
        bounds: TimelineBounds,
        lane_y: int,
        config: TimelineConfig,
    ) -> str:
        """Render a single invocation bar with tooltip."""
        x = bounds.time_to_x(bar.start_time)
        width = bounds.duration_to_width(bar.duration_seconds)
        height = config.bar_height

        # Escape tooltip and invocation_id for XML
        tooltip = escape(bar.tooltip)
        inv_id = escape(bar.invocation_id)

        return (
            f'  <g class="invocation-bar">'
            f'<rect x="{x:.1f}" y="{lane_y}" width="{width:.1f}" height="{height}" '
            f'fill="{bar.color}" rx="{self.style.bar_rx}" '
            f'opacity="{self.style.bar_opacity}" filter="url(#shadow)" '
            f'data-invocation-id="{inv_id}">'
            f"<title>{tooltip}</title></rect></g>"
        )

    def _render_lines(self, data: TimelineData) -> str:
        """Render connecting lines between status points across all lanes."""
        lines: list[str] = []
        bounds = data.bounds

        # Render global lines that may span across lanes
        for line in data.global_lines:
            line_svg = self._render_single_line(line, bounds, data)
            if line_svg:
                lines.append(line_svg)

        return "\n".join(lines)

    def _render_single_line(
        self,
        line: StatusLine,
        bounds: TimelineBounds,
        data: TimelineData,
    ) -> str:
        """Render a single connecting line between status points."""
        config = data.config

        # Get Y positions for from and to runners
        from_lane = data.lanes.get(line.from_runner_id)
        to_lane = data.lanes.get(line.to_runner_id)

        if not from_lane or not to_lane:
            return ""

        # Calculate base Y position matching point rendering:
        # lane_y_position + bar_y_offset + bar_height // 2 (centers on the bar)
        from_base_y = (
            data.lane_y_position(from_lane)
            + config.bar_y_offset
            + config.bar_height // 2
        )
        to_base_y = (
            data.lane_y_position(to_lane) + config.bar_y_offset + config.bar_height // 2
        )

        # Apply sub-lane offset (same calculation as points/segments)
        sub_lane_height = config.bar_height + 2
        from_y = from_base_y + line.from_sub_lane * sub_lane_height
        to_y = to_base_y + line.to_sub_lane * sub_lane_height

        x1 = bounds.time_to_x(line.start_time)
        x2 = bounds.time_to_x(line.end_time)

        # Escape invocation_id for XML
        inv_id = escape(line.invocation_id)

        return (
            f'  <line x1="{x1:.1f}" y1="{from_y}" '
            f'x2="{x2:.1f}" y2="{to_y}" '
            f'stroke="{line.color}" stroke-width="{self.style.line_width}" '
            f'opacity="{self.style.line_opacity}" stroke-dasharray="4,2" '
            f'data-invocation-id="{inv_id}"/>'
        )

    def _render_segments(self, data: TimelineData) -> str:
        """Render status segments (bars for worker-occupying statuses)."""
        lines: list[str] = []
        config = data.config
        bounds = data.bounds

        for lane in data.get_sorted_lanes():
            lane_y = data.lane_y_position(lane) + config.bar_y_offset

            for segment in lane.segments:
                segment_svg = self._render_single_segment(
                    segment, bounds, lane_y, config
                )
                lines.append(segment_svg)

        return "\n".join(lines)

    def _render_single_segment(
        self,
        segment: StatusSegment,
        bounds: TimelineBounds,
        lane_y: int,
        config: TimelineConfig,
    ) -> str:
        """Render a single status segment bar with sub-lane offset."""
        x = bounds.time_to_x(segment.start_time)
        width = bounds.duration_to_width(segment.duration_seconds)
        height = config.bar_height

        # Apply sub-lane offset for overlapping invocations
        sub_lane_offset = segment.sub_lane * (config.bar_height + 2)
        y = lane_y + sub_lane_offset

        # Escape tooltip and invocation_id for XML
        tooltip = escape(segment.tooltip)
        inv_id = escape(segment.invocation_id)

        # Build base segment
        parts = [
            '  <g class="status-segment">',
            f'<rect x="{x:.1f}" y="{y}" width="{width:.1f}" height="{height}" '
            f'fill="{segment.color}" rx="{self.style.bar_rx}" '
            f'opacity="{self.style.bar_opacity}" filter="url(#shadow)" '
            f'data-invocation-id="{inv_id}" data-status="{escape(segment.status)}">'
            f"<title>{tooltip}</title></rect>",
        ]

        # Add striped overlay for ongoing segments
        if segment.is_ongoing:
            parts.append(
                f'<rect x="{x:.1f}" y="{y}" width="{width:.1f}" height="{height}" '
                f'fill="url(#ongoing-stripes)" rx="{self.style.bar_rx}"/>'
            )

        parts.append("</g>")
        return "".join(parts)

    def _render_points(self, data: TimelineData) -> str:
        """Render status points (circles for all status changes)."""
        lines: list[str] = []
        config = data.config
        bounds = data.bounds

        for lane in data.get_sorted_lanes():
            # Base Y position at bar_y_offset + half bar_height to center points
            lane_base_y = (
                data.lane_y_position(lane)
                + config.bar_y_offset
                + config.bar_height // 2
            )

            for point in lane.points:
                point_svg = self._render_single_point(
                    point, bounds, lane_base_y, config
                )
                lines.append(point_svg)

        return "\n".join(lines)

    def _render_single_point(
        self,
        point: StatusPoint,
        bounds: TimelineBounds,
        lane_y: int,
        config: TimelineConfig,
    ) -> str:
        """Render a single status point circle with sub-lane offset."""
        x = bounds.time_to_x(point.timestamp)

        # Apply sub-lane offset for overlapping invocations
        # Center the point within the sub-lane bar area
        sub_lane_offset = point.sub_lane * (config.bar_height + 2)
        y = lane_y + sub_lane_offset

        # Escape tooltip and invocation_id for XML
        tooltip = escape(point.tooltip)
        inv_id = escape(point.invocation_id)

        return (
            f'  <g class="status-point">'
            f'<circle cx="{x:.1f}" cy="{y}" r="{self.style.point_radius}" '
            f'fill="{point.color}" stroke="#ffffff" stroke-width="1.5" '
            f'data-invocation-id="{inv_id}" data-status="{escape(point.status)}">'
            f"<title>{tooltip}</title></circle></g>"
        )

    def _render_legend(self, data: TimelineData) -> str:
        """Render status color legend at the bottom."""
        lines: list[str] = []
        config = data.config

        # Calculate lanes bottom position using dynamic lane heights
        sorted_lanes = data.get_sorted_lanes()
        if not sorted_lanes:
            lanes_bottom = config.top_margin + config.lane_height
        else:
            lanes_bottom = config.top_margin
            for i, lane in enumerate(sorted_lanes):
                lanes_bottom += lane.lane_height(config)
                if i < len(sorted_lanes) - 1:
                    lanes_bottom += config.lane_padding

        # Position legend below lanes with padding
        legend_y = lanes_bottom + 15
        x_offset = config.left_margin

        for status, color in STATUS_COLORS.items():
            # Color box
            lines.append(
                f'  <rect x="{x_offset}" y="{legend_y}" width="12" height="12" '
                f'fill="{color}" rx="2"/>'
            )
            # Label
            lines.append(
                f'  <text x="{x_offset + 16}" y="{legend_y + 10}" '
                f'fill="{self.style.text_color}" '
                f'font-size="{self.style.font_size - 2}px">{escape(status)}</text>'
            )
            x_offset += len(status) * 7 + 30

            # Wrap to next line if needed
            if x_offset > config.width - 100:
                x_offset = config.left_margin
                legend_y += 18

        return "\n".join(lines)

    def _calculate_tick_positions(self, bounds: TimelineBounds) -> list[datetime]:
        """
        Calculate tick positions based on timeline duration.

        Returns evenly spaced ticks that align to reasonable time boundaries.
        Uses config.resolution_seconds if specified, otherwise auto-calculates.
        """
        duration = bounds.duration_seconds
        config = bounds.config

        # Use configured resolution or auto-calculate
        # Aim for roughly 5-10 tick marks on the timeline
        if config.resolution_seconds is not None:
            interval_seconds = config.resolution_seconds
        elif duration <= 5:  # Up to 5 seconds
            interval_seconds = 1
        elif duration <= 15:  # Up to 15 seconds
            interval_seconds = 2
        elif duration <= 30:  # Up to 30 seconds
            interval_seconds = 5
        elif duration <= 60:  # Up to 1 minute
            interval_seconds = 10
        elif duration <= 120:  # Up to 2 minutes
            interval_seconds = 15
        elif duration <= 300:  # Up to 5 minutes
            interval_seconds = 30
        elif duration <= 600:  # Up to 10 minutes
            interval_seconds = 60
        elif duration <= 3600:  # Up to 1 hour
            interval_seconds = 300
        elif duration <= 21600:  # Up to 6 hours
            interval_seconds = 1800
        else:  # More than 6 hours
            interval_seconds = 3600

        # Generate tick positions
        ticks: list[datetime] = []
        current = bounds.start_time

        while current <= bounds.end_time:
            ticks.append(current)
            current = current + timedelta(seconds=interval_seconds)

        return ticks

    def _format_time_label(self, time: datetime, bounds: TimelineBounds) -> str:
        """
        Format time for axis labels in UTC.

        Uses appropriate format based on timeline duration.
        Always formats in UTC to match the displayed time range.
        """
        duration = bounds.duration_seconds
        # Ensure we're working with UTC time
        utc_time = time.astimezone(UTC) if time.tzinfo else time

        if duration <= 10:
            # Very short duration - show with milliseconds
            ms = utc_time.microsecond // 1000
            return f"{utc_time.strftime('%H:%M:%S')}.{ms:03d}"
        elif duration <= 60:
            return utc_time.strftime("%H:%M:%S")
        elif duration <= 3600:
            return utc_time.strftime("%H:%M:%S")
        elif duration <= 86400:
            return utc_time.strftime("%H:%M")
        else:
            return utc_time.strftime("%m/%d %H:%M")


def generate_svg(data: TimelineData, style: SVGStyle | None = None) -> str:
    """
    Generate SVG markup from TimelineData.

    Convenience function that creates a renderer and renders the data.

    :param TimelineData data: Complete timeline data
    :param SVGStyle | None style: Optional style configuration
    :return: SVG markup as string
    """
    renderer = TimelineSVGRenderer(style=style)
    return renderer.render(data)
