"""
SVG renderer for timeline visualization.

TimelineSVGRenderer orchestrates sub-modules:
  - render_axis    : grid, time axis, legend
  - render_lanes   : lane backgrounds, headers, labels
  - render_elements: bars, segments, points, lines
"""

import logging
import time as _time
from dataclasses import dataclass

from pynmon.util.svg.render_axis import render_grid, render_legend, render_time_axis
from pynmon.util.svg.render_elements import (
    render_bars,
    render_lines,
    render_points,
    render_segments,
)
from pynmon.util.svg.render_lanes import (
    render_group_containers,
    render_group_headers,
    render_lane_backgrounds,
    render_lane_labels,
)
from pynmon.util.svg.timeline_data import TimelineData

logger = logging.getLogger("pynmon.util.svg.renderer")


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
    :param str text_color: Primary text color
    :param str label_color: Lane label color
    :param float bar_opacity: Opacity for bars
    :param int bar_rx: Border radius for bars
    :param int point_radius: Radius for status point circles
    :param float line_opacity: Opacity for connecting lines
    :param int line_width: Stroke width for connecting lines
    """

    font_family: str = "system-ui, -apple-system, sans-serif"
    font_size: int = 12
    background_color: str = "#ffffff"
    lane_background: str = "#f8f9fa"
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
    """Renders TimelineData to an SVG string."""

    def __init__(self, style: SVGStyle | None = None) -> None:
        self.style = style or SVGStyle()

    def render(self, data: TimelineData) -> str:
        """
        Render TimelineData to a complete SVG document.

        :param TimelineData data: Complete timeline data
        :return: SVG markup as string
        """
        t0 = _time.monotonic()
        s = self.style
        parts = [
            self._header(data),
            self._defs(data),
            f'  <rect x="0" y="0" width="{data.config.width}" height="{data.total_height}" fill="{s.background_color}"/>',
            render_group_containers(data, s),
            render_lane_backgrounds(data, s),
            render_grid(data, s),
            render_time_axis(data, s),
            render_group_headers(data, s),
            render_lane_labels(data, s),
            render_segments(data, s),
            render_bars(data, s),
            render_lines(data, s),
            render_points(data, s),
            render_legend(data, s),
            "</svg>",
        ]
        svg = "\n".join(parts)
        logger.debug(
            f"render: {len(data.lanes)} lanes, {len(data.global_lines)} lines, "
            f"{sum(len(lane.segments) for lane in data.lanes.values())} segments, "
            f"{sum(len(lane.points) for lane in data.lanes.values())} points "
            f"\u2192 {len(svg):,} chars in {_time.monotonic() - t0:.3f}s"
        )
        return svg

    def _header(self, data: TimelineData) -> str:
        """SVG opening tag with viewBox for responsive scaling."""
        w, h = data.config.width, data.total_height
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="100%" '
            f'viewBox="0 0 {w} {h}" preserveAspectRatio="xMinYMin meet" '
            f'style="font-family: {self.style.font_family}; font-size: {self.style.font_size}px;">'
        )

    def _defs(self, data: TimelineData) -> str:
        """Reusable SVG defs: shadow filter, clip path, ongoing-segment pattern."""
        lm = data.config.left_margin
        return (
            f"  <defs>\n"
            f'    <filter id="shadow" x="-10%" y="-10%" width="120%" height="120%">'
            f'<feDropShadow dx="0" dy="1" stdDeviation="1" flood-opacity="0.15"/></filter>\n'
            f'    <clipPath id="label-clip"><rect x="0" y="0" width="{lm - 10}" height="100%"/></clipPath>\n'
            f'    <pattern id="ongoing-stripes" patternUnits="userSpaceOnUse" width="8" height="8" patternTransform="rotate(45)">'
            f'<line x1="0" y1="0" x2="0" y2="8" stroke="rgba(255,255,255,0.3)" stroke-width="4"/></pattern>\n'
            f"  </defs>"
        )


def generate_svg(data: TimelineData, style: SVGStyle | None = None) -> str:
    """Convenience wrapper around TimelineSVGRenderer.render()."""
    return TimelineSVGRenderer(style).render(data)
