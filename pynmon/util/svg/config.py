"""
Timeline configuration model.

Single-responsibility module: TimelineConfig holds all rendering parameters.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class TimelineConfig:
    """
    Configuration options for timeline rendering.

    Controls dimensions, spacing, and visual properties of the timeline.

    :param int width: Total SVG width in pixels
    :param int lane_height: Height of each runner lane in pixels
    :param int lane_padding: Vertical padding between lanes in pixels
    :param int left_margin: Left margin for runner labels in pixels
    :param int top_margin: Top margin for time axis in pixels
    :param int bar_height: Height of invocation bars in pixels
    :param int min_bar_width: Minimum width for very short invocations in pixels
    :param int | None resolution_seconds: Fixed tick interval (None = auto)
    """

    width: int = 2000
    lane_height: int = 32
    lane_padding: int = 2
    left_margin: int = 320
    top_margin: int = 50
    bar_height: int = 20
    min_bar_width: int = 2
    resolution_seconds: int | None = None

    @property
    def content_width(self) -> int:
        """Width available for timeline content (excluding left margin)."""
        return self.width - self.left_margin

    @property
    def bar_y_offset(self) -> int:
        """Vertical offset for bars within a lane (centers bar in lane)."""
        return (self.lane_height - self.bar_height) // 2
