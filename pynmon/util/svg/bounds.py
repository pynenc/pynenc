"""
Timeline bounds and coordinate mapping.

Single-responsibility module: TimelineBounds maps timestamps to SVG pixels.
"""

from dataclasses import dataclass, field
from datetime import datetime

from pynmon.util.svg.config import TimelineConfig


@dataclass
class TimelineBounds:
    """
    Time range and coordinate mapping for the timeline.

    Converts timestamps to pixel x-coordinates and durations to widths.

    :param datetime start_time: Start of the time range
    :param datetime end_time: End of the time range
    :param TimelineConfig config: Configuration for coordinate calculation
    """

    start_time: datetime
    end_time: datetime
    config: TimelineConfig = field(default_factory=TimelineConfig)

    @property
    def duration_seconds(self) -> float:
        """Total duration of the timeline in seconds."""
        return (self.end_time - self.start_time).total_seconds()

    def time_to_x(self, timestamp: datetime) -> float:
        """
        Convert a timestamp to an x-coordinate.

        :param datetime timestamp: The timestamp to convert
        :return: X coordinate in pixels (from left edge)
        """
        if self.duration_seconds == 0:
            return float(self.config.left_margin)
        elapsed = (timestamp - self.start_time).total_seconds()
        ratio = elapsed / self.duration_seconds
        return self.config.left_margin + (ratio * self.config.content_width)

    def duration_to_width(self, seconds: float) -> float:
        """
        Convert a duration to pixel width.

        :param float seconds: Duration in seconds
        :return: Width in pixels (at least min_bar_width)
        """
        if self.duration_seconds == 0:
            return float(self.config.min_bar_width)
        ratio = seconds / self.duration_seconds
        return max(ratio * self.config.content_width, self.config.min_bar_width)
