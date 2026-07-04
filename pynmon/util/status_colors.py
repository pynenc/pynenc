"""
Unified status color definitions for pynmon.

This module is the canonical palette for invocation statuses across SVG, HTML
and JavaScript renderers. The status-to-background mapping lives in one place,
and status badges use one shared white foreground for readability.

The color scheme follows a semantic approach:
- Neutral states: Gray tones (REGISTERED, REROUTED)
- Waiting states: Orange/Yellow tones (PENDING, CONCURRENCY_CONTROLLED)
- Active states: Blue tones (RUNNING)
- Success states: Green tones (SUCCESS)
- Error states: Red tones (FAILED, KILLED)
- Special states: Purple/Teal tones (RETRY, PAUSED)

Status visualization types:
- SEGMENT statuses: Occupy the worker/runner for a duration (PENDING, RUNNING, PAUSED)
- POINT statuses: Punctual events at a specific moment (all others)

Key components:
- STATUS_COLORS: Hex color mapping for SVG/CSS
- SEGMENT_STATUSES: Statuses that occupy worker and show as segments
- OUTCOME_STATUSES: Final execution outcomes that color the preceding segment
"""

from dataclasses import dataclass
from typing import Any


# Hex color mapping for SVG and CSS direct styling
# Only includes statuses from pynenc.invocation.status.InvocationStatus
STATUS_COLORS: dict[str, str] = {
    "REGISTERED": "#95a5a6",  # Gray - queued
    "CONCURRENCY_CONTROLLED": "#e67e22",  # Dark orange - blocked
    "CONCURRENCY_CONTROLLED_FINAL": "#d35400",  # Darker orange - blocked final
    "REROUTED": "#16a085",  # Dark teal - rerouted
    "PENDING": "#f39c12",  # Orange - waiting to run
    "PENDING_RECOVERY": "#e67e22",  # Dark orange - timeout recovery
    "RUNNING": "#3498db",  # Blue - active
    "RUNNING_RECOVERY": "#e67e22",  # Dark orange - runner recovery
    "PAUSED": "#1abc9c",  # Teal - paused
    "KILLED": "#c0392b",  # Dark red - killed
    "SUCCESS": "#27ae60",  # Green - completed
    "FAILED": "#e74c3c",  # Red - error
    "RETRY": "#9b59b6",  # Purple - retrying
}

DEFAULT_STATUS_COLOR = "#7f8c8d"  # Gray for unknown statuses
STATUS_BADGE_TEXT_COLOR = "#ffffff"


# Statuses that occupy a worker and should be shown as segments (duration bars)
# These represent time spent actively using resources
SEGMENT_STATUSES: frozenset[str] = frozenset(
    {
        "PENDING",  # Time between picked from queue and starting execution
        "RUNNING",  # Active execution time
        "PAUSED",  # Paused but still holding resources
    }
)

# Statuses that are instantaneous events (rendered as points/circles only)
# These are transitions that don't occupy worker time
POINT_ONLY_STATUSES: frozenset[str] = frozenset(
    {s for s in STATUS_COLORS if s not in SEGMENT_STATUSES}
)

# Final outcome statuses that should color the preceding RUNNING segment
# When a segment ends with one of these, the segment takes this status's color
OUTCOME_STATUSES: frozenset[str] = frozenset(
    {
        "SUCCESS",
        "FAILED",
        "RETRY",
        "KILLED",
    }
)


@dataclass(frozen=True)
class StatusColors:
    """
    Status color information for a specific status.

    Provides the background color plus the foreground color used for badges.

    :param str hex_color: Hex color string (e.g., "#3498db")
    :param str text_color: Shared foreground color for status badges.
    """

    hex_color: str
    text_color: str


def _normalize_status_name(status: Any) -> str:
    """Return a canonical status name for color lookups."""
    if status is None:
        return ""
    if hasattr(status, "name"):
        status = status.name

    status_upper = str(status).strip().upper()
    # Some UI surfaces use RETRYING as a display-only label; normalize it to
    # RETRY so both timeline and status-history views share the same purple.
    return "RETRY" if status_upper == "RETRYING" else status_upper


STATUS_COLOR_STYLES: dict[str, dict[str, str]] = {
    status: {"background": hex_color} for status, hex_color in STATUS_COLORS.items()
}
DEFAULT_STATUS_STYLE: dict[str, str] = {
    "background": DEFAULT_STATUS_COLOR,
}


def get_status_colors(status: Any) -> StatusColors:
    """
    Get color information for a status.

    :param status: Status name or enum-like object (case-insensitive)
    :return: StatusColors with background and contrast values
    """
    status_upper = _normalize_status_name(status)
    style = STATUS_COLOR_STYLES.get(status_upper, DEFAULT_STATUS_STYLE)
    return StatusColors(
        hex_color=style["background"],
        text_color=STATUS_BADGE_TEXT_COLOR,
    )


def get_hex_color(status: Any) -> str:
    """
    Get hex color for a status.

    :param status: Status name or enum-like object (case-insensitive)
    :return: Hex color string
    """
    return STATUS_COLORS.get(_normalize_status_name(status), DEFAULT_STATUS_COLOR)


def is_segment_status(status: str) -> bool:
    """
    Check if a status should be rendered as a segment (occupies worker time).

    Segment statuses represent work being done and take duration on the timeline.
    Point-only statuses are instantaneous events rendered as circles.

    :param str status: Status name (case-insensitive)
    :return: True if status should be a segment, False for point-only
    """
    return _normalize_status_name(status) in SEGMENT_STATUSES


def is_point_only_status(status: str) -> bool:
    """
    Check if a status should be rendered only as a point (instantaneous event).

    :param str status: Status name (case-insensitive)
    :return: True if status is point-only, False if segment
    """
    return _normalize_status_name(status) in POINT_ONLY_STATUSES
