"""
Unified status color definitions for pynmon.

This module provides consistent color mappings for invocation statuses across
all pynmon components including SVG timelines, HTML templates, and JavaScript.

The color scheme follows a semantic approach:
- Neutral states: Gray tones (REGISTERED, REROUTED)
- Waiting states: Orange/Yellow tones (PENDING, CONCURRENCY_CONTROLLED)
- Active states: Blue tones (RUNNING, RESUMED)
- Success states: Green tones (SUCCESS)
- Error states: Red tones (FAILED, KILLED)
- Special states: Purple/Teal tones (RETRY, PAUSED)

Status visualization types:
- SEGMENT statuses: Occupy the worker/runner for a duration (PENDING, RUNNING, PAUSED, RESUMED)
- POINT statuses: Punctual events at a specific moment (all others)

Key components:
- STATUS_COLORS: Hex color mapping for SVG/CSS
- STATUS_BOOTSTRAP_CLASSES: Bootstrap class mapping for HTML templates
- SEGMENT_STATUSES: Statuses that occupy worker and show as segments
- OUTCOME_STATUSES: Final execution outcomes that color the preceding segment
"""

from dataclasses import dataclass


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
    "RESUMED": "#2980b9",  # Dark blue - resumed
    "KILLED": "#c0392b",  # Dark red - killed
    "SUCCESS": "#27ae60",  # Green - completed
    "FAILED": "#e74c3c",  # Red - error
    "RETRY": "#9b59b6",  # Purple - retrying
}

DEFAULT_STATUS_COLOR = "#7f8c8d"  # Gray for unknown statuses


# Statuses that occupy a worker and should be shown as segments (duration bars)
# These represent time spent actively using resources
SEGMENT_STATUSES: frozenset[str] = frozenset(
    {
        "PENDING",  # Time between picked from queue and starting execution
        "RUNNING",  # Active execution time
        "PAUSED",  # Paused but still holding resources
        "RESUMED",  # Resumed execution time
    }
)

# Statuses that are instantaneous events (rendered as points/circles only)
# These are transitions that don't occupy worker time
POINT_ONLY_STATUSES: frozenset[str] = frozenset(
    {s for s in STATUS_COLORS if s not in SEGMENT_STATUSES}
)

# Final outcome statuses that should color the preceding RUNNING/RESUMED segment
# When a segment ends with one of these, the segment takes this status's color
OUTCOME_STATUSES: frozenset[str] = frozenset(
    {
        "SUCCESS",
        "FAILED",
        "RETRY",
        "KILLED",
    }
)

# Bootstrap class mapping for HTML template badges
# Using bg-* classes for consistent Bootstrap 5 styling
STATUS_BOOTSTRAP_CLASSES: dict[str, str] = {
    "REGISTERED": "secondary",
    "CONCURRENCY_CONTROLLED": "warning",
    "CONCURRENCY_CONTROLLED_FINAL": "warning",
    "REROUTED": "info",
    "PENDING": "warning",
    "PENDING_RECOVERY": "warning",
    "RUNNING": "info",
    "RUNNING_RECOVERY": "warning",
    "PAUSED": "primary",
    "RESUMED": "info",
    "KILLED": "danger",
    "SUCCESS": "success",
    "FAILED": "danger",
    "RETRY": "secondary",
}

DEFAULT_BOOTSTRAP_CLASS = "secondary"


@dataclass(frozen=True)
class StatusColors:
    """
    Status color information for a specific status.

    Provides both hex colors for SVG/CSS and Bootstrap classes for templates.

    :param str hex_color: Hex color string (e.g., "#3498db")
    :param str bootstrap_class: Bootstrap class name (e.g., "info")
    """

    hex_color: str
    bootstrap_class: str


def get_status_colors(status: str) -> StatusColors:
    """
    Get color information for a status.

    :param str status: Status name (case-insensitive)
    :return: StatusColors with hex and bootstrap values
    """
    status_upper = status.upper()
    return StatusColors(
        hex_color=STATUS_COLORS.get(status_upper, DEFAULT_STATUS_COLOR),
        bootstrap_class=STATUS_BOOTSTRAP_CLASSES.get(
            status_upper, DEFAULT_BOOTSTRAP_CLASS
        ),
    )


def get_hex_color(status: str) -> str:
    """
    Get hex color for a status.

    :param str status: Status name (case-insensitive)
    :return: Hex color string
    """
    return STATUS_COLORS.get(status.upper(), DEFAULT_STATUS_COLOR)


def get_bootstrap_class(status: str) -> str:
    """
    Get Bootstrap class for a status.

    :param str status: Status name (case-insensitive)
    :return: Bootstrap class name (without 'bg-' prefix)
    """
    return STATUS_BOOTSTRAP_CLASSES.get(status.upper(), DEFAULT_BOOTSTRAP_CLASS)


def is_segment_status(status: str) -> bool:
    """
    Check if a status should be rendered as a segment (occupies worker time).

    Segment statuses represent work being done and take duration on the timeline.
    Point-only statuses are instantaneous events rendered as circles.

    :param str status: Status name (case-insensitive)
    :return: True if status should be a segment, False for point-only
    """
    return status.upper() in SEGMENT_STATUSES


def is_point_only_status(status: str) -> bool:
    """
    Check if a status should be rendered only as a point (instantaneous event).

    :param str status: Status name (case-insensitive)
    :return: True if status is point-only, False if segment
    """
    return status.upper() in POINT_ONLY_STATUSES


# JavaScript constants for client-side status color mapping
# This string can be included in templates
JS_STATUS_COLORS = """
const STATUS_COLORS = {
  'REGISTERED': '#95a5a6',
  'CONCURRENCY_CONTROLLED': '#e67e22',
  'CONCURRENCY_CONTROLLED_FINAL': '#d35400',
  'REROUTED': '#16a085',
  'PENDING': '#f39c12',
  'PENDING_RECOVERY': '#e67e22',
  'RUNNING': '#3498db',
  'RUNNING_RECOVERY': '#e67e22',
  'PAUSED': '#1abc9c',
  'RESUMED': '#2980b9',
  'KILLED': '#c0392b',
  'SUCCESS': '#27ae60',
  'FAILED': '#e74c3c',
  'RETRY': '#9b59b6'
};

const STATUS_BOOTSTRAP_CLASSES = {
  'REGISTERED': 'secondary',
  'CONCURRENCY_CONTROLLED': 'warning',
  'CONCURRENCY_CONTROLLED_FINAL': 'warning',
  'REROUTED': 'info',
  'PENDING': 'warning',
  'PENDING_RECOVERY': 'warning',
  'RUNNING': 'info',
  'RUNNING_RECOVERY': 'warning',
  'PAUSED': 'primary',
  'RESUMED': 'info',
  'KILLED': 'danger',
  'SUCCESS': 'success',
  'FAILED': 'danger',
  'RETRY': 'secondary'
};

// Statuses that occupy worker time (rendered as segments)
const SEGMENT_STATUSES = new Set(['RUNNING', 'RESUMED', 'PENDING']);

// Statuses that are instantaneous (rendered as points)
const POINT_ONLY_STATUSES = new Set(Object.keys(STATUS_COLORS).filter(s => !SEGMENT_STATUSES.has(s)));

function getStatusClass(status) {
  return STATUS_BOOTSTRAP_CLASSES[status] || 'secondary';
}

function getStatusColor(status) {
  return STATUS_COLORS[status] || '#7f8c8d';
}

function isSegmentStatus(status) {
  return SEGMENT_STATUSES.has(status);
}
"""
