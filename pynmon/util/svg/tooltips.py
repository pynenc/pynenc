"""
Tooltip formatting for timeline visualization elements.

Standalone functions for creating consistent tooltip text.
"""

from datetime import datetime

from pynmon.util.svg.runner_info import RunnerInfo


def format_status_point_tooltip(
    invocation_id: str,
    status: str,
    timestamp: datetime,
    runner_info: RunnerInfo,
) -> str:
    """
    Format tooltip text for a status point.

    :param invocation_id: The invocation identifier
    :param status: Status name (uppercase)
    :param timestamp: When the status occurred
    :param runner_info: Runner context information
    :return: Formatted tooltip string
    """
    lines = [
        f"Invocation: {invocation_id}",
        f"Status: {status}",
        f"Time: {timestamp.strftime('%H:%M:%S')}",
        f"Runner: {runner_info.label}",
    ]
    if runner_info.has_parent:
        parent_display = (
            runner_info.parent_runner_id[:7] if runner_info.parent_runner_id else ""
        )
        lines.append(f"Parent: {runner_info.parent_runner_cls}({parent_display})")
    return "\n".join(lines)


def format_segment_tooltip(
    invocation_id: str,
    status: str,
    duration_seconds: float,
    is_ongoing: bool = False,
) -> str:
    """
    Format tooltip text for a status segment.

    :param invocation_id: The invocation identifier
    :param status: Status name (uppercase)
    :param duration_seconds: Segment duration in seconds
    :param is_ongoing: True if segment is still active
    :return: Formatted tooltip string
    """
    duration_str = f"{duration_seconds:.2f}s"
    if is_ongoing:
        duration_str += " (ongoing)"

    return f"Invocation: {invocation_id}\nStatus: {status}\nDuration: {duration_str}"
