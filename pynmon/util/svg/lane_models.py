"""
Lane models for timeline visualization.

LaneGroup and RunnerLane are the structural containers that hold
visual elements and define the vertical layout of the timeline.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pynmon.util.svg.config import TimelineConfig
from pynmon.util.svg.status_elements import (
    InvocationBar,
    StatusLine,
    StatusPoint,
    StatusSegment,
)

if TYPE_CHECKING:
    pass

_TRUNCATE_LENGTH = 7


@dataclass
class LaneGroup:
    """
    Visual grouping header for hierarchical runner contexts.

    :param str group_id: Unique identifier (typically parent's runner_id)
    :param str hostname: Host where runners are executing
    :param str runner_cls: Class name of the parent runner
    :param str runner_id: Runner ID of the parent
    :param int pid: Process ID
    :param int thread_id: Thread ID
    :param list[str] child_lane_ids: Ordered list of child lane IDs
    """

    group_id: str
    hostname: str
    runner_cls: str
    runner_id: str
    pid: int
    thread_id: int = 0
    child_lane_ids: list[str] = field(default_factory=list)

    @property
    def display_runner_id(self) -> str:
        """Truncated runner_id for display."""
        r = self.runner_id
        return r[:_TRUNCATE_LENGTH] if len(r) > _TRUNCATE_LENGTH else r

    @property
    def header_label(self) -> str:
        """Format header label for the group."""
        return f"{self.runner_cls}({self.display_runner_id})"

    @property
    def details_line(self) -> str:
        """Format detailed group info (hostname, pid, thread)."""
        return f"{self.hostname} (pid:{self.pid} thread:{self.thread_id})"

    def add_child_lane(self, lane_id: str) -> None:
        """Add a child lane to this group."""
        if lane_id not in self.child_lane_ids:
            self.child_lane_ids.append(lane_id)


@dataclass
class RunnerLane:
    """
    Horizontal lane representing a specific runner on the timeline.

    :param str runner_id: Unique identifier for the runner
    :param str hostname: Host where the runner is executing
    :param str label: Display label (runner class + truncated id)
    :param str color: Base color for this runner's elements
    :param int lane_index: Vertical position index (0-based)
    :param str runner_cls: Class name of the runner
    :param int pid: Process ID
    :param int thread_id: Thread ID
    :param str group_id: ID of the LaneGroup this lane belongs to
    :param bool is_group_header: True if this lane is the parent/group header
    """

    runner_id: str
    hostname: str
    label: str
    color: str
    lane_index: int = 0
    runner_cls: str = "Unknown"
    pid: int = 0
    thread_id: int = 0
    group_id: str = ""
    is_group_header: bool = False
    bars: list[InvocationBar] = field(default_factory=list)
    points: list[StatusPoint] = field(default_factory=list)
    segments: list[StatusSegment] = field(default_factory=list)
    lines: list[StatusLine] = field(default_factory=list)

    @property
    def display_runner_id(self) -> str:
        """Truncated runner_id for display."""
        r = self.runner_id
        return r[:_TRUNCATE_LENGTH] if len(r) > _TRUNCATE_LENGTH else r

    @property
    def details_line(self) -> str:
        """Detailed runner info (hostname, pid, thread)."""
        return f"{self.hostname} (pid:{self.pid} thread:{self.thread_id})"

    @property
    def is_child_lane(self) -> bool:
        """True if this is a child lane within a group."""
        return bool(self.group_id) and not self.is_group_header

    def format_child_details(
        self, parent_hostname: str, parent_pid: int, parent_thread_id: int
    ) -> str:
        """Show only fields that differ from the parent group."""
        parts = []
        if self.hostname != parent_hostname:
            parts.append(f"host:{self.hostname}")
        if self.pid != parent_pid:
            parts.append(f"pid:{self.pid}")
        if self.thread_id != parent_thread_id:
            parts.append(f"thread:{self.thread_id}")
        return f" ({', '.join(parts)})" if parts else ""

    @property
    def max_sub_lane(self) -> int:
        """Maximum sub-lane index in use (0 if none)."""
        return max(
            max((p.sub_lane for p in self.points), default=0),
            max((s.sub_lane for s in self.segments), default=0),
        )

    def lane_height(self, config: TimelineConfig) -> int:
        """Effective height accommodating all sub-lanes."""
        return max(
            config.lane_height,
            (self.max_sub_lane + 1) * (config.bar_height + 2) + 8,
        )

    def y_position(self, config: TimelineConfig) -> int:
        """Y-coordinate for this lane (simple, ignoring variable heights)."""
        return config.top_margin + (
            self.lane_index * (config.lane_height + config.lane_padding)
        )

    def add_bar(self, bar: InvocationBar) -> None:
        """Add an invocation bar."""
        self.bars.append(bar)

    def add_point(self, point: StatusPoint) -> None:
        """Add a status point."""
        self.points.append(point)

    def add_segment(self, segment: StatusSegment) -> None:
        """Add a status segment."""
        self.segments.append(segment)

    def add_line(self, line: StatusLine) -> None:
        """Add a connecting line."""
        self.lines.append(line)
