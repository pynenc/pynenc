"""
Data models for SVG timeline visualization.

This module defines the data structures used to represent timeline visualization
data. These models are decoupled from both the data source (InvocationHistory)
and the rendering target (SVG), enabling clean separation of concerns.

Key components:
- TimelineConfig: Rendering configuration options
- TimelineBounds: Time range and coordinate mapping
- LaneGroup: Visual grouping for hierarchical runner contexts
- RunnerLane: Horizontal lane for a specific runner
- StatusPoint: Circle representing an instantaneous status event
- StatusSegment: Bar representing a status that occupies worker time
- StatusLine: Line connecting consecutive status points
- InvocationBar: Legacy bar representation (being replaced by points/segments)
- TimelineData: Complete timeline visualization data
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynmon.util.svg.builder import RunnerInfo


# Truncation length for IDs (7 chars like Git short SHA)
_TRUNCATE_LENGTH = 7


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
    :param int | None resolution_seconds: Fixed tick interval in seconds (None for auto)
    """

    width: int = 2000  # Wider default for better resolution
    lane_height: int = 32
    lane_padding: int = 2
    left_margin: int = 320  # Space for runner labels (two lines)
    top_margin: int = 50
    bar_height: int = 20
    min_bar_width: int = 2
    resolution_seconds: int | None = (
        None  # None = auto, or specific seconds (e.g., 300 for 5min)
    )

    @property
    def content_width(self) -> int:
        """Width available for timeline content (excluding left margin)."""
        return self.width - self.left_margin

    @property
    def bar_y_offset(self) -> int:
        """Vertical offset for bars within a lane (centers bar in lane)."""
        return (self.lane_height - self.bar_height) // 2


@dataclass
class TimelineBounds:
    """
    Time range and coordinate mapping for the timeline.

    Provides methods to convert between timestamps and pixel coordinates.

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
        Convert a timestamp to x-coordinate.

        :param datetime timestamp: The timestamp to convert
        :return: X coordinate in pixels (relative to left margin)
        """
        if self.duration_seconds == 0:
            return 0.0
        elapsed = (timestamp - self.start_time).total_seconds()
        ratio = elapsed / self.duration_seconds
        return self.config.left_margin + (ratio * self.config.content_width)

    def duration_to_width(self, seconds: float) -> float:
        """
        Convert a duration in seconds to pixel width.

        :param float seconds: Duration in seconds
        :return: Width in pixels (at least min_bar_width)
        """
        if self.duration_seconds == 0:
            return float(self.config.min_bar_width)
        ratio = seconds / self.duration_seconds
        width = ratio * self.config.content_width
        return max(width, self.config.min_bar_width)


@dataclass(frozen=True)
class InvocationBar:
    """
    Visual representation of an invocation segment on the timeline.

    Represents a contiguous period where an invocation was in a particular
    status on a specific runner. An invocation may have multiple bars if it
    changes status or moves between runners.

    :param str invocation_id: The invocation identifier
    :param str task_id: The task identifier
    :param datetime start_time: Start of this segment
    :param datetime end_time: End of this segment (or current time if ongoing)
    :param str status: The invocation status during this segment
    :param str color: Hex color for rendering this bar
    :param str tooltip: Hover text for this bar
    """

    invocation_id: str
    task_id: str
    start_time: datetime
    end_time: datetime
    status: str
    color: str
    tooltip: str = ""

    @property
    def duration_seconds(self) -> float:
        """Duration of this segment in seconds."""
        return (self.end_time - self.start_time).total_seconds()


@dataclass(frozen=True)
class StatusPoint:
    """
    Circle representing an instantaneous status event on the timeline.

    Status points mark specific moments when a status change occurred.
    All status changes are rendered as points, with segment statuses
    also getting a bar extending from the point.

    :param str invocation_id: The invocation identifier
    :param str task_id: The task identifier
    :param datetime timestamp: When this status occurred
    :param str status: The invocation status
    :param str color: Hex color for rendering this point
    :param str tooltip: Hover text for this point
    :param int order: Sequence order within the invocation (0-based)
    :param int sub_lane: Vertical sub-lane for overlapping invocations (0-based)
    """

    invocation_id: str
    task_id: str
    timestamp: datetime
    status: str
    color: str
    tooltip: str = ""
    order: int = 0
    sub_lane: int = 0


@dataclass(frozen=True)
class StatusSegment:
    """
    Bar representing a status that occupies worker time on the timeline.

    Segment statuses (RUNNING, RESUMED, PENDING) show as bars because they
    represent actual work being done that takes time. The segment extends
    from the status point to either the next status or current time.

    :param str invocation_id: The invocation identifier
    :param str task_id: The task identifier
    :param datetime start_time: Start of this segment (when status began)
    :param datetime end_time: End of segment (next status time or now)
    :param str status: The invocation status during this segment
    :param str color: Hex color for rendering (may be next status color)
    :param str tooltip: Hover text for this segment
    :param str | None next_status: The status that ended this segment
    :param int sub_lane: Vertical sub-lane for overlapping invocations (0-based)
    :param bool is_ongoing: True if segment extends to timeline end without resolution
    """

    invocation_id: str
    task_id: str
    start_time: datetime
    end_time: datetime
    status: str
    color: str
    tooltip: str = ""
    next_status: str | None = None
    sub_lane: int = 0
    is_ongoing: bool = False

    @property
    def duration_seconds(self) -> float:
        """Duration of this segment in seconds."""
        return (self.end_time - self.start_time).total_seconds()


@dataclass(frozen=True)
class StatusLine:
    """
    Line connecting consecutive status points on the timeline.

    Lines show the flow between status changes, making it easy to follow
    an invocation's journey. The line color matches the origin status.
    Lines can connect points across different runners/lanes.

    :param str invocation_id: The invocation identifier
    :param datetime start_time: Start point timestamp
    :param datetime end_time: End point timestamp
    :param str from_status: The origin status (determines line color)
    :param str to_status: The destination status
    :param str color: Hex color for the line (from origin status)
    :param str from_runner_id: Runner ID where the line starts
    :param str to_runner_id: Runner ID where the line ends
    :param int from_sub_lane: Sub-lane index at the start point
    :param int to_sub_lane: Sub-lane index at the end point
    """

    invocation_id: str
    start_time: datetime
    end_time: datetime
    from_status: str
    to_status: str
    color: str
    from_runner_id: str = ""
    to_runner_id: str = ""
    from_sub_lane: int = 0
    to_sub_lane: int = 0


@dataclass
class LaneGroup:
    """
    Visual grouping header for hierarchical runner contexts.

    Represents the parent context that groups child runner lanes together.
    Displays hostname, pid, and parent runner info.

    :param str group_id: Unique identifier (typically parent's runner_id)
    :param str hostname: Host where runners are executing
    :param str runner_cls: Class name of the parent runner
    :param str runner_id: Runner ID of the parent
    :param int pid: Process ID
    :param int thread_id: Thread ID
    :param list[str] child_lane_ids: Ordered list of child lane IDs in this group
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
        """Get truncated runner_id for display."""
        return (
            self.runner_id[:_TRUNCATE_LENGTH]
            if len(self.runner_id) > _TRUNCATE_LENGTH
            else self.runner_id
        )

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

    Each runner context (whether parent or child) gets its own lane.
    Lanes are grouped under LaneGroups for hierarchical display.

    :param str runner_id: Unique identifier for the runner
    :param str hostname: Host where the runner is executing
    :param str label: Display label for the lane (runner class + truncated id)
    :param str color: Base color for this runner's elements
    :param int lane_index: Vertical position index (0-based)
    :param str runner_cls: Class name of the runner
    :param int pid: Process ID
    :param str group_id: ID of the LaneGroup this lane belongs to (empty if standalone)
    :param bool is_group_header: True if this lane represents the parent/group header
    :param list[InvocationBar] bars: Legacy invocation bars (deprecated)
    :param list[StatusPoint] points: Status change points (circles)
    :param list[StatusSegment] segments: Worker-occupying status bars
    :param list[StatusLine] lines: Connecting lines between points
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
        """Get truncated runner_id for display."""
        return (
            self.runner_id[:_TRUNCATE_LENGTH]
            if len(self.runner_id) > _TRUNCATE_LENGTH
            else self.runner_id
        )

    @property
    def details_line(self) -> str:
        """Format detailed runner info (hostname, pid, thread)."""
        return f"{self.hostname} (pid:{self.pid} thread:{self.thread_id})"

    def format_child_details(
        self, parent_hostname: str, parent_pid: int, parent_thread_id: int
    ) -> str:
        """Format child lane details showing only what differs from parent."""
        parts = []
        if self.hostname != parent_hostname:
            parts.append(f"host:{self.hostname}")
        if self.pid != parent_pid:
            parts.append(f"pid:{self.pid}")
        if self.thread_id != parent_thread_id:
            parts.append(f"thread:{self.thread_id}")
        return f" ({', '.join(parts)})" if parts else ""

    @property
    def is_child_lane(self) -> bool:
        """Check if this lane is a child lane within a group."""
        return bool(self.group_id) and not self.is_group_header

    def add_bar(self, bar: InvocationBar) -> None:
        """Add an invocation bar to this lane."""
        self.bars.append(bar)

    def add_point(self, point: StatusPoint) -> None:
        """Add a status point to this lane."""
        self.points.append(point)

    def add_segment(self, segment: StatusSegment) -> None:
        """Add a status segment to this lane."""
        self.segments.append(segment)

    def add_line(self, line: StatusLine) -> None:
        """Add a connecting line to this lane."""
        self.lines.append(line)

    @property
    def max_sub_lane(self) -> int:
        """
        Get the maximum sub-lane index used in this lane.

        :return: Maximum sub-lane index (0 if no sub-lanes used)
        """
        max_point = max((p.sub_lane for p in self.points), default=0)
        max_segment = max((s.sub_lane for s in self.segments), default=0)
        return max(max_point, max_segment)

    def lane_height(self, config: TimelineConfig) -> int:
        """
        Calculate the effective height for this lane based on sub-lanes.

        :param TimelineConfig config: Timeline configuration
        :return: Height in pixels that accommodates all sub-lanes
        """
        num_sub_lanes = self.max_sub_lane + 1
        # Each sub-lane needs bar_height + 2px padding
        return max(config.lane_height, num_sub_lanes * (config.bar_height + 2) + 8)

    def y_position(self, config: TimelineConfig) -> int:
        """
        Calculate y-coordinate for this lane.

        :param TimelineConfig config: Timeline configuration
        :return: Y coordinate in pixels
        """
        return config.top_margin + (
            self.lane_index * (config.lane_height + config.lane_padding)
        )


@dataclass
class TimelineData:
    """
    Complete timeline visualization data.

    Aggregates all data needed to render a timeline SVG, including
    bounds, lanes, groups, and configuration.

    :param TimelineBounds bounds: Time range and coordinate mapping
    :param dict[str, RunnerLane] lanes: Runner lanes indexed by runner_id
    :param dict[str, LaneGroup] groups: Lane groups indexed by group_id
    :param TimelineConfig config: Rendering configuration
    :param list[StatusLine] global_lines: Lines connecting points across lanes
    """

    bounds: TimelineBounds
    lanes: dict[str, RunnerLane] = field(default_factory=dict)
    groups: dict[str, LaneGroup] = field(default_factory=dict)
    config: TimelineConfig = field(default_factory=TimelineConfig)
    global_lines: list[StatusLine] = field(default_factory=list)

    def add_global_line(self, line: StatusLine) -> None:
        """Add a connecting line that may span across lanes."""
        self.global_lines.append(line)

    def get_or_create_group(
        self,
        group_id: str,
        hostname: str,
        runner_cls: str,
        runner_id: str,
        pid: int,
        thread_id: int = 0,
    ) -> LaneGroup:
        """
        Get existing group or create a new one.

        :param str group_id: Unique identifier for the group
        :param str hostname: Host where runners are executing
        :param str runner_cls: Class name of the parent runner
        :param str runner_id: Runner ID of the parent
        :param int pid: Process ID
        :param int thread_id: Thread ID
        :return: The LaneGroup
        """
        if group_id not in self.groups:
            self.groups[group_id] = LaneGroup(
                group_id=group_id,
                hostname=hostname,
                runner_cls=runner_cls,
                runner_id=runner_id,
                pid=pid,
                thread_id=thread_id,
            )
        return self.groups[group_id]

    def lane_y_position(self, lane: RunnerLane) -> int:
        """
        Calculate y-coordinate for a lane, accounting for variable lane heights.

        :param RunnerLane lane: The lane to calculate position for
        :return: Y coordinate in pixels
        """
        sorted_lanes = self.get_sorted_lanes()
        y = self.config.top_margin
        for prev_lane in sorted_lanes:
            if prev_lane.lane_index >= lane.lane_index:
                break
            y += prev_lane.lane_height(self.config) + self.config.lane_padding
        return y

    @property
    def total_height(self) -> int:
        """Calculate total SVG height based on variable lane heights plus legend space."""
        sorted_lanes = self.get_sorted_lanes()
        if not sorted_lanes:
            lanes_height = self.config.top_margin + self.config.lane_height
        else:
            lanes_height = self.config.top_margin
            for i, lane in enumerate(sorted_lanes):
                lanes_height += lane.lane_height(self.config)
                if i < len(sorted_lanes) - 1:
                    lanes_height += self.config.lane_padding
        # Add space for legend at the bottom (enough for 2-3 rows of status items)
        legend_height = 70
        return lanes_height + legend_height

    def get_or_create_lane(
        self,
        runner_id: str,
        runner_info: "RunnerInfo",
        color: str,
        group_id: str = "",
        is_group_header: bool = False,
    ) -> RunnerLane:
        """
        Get existing lane or create a new one for the runner.

        :param str runner_id: Unique identifier for the runner
        :param RunnerInfo runner_info: Full runner information
        :param str color: Base color for this runner
        :param str group_id: ID of the group this lane belongs to (empty if standalone)
        :param bool is_group_header: True if this lane represents the parent/default lane
        :return: The RunnerLane for this runner
        """
        if runner_id not in self.lanes:
            lane_index = len(self.lanes)
            self.lanes[runner_id] = RunnerLane(
                runner_id=runner_id,
                hostname=runner_info.hostname,
                label=runner_info.label,
                color=color,
                lane_index=lane_index,
                runner_cls=runner_info.runner_cls,
                pid=runner_info.pid,
                thread_id=runner_info.thread_id,
                group_id=group_id,
                is_group_header=is_group_header,
            )
            # If this lane belongs to a group, add it to the group's child list
            if group_id and group_id in self.groups:
                self.groups[group_id].add_child_lane(runner_id)
        return self.lanes[runner_id]

    def get_sorted_lanes(self) -> list[RunnerLane]:
        """Get lanes sorted by lane_index."""
        return sorted(self.lanes.values(), key=lambda lane: lane.lane_index)

    def get_lanes_for_group(self, group_id: str) -> list[RunnerLane]:
        """Get all lanes belonging to a group, sorted by lane_index."""
        return sorted(
            [lane for lane in self.lanes.values() if lane.group_id == group_id],
            key=lambda lane: lane.lane_index,
        )

    def get_group_bounds(self, group_id: str) -> tuple[int, int, int]:
        """
        Get the y-position bounds for a lane group.

        :param str group_id: The group ID
        :return: Tuple of (y_start, y_end, total_height)
        """
        group_lanes = self.get_lanes_for_group(group_id)
        if not group_lanes:
            return (0, 0, 0)

        # Get the first and last lane positions
        first_lane = group_lanes[0]
        last_lane = group_lanes[-1]

        y_start = self.lane_y_position(first_lane)
        y_end = self.lane_y_position(last_lane) + last_lane.lane_height(self.config)
        total_height = y_end - y_start

        return (y_start, y_end, total_height)

    def get_sorted_groups(self) -> list[LaneGroup]:
        """Get groups sorted by their first lane's position."""
        groups_with_lanes = []
        for group_id, group in self.groups.items():
            lanes = self.get_lanes_for_group(group_id)
            if lanes:
                first_lane_index = min(lane.lane_index for lane in lanes)
                groups_with_lanes.append((first_lane_index, group))
            else:
                # Group with no lanes yet - add at position based on group order
                groups_with_lanes.append((len(self.lanes), group))

        groups_with_lanes.sort(key=lambda x: x[0])
        return [g for _, g in groups_with_lanes]
