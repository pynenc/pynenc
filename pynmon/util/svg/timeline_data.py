"""
TimelineData: root container for all timeline visualization data.

Aggregates bounds, lanes, groups, and configuration. Provides
factory methods to create or retrieve lanes and groups.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pynmon.util.svg.bounds import TimelineBounds
from pynmon.util.svg.config import TimelineConfig
from pynmon.util.svg.lane_models import LaneGroup, RunnerLane
from pynmon.util.svg.status_elements import StatusLine

if TYPE_CHECKING:
    from pynmon.util.svg.runner_info import RunnerInfo


@dataclass
class TimelineData:
    """
    Complete timeline visualization data.

    :param TimelineBounds bounds: Time range and coordinate mapping
    :param dict[str, RunnerLane] lanes: Runner lanes indexed by runner_id
    :param dict[str, LaneGroup] groups: Lane groups indexed by group_id
    :param TimelineConfig config: Rendering configuration
    :param list[StatusLine] global_lines: Lines that may span across lanes
    """

    bounds: TimelineBounds
    lanes: dict[str, RunnerLane] = field(default_factory=dict)
    groups: dict[str, LaneGroup] = field(default_factory=dict)
    config: TimelineConfig = field(default_factory=TimelineConfig)
    global_lines: list[StatusLine] = field(default_factory=list)

    def add_global_line(self, line: StatusLine) -> None:
        """Add a line that may span across lanes."""
        self.global_lines.append(line)

    def get_sorted_lanes(self) -> list[RunnerLane]:
        """Lanes sorted by lane_index."""
        return sorted(self.lanes.values(), key=lambda lane: lane.lane_index)

    def get_lanes_for_group(self, group_id: str) -> list[RunnerLane]:
        """Lanes in a group sorted by lane_index."""
        return sorted(
            [lane for lane in self.lanes.values() if lane.group_id == group_id],
            key=lambda lane: lane.lane_index,
        )

    def get_sorted_groups(self) -> list[LaneGroup]:
        """Groups sorted by their first lane's position."""
        items = []
        for gid, group in self.groups.items():
            lanes = self.get_lanes_for_group(gid)
            idx = min((lane.lane_index for lane in lanes), default=len(self.lanes))
            items.append((idx, group))
        return [g for _, g in sorted(items, key=lambda x: x[0])]

    def lane_y_position(self, lane: RunnerLane) -> int:
        """Y position accounting for variable lane heights above this lane."""
        y = self.config.top_margin
        for prev in self.get_sorted_lanes():
            if prev.lane_index >= lane.lane_index:
                break
            y += prev.lane_height(self.config) + self.config.lane_padding
        return y

    def get_group_bounds(self, group_id: str) -> tuple[int, int, int]:
        """Return (y_start, y_end, total_height) for a lane group."""
        lanes = self.get_lanes_for_group(group_id)
        if not lanes:
            return (0, 0, 0)
        y_start = self.lane_y_position(lanes[0])
        y_end = self.lane_y_position(lanes[-1]) + lanes[-1].lane_height(self.config)
        return y_start, y_end, y_end - y_start

    @property
    def total_height(self) -> int:
        """Total SVG height including legend space."""
        lanes = self.get_sorted_lanes()
        if not lanes:
            return self.config.top_margin + self.config.lane_height + 70
        h = self.config.top_margin
        for i, lane in enumerate(lanes):
            h += lane.lane_height(self.config)
            if i < len(lanes) - 1:
                h += self.config.lane_padding
        return h + 70  # legend space

    def get_or_create_group(
        self,
        group_id: str,
        hostname: str,
        runner_cls: str,
        runner_id: str,
        pid: int,
        thread_id: int = 0,
    ) -> LaneGroup:
        """Get existing group or create a new one."""
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

    def get_or_create_lane(
        self,
        runner_id: str,
        runner_info: "RunnerInfo",
        color: str,
        group_id: str = "",
        is_group_header: bool = False,
    ) -> RunnerLane:
        """Get existing lane or create a new one for the runner."""
        if runner_id not in self.lanes:
            self.lanes[runner_id] = RunnerLane(
                runner_id=runner_id,
                hostname=runner_info.hostname,
                label=runner_info.label,
                color=color,
                lane_index=len(self.lanes),
                runner_cls=runner_info.runner_cls,
                pid=runner_info.pid,
                thread_id=runner_info.thread_id,
                group_id=group_id,
                is_group_header=is_group_header,
            )
            if group_id and group_id in self.groups:
                self.groups[group_id].add_child_lane(runner_id)
        return self.lanes[runner_id]
