"""
Timeline data builder for SVG visualization.

Transforms InvocationHistory batches into TimelineData for rendering.
Uses clean, focused functions with minimal nesting.
"""

import logging
import time as _time
from collections import defaultdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING, NamedTuple

from pynenc.runner.runner_context import RunnerContext
from pynmon.util.colors import ColorScheme
from pynmon.util.status_colors import is_segment_status
from pynmon.util.svg.elements import (
    create_status_line,
    create_status_point,
    create_status_segment,
    SegmentParams,
)
from pynmon.util.svg.lane_assignment import (
    ElementLaneKey,
    TimeInterval,
    VisualElement,
    assign_lanes,
)
from pynmon.util.svg.models import TimelineBounds, TimelineConfig, TimelineData
from pynmon.util.svg.runner_info import RunnerInfo

if TYPE_CHECKING:
    from pynenc.state_backend.base_state_backend import InvocationHistory

logger = logging.getLogger("pynmon.util.svg.builder")


class HistoryEntry(NamedTuple):
    """A single history entry for an invocation."""

    timestamp: datetime
    status: str
    runner_info: RunnerInfo
    registered_by_inv_id: str | None


_EXTERNAL_CLS = "ExternalRunner"
_COLLAPSED_EXTERNAL_ID = "__collapsed_external_runners__"


class TimelineDataBuilder:
    """
    Builds TimelineData from InvocationHistory batches.

    Usage:
        builder = TimelineDataBuilder()
        for batch in state_backend.iter_history_in_timerange(start, end):
            builder.add_history_batch(batch)
        timeline_data = builder.build()

    The builder tracks:
    - Time bounds (min/max timestamps)
    - Runner information for lane creation (hierarchical: runner → queue consumers)
    - Invocation status transitions for bar creation
    """

    def __init__(
        self,
        config: TimelineConfig | None = None,
        color_scheme: ColorScheme | None = None,
        collapse_external: bool = True,
    ) -> None:
        """
        Initialize the builder.

        :param TimelineConfig | None config: Configuration for rendering
        :param ColorScheme | None color_scheme: Color scheme for runner colors
        :param bool collapse_external: Collapse standalone external runners into one lane
        """
        self.config = config or TimelineConfig()
        self.color_scheme = color_scheme or ColorScheme()
        self.collapse_external = collapse_external

        # Track time bounds
        self._min_time: datetime | None = None
        self._max_time: datetime | None = None

        # Track history entries by invocation_id for segment building
        self._history_by_invocation: dict[str, list[HistoryEntry]] = defaultdict(list)

        # Track runners for lane creation: lane_id -> RunnerInfo
        self._runners: dict[str, RunnerInfo] = {}
        # Track child runners per parent: parent_runner_id -> set of child runner_ids
        self._child_runners_by_parent: dict[str, set[str]] = defaultdict(set)

    def add_history_batch(
        self,
        batch: list["InvocationHistory"],
        runner_contexts: dict[str, "RunnerContext"],
    ) -> None:
        """
        Add a batch of InvocationHistory entries to the builder.

        :param list[InvocationHistory] batch: Batch of history entries
        :param dict[str, RunnerContext] runner_contexts: Mapping of runner_context_id to RunnerContext
        """
        for history in batch:
            try:
                runner_context = runner_contexts[history.runner_context_id]
                self._add_history_entry(history, runner_context)
            except (AttributeError, TypeError, ValueError):
                continue

    def _add_history_entry(
        self, history: "InvocationHistory", runner_context: "RunnerContext"
    ) -> None:
        """Process a single history entry."""
        runner_info = RunnerInfo.from_context(runner_context)
        runner_info = self._maybe_collapse_external(runner_info)
        if not self._is_valid_entry(history, runner_info):
            return

        self._update_time_bounds(history.timestamp)
        self._track_runner(runner_info)
        self._store_history_entry(history, runner_info)

    def _maybe_collapse_external(self, info: RunnerInfo) -> RunnerInfo:
        """Collapse standalone external runners into one shared lane.

        :param RunnerInfo info: Original runner info
        :return: Remapped info if collapsible, otherwise unchanged
        """
        if not self.collapse_external:
            return info
        if _EXTERNAL_CLS not in info.runner_cls or info.has_parent:
            return info
        return RunnerInfo(
            runner_cls=info.runner_cls,
            runner_id=_COLLAPSED_EXTERNAL_ID,
            hostname="(collapsed)",
            pid=0,
            thread_id=0,
        )

    def _is_valid_entry(
        self, history: "InvocationHistory", runner_info: RunnerInfo
    ) -> bool:
        """Check if history entry has all required fields."""
        return all(
            [
                history.timestamp,
                history.invocation_id,
                history.status_record.status.value,
                runner_info.runner_id,
            ]
        )

    def _update_time_bounds(self, timestamp: datetime) -> None:
        """Update min/max time bounds."""
        if self._min_time is None or timestamp < self._min_time:
            self._min_time = timestamp
        if self._max_time is None or timestamp > self._max_time:
            self._max_time = timestamp

    def _track_runner(self, runner_info: RunnerInfo) -> None:
        """Track runner for lane creation."""
        if runner_info.lane_id not in self._runners:
            self._runners[runner_info.lane_id] = runner_info

        if runner_info.has_parent and runner_info.parent_runner_id:
            self._child_runners_by_parent[runner_info.parent_runner_id].add(
                runner_info.runner_id
            )

    def _store_history_entry(
        self, history: "InvocationHistory", runner_info: RunnerInfo
    ) -> None:
        """Store history entry for later processing."""
        registered_by = getattr(history, "registered_by_inv_id", None)

        self._history_by_invocation[history.invocation_id].append(
            HistoryEntry(
                timestamp=history.timestamp,
                status=history.status_record.status.value,
                runner_info=runner_info,
                registered_by_inv_id=registered_by,
            )
        )

    def build(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> TimelineData:
        """
        Build the final TimelineData.

        :param start_time: Start time (defaults to min observed time)
        :param end_time: End time (defaults to max observed time)
        :return: Complete TimelineData ready for rendering
        """
        if self._min_time is None or self._max_time is None:
            return self._create_empty_timeline(start_time, end_time)

        actual_start = start_time or self._min_time
        actual_end = end_time or self._max_time
        timeline_data = self._create_timeline_data(actual_start, actual_end)
        n_inv = len(self._history_by_invocation)

        t0 = _time.monotonic()
        groups_info, child_runners = self._collect_groups_info()
        self._create_lanes(timeline_data, groups_info, child_runners)
        logger.debug(
            f"build: {len(timeline_data.lanes)} lanes for {n_inv} invocations "
            f"in {_time.monotonic() - t0:.3f}s"
        )

        t1 = _time.monotonic()
        sub_lane_map = self._compute_sub_lanes(actual_end)
        logger.debug(
            f"build: sub-lane assignment ({len(sub_lane_map)} keys) "
            f"in {_time.monotonic() - t1:.3f}s"
        )

        t2 = _time.monotonic()
        self._build_all_elements(timeline_data, actual_end, sub_lane_map)
        logger.debug(f"build: elements built in {_time.monotonic() - t2:.3f}s")

        return timeline_data

    def _create_empty_timeline(
        self, start_time: datetime | None, end_time: datetime | None
    ) -> TimelineData:
        """Create empty timeline when no data available."""
        now = datetime.now(UTC)
        bounds = TimelineBounds(
            start_time=start_time or now,
            end_time=end_time or now,
            config=self.config,
        )
        return TimelineData(bounds=bounds, config=self.config)

    def _create_timeline_data(
        self, start_time: datetime, end_time: datetime
    ) -> TimelineData:
        """Create TimelineData with proper bounds."""
        bounds = TimelineBounds(
            start_time=start_time,
            end_time=end_time,
            config=self.config,
        )
        return TimelineData(bounds=bounds, config=self.config)

    def _collect_groups_info(
        self,
    ) -> tuple[dict[str, RunnerInfo], dict[str, list[str]]]:
        """Collect group info and child runners by group."""
        groups_info: dict[str, RunnerInfo] = {}
        child_runners: dict[str, list[str]] = defaultdict(list)

        for runner_id, runner_info in self._runners.items():
            group_id = runner_info.group_id

            if runner_info.has_parent:
                child_runners[group_id].append(runner_id)
                if group_id not in groups_info:
                    groups_info[group_id] = self._create_synthetic_parent(runner_info)
            else:
                groups_info[group_id] = runner_info

        return groups_info, child_runners

    def _create_synthetic_parent(self, child_info: RunnerInfo) -> RunnerInfo:
        """Create synthetic parent info from child's parent references."""
        return RunnerInfo(
            runner_cls=child_info.parent_runner_cls or "Unknown",
            runner_id=child_info.group_id,
            hostname=child_info.hostname,
            pid=child_info.pid,
            thread_id=child_info.thread_id,
        )

    def _create_lanes(
        self,
        timeline_data: TimelineData,
        groups_info: dict[str, RunnerInfo],
        child_runners: dict[str, list[str]],
    ) -> None:
        """Create groups and lanes for all runners."""
        for group_id, parent_info in groups_info.items():
            color = self.color_scheme.get_parent_color(parent_info.runner_cls, group_id)
            self._create_group_and_lanes(
                timeline_data,
                group_id,
                parent_info,
                color,
                child_runners.get(group_id, []),
            )

    def _create_group_and_lanes(
        self,
        data: TimelineData,
        group_id: str,
        parent_info: RunnerInfo,
        color: str,
        children: list[str],
    ) -> None:
        """Create group and its lanes."""
        data.get_or_create_group(
            group_id=group_id,
            hostname=parent_info.hostname,
            runner_cls=parent_info.runner_cls,
            runner_id=parent_info.runner_id,
            pid=parent_info.pid,
            thread_id=parent_info.thread_id,
        )

        has_children = bool(children)
        parent_runner_info = self._runners.get(group_id)

        if has_children:
            runner_for_lane = parent_runner_info or parent_info
            data.get_or_create_lane(
                runner_id=group_id,
                runner_info=runner_for_lane,
                color=color,
                group_id=group_id,
                is_group_header=True,
            )
        elif parent_runner_info:
            data.get_or_create_lane(
                runner_id=group_id,
                runner_info=parent_runner_info,
                color=color,
                group_id="",
                is_group_header=False,
            )

        for child_id in sorted(children):
            if child_info := self._runners.get(child_id):
                data.get_or_create_lane(
                    runner_id=child_id,
                    runner_info=child_info,
                    color=color,
                    group_id=group_id,
                    is_group_header=False,
                )

    def _build_all_elements(
        self,
        timeline_data: TimelineData,
        end_time: datetime,
        sub_lane_map: dict[ElementLaneKey, int],
    ) -> None:
        """Build visualization elements for all invocations."""
        for inv_id, entries in self._history_by_invocation.items():
            self._build_invocation_elements(
                inv_id, entries, end_time, timeline_data, sub_lane_map
            )

    def _compute_sub_lanes(self, end_time: datetime) -> dict[ElementLaneKey, int]:
        """Compute sub-lane assignments for visual elements."""
        elements = self._collect_visual_elements(end_time)
        return assign_lanes(elements)

    def _collect_visual_elements(self, end_time: datetime) -> list[VisualElement]:
        """Collect all visual elements from history entries."""
        elements: list[VisualElement] = []

        for inv_id, entries in self._history_by_invocation.items():
            entries_by_lane = self._group_entries_by_lane(entries)
            for lane_id, lane_entries in entries_by_lane.items():
                elements.extend(
                    self._create_lane_elements(inv_id, lane_id, lane_entries, end_time)
                )

        return elements

    def _group_entries_by_lane(
        self, entries: list[HistoryEntry]
    ) -> dict[str, list[tuple[datetime, str, str | None]]]:
        """Group history entries by lane_id."""
        entries_by_lane: dict[str, list[tuple[datetime, str, str | None]]] = (
            defaultdict(list)
        )
        for entry in entries:
            entries_by_lane[entry.runner_info.lane_id].append(
                (entry.timestamp, entry.status, entry.registered_by_inv_id)
            )
        return entries_by_lane

    def _create_lane_elements(
        self,
        inv_id: str,
        lane_id: str,
        lane_entries: list[tuple[datetime, str, str | None]],
        end_time: datetime,
    ) -> list[VisualElement]:
        """Create visual elements for a lane."""
        elements: list[VisualElement] = []
        sorted_entries = sorted(lane_entries, key=lambda e: e[0])

        for i, (_ts, status, registered_by) in enumerate(sorted_entries):
            status_upper = status.upper()
            interval = self._compute_interval(sorted_entries, i, status_upper, end_time)

            elements.append(
                VisualElement(
                    invocation_id=inv_id,
                    runner_id=lane_id,
                    interval=interval,
                    status=status_upper,
                    registered_by_inv_id=registered_by
                    if status_upper == "REGISTERED"
                    else None,
                )
            )

        return elements

    def _compute_interval(
        self,
        entries: list[tuple[datetime, str, str | None]],
        index: int,
        status: str,
        end_time: datetime,
    ) -> TimeInterval:
        """Compute time interval for an entry."""
        ts = entries[index][0]

        if is_segment_status(status):
            seg_end = entries[index + 1][0] if index + 1 < len(entries) else end_time
            return TimeInterval(ts, seg_end)

        return TimeInterval(ts, ts)

    def _build_invocation_elements(
        self,
        inv_id: str,
        entries: list[HistoryEntry],
        end_time: datetime,
        data: TimelineData,
        sub_lane_map: dict[ElementLaneKey, int],
    ) -> None:
        """Build points, segments, and lines for an invocation."""
        if not entries:
            return

        sorted_entries = sorted(entries, key=lambda e: e.timestamp)
        prev_entry: HistoryEntry | None = None

        for order, entry in enumerate(sorted_entries):
            sub_lane = self._get_sub_lane(inv_id, entry, sub_lane_map)
            self._add_point_to_lane(data, inv_id, entry, order, sub_lane)

            if prev_entry:
                prev_sub_lane = self._get_sub_lane(inv_id, prev_entry, sub_lane_map)
                self._maybe_add_segment(data, inv_id, prev_entry, entry, prev_sub_lane)
                self._add_connecting_line(
                    data, inv_id, prev_entry, entry, prev_sub_lane, sub_lane
                )

            prev_entry = entry

        self._handle_final_segment(data, inv_id, prev_entry, end_time, sub_lane_map)

    def _get_sub_lane(
        self, inv_id: str, entry: HistoryEntry, sub_lane_map: dict[ElementLaneKey, int]
    ) -> int:
        """Get sub-lane for an entry."""
        key = ElementLaneKey(inv_id, entry.runner_info.lane_id, entry.timestamp)
        return sub_lane_map.get(key, 0)

    def _add_point_to_lane(
        self,
        data: TimelineData,
        inv_id: str,
        entry: HistoryEntry,
        order: int,
        sub_lane: int,
    ) -> None:
        """Add a status point to the appropriate lane."""
        point = create_status_point(
            invocation_id=inv_id,
            timestamp=entry.timestamp,
            status=entry.status.upper(),
            runner_info=entry.runner_info,
            order=order,
            sub_lane=sub_lane,
        )
        if lane := data.lanes.get(entry.runner_info.lane_id):
            lane.add_point(point)

    def _maybe_add_segment(
        self,
        data: TimelineData,
        inv_id: str,
        prev_entry: HistoryEntry,
        current_entry: HistoryEntry,
        prev_sub_lane: int,
    ) -> None:
        """Add segment if previous status was a segment status."""
        prev_status = prev_entry.status.upper()
        if not is_segment_status(prev_status):
            return

        segment = create_status_segment(
            SegmentParams(
                invocation_id=inv_id,
                start_time=prev_entry.timestamp,
                end_time=current_entry.timestamp,
                status=prev_status,
                next_status=current_entry.status.upper(),
                sub_lane=prev_sub_lane,
            )
        )

        if lane := data.lanes.get(prev_entry.runner_info.lane_id):
            lane.add_segment(segment)

    def _add_connecting_line(
        self,
        data: TimelineData,
        inv_id: str,
        prev_entry: HistoryEntry,
        entry: HistoryEntry,
        prev_sub_lane: int,
        sub_lane: int,
    ) -> None:
        """Add connecting line between consecutive status points."""
        line = create_status_line(
            invocation_id=inv_id,
            start_time=prev_entry.timestamp,
            end_time=entry.timestamp,
            from_status=prev_entry.status.upper(),
            to_status=entry.status.upper(),
            from_runner_id=prev_entry.runner_info.lane_id,
            to_runner_id=entry.runner_info.lane_id,
            from_sub_lane=prev_sub_lane,
            to_sub_lane=sub_lane,
        )
        data.add_global_line(line)

    def _handle_final_segment(
        self,
        data: TimelineData,
        inv_id: str,
        last_entry: HistoryEntry | None,
        end_time: datetime,
        sub_lane_map: dict[ElementLaneKey, int],
    ) -> None:
        """Handle ongoing segment for the last entry if applicable."""
        if last_entry is None:
            return

        status = last_entry.status.upper()
        final_statuses = {"SUCCESS", "FAILED", "CONCURRENCY_CONTROLLED_FINAL", "KILLED"}

        if not is_segment_status(status) or status in final_statuses:
            return

        sub_lane = self._get_sub_lane(inv_id, last_entry, sub_lane_map)
        segment = create_status_segment(
            SegmentParams(
                invocation_id=inv_id,
                start_time=last_entry.timestamp,
                end_time=end_time,
                status=status,
                sub_lane=sub_lane,
                is_ongoing=True,
            )
        )

        if lane := data.lanes.get(last_entry.runner_info.lane_id):
            lane.add_segment(segment)

    def clear(self) -> None:
        """Reset the builder for reuse."""
        self._min_time = None
        self._max_time = None
        self._history_by_invocation.clear()
        self._runners.clear()
        self._child_runners_by_parent.clear()
