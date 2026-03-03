"""
Element building helpers for the TimelineDataBuilder.

Standalone functions that convert HistoryEntry sequences into
StatusPoint, StatusSegment, and StatusLine objects attached to lanes.
"""

from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING

from pynmon.util.status_colors import is_segment_status
from pynmon.util.svg.elements import (
    SegmentParams,
    create_status_line,
    create_status_point,
    create_status_segment,
)
from pynmon.util.svg.lane_assignment import ElementLaneKey, TimeInterval, VisualElement
from pynmon.util.svg.timeline_data import TimelineData

if TYPE_CHECKING:
    from pynmon.util.svg.builder import HistoryEntry


_FINAL_STATUSES = {"SUCCESS", "FAILED", "CONCURRENCY_CONTROLLED_FINAL", "KILLED"}


def collect_visual_elements(
    history: dict[str, list["HistoryEntry"]], end_time: datetime
) -> list[VisualElement]:
    """Produce all VisualElement objects from accumulated history."""
    elements: list[VisualElement] = []
    for inv_id, entries in history.items():
        by_lane = _group_by_lane(entries)
        for lane_id, lane_entries in by_lane.items():
            elements.extend(_elements_for_lane(inv_id, lane_id, lane_entries, end_time))
    return elements


def _group_by_lane(
    entries: list["HistoryEntry"],
) -> dict[str, list[tuple[datetime, str, str | None]]]:
    """Group entries by lane_id → [(timestamp, status, registered_by)]."""
    by_lane: dict[str, list[tuple[datetime, str, str | None]]] = defaultdict(list)
    for e in entries:
        by_lane[e.runner_info.lane_id].append(
            (e.timestamp, e.status, e.registered_by_inv_id)
        )
    return by_lane


def _elements_for_lane(
    inv_id: str,
    lane_id: str,
    lane_entries: list[tuple[datetime, str, str | None]],
    end_time: datetime,
) -> list[VisualElement]:
    """Create VisualElement list for an invocation on one lane."""
    elements: list[VisualElement] = []
    for i, (ts, status, registered_by) in enumerate(
        sorted(lane_entries, key=lambda e: e[0])
    ):
        status_upper = status.upper()
        if is_segment_status(status_upper):
            seg_end = lane_entries[i + 1][0] if i + 1 < len(lane_entries) else end_time
            interval = TimeInterval(ts, seg_end)
        else:
            interval = TimeInterval(ts, ts)
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


def build_all_elements(
    history: dict[str, list["HistoryEntry"]],
    data: TimelineData,
    end_time: datetime,
    sub_lane_map: dict[ElementLaneKey, int],
) -> None:
    """Attach StatusPoint, StatusSegment, and StatusLine to their lanes."""
    for inv_id, entries in history.items():
        _build_invocation_elements(inv_id, entries, data, end_time, sub_lane_map)


def _build_invocation_elements(
    inv_id: str,
    entries: list["HistoryEntry"],
    data: TimelineData,
    end_time: datetime,
    sub_lane_map: dict[ElementLaneKey, int],
) -> None:
    """Build all visual elements for one invocation."""
    if not entries:
        return
    sorted_entries = sorted(entries, key=lambda e: e.timestamp)
    prev = None
    for order, entry in enumerate(sorted_entries):
        sub = _get_sub_lane(inv_id, entry, sub_lane_map)
        _add_point(data, inv_id, entry, order, sub)
        if prev is not None:
            prev_sub = _get_sub_lane(inv_id, prev, sub_lane_map)
            _maybe_add_segment(data, inv_id, prev, entry, prev_sub)
            _add_line(data, inv_id, prev, entry, prev_sub, sub)
        prev = entry
    _handle_final_segment(data, inv_id, prev, end_time, sub_lane_map)


def _get_sub_lane(
    inv_id: str, entry: "HistoryEntry", sub_lane_map: dict[ElementLaneKey, int]
) -> int:
    """Look up sub-lane for an entry (default 0)."""
    return sub_lane_map.get(
        ElementLaneKey(inv_id, entry.runner_info.lane_id, entry.timestamp), 0
    )


def _add_point(
    data: TimelineData, inv_id: str, entry: "HistoryEntry", order: int, sub_lane: int
) -> None:
    """Create and attach a StatusPoint to the appropriate lane."""
    point = create_status_point(
        inv_id,
        entry.timestamp,
        entry.status.upper(),
        entry.runner_info,
        order,
        sub_lane,
    )
    if lane := data.lanes.get(entry.runner_info.lane_id):
        lane.add_point(point)


def _maybe_add_segment(
    data: TimelineData,
    inv_id: str,
    prev: "HistoryEntry",
    current: "HistoryEntry",
    prev_sub: int,
) -> None:
    """Add a StatusSegment if the previous status was a segment status."""
    status = prev.status.upper()
    if not is_segment_status(status):
        return
    segment = create_status_segment(
        SegmentParams(
            invocation_id=inv_id,
            start_time=prev.timestamp,
            end_time=current.timestamp,
            status=status,
            next_status=current.status.upper(),
            sub_lane=prev_sub,
        )
    )
    if lane := data.lanes.get(prev.runner_info.lane_id):
        lane.add_segment(segment)


def _add_line(
    data: TimelineData,
    inv_id: str,
    prev: "HistoryEntry",
    current: "HistoryEntry",
    prev_sub: int,
    sub: int,
) -> None:
    """Add a StatusLine connecting two consecutive status points."""
    line = create_status_line(
        inv_id,
        prev.timestamp,
        current.timestamp,
        prev.status.upper(),
        current.status.upper(),
        prev.runner_info.lane_id,
        current.runner_info.lane_id,
        prev_sub,
        sub,
    )
    data.add_global_line(line)


def _handle_final_segment(
    data: TimelineData,
    inv_id: str,
    last: "HistoryEntry | None",
    end_time: datetime,
    sub_lane_map: dict[ElementLaneKey, int],
) -> None:
    """Handle an ongoing segment if the last entry is still a segment status."""
    if last is None:
        return
    status = last.status.upper()
    if not is_segment_status(status) or status in _FINAL_STATUSES:
        return
    sub_lane = _get_sub_lane(inv_id, last, sub_lane_map)
    segment = create_status_segment(
        SegmentParams(
            inv_id, last.timestamp, end_time, status, sub_lane=sub_lane, is_ongoing=True
        )
    )
    if lane := data.lanes.get(last.runner_info.lane_id):
        lane.add_segment(segment)
