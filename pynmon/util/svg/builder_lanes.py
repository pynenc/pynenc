"""
Lane creation helpers for the TimelineDataBuilder.

Standalone functions that build LaneGroup and RunnerLane objects in a
TimelineData from collected runner information.
"""

from typing import TYPE_CHECKING

from pynmon.util.colors import ColorScheme
from pynmon.util.svg.runner_info import RunnerInfo
from pynmon.util.svg.timeline_data import TimelineData

if TYPE_CHECKING:
    pass


def create_lanes(
    timeline_data: TimelineData,
    groups_info: dict[str, RunnerInfo],
    child_runners: dict[str, list[str]],
    runners: dict[str, RunnerInfo],
    color_scheme: ColorScheme,
) -> None:
    """Create all groups and lanes from runner information."""
    for group_id, parent_info in groups_info.items():
        color = color_scheme.get_parent_color(parent_info.runner_cls, group_id)
        children = child_runners.get(group_id, [])
        _create_group_and_lanes(
            timeline_data, group_id, parent_info, color, children, runners
        )


def _create_group_and_lanes(
    data: TimelineData,
    group_id: str,
    parent_info: RunnerInfo,
    color: str,
    children: list[str],
    runners: dict[str, RunnerInfo],
) -> None:
    """Create a group header and its child lanes."""
    data.get_or_create_group(
        group_id=group_id,
        hostname=parent_info.hostname,
        runner_cls=parent_info.runner_cls,
        runner_id=parent_info.runner_id,
        pid=parent_info.pid,
        thread_id=parent_info.thread_id,
    )
    parent_runner_info = runners.get(group_id)
    if children:
        _create_parent_lane(data, group_id, parent_runner_info or parent_info, color)
    elif parent_runner_info:
        data.get_or_create_lane(
            runner_id=group_id, runner_info=parent_runner_info, color=color
        )
    for child_id in sorted(children):
        if child_info := runners.get(child_id):
            data.get_or_create_lane(
                runner_id=child_id,
                runner_info=child_info,
                color=color,
                group_id=group_id,
            )


def _create_parent_lane(
    data: TimelineData, group_id: str, runner_info: RunnerInfo, color: str
) -> None:
    """Create the header lane for a group that has child lanes."""
    data.get_or_create_lane(
        runner_id=group_id,
        runner_info=runner_info,
        color=color,
        group_id=group_id,
        is_group_header=True,
    )


def collect_groups_info(
    runners: dict[str, RunnerInfo],
) -> tuple[dict[str, RunnerInfo], dict[str, list[str]]]:
    """
    Partition runners into group-parent info and child runner lists.

    :return: (groups_info dict, child_runners dict)
    """
    groups_info: dict[str, RunnerInfo] = {}
    child_runners: dict[str, list[str]] = {}

    for runner_id, runner_info in runners.items():
        group_id = runner_info.group_id
        if runner_info.has_parent:
            child_runners.setdefault(group_id, []).append(runner_id)
            if group_id not in groups_info:
                groups_info[group_id] = _synthetic_parent(runner_info)
        else:
            groups_info[group_id] = runner_info

    return groups_info, child_runners


def _synthetic_parent(child: RunnerInfo) -> RunnerInfo:
    """Create a synthetic parent RunnerInfo from a child's parent references."""
    return RunnerInfo(
        runner_cls=child.parent_runner_cls or "Unknown",
        runner_id=child.group_id,
        hostname=child.hostname,
        pid=child.pid,
        thread_id=child.thread_id,
    )
