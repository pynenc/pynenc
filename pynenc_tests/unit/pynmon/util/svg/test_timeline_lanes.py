"""
Unit tests for RunnerLane and TimelineData dataclasses.

Tests lane management, bar handling, and timeline data aggregation.
"""

from datetime import UTC, datetime, timedelta


from pynmon.util.svg.builder import RunnerInfo
from pynmon.util.svg.models import (
    InvocationBar,
    RunnerLane,
    TimelineBounds,
    TimelineConfig,
    TimelineData,
)


# =============================================================================
# RunnerLane Tests
# =============================================================================


def test_lane_creation() -> None:
    """Test basic lane creation."""
    lane = RunnerLane(
        runner_id="ThreadRunner@host1-1234",
        hostname="host1",
        label="ThreadRunner",
        color="#3498db",
        lane_index=0,
    )
    assert lane.runner_id == "ThreadRunner@host1-1234"
    assert lane.hostname == "host1"
    assert lane.label == "ThreadRunner"
    assert lane.bars == []


def test_lane_add_bar() -> None:
    """Test adding bars to a lane."""
    lane = RunnerLane(
        runner_id="runner1",
        hostname="host1",
        label="Runner 1",
        color="#3498db",
    )
    start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    bar = InvocationBar(
        invocation_id="inv-1",
        start_time=start,
        end_time=start + timedelta(minutes=5),
        status="RUNNING",
        color="#3498db",
    )
    lane.add_bar(bar)
    assert len(lane.bars) == 1
    assert lane.bars[0] == bar


def test_lane_y_position() -> None:
    """Test y-position calculation."""
    config = TimelineConfig(top_margin=50, lane_height=40, lane_padding=4)

    lane0 = RunnerLane(
        runner_id="r0", hostname="h", label="L0", color="#000", lane_index=0
    )
    lane1 = RunnerLane(
        runner_id="r1", hostname="h", label="L1", color="#000", lane_index=1
    )
    lane2 = RunnerLane(
        runner_id="r2", hostname="h", label="L2", color="#000", lane_index=2
    )

    assert lane0.y_position(config) == 50  # top_margin
    assert lane1.y_position(config) == 94  # 50 + (1 * 44)
    assert lane2.y_position(config) == 138  # 50 + (2 * 44)


# =============================================================================
# TimelineData Tests
# =============================================================================


def _create_timeline_data() -> TimelineData:
    """Create a standard TimelineData for testing."""
    start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(2025, 1, 1, 1, 0, 0, tzinfo=UTC)
    config = TimelineConfig()
    bounds = TimelineBounds(start_time=start, end_time=end, config=config)
    return TimelineData(bounds=bounds, config=config)


def test_timeline_total_height_empty() -> None:
    """Test height with no lanes."""
    timeline_data = _create_timeline_data()
    # With no lanes, should have at least space for one lane plus legend
    expected = timeline_data.config.top_margin + timeline_data.config.lane_height + 70
    assert timeline_data.total_height == expected


def test_timeline_total_height_with_lanes() -> None:
    """Test height calculation with multiple lanes."""
    timeline_data = _create_timeline_data()
    timeline_data.get_or_create_lane(
        "r1", RunnerInfo("Runner", "r1", "host1", 1001), "#aaa"
    )
    timeline_data.get_or_create_lane(
        "r2", RunnerInfo("Runner", "r2", "host2", 1002), "#bbb"
    )
    timeline_data.get_or_create_lane(
        "r3", RunnerInfo("Runner", "r3", "host3", 1003), "#ccc"
    )

    config = timeline_data.config
    # 3 lanes: top_margin + (3 * lane_height) + (2 * lane_padding) + legend_height
    expected = (
        config.top_margin + (3 * config.lane_height) + (2 * config.lane_padding) + 70
    )
    assert timeline_data.total_height == expected


def test_timeline_get_or_create_lane_creates_new() -> None:
    """Test creating a new lane."""
    timeline_data = _create_timeline_data()
    runner_info = RunnerInfo("TestRunner", "runner1", "host1", 1001)
    lane = timeline_data.get_or_create_lane("runner1", runner_info, "#3498db")
    assert lane.runner_id == "runner1"
    assert lane.hostname == "host1"
    assert lane.color == "#3498db"
    assert lane.lane_index == 0
    assert lane.label == "TestRunner(runner1)"


def test_timeline_get_or_create_lane_returns_existing() -> None:
    """Test that same runner_id returns existing lane."""
    timeline_data = _create_timeline_data()
    runner_info = RunnerInfo("TestRunner", "runner1", "host1", 1001)
    lane1 = timeline_data.get_or_create_lane("runner1", runner_info, "#3498db")
    lane2 = timeline_data.get_or_create_lane("runner1", runner_info, "#different")
    assert lane1 is lane2
    assert lane1.color == "#3498db"  # Original color preserved


def test_timeline_get_or_create_lane_increments_index() -> None:
    """Test that lane indices increment correctly."""
    timeline_data = _create_timeline_data()
    lane1 = timeline_data.get_or_create_lane(
        "r1", RunnerInfo("Runner", "r1", "h1", 1), "#aaa"
    )
    lane2 = timeline_data.get_or_create_lane(
        "r2", RunnerInfo("Runner", "r2", "h2", 2), "#bbb"
    )
    lane3 = timeline_data.get_or_create_lane(
        "r3", RunnerInfo("Runner", "r3", "h3", 3), "#ccc"
    )
    assert lane1.lane_index == 0
    assert lane2.lane_index == 1
    assert lane3.lane_index == 2


def test_timeline_get_sorted_lanes() -> None:
    """Test lanes are returned sorted by index."""
    timeline_data = _create_timeline_data()
    timeline_data.get_or_create_lane("r3", RunnerInfo("Runner", "r3", "h3", 3), "#ccc")
    timeline_data.get_or_create_lane("r1", RunnerInfo("Runner", "r1", "h1", 1), "#aaa")
    timeline_data.get_or_create_lane("r2", RunnerInfo("Runner", "r2", "h2", 2), "#bbb")

    sorted_lanes = timeline_data.get_sorted_lanes()
    assert [lane.runner_id for lane in sorted_lanes] == ["r3", "r1", "r2"]
    assert [lane.lane_index for lane in sorted_lanes] == [0, 1, 2]
