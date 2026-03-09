"""
Unit tests for TimelineDataBuilder.

Tests the builder's ability to transform InvocationHistory batches
into TimelineData structures for SVG rendering.
"""

from datetime import UTC, datetime, timedelta

from pynenc.invocation.status import InvocationStatus, InvocationStatusRecord
from pynenc.runner.runner_context import RunnerContext
from pynenc.state_backend.base_state_backend import InvocationHistory
from pynmon.util.status_colors import STATUS_COLORS
from pynmon.util.svg.builder import TimelineDataBuilder
from pynmon.util.svg.models import TimelineConfig


def create_runner_context(runner_id: str, hostname: str) -> RunnerContext:
    # Create runner_context that simulates RunnerContext without parent
    return RunnerContext(
        runner_cls="ThreadRunner",
        runner_id=runner_id,
        parent_ctx=None,
        hostname=hostname,
        pid=1234,
        thread_id=1,
    )


def create_history(
    invocation_id: str,
    status: InvocationStatus,
    runner_context_id: str,
    timestamp: datetime,
) -> InvocationHistory:
    """Create a mock InvocationHistory object."""
    inv_history = InvocationHistory(
        invocation_id=invocation_id,
        status_record=InvocationStatusRecord(status=status),
        runner_context_id=runner_context_id,
    )
    inv_history._timestamp = timestamp
    return inv_history


def test_empty_build() -> None:
    """Building with no data returns empty timeline."""
    builder = TimelineDataBuilder()
    data = builder.build()

    assert len(data.lanes) == 0
    assert data.bounds.duration_seconds == 0


def test_single_history_entry() -> None:
    """Building with a single history entry creates one lane."""
    builder = TimelineDataBuilder()
    timestamp = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

    runner_ctx = create_runner_context("ThreadRunner@host1-1234", "host1")
    history = create_history(
        invocation_id="inv-123",
        status=InvocationStatus.RUNNING,
        runner_context_id=runner_ctx.runner_id,
        timestamp=timestamp,
    )
    builder.add_history_batch([history], {runner_ctx.runner_id: runner_ctx})
    data = builder.build()

    assert len(data.lanes) == 1
    assert "ThreadRunner@host1-1234" in data.lanes
    lane = data.lanes["ThreadRunner@host1-1234"]
    assert lane.hostname == "host1"
    # RUNNING is a segment status, so we should have 1 point and 1 segment
    assert len(lane.points) == 1
    assert len(lane.segments) == 1
    assert lane.points[0].status == "RUNNING"


def test_multiple_runners() -> None:
    """Multiple runners create separate lanes."""
    builder = TimelineDataBuilder()
    base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    runner_ctx_1 = create_runner_context("runner1@host1", "host1")
    runner_ctx_2 = create_runner_context("runner2@host2", "host2")
    histories = [
        create_history(
            "inv-1", InvocationStatus.RUNNING, runner_ctx_1.runner_id, base_time
        ),
        create_history(
            "inv-2",
            InvocationStatus.RUNNING,
            runner_ctx_2.runner_id,
            base_time + timedelta(seconds=1),
        ),
    ]
    runner_dict = {
        runner_ctx_1.runner_id: runner_ctx_1,
        runner_ctx_2.runner_id: runner_ctx_2,
    }

    builder.add_history_batch(histories, runner_dict)
    data = builder.build()

    assert len(data.lanes) == 2
    assert "runner1@host1" in data.lanes
    assert "runner2@host2" in data.lanes


def test_status_transitions() -> None:
    """Status transitions create multiple segments."""
    builder = TimelineDataBuilder()
    base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

    runner_ctx = create_runner_context("runner1@host1", "host1")
    histories = [
        create_history(
            "inv-1", InvocationStatus.PENDING, runner_ctx.runner_id, base_time
        ),
        create_history(
            "inv-1",
            InvocationStatus.RUNNING,
            runner_ctx.runner_id,
            base_time + timedelta(seconds=10),
        ),
        create_history(
            "inv-1",
            InvocationStatus.SUCCESS,
            runner_ctx.runner_id,
            base_time + timedelta(seconds=60),
        ),
    ]
    runner_dict = {runner_ctx.runner_id: runner_ctx}

    builder.add_history_batch(histories, runner_dict)
    data = builder.build()

    lane = data.lanes["runner1@host1"]
    # 3 status changes = 3 points
    assert len(lane.points) == 3
    # PENDING and RUNNING are segment statuses, SUCCESS is final
    assert len(lane.segments) == 2

    # Check status progression in points
    statuses = [point.status for point in lane.points]
    assert statuses == ["PENDING", "RUNNING", "SUCCESS"]


def test_time_bounds_computed() -> None:
    """Time bounds are computed from history entries."""
    builder = TimelineDataBuilder()
    start = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    end = datetime(2025, 1, 1, 13, 0, 0, tzinfo=UTC)

    runner_ctx_1 = create_runner_context("runner1@host1", "host1")
    runner_ctx_2 = create_runner_context("runner2@host2", "host2")
    histories = [
        create_history(
            "inv-1", InvocationStatus.RUNNING, runner_ctx_1.runner_id, start
        ),
        create_history("inv-2", InvocationStatus.RUNNING, runner_ctx_2.runner_id, end),
    ]
    runner_dict = {
        runner_ctx_1.runner_id: runner_ctx_1,
        runner_ctx_2.runner_id: runner_ctx_2,
    }

    builder.add_history_batch(histories, runner_dict)
    data = builder.build()

    assert data.bounds.start_time == start
    assert data.bounds.end_time == end
    assert data.bounds.duration_seconds == 3600.0


def test_custom_config() -> None:
    """Custom config is passed through to TimelineData."""
    config = TimelineConfig(width=800, lane_height=30)
    builder = TimelineDataBuilder(config=config)
    runner_ctx = create_runner_context("runner1@host1", "host1")
    history = create_history(
        "inv-1",
        InvocationStatus.RUNNING,
        runner_ctx.runner_id,
        datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
    )

    builder.add_history_batch([history], {runner_ctx.runner_id: runner_ctx})
    data = builder.build()

    assert data.config.width == 800
    assert data.config.lane_height == 30


def test_incremental_batches() -> None:
    """Multiple batches can be added incrementally."""
    builder = TimelineDataBuilder()
    base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

    runner_ctx = create_runner_context("runner1@host1", "host1")
    batch1 = [
        create_history(
            "inv-1", InvocationStatus.PENDING, runner_ctx.runner_id, base_time
        )
    ]
    batch2 = [
        create_history(
            "inv-1",
            InvocationStatus.RUNNING,
            runner_ctx.runner_id,
            base_time + timedelta(seconds=10),
        )
    ]
    batch3 = [
        create_history(
            "inv-1",
            InvocationStatus.SUCCESS,
            runner_ctx.runner_id,
            base_time + timedelta(seconds=60),
        )
    ]
    runner_dict = {runner_ctx.runner_id: runner_ctx}

    builder.add_history_batch(batch1, runner_dict)
    builder.add_history_batch(batch2, runner_dict)
    builder.add_history_batch(batch3, runner_dict)

    data = builder.build()

    lane = data.lanes["runner1@host1"]
    # 3 status changes = 3 points
    assert len(lane.points) == 3
    # PENDING and RUNNING are segment statuses
    assert len(lane.segments) == 2


def test_clear_resets_builder() -> None:
    """clear() resets the builder state."""
    builder = TimelineDataBuilder()
    base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

    runner_ctx = create_runner_context("runner1@host1", "host1")
    history = create_history(
        "inv-1", InvocationStatus.RUNNING, runner_ctx.runner_id, base_time
    )
    builder.add_history_batch([history], {runner_ctx.runner_id: runner_ctx})

    builder.clear()
    data = builder.build()

    assert len(data.lanes) == 0


def test_status_colors() -> None:
    """Status colors are applied correctly to points."""
    builder = TimelineDataBuilder()
    base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

    runner_ctx = create_runner_context("runner1@host1", "host1")
    histories = [
        create_history(
            "inv-1", InvocationStatus.RUNNING, runner_ctx.runner_id, base_time
        ),
        create_history(
            "inv-1",
            InvocationStatus.SUCCESS,
            runner_ctx.runner_id,
            base_time + timedelta(seconds=10),
        ),
    ]

    builder.add_history_batch(histories, {runner_ctx.runner_id: runner_ctx})
    data = builder.build()

    lane = data.lanes["runner1@host1"]
    # Check point colors
    running_point = lane.points[0]
    success_point = lane.points[1]

    assert running_point.color == STATUS_COLORS["RUNNING"]
    assert success_point.color == STATUS_COLORS["SUCCESS"]


def test_bar_has_tooltip() -> None:
    """Points have tooltips with invocation and status info."""
    builder = TimelineDataBuilder()
    base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    runner_ctx = create_runner_context("runner1@host1", "host1")
    history = create_history(
        "inv-123", InvocationStatus.RUNNING, runner_ctx.runner_id, base_time
    )
    builder.add_history_batch([history], {runner_ctx.runner_id: runner_ctx})
    data = builder.build()

    point = data.lanes["runner1@host1"].points[0]
    assert "inv-123" in point.tooltip
    assert "RUNNING" in point.tooltip


def test_final_status_has_no_segment() -> None:
    """Final statuses (SUCCESS/FAILED) are points only, no segments."""
    builder = TimelineDataBuilder()
    base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    end_time = datetime(2025, 1, 1, 13, 0, 0, tzinfo=UTC)
    runner_ctx = create_runner_context("runner1@host1", "host1")
    history = create_history(
        "inv-1",
        InvocationStatus.SUCCESS,
        runner_ctx.runner_id,
        base_time + timedelta(minutes=30),
    )
    builder.add_history_batch([history], {runner_ctx.runner_id: runner_ctx})
    data = builder.build(end_time=end_time)

    # SUCCESS is a point-only status (final), so there's only a point, no segment
    lane = data.lanes["runner1@host1"]
    assert len(lane.points) == 1
    assert lane.points[0].status == "SUCCESS"
    # No segments for final status
    assert len(lane.segments) == 0


def test_non_final_status_extends_to_end() -> None:
    """RUNNING status extends to end_time if not followed by another status."""
    builder = TimelineDataBuilder()
    base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    end_time = datetime(2025, 1, 1, 13, 0, 0, tzinfo=UTC)
    runner_ctx = create_runner_context("runner1@host1", "host1")
    history = create_history(
        "inv-1", InvocationStatus.RUNNING, runner_ctx.runner_id, base_time
    )
    builder.add_history_batch([history], {runner_ctx.runner_id: runner_ctx})
    data = builder.build(end_time=end_time)

    lane = data.lanes["runner1@host1"]
    # RUNNING is a segment status
    assert len(lane.segments) == 1
    segment = lane.segments[0]
    # RUNNING is non-final, so end_time should be the build end_time
    assert segment.end_time == end_time


def test_ongoing_segment_is_marked() -> None:
    """Segments without resolution are marked as ongoing."""
    builder = TimelineDataBuilder()
    base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    end_time = datetime(2025, 1, 1, 13, 0, 0, tzinfo=UTC)
    runner_ctx = create_runner_context("runner1@host1", "host1")
    history = create_history(
        "inv-1", InvocationStatus.RUNNING, runner_ctx.runner_id, base_time
    )
    builder.add_history_batch([history], {runner_ctx.runner_id: runner_ctx})
    data = builder.build(end_time=end_time)

    lane = data.lanes["runner1@host1"]
    segment = lane.segments[0]
    # Segment extending to end_time without resolution should be marked ongoing
    assert segment.is_ongoing is True
    assert segment.next_status is None


def test_completed_segment_not_marked_ongoing() -> None:
    """Segments with a following status are not marked as ongoing."""
    builder = TimelineDataBuilder()
    base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    runner_ctx = create_runner_context("runner1@host1", "host1")
    history1 = create_history(
        "inv-1", InvocationStatus.RUNNING, runner_ctx.runner_id, base_time
    )
    history2 = create_history(
        "inv-1",
        InvocationStatus.SUCCESS,
        runner_ctx.runner_id,
        base_time + timedelta(seconds=30),
    )
    builder.add_history_batch([history1, history2], {runner_ctx.runner_id: runner_ctx})
    data = builder.build()

    lane = data.lanes["runner1@host1"]
    segment = lane.segments[0]
    # Segment that completed with SUCCESS should not be marked ongoing
    assert segment.is_ongoing is False
    assert segment.next_status == "SUCCESS"


def test_custom_start_and_end_time() -> None:
    """Custom start and end times override the computed bounds."""
    builder = TimelineDataBuilder()
    # Data occurs at 12:30
    data_time = datetime(2025, 1, 1, 12, 30, 0, tzinfo=UTC)
    # But we want to show the full hour: 12:00 to 13:00
    start_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    end_time = datetime(2025, 1, 1, 13, 0, 0, tzinfo=UTC)
    runner_ctx = create_runner_context("runner1@host1", "host1")
    history = create_history(
        "inv-1", InvocationStatus.SUCCESS, runner_ctx.runner_id, data_time
    )
    builder.add_history_batch([history], {runner_ctx.runner_id: runner_ctx})
    data = builder.build(start_time=start_time, end_time=end_time)

    # Bounds should use the provided times, not the data times
    assert data.bounds.start_time == start_time
    assert data.bounds.end_time == end_time
    assert data.bounds.duration_seconds == 3600.0  # 1 hour
