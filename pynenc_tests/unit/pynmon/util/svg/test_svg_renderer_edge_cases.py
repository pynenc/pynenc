"""
Tests for edge cases in SVG rendering.
"""

from datetime import UTC, datetime, timedelta


from pynmon.util.svg import (
    InvocationBar,
    RunnerLane,
    TimelineBounds,
    TimelineConfig,
    TimelineData,
    TimelineSVGRenderer,
)


def test_render_with_special_characters_in_tooltip(config: TimelineConfig) -> None:
    """Test rendering handles special characters in tooltips."""
    start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 1, 10, 1, 0, tzinfo=UTC)
    bounds = TimelineBounds(start_time=start, end_time=end, config=config)
    data = TimelineData(bounds=bounds, config=config)

    lane = RunnerLane(
        runner_id="runner@host",
        hostname="host",
        label="runner",
        color="#e74c3c",
        lane_index=0,
    )

    bar = InvocationBar(
        invocation_id="inv-001",
        start_time=start,
        end_time=start + timedelta(seconds=30),
        status="RUNNING",
        color="#3498db",
        tooltip='Test <script>alert("xss")</script> & more',
    )
    lane.add_bar(bar)
    data.lanes[lane.runner_id] = lane

    renderer = TimelineSVGRenderer()
    svg = renderer.render(data)

    assert "&lt;script&gt;" in svg
    assert "&amp;" in svg


def test_render_with_special_characters_in_label(config: TimelineConfig) -> None:
    """Test rendering handles special characters in lane labels."""
    start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 1, 10, 1, 0, tzinfo=UTC)
    bounds = TimelineBounds(start_time=start, end_time=end, config=config)
    data = TimelineData(bounds=bounds, config=config)

    lane = RunnerLane(
        runner_id="runner<test>&host",
        hostname="host<1>",
        label="runner<test>",
        color="#e74c3c",
        lane_index=0,
    )
    data.lanes[lane.runner_id] = lane

    renderer = TimelineSVGRenderer()
    svg = renderer.render(data)

    assert "&lt;test&gt;" in svg


def test_render_zero_duration_bar(config: TimelineConfig) -> None:
    """Test rendering handles zero-duration bars."""
    start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 1, 10, 1, 0, tzinfo=UTC)
    bounds = TimelineBounds(start_time=start, end_time=end, config=config)
    data = TimelineData(bounds=bounds, config=config)

    lane = RunnerLane(
        runner_id="runner@host",
        hostname="host",
        label="runner",
        color="#e74c3c",
        lane_index=0,
    )

    bar = InvocationBar(
        invocation_id="inv-001",
        start_time=start + timedelta(seconds=30),
        end_time=start + timedelta(seconds=30),
        status="SUCCESS",
        color="#27ae60",
        tooltip="Instant completion",
    )
    lane.add_bar(bar)
    data.lanes[lane.runner_id] = lane

    renderer = TimelineSVGRenderer()
    svg = renderer.render(data)

    assert 'class="invocation-bar"' in svg
