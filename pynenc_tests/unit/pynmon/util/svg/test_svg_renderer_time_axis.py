"""
Tests for time axis rendering.
"""

from datetime import UTC, datetime, timedelta

from pynmon.util.svg import TimelineBounds, TimelineConfig
from pynmon.util.svg.render_axis import _format_tick_label, _tick_positions


def test_short_duration_ticks(config: TimelineConfig) -> None:
    """Test tick interval for short duration."""
    start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
    end = start + timedelta(seconds=30)
    bounds = TimelineBounds(start_time=start, end_time=end, config=config)

    ticks = _tick_positions(bounds)

    assert len(ticks) >= 3


def test_medium_duration_ticks(config: TimelineConfig) -> None:
    """Test tick interval for medium duration."""
    start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
    end = start + timedelta(minutes=10)
    bounds = TimelineBounds(start_time=start, end_time=end, config=config)

    ticks = _tick_positions(bounds)

    assert len(ticks) >= 5
    assert len(ticks) <= 20


def test_long_duration_ticks(config: TimelineConfig) -> None:
    """Test tick interval for long duration."""
    start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=2)
    bounds = TimelineBounds(start_time=start, end_time=end, config=config)

    ticks = _tick_positions(bounds)

    assert len(ticks) >= 4
    assert len(ticks) <= 10


def test_time_label_format_short(config: TimelineConfig) -> None:
    """Test time label format for short duration."""
    start = datetime(2024, 1, 1, 10, 30, 15, tzinfo=UTC)
    end = start + timedelta(seconds=30)
    bounds = TimelineBounds(start_time=start, end_time=end, config=config)

    label = _format_tick_label(start, bounds)

    assert "10:30:15" in label


def test_time_label_format_long(config: TimelineConfig) -> None:
    """Test time label format for long duration."""
    start = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
    end = start + timedelta(days=2)
    bounds = TimelineBounds(start_time=start, end_time=end, config=config)

    label = _format_tick_label(start, bounds)

    assert "01/15" in label
