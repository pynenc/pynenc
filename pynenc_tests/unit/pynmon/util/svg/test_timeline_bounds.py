"""
Unit tests for TimelineBounds and InvocationBar dataclasses.

Tests coordinate mapping, duration calculations, and bar properties.
"""

from datetime import UTC, datetime, timedelta

import pytest


from pynmon.util.svg.models import InvocationBar, TimelineBounds, TimelineConfig


# =============================================================================
# TimelineBounds Tests
# =============================================================================


@pytest.fixture
def standard_bounds() -> TimelineBounds:
    """Create a standard bounds for testing (1 hour duration)."""
    start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(2025, 1, 1, 1, 0, 0, tzinfo=UTC)
    config = TimelineConfig(width=1200, left_margin=200)
    return TimelineBounds(start_time=start, end_time=end, config=config)


def test_bounds_duration_seconds(standard_bounds: TimelineBounds) -> None:
    """Test duration calculation."""
    assert standard_bounds.duration_seconds == 3600.0  # 1 hour in seconds


def test_bounds_time_to_x_at_start(standard_bounds: TimelineBounds) -> None:
    """Test x-coordinate at start time equals left margin."""
    x = standard_bounds.time_to_x(standard_bounds.start_time)
    assert x == 200.0  # left_margin


def test_bounds_time_to_x_at_end(standard_bounds: TimelineBounds) -> None:
    """Test x-coordinate at end time equals width."""
    x = standard_bounds.time_to_x(standard_bounds.end_time)
    assert x == 1200.0  # width


def test_bounds_time_to_x_at_midpoint(standard_bounds: TimelineBounds) -> None:
    """Test x-coordinate at midpoint."""
    midpoint = standard_bounds.start_time + timedelta(minutes=30)
    x = standard_bounds.time_to_x(midpoint)
    # left_margin + (0.5 * content_width) = 200 + (0.5 * 1000) = 700
    assert x == 700.0


def test_bounds_time_to_x_zero_duration() -> None:
    """Test x-coordinate when start equals end."""
    start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    bounds = TimelineBounds(start_time=start, end_time=start)
    x = bounds.time_to_x(start)
    assert x == 0.0


def test_bounds_duration_to_width(standard_bounds: TimelineBounds) -> None:
    """Test duration to width conversion."""
    # 30 minutes = 1800 seconds = half the duration
    width = standard_bounds.duration_to_width(1800)
    # 0.5 * content_width = 0.5 * 1000 = 500
    assert width == 500.0


def test_bounds_duration_to_width_respects_minimum(
    standard_bounds: TimelineBounds,
) -> None:
    """Test that very short durations get minimum width."""
    width = standard_bounds.duration_to_width(0.001)
    assert width == standard_bounds.config.min_bar_width


def test_bounds_duration_to_width_zero_duration() -> None:
    """Test width calculation when bounds have zero duration."""
    start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    bounds = TimelineBounds(start_time=start, end_time=start)
    width = bounds.duration_to_width(100)
    assert width == bounds.config.min_bar_width


# =============================================================================
# InvocationBar Tests
# =============================================================================


def test_bar_creation() -> None:
    """Test basic bar creation."""
    start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(2025, 1, 1, 0, 5, 0, tzinfo=UTC)
    bar = InvocationBar(
        invocation_id="inv-123",
        start_time=start,
        end_time=end,
        status="RUNNING",
        color="#3498db",
        tooltip="Processing data",
    )
    assert bar.invocation_id == "inv-123"
    assert bar.status == "RUNNING"
    assert bar.color == "#3498db"


def test_bar_duration_seconds() -> None:
    """Test duration calculation."""
    start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(2025, 1, 1, 0, 5, 0, tzinfo=UTC)  # 5 minutes
    bar = InvocationBar(
        invocation_id="inv-123",
        start_time=start,
        end_time=end,
        status="RUNNING",
        color="#000",
    )
    assert bar.duration_seconds == 300.0  # 5 minutes in seconds


def test_bar_frozen_immutability() -> None:
    """Test that bar is immutable."""
    start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    bar = InvocationBar(
        invocation_id="inv-123",
        start_time=start,
        end_time=start,
        status="RUNNING",
        color="#000",
    )
    with pytest.raises(AttributeError):
        bar.status = "SUCCESS"  # type: ignore
