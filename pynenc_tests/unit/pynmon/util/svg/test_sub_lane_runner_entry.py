"""
Tests for RunnerEntry in sub-lane computation.
"""

from datetime import UTC, datetime

from pynmon.util.svg.sub_lane import RunnerEntry


def test_runner_entry_segment_status_running() -> None:
    """RUNNING status is correctly identified as segment."""
    entry = RunnerEntry(timestamp=datetime.now(UTC), status="RUNNING")
    assert entry.is_segment is True


def test_runner_entry_segment_status_pending() -> None:
    """PENDING status is correctly identified as segment."""
    entry = RunnerEntry(timestamp=datetime.now(UTC), status="PENDING")
    assert entry.is_segment is True


def test_runner_entry_point_status_registered() -> None:
    """REGISTERED status is correctly identified as point."""
    entry = RunnerEntry(timestamp=datetime.now(UTC), status="REGISTERED")
    assert entry.is_segment is False


def test_runner_entry_point_status_success() -> None:
    """SUCCESS status is correctly identified as point."""
    entry = RunnerEntry(timestamp=datetime.now(UTC), status="SUCCESS")
    assert entry.is_segment is False
