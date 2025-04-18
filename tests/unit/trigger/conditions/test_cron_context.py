"""
Tests for CronContext class in time-based trigger conditions.

This module verifies the behavior of the CronContext class, including
its ability to track last execution times for cron scheduling.
"""

from datetime import datetime
from unittest.mock import MagicMock

from pynenc.trigger.conditions.cron import CronContext


def test_timestamp_default() -> None:
    """Test that a timestamp is created by default."""
    context = CronContext()
    assert hasattr(context, "timestamp")
    assert isinstance(context.timestamp, datetime)


def test_timestamp_custom() -> None:
    """Test that a custom timestamp is used when provided."""
    timestamp = datetime(2023, 1, 1, 12, 0, 0)
    context = CronContext(timestamp=timestamp)
    assert context.timestamp == timestamp


def test_last_execution_default() -> None:
    """Test that last_execution defaults to None."""
    context = CronContext()
    assert context.last_execution is None


def test_last_execution_custom() -> None:
    """Test that a custom last_execution is used when provided."""
    last_time = datetime(2023, 1, 1, 12, 0, 0)
    context = CronContext(last_execution=last_time)
    assert context.last_execution == last_time


def test_to_json_without_last_execution() -> None:
    """Test serialization without last_execution."""
    context = CronContext()
    app_mock = MagicMock()
    result = context._to_json(app_mock)
    assert "last_execution" not in result


def test_to_json_with_last_execution() -> None:
    """Test serialization with last_execution."""
    last_time = datetime(2023, 1, 1, 12, 0, 0)
    context = CronContext(last_execution=last_time)
    app_mock = MagicMock()
    result = context._to_json(app_mock)
    assert "last_execution" in result
    assert result["last_execution"] == "2023-01-01T12:00:00"


def test_from_json_with_last_execution() -> None:
    """Test deserialization with last_execution."""
    app_mock = MagicMock()
    data = {"last_execution": "2023-01-01T12:00:00"}
    context = CronContext._from_json(data, app_mock)
    assert context.last_execution == datetime(2023, 1, 1, 12, 0, 0)


def test_from_json_with_invalid_last_execution() -> None:
    """Test deserialization with invalid last_execution format."""
    app_mock = MagicMock()
    data = {"last_execution": "invalid-date"}
    context = CronContext._from_json(data, app_mock)
    assert context.last_execution is None
    app_mock.logger.error.assert_called_once()
