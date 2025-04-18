"""
Unit tests for base trigger conditions.

Tests the base classes and context objects used by all condition implementations.
"""

from datetime import datetime, timezone
from unittest.mock import Mock

from pynenc.arguments import Arguments
from pynenc.invocation.status import InvocationStatus
from pynenc.trigger.conditions import (
    CronContext,
    EventContext,
    ResultContext,
    StatusContext,
)


def test_time_context_default() -> None:
    """Test CronContext with default timestamp."""
    context = CronContext()
    assert hasattr(context, "timestamp")
    assert isinstance(context.timestamp, datetime)
    # Should be close to current time
    now = datetime.now(timezone.utc)
    if context.timestamp.tzinfo is None:
        # If timestamp is naive, make now naive too
        now = now.replace(tzinfo=None)
    diff = now - context.timestamp
    assert diff.total_seconds() < 1


def test_time_context_custom() -> None:
    """Test CronContext with custom timestamp."""
    timestamp = datetime(2023, 1, 1, tzinfo=timezone.utc)
    context = CronContext()
    context.timestamp = timestamp
    assert context.timestamp == timestamp


def test_event_context() -> None:
    """Test EventContext attributes."""
    context = EventContext(
        event_id="event1",
        event_code="test_event",
        payload={"param1": "value1"},
    )
    assert context.event_id == "event1"
    assert context.payload == {"param1": "value1"}


def test_status_context() -> None:
    """Test StatusContext attributes."""
    status_mock = Mock()
    context = StatusContext(
        task_id="task1",
        call_id="call1",
        invocation_id="inv1",
        status=status_mock,
        arguments=Arguments(),
    )
    assert context.task_id == "task1"
    assert context.call_id == "call1"
    assert context.invocation_id == "inv1"
    assert context.status == status_mock


def test_result_context() -> None:
    """Test ResultContext attributes."""
    context = ResultContext(
        task_id="task1",
        call_id="call1",
        invocation_id="inv1",
        status=InvocationStatus.SUCCESS,
        result=42,
        arguments=Arguments(),
    )
    assert context.task_id == "task1"
    assert context.call_id == "call1"
    assert context.invocation_id == "inv1"
    assert context.status == InvocationStatus.SUCCESS
    assert context.result == 42
