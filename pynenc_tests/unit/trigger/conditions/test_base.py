"""
Unit tests for base trigger conditions.

Tests the base classes and context objects used by all condition implementations.
"""

from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from pynenc.arguments import Arguments
from pynenc.identifiers.call_id import CallId
from pynenc.identifiers.invocation_id import InvocationId
from pynenc.invocation.status import InvocationStatus
from pynenc.identifiers.task_id import TaskId
from pynenc.trigger.conditions import (
    CronContext,
    EventContext,
    ResultContext,
    StatusContext,
)


@pytest.fixture
def call_id() -> CallId:
    """Fixture providing a sample TaskId for testing."""
    task_id = TaskId("test_task_id", "test_func")
    return CallId(task_id=task_id, args_id="args123")


@pytest.fixture
def inv_id() -> InvocationId:
    """Fixture providing a sample InvocationId for testing."""
    return InvocationId("inv123")


def test_time_context_default() -> None:
    """Test CronContext with default timestamp."""
    context = CronContext()
    assert hasattr(context, "timestamp")
    assert isinstance(context.timestamp, datetime)
    # Should be close to current time
    now = datetime.now(UTC)
    if context.timestamp.tzinfo is None:
        # If timestamp is naive, make now naive too
        now = now.replace(tzinfo=None)
    diff = now - context.timestamp
    assert diff.total_seconds() < 1


def test_time_context_custom() -> None:
    """Test CronContext with custom timestamp."""
    timestamp = datetime(2023, 1, 1, tzinfo=UTC)
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


def test_status_context(call_id: CallId, inv_id: InvocationId) -> None:
    """Test StatusContext attributes."""
    status_mock = Mock()
    context = StatusContext(
        call_id=call_id,
        invocation_id=inv_id,
        status=status_mock,
        arguments=Arguments(),
        disable_cache_args=(),
    )
    assert context.call_id == call_id
    assert context.invocation_id == inv_id
    assert context.status == status_mock


def test_result_context(call_id: CallId, inv_id: InvocationId) -> None:
    """Test ResultContext attributes."""
    context = ResultContext(
        call_id=call_id,
        invocation_id=inv_id,
        status=InvocationStatus.SUCCESS,
        result=42,
        arguments=Arguments(),
        disable_cache_args=(),
    )
    assert context.call_id == call_id
    assert context.invocation_id == inv_id
    assert context.status == InvocationStatus.SUCCESS
    assert context.result == 42
