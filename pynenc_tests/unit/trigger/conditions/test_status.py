"""
Unit tests for status trigger conditions.

Tests the functionality of StatusCondition which triggers
based on task status changes with optional call argument filtering.
"""

from typing import TYPE_CHECKING

from pynenc.arguments import Arguments
from pynenc.invocation.status import InvocationStatus
from pynenc.trigger.arguments import create_argument_filter
from pynenc.trigger.conditions.status import StatusCondition, StatusContext

if TYPE_CHECKING:
    from pynenc.identifiers.call_id import CallId
    from pynenc.identifiers.task_id import TaskId
    from pynenc.identifiers.invocation_id import InvocationId


no_arg_filter = create_argument_filter(None)


def test_task_status_condition_init(task_id: "TaskId") -> None:
    """Test initialization of StatusCondition."""
    # Test with string status, no arguments
    condition = StatusCondition(task_id, [InvocationStatus.RUNNING], no_arg_filter)
    assert condition.task_id == task_id
    assert condition.statuses == [InvocationStatus.RUNNING]
    assert condition.arguments_filter == no_arg_filter

    # Test with list of statuses
    condition = StatusCondition(
        task_id, [InvocationStatus.RUNNING, InvocationStatus.SUCCESS], no_arg_filter
    )
    assert condition.task_id == task_id
    assert condition.statuses == [InvocationStatus.RUNNING, InvocationStatus.SUCCESS]
    assert condition.arguments_filter == no_arg_filter

    # Test with call arguments
    arguments_filter = create_argument_filter({"arg1": "value1"})
    condition = StatusCondition(task_id, [InvocationStatus.RUNNING], arguments_filter)
    assert condition.task_id == task_id
    assert condition.statuses == [InvocationStatus.RUNNING]
    assert condition.arguments_filter == arguments_filter


def test_task_status_condition_id(task_id: "TaskId", other_task_id: "TaskId") -> None:
    """Test condition_id generation."""
    # Same task and statuses should have same ID
    condition1 = StatusCondition(
        task_id, [InvocationStatus.RUNNING, InvocationStatus.SUCCESS], no_arg_filter
    )
    condition2 = StatusCondition(
        task_id, [InvocationStatus.SUCCESS, InvocationStatus.RUNNING], no_arg_filter
    )
    assert condition1.condition_id == condition2.condition_id

    # Different tasks should have different IDs
    condition3 = StatusCondition(
        other_task_id,
        [InvocationStatus.RUNNING, InvocationStatus.SUCCESS],
        no_arg_filter,
    )
    assert condition1.condition_id != condition3.condition_id

    # Different statuses should have different IDs
    condition4 = StatusCondition(task_id, [InvocationStatus.FAILED], no_arg_filter)
    assert condition1.condition_id != condition4.condition_id

    # Same arguments should have same ID
    arguments_filter1 = create_argument_filter({"arg1": "value1"})
    condition5 = StatusCondition(task_id, [InvocationStatus.RUNNING], arguments_filter1)
    condition6 = StatusCondition(task_id, [InvocationStatus.RUNNING], arguments_filter1)
    assert condition5.condition_id == condition6.condition_id

    # Different arguments should have different IDs
    arguments_filter2 = create_argument_filter({"arg1": "value2"})
    condition7 = StatusCondition(task_id, [InvocationStatus.RUNNING], arguments_filter2)
    assert condition5.condition_id != condition7.condition_id


def test_task_status_condition_is_satisfied_by_matching_task_and_status(
    call_id: "CallId", inv_id: "InvocationId"
) -> None:
    """Test is_satisfied_by with matching task ID and status."""
    # Create a status condition
    condition = StatusCondition(
        call_id.task_id,
        [InvocationStatus.RUNNING, InvocationStatus.SUCCESS],
        no_arg_filter,
    )

    # Create a context with RUNNING status
    context = StatusContext(
        call_id=call_id,
        invocation_id=inv_id,
        status=InvocationStatus.RUNNING,
        arguments=Arguments(),
        disable_cache_args=(),
    )

    # Status is RUNNING which is in our list
    assert condition.is_satisfied_by(context)

    # Change status to another one in our list
    context_success = StatusContext(
        call_id=call_id,
        invocation_id=inv_id,
        arguments=Arguments(),
        status=InvocationStatus.SUCCESS,
        disable_cache_args=(),
    )
    assert condition.is_satisfied_by(context_success)

    # Change status to one not in our list
    context_failed = StatusContext(
        call_id=call_id,
        invocation_id=inv_id,
        arguments=Arguments(),
        status=InvocationStatus.FAILED,
        disable_cache_args=(),
    )
    assert not condition.is_satisfied_by(context_failed)


def test_task_status_condition_is_satisfied_by_non_matching_task(
    task_id: "TaskId", other_call_id: "CallId", inv_id: "InvocationId"
) -> None:
    """Test is_satisfied_by with non-matching task ID."""
    # Create a status condition
    condition = StatusCondition(
        task_id, [InvocationStatus.RUNNING, InvocationStatus.SUCCESS], no_arg_filter
    )

    # Create a context with different task_id
    context = StatusContext(
        call_id=other_call_id,
        invocation_id=inv_id,
        arguments=Arguments(),
        status=InvocationStatus.RUNNING,
        disable_cache_args=(),
    )

    assert not condition.is_satisfied_by(context)


def test_task_status_condition_with_matching_arguments(
    call_id: "CallId", inv_id: "InvocationId"
) -> None:
    """Test is_satisfied_by with matching call arguments."""
    # Create condition with specific arguments
    arguments_filter = create_argument_filter({"arg1": "value1", "arg2": 42})
    condition = StatusCondition(
        call_id.task_id, [InvocationStatus.RUNNING], arguments_filter
    )
    # Create context with matching arguments
    context = StatusContext(
        call_id=call_id,
        invocation_id=inv_id,
        status=InvocationStatus.RUNNING,
        arguments=Arguments({"arg1": "value1", "arg2": 42, "extra": "ignored"}),
        disable_cache_args=(),
    )

    # Should be satisfied
    assert condition.is_satisfied_by(context)


def test_task_status_condition_with_non_matching_arguments(
    call_id: "CallId", inv_id: "InvocationId"
) -> None:
    """Test is_satisfied_by with non-matching call arguments."""
    # Create condition with specific arguments
    arguments_filter = create_argument_filter({"arg1": "value1", "arg2": 42})
    condition = StatusCondition(
        call_id.task_id, [InvocationStatus.RUNNING], arguments_filter
    )
    # Create context with non-matching arguments
    context1 = StatusContext(
        call_id=call_id,
        invocation_id=inv_id,
        status=InvocationStatus.RUNNING,
        arguments=Arguments({"arg1": "value1", "arg2": 99}),  # Different arg2 value
        disable_cache_args=(),
    )

    # Missing argument
    context2 = StatusContext(
        call_id=call_id,
        invocation_id=inv_id,
        status=InvocationStatus.RUNNING,
        arguments=Arguments({"arg1": "value1"}),  # Missing arg2
        disable_cache_args=(),
    )

    # Should not be satisfied in either case
    assert not condition.is_satisfied_by(context1)
    assert not condition.is_satisfied_by(context2)


def test_task_status_condition_with_no_arguments_restriction(
    call_id: "CallId", inv_id: "InvocationId"
) -> None:
    """Test that a condition with no arguments matches any call arguments."""
    # Create condition with no argument restrictions
    condition = StatusCondition(
        call_id.task_id, [InvocationStatus.RUNNING], no_arg_filter
    )

    # Create context with arguments
    context = StatusContext(
        call_id=call_id,
        invocation_id=inv_id,
        status=InvocationStatus.RUNNING,
        arguments=Arguments({"arg1": "value1", "arg2": 42}),
        disable_cache_args=(),
    )

    # Should match any arguments
    assert condition.is_satisfied_by(context)


def test_task_status_condition_affects_task(
    task_id: "TaskId", other_task_id: "TaskId"
) -> None:
    """Test affects_task method."""
    condition = StatusCondition(task_id, [InvocationStatus.RUNNING], no_arg_filter)

    assert condition.affects_task(task_id)
    assert not condition.affects_task(other_task_id)
