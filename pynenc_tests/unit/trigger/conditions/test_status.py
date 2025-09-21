"""
Unit tests for status trigger conditions.

Tests the functionality of StatusCondition which triggers
based on task status changes with optional call argument filtering.
"""

from pynenc.arguments import Arguments
from pynenc.invocation.status import InvocationStatus
from pynenc.trigger.arguments import create_argument_filter
from pynenc.trigger.conditions.status import StatusCondition, StatusContext

no_arg_filter = create_argument_filter(None)


def test_task_status_condition_init() -> None:
    """Test initialization of StatusCondition."""
    # Test with string status, no arguments
    condition = StatusCondition("task1", [InvocationStatus.RUNNING], no_arg_filter)
    assert condition.task_id == "task1"
    assert condition.statuses == [InvocationStatus.RUNNING]
    assert condition.arguments_filter == no_arg_filter

    # Test with list of statuses
    condition = StatusCondition(
        "task1", [InvocationStatus.RUNNING, InvocationStatus.SUCCESS], no_arg_filter
    )
    assert condition.task_id == "task1"
    assert condition.statuses == [InvocationStatus.RUNNING, InvocationStatus.SUCCESS]
    assert condition.arguments_filter == no_arg_filter

    # Test with call arguments
    arguments_filter = create_argument_filter({"arg1": "value1"})
    condition = StatusCondition("task1", [InvocationStatus.RUNNING], arguments_filter)
    assert condition.task_id == "task1"
    assert condition.statuses == [InvocationStatus.RUNNING]
    assert condition.arguments_filter == arguments_filter


def test_task_status_condition_id() -> None:
    """Test condition_id generation."""
    # Same task and statuses should have same ID
    condition1 = StatusCondition(
        "task1", [InvocationStatus.RUNNING, InvocationStatus.SUCCESS], no_arg_filter
    )
    condition2 = StatusCondition(
        "task1", [InvocationStatus.SUCCESS, InvocationStatus.RUNNING], no_arg_filter
    )
    assert condition1.condition_id == condition2.condition_id

    # Different tasks should have different IDs
    condition3 = StatusCondition(
        "task2", [InvocationStatus.RUNNING, InvocationStatus.SUCCESS], no_arg_filter
    )
    assert condition1.condition_id != condition3.condition_id

    # Different statuses should have different IDs
    condition4 = StatusCondition("task1", [InvocationStatus.FAILED], no_arg_filter)
    assert condition1.condition_id != condition4.condition_id

    # Same arguments should have same ID
    arguments_filter1 = create_argument_filter({"arg1": "value1"})
    condition5 = StatusCondition("task1", [InvocationStatus.RUNNING], arguments_filter1)
    condition6 = StatusCondition("task1", [InvocationStatus.RUNNING], arguments_filter1)
    assert condition5.condition_id == condition6.condition_id

    # Different arguments should have different IDs
    arguments_filter2 = create_argument_filter({"arg1": "value2"})
    condition7 = StatusCondition("task1", [InvocationStatus.RUNNING], arguments_filter2)
    assert condition5.condition_id != condition7.condition_id


def test_task_status_condition_is_satisfied_by_matching_task_and_status() -> None:
    """Test is_satisfied_by with matching task ID and status."""
    # Create a status condition
    condition = StatusCondition(
        "task1", [InvocationStatus.RUNNING, InvocationStatus.SUCCESS], no_arg_filter
    )

    # Create a context with RUNNING status
    context = StatusContext(
        task_id="task1",
        call_id="call1",
        invocation_id="inv1",
        status=InvocationStatus.RUNNING,
        arguments=Arguments(),
    )

    # Status is RUNNING which is in our list
    assert condition.is_satisfied_by(context)

    # Change status to another one in our list
    context_success = StatusContext(
        task_id="task1",
        call_id="call1",
        invocation_id="inv1",
        arguments=Arguments(),
        status=InvocationStatus.SUCCESS,
    )
    assert condition.is_satisfied_by(context_success)

    # Change status to one not in our list
    context_failed = StatusContext(
        task_id="task1",
        call_id="call1",
        invocation_id="inv1",
        arguments=Arguments(),
        status=InvocationStatus.FAILED,
    )
    assert not condition.is_satisfied_by(context_failed)


def test_task_status_condition_is_satisfied_by_non_matching_task() -> None:
    """Test is_satisfied_by with non-matching task ID."""
    # Create a status condition
    condition = StatusCondition(
        "task1", [InvocationStatus.RUNNING, InvocationStatus.SUCCESS], no_arg_filter
    )

    # Create a context with different task_id
    context = StatusContext(
        task_id="different_task",
        call_id="call1",
        invocation_id="inv1",
        arguments=Arguments(),
        status=InvocationStatus.RUNNING,
    )

    assert not condition.is_satisfied_by(context)


def test_task_status_condition_with_matching_arguments() -> None:
    """Test is_satisfied_by with matching call arguments."""
    # Create condition with specific arguments
    arguments_filter = create_argument_filter({"arg1": "value1", "arg2": 42})
    condition = StatusCondition("task1", [InvocationStatus.RUNNING], arguments_filter)

    # Create context with matching arguments
    context = StatusContext(
        task_id="task1",
        call_id="call1",
        invocation_id="inv1",
        status=InvocationStatus.RUNNING,
        arguments=Arguments({"arg1": "value1", "arg2": 42, "extra": "ignored"}),
    )

    # Should be satisfied
    assert condition.is_satisfied_by(context)


def test_task_status_condition_with_non_matching_arguments() -> None:
    """Test is_satisfied_by with non-matching call arguments."""
    # Create condition with specific arguments
    arguments_filter = create_argument_filter({"arg1": "value1", "arg2": 42})
    condition = StatusCondition("task1", [InvocationStatus.RUNNING], arguments_filter)

    # Create context with non-matching arguments
    context1 = StatusContext(
        task_id="task1",
        call_id="call1",
        invocation_id="inv1",
        status=InvocationStatus.RUNNING,
        arguments=Arguments({"arg1": "value1", "arg2": 99}),  # Different arg2 value
    )

    # Missing argument
    context2 = StatusContext(
        task_id="task1",
        call_id="call1",
        invocation_id="inv1",
        status=InvocationStatus.RUNNING,
        arguments=Arguments({"arg1": "value1"}),  # Missing arg2
    )

    # Should not be satisfied in either case
    assert not condition.is_satisfied_by(context1)
    assert not condition.is_satisfied_by(context2)


def test_task_status_condition_with_no_arguments_restriction() -> None:
    """Test that a condition with no arguments matches any call arguments."""
    # Create condition with no argument restrictions
    condition = StatusCondition("task1", [InvocationStatus.RUNNING], no_arg_filter)

    # Create context with arguments
    context = StatusContext(
        task_id="task1",
        call_id="call1",
        invocation_id="inv1",
        status=InvocationStatus.RUNNING,
        arguments=Arguments({"arg1": "value1", "arg2": 42}),
    )

    # Should match any arguments
    assert condition.is_satisfied_by(context)


def test_task_status_condition_affects_task() -> None:
    """Test affects_task method."""
    condition = StatusCondition("task1", [InvocationStatus.RUNNING], no_arg_filter)

    assert condition.affects_task("task1")
    assert not condition.affects_task("other_task")
