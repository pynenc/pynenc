"""
Unit tests for exception trigger conditions.

Tests the functionality of ExceptionCondition which triggers
based on exceptions raised by tasks with optional call argument filtering.
"""

from pynenc.arguments import Arguments
from pynenc.identifiers.call_id import CallId
from pynenc.identifiers.invocation_id import InvocationId
from pynenc.invocation.status import InvocationStatus
from pynenc.identifiers.task_id import TaskId
from pynenc.trigger.arguments import create_argument_filter
from pynenc.trigger.conditions.exception import ExceptionCondition, ExceptionContext

no_arg_filter = create_argument_filter(None)


def test_exception_condition_init(task_id: TaskId) -> None:
    """Test initialization of ExceptionCondition."""
    # Test with single exception type, no arguments
    condition = ExceptionCondition(task_id, no_arg_filter, ["ValueError"])
    assert condition.task_id == task_id
    assert condition.statuses == [InvocationStatus.FAILED]
    assert condition.exception_types == ["ValueError"]
    assert condition.arguments_filter == no_arg_filter

    # Test with multiple exception types
    condition = ExceptionCondition(
        task_id, no_arg_filter, ["ValueError", "RuntimeError"]
    )
    assert condition.task_id == task_id
    assert condition.statuses == [InvocationStatus.FAILED]
    assert condition.exception_types == ["ValueError", "RuntimeError"]
    assert condition.arguments_filter == no_arg_filter

    # Test with empty exception types list (matches any exception)
    condition = ExceptionCondition(task_id, no_arg_filter, [])
    assert condition.task_id == task_id
    assert condition.statuses == [InvocationStatus.FAILED]
    assert condition.exception_types == []
    assert condition.arguments_filter == no_arg_filter

    # Test with call arguments
    arguments_filter = create_argument_filter({"arg1": "value1"})
    condition = ExceptionCondition(task_id, arguments_filter, ["ValueError"])
    assert condition.task_id == task_id
    assert condition.statuses == [InvocationStatus.FAILED]
    assert condition.exception_types == ["ValueError"]
    assert condition.arguments_filter == arguments_filter


def test_exception_condition_id(task_id: TaskId) -> None:
    """Test condition_id generation."""
    # Same task and exception types should have same ID
    condition1 = ExceptionCondition(
        task_id, no_arg_filter, ["ValueError", "RuntimeError"]
    )
    condition2 = ExceptionCondition(
        task_id, no_arg_filter, ["RuntimeError", "ValueError"]
    )
    assert condition1.condition_id == condition2.condition_id

    # Different tasks should have different IDs
    task_id2 = TaskId("module", "other_task")
    condition3 = ExceptionCondition(
        task_id2, no_arg_filter, ["ValueError", "RuntimeError"]
    )
    assert condition1.condition_id != condition3.condition_id

    # Different exception types should have different IDs
    condition4 = ExceptionCondition(task_id, no_arg_filter, ["IndexError"])
    assert condition1.condition_id != condition4.condition_id

    # Same arguments should have same ID
    arguments_filter1 = create_argument_filter({"arg1": "value1"})
    condition5 = ExceptionCondition(task_id, arguments_filter1, ["ValueError"])
    condition6 = ExceptionCondition(task_id, arguments_filter1, ["ValueError"])
    assert condition5.condition_id == condition6.condition_id

    # Different arguments should have different IDs
    arguments_filter2 = create_argument_filter({"arg1": "value2"})
    condition7 = ExceptionCondition(task_id, arguments_filter2, ["ValueError"])
    assert condition5.condition_id != condition7.condition_id


def test_exception_condition_is_satisfied_by_matching_task_and_exception(
    call_id: CallId, inv_id: InvocationId
) -> None:
    """Test is_satisfied_by with matching task ID and exception."""
    # Create an exception condition
    condition = ExceptionCondition(
        call_id.task_id, no_arg_filter, ["ValueError", "RuntimeError"]
    )

    # Create a context with matching exception type
    context = ExceptionContext(
        call_id=call_id,
        invocation_id=inv_id,
        status=InvocationStatus.FAILED,
        arguments=Arguments(),
        exception_type="ValueError",
        exception_message="Something went wrong",
        disable_cache_args=(),
    )

    # Exception type matches, should be satisfied
    assert condition.is_satisfied_by(context)

    # Change exception type to another one in our list
    context_runtime = ExceptionContext(
        call_id=call_id,
        invocation_id=inv_id,
        arguments=Arguments(),
        status=InvocationStatus.FAILED,
        exception_type="RuntimeError",
        exception_message="Something went wrong",
        disable_cache_args=(),
    )
    assert condition.is_satisfied_by(context_runtime)

    # Change exception type to one not in our list
    context_type_error = ExceptionContext(
        call_id=call_id,
        invocation_id=inv_id,
        arguments=Arguments(),
        status=InvocationStatus.FAILED,
        exception_type="TypeError",
        exception_message="Something went wrong",
        disable_cache_args=(),
    )
    assert not condition.is_satisfied_by(context_type_error)


def test_exception_condition_with_empty_list_matches_any_exception(
    call_id: CallId, inv_id: InvocationId
) -> None:
    """Test that an empty exception list matches any exception."""
    # Create a condition with empty exception types list
    condition = ExceptionCondition(call_id.task_id, no_arg_filter, [])

    # Create a context with any exception
    context = ExceptionContext(
        call_id=call_id,
        invocation_id=inv_id,
        status=InvocationStatus.FAILED,
        arguments=Arguments(),
        exception_type="SomeRandomException",
        exception_message="Any error message",
        disable_cache_args=(),
    )

    # Should match any exception
    assert condition.is_satisfied_by(context)


def test_exception_condition_requires_failed_status(
    call_id: CallId, inv_id: InvocationId
) -> None:
    """Test that exception conditions only match on FAILED status."""
    # Create an exception condition
    condition = ExceptionCondition(call_id.task_id, no_arg_filter, ["ValueError"])

    # Create a context with matching exception but wrong status
    context_success = ExceptionContext(
        call_id=call_id,
        invocation_id=inv_id,
        status=InvocationStatus.SUCCESS,  # Not FAILED
        arguments=Arguments(),
        exception_type="ValueError",
        exception_message="Something went wrong",
        disable_cache_args=(),
    )

    # Should not be satisfied with non-FAILED status
    assert not condition.is_satisfied_by(context_success)


def test_exception_condition_is_satisfied_by_non_matching_task(
    call_id: CallId, inv_id: InvocationId, other_call_id: CallId
) -> None:
    """Test is_satisfied_by with non-matching task ID."""
    # Create an exception condition
    condition = ExceptionCondition(call_id.task_id, no_arg_filter, ["ValueError"])

    # Create a context with different task_id
    context = ExceptionContext(
        call_id=other_call_id,
        invocation_id=inv_id,
        arguments=Arguments(),
        status=InvocationStatus.FAILED,
        exception_type="ValueError",
        exception_message="Something went wrong",
        disable_cache_args=(),
    )

    assert not condition.is_satisfied_by(context)


def test_exception_condition_with_matching_arguments(
    call_id: "CallId", inv_id: "InvocationId"
) -> None:
    """Test is_satisfied_by with matching call arguments."""
    # Create condition with specific arguments
    arguments_filter = create_argument_filter({"arg1": "value1", "arg2": 42})
    condition = ExceptionCondition(call_id.task_id, arguments_filter, ["ValueError"])

    # Create context with matching arguments
    context = ExceptionContext(
        call_id=call_id,
        invocation_id=inv_id,
        status=InvocationStatus.FAILED,
        arguments=Arguments({"arg1": "value1", "arg2": 42, "extra": "ignored"}),
        exception_type="ValueError",
        exception_message="Something went wrong",
        disable_cache_args=(),
    )

    # Should be satisfied
    assert condition.is_satisfied_by(context)


def test_exception_condition_with_non_matching_arguments(
    call_id: CallId, inv_id: InvocationId
) -> None:
    """Test is_satisfied_by with non-matching call arguments."""
    # Create condition with specific arguments
    arguments_filter = create_argument_filter({"arg1": "value1", "arg2": 42})
    condition = ExceptionCondition(call_id.task_id, arguments_filter, ["ValueError"])
    # Create context with non-matching arguments
    context1 = ExceptionContext(
        call_id=call_id,
        invocation_id=inv_id,
        status=InvocationStatus.FAILED,
        arguments=Arguments({"arg1": "value1", "arg2": 99}),  # Different arg2 value
        exception_type="ValueError",
        exception_message="Something went wrong",
        disable_cache_args=(),
    )

    # Missing argument
    context2 = ExceptionContext(
        call_id=call_id,
        invocation_id=inv_id,
        status=InvocationStatus.FAILED,
        arguments=Arguments({"arg1": "value1"}),  # Missing arg2
        exception_type="ValueError",
        exception_message="Something went wrong",
        disable_cache_args=(),
    )

    # Should not be satisfied in either case
    assert not condition.is_satisfied_by(context1)
    assert not condition.is_satisfied_by(context2)


def test_exception_condition_affects_task(call_id: CallId) -> None:
    """Test affects_task method."""
    condition = ExceptionCondition(call_id.task_id, no_arg_filter, ["ValueError"])

    assert condition.affects_task(call_id.task_id)
    other_task_id = TaskId("module", "other_task")
    assert not condition.affects_task(other_task_id)
