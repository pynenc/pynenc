"""
Unit tests for result trigger conditions.

Tests the functionality of ResultCondition which triggers based on
the result of a task execution with optional call argument filtering.
"""

from typing import TYPE_CHECKING

from pynenc.arguments import Arguments
from pynenc.trigger.arguments import create_argument_filter
from pynenc.trigger.arguments.result_filter import (
    CallableResultFilter,
    ResultFilterProtocol,
    StaticResultFilter,
    create_result_filter,
)
from pynenc.trigger.conditions.result import ResultCondition, ResultContext

if TYPE_CHECKING:
    from pynenc.identifiers.call_id import CallId
    from pynenc.identifiers.invocation_id import InvocationId
    from pynenc.identifiers.task_id import TaskId

no_arg_filter = create_argument_filter(None)


def test_result_condition_init(task_id: "TaskId") -> None:
    """Test initialization of ResultCondition."""
    # Test with simple expected result, no arguments
    condition = ResultCondition(
        task_id,
        result_filter=create_result_filter(42),
        arguments_filter=no_arg_filter,
    )
    assert condition.task_id == task_id
    assert isinstance(condition.result_filter, ResultFilterProtocol)
    assert condition.result_filter.filter_result(42)
    assert not condition.result_filter.filter_result(99)
    assert condition.arguments_filter == no_arg_filter

    # Test with complex expected result
    complex_result = {"status": "completed", "value": 100}
    condition = ResultCondition(
        task_id,
        result_filter=create_result_filter(complex_result),
        arguments_filter=no_arg_filter,
    )
    assert condition.task_id == task_id
    assert isinstance(condition.result_filter, ResultFilterProtocol)
    assert condition.result_filter.filter_result(complex_result)
    assert not condition.result_filter.filter_result({"status": "failed"})
    assert condition.arguments_filter == no_arg_filter

    # Test with call arguments
    arguments_filter = create_argument_filter({"arg1": "value1"})
    condition = ResultCondition(
        task_id,
        result_filter=create_result_filter(42),
        arguments_filter=arguments_filter,
    )
    assert condition.task_id == task_id
    assert isinstance(condition.result_filter, ResultFilterProtocol)
    assert condition.result_filter.filter_result(42)
    assert condition.arguments_filter == arguments_filter

    # Test with explicit ResultFilterProtocol
    result_filter = StaticResultFilter(42)
    condition = ResultCondition(
        task_id, result_filter=result_filter, arguments_filter=no_arg_filter
    )
    assert condition.task_id == task_id
    assert condition.result_filter is result_filter
    assert condition.result_filter.filter_result(42)


def test_result_condition_id(task_id: "TaskId", other_task_id: "TaskId") -> None:
    """Test condition_id generation."""
    # Same task and expected result should have same ID
    condition1 = ResultCondition(
        task_id,
        result_filter=create_result_filter(42),
        arguments_filter=no_arg_filter,
    )
    condition2 = ResultCondition(
        task_id,
        result_filter=create_result_filter(42),
        arguments_filter=no_arg_filter,
    )
    assert condition1.condition_id == condition2.condition_id

    # Different tasks should have different IDs
    condition3 = ResultCondition(
        other_task_id,
        result_filter=create_result_filter(42),
        arguments_filter=no_arg_filter,
    )
    assert condition1.condition_id != condition3.condition_id

    # Different expected results should have different IDs
    condition4 = ResultCondition(
        task_id,
        result_filter=create_result_filter(99),
        arguments_filter=no_arg_filter,
    )
    assert condition1.condition_id != condition4.condition_id

    # Same arguments should have same ID
    arguments_filter1 = create_argument_filter({"arg1": "value1"})
    condition5 = ResultCondition(
        task_id,
        result_filter=create_result_filter(42),
        arguments_filter=arguments_filter1,
    )
    condition6 = ResultCondition(
        task_id,
        result_filter=create_result_filter(42),
        arguments_filter=arguments_filter1,
    )
    assert condition5.condition_id == condition6.condition_id

    # Different arguments should have different IDs
    arguments_filter2 = create_argument_filter({"arg1": "value2"})
    condition7 = ResultCondition(
        task_id,
        result_filter=create_result_filter(42),
        arguments_filter=arguments_filter2,
    )
    assert condition5.condition_id != condition7.condition_id


def test_result_condition_is_satisfied_by_matching_task_and_result(
    call_id: "CallId", inv_id: "InvocationId"
) -> None:
    """Test is_satisfied_by with matching task ID and result."""
    # Create a result condition
    condition = ResultCondition(
        call_id.task_id,
        result_filter=create_result_filter(42),
        arguments_filter=no_arg_filter,
    )

    # Create a context with matching result
    context = ResultContext(
        call_id=call_id,
        invocation_id=inv_id,
        result=42,
        arguments=Arguments(),
        status=condition.statuses[0],  # Use the SUCCESS status
        disable_cache_args=(),
    )

    # Result matches, should be satisfied
    assert condition.is_satisfied_by(context)

    # Change result to non-matching value
    context_different = ResultContext(
        call_id=call_id,
        invocation_id=inv_id,
        arguments=Arguments(),
        status=condition.statuses[0],  # Use the SUCCESS status
        result=99,
        disable_cache_args=(),
    )
    assert not condition.is_satisfied_by(context_different)


def test_result_condition_is_satisfied_by_non_matching_task(
    call_id: "CallId", inv_id: "InvocationId", other_call_id: "CallId"
) -> None:
    """Test is_satisfied_by with non-matching task ID."""
    # Create a result condition
    condition = ResultCondition(
        call_id.task_id,
        result_filter=create_result_filter(42),
        arguments_filter=no_arg_filter,
    )

    # Create a context with different task_id
    context = ResultContext(
        call_id=other_call_id,
        invocation_id=inv_id,
        arguments=Arguments(),
        status=condition.statuses[0],  # Use the SUCCESS status
        result=42,  # Would satisfy condition if task ID matched
        disable_cache_args=(),
    )

    assert not condition.is_satisfied_by(context)


def test_result_condition_with_matching_arguments(
    call_id: "CallId", inv_id: "InvocationId"
) -> None:
    """Test is_satisfied_by with matching call arguments."""
    # Create condition with specific arguments
    arguments_filter = create_argument_filter({"arg1": "value1", "arg2": 42})
    condition = ResultCondition(
        call_id.task_id,
        result_filter=create_result_filter(42),
        arguments_filter=arguments_filter,
    )

    # Create context with matching arguments
    context = ResultContext(
        call_id=call_id,
        invocation_id=inv_id,
        result=42,
        arguments=Arguments({"arg1": "value1", "arg2": 42, "extra": "ignored"}),
        status=condition.statuses[0],  # Use the SUCCESS status
        disable_cache_args=(),
    )

    # Should be satisfied
    assert condition.is_satisfied_by(context)


def test_result_condition_with_non_matching_arguments(
    call_id: "CallId", inv_id: "InvocationId"
) -> None:
    """Test is_satisfied_by with non-matching call arguments."""
    # Create condition with specific arguments
    arguments_filter = create_argument_filter({"arg1": "value1", "arg2": 42})
    condition = ResultCondition(
        call_id.task_id,
        result_filter=create_result_filter(42),
        arguments_filter=arguments_filter,
    )

    # Create context with non-matching arguments
    context1 = ResultContext(
        call_id=call_id,
        invocation_id=inv_id,
        result=42,
        arguments=Arguments({"arg1": "value1", "arg2": 99}),  # Different arg2 value
        status=condition.statuses[0],  # Use the SUCCESS status
        disable_cache_args=(),
    )

    # Missing argument
    context2 = ResultContext(
        call_id=call_id,
        invocation_id=inv_id,
        result=42,
        arguments=Arguments({"arg1": "value1"}),  # Missing arg2
        status=condition.statuses[0],  # Use the SUCCESS status
        disable_cache_args=(),
    )

    # Should not be satisfied in either case
    assert not condition.is_satisfied_by(context1)
    assert not condition.is_satisfied_by(context2)


def test_result_condition_with_no_arguments_restriction(
    call_id: "CallId", inv_id: "InvocationId"
) -> None:
    """Test that a condition with no arguments matches any call arguments."""
    # Create condition with no argument restrictions
    condition = ResultCondition(
        call_id.task_id,
        result_filter=create_result_filter(42),
        arguments_filter=no_arg_filter,
    )

    # Create context with arguments
    context = ResultContext(
        call_id=call_id,
        invocation_id=inv_id,
        result=42,
        arguments=Arguments({"arg1": "value1", "arg2": 42}),
        status=condition.statuses[0],  # Use the SUCCESS status
        disable_cache_args=(),
    )

    # Should match any arguments
    assert condition.is_satisfied_by(context)


def test_result_condition_affects_task(
    task_id: "TaskId", other_task_id: "TaskId"
) -> None:
    """Test affects_task method."""
    condition = ResultCondition(
        task_id,
        result_filter=create_result_filter(42),
        arguments_filter=no_arg_filter,
    )

    assert condition.affects_task(task_id)
    assert not condition.affects_task(other_task_id)


def test_result_condition_complex_result_matching(
    call_id: "CallId", inv_id: "InvocationId"
) -> None:
    """Test matching with complex result objects like dictionaries and lists."""
    # Dictionary result
    dict_result = {"status": "success", "data": [1, 2, 3]}
    condition = ResultCondition(
        call_id.task_id,
        result_filter=create_result_filter(dict_result),
        arguments_filter=no_arg_filter,
    )

    context = ResultContext(
        call_id=call_id,
        invocation_id=inv_id,
        result={"status": "success", "data": [1, 2, 3]},  # Same structure
        arguments=Arguments(),
        status=condition.statuses[0],  # Use the SUCCESS status
        disable_cache_args=(),
    )
    assert condition.is_satisfied_by(context)

    # Different dict order should still match
    context.result = {"data": [1, 2, 3], "status": "success"}
    assert condition.is_satisfied_by(context)

    # Different value shouldn't match
    context.result = {"status": "failure", "data": [1, 2, 3]}
    assert not condition.is_satisfied_by(context)

    # List result
    list_result = [1, 2, 3]
    condition = ResultCondition(
        call_id.task_id,
        result_filter=create_result_filter(list_result),
        arguments_filter=no_arg_filter,
    )

    context.result = [1, 2, 3]
    assert condition.is_satisfied_by(context)

    context.result = [3, 2, 1]
    assert not condition.is_satisfied_by(context)


def result_greater_than_10(result: int) -> bool:
    return result > 10


def test_callable_result_filter(call_id: "CallId", inv_id: "InvocationId") -> None:
    """Test filtering results with a callable function."""

    # Create a condition with the callable result filter
    condition = ResultCondition(
        call_id.task_id,
        result_filter=CallableResultFilter(result_greater_than_10),
        arguments_filter=no_arg_filter,
    )

    # Test with result > 10
    context = ResultContext(
        call_id=call_id,
        invocation_id=inv_id,
        result=15,
        arguments=Arguments(),
        status=condition.statuses[0],  # Use the SUCCESS status
        disable_cache_args=(),
    )
    assert condition.is_satisfied_by(context)

    # Test with result <= 10
    context.result = 5
    assert not condition.is_satisfied_by(context)

    # Test with direct callable
    direct_condition = ResultCondition(
        call_id.task_id,
        result_filter=create_result_filter(result_greater_than_10),
        arguments_filter=no_arg_filter,
    )

    context.result = 15
    assert direct_condition.is_satisfied_by(context)

    context.result = 5
    assert not direct_condition.is_satisfied_by(context)
