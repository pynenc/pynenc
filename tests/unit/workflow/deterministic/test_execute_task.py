"""
Unit tests for deterministic task execution.

This module tests the execute_task method of the DeterministicExecutor,
verifying that it returns the same DistributedInvocation for identical calls.
"""
from typing import TYPE_CHECKING

from pynenc.workflow.deterministic import DeterministicExecutor

if TYPE_CHECKING:
    from pynenc.task import Task


def test_execute_task_returns_distributed_invocation(
    deterministic_executor: DeterministicExecutor, app_with_task: "Task"
) -> None:
    """Test that execute_task returns a DistributedInvocation object."""
    from pynenc.invocation.dist_invocation import DistributedInvocation

    result = deterministic_executor.execute_task(app_with_task, 10, 20)

    assert isinstance(result, DistributedInvocation)


def test_execute_task_same_args_same_invocation_id(
    deterministic_executor: DeterministicExecutor, app_with_task: "Task"
) -> None:
    """Test that same arguments return the same invocation_id."""
    # Execute task twice with identical arguments
    result1 = deterministic_executor.execute_task(app_with_task, 10, 20)
    result2 = deterministic_executor.execute_task(app_with_task, 10, 20)

    # Should return same invocation_id (cached)
    assert result1.invocation_id == result2.invocation_id
    assert result1.call_id == result2.call_id


def test_execute_task_different_args_different_invocation_ids(
    deterministic_executor: DeterministicExecutor, app_with_task: "Task"
) -> None:
    """Test that different arguments produce different invocation_ids."""
    result1 = deterministic_executor.execute_task(app_with_task, 10, 20)
    result2 = deterministic_executor.execute_task(app_with_task, 5, 15)
    result3 = deterministic_executor.execute_task(app_with_task, 100, 200)

    # All should have different invocation_ids
    invocation_ids = {
        result1.invocation_id,
        result2.invocation_id,
        result3.invocation_id,
    }
    assert len(invocation_ids) == 3

    # All should have different call_ids
    call_ids = {result1.call_id, result2.call_id, result3.call_id}
    assert len(call_ids) == 3


def test_execute_task_deterministic_replay(
    deterministic_executor: DeterministicExecutor, app_with_task: "Task"
) -> None:
    """Test that execute_task is deterministic across different executor instances."""
    # Execute task first time
    result1 = deterministic_executor.execute_task(app_with_task, 5, 15)

    # Create new executor with same workflow identity
    new_executor = DeterministicExecutor(
        deterministic_executor.workflow_identity, deterministic_executor.app
    )

    # Execute same task with same arguments - should replay
    result2 = new_executor.execute_task(app_with_task, 5, 15)

    # Should return same invocation_id
    assert result1.invocation_id == result2.invocation_id
    assert result1.call_id == result2.call_id


def test_execute_task_stores_invocation_id_only(
    deterministic_executor: DeterministicExecutor, app_with_task: "Task"
) -> None:
    """Test that execute_task stores only the invocation_id, not the whole invocation."""
    from pynenc.arguments import Arguments
    from pynenc.call import Call

    # Execute task
    result = deterministic_executor.execute_task(app_with_task, 42, 58)

    # Create expected storage key
    arguments = Arguments.from_call(app_with_task.func, 42, 58)
    call: "Call" = Call(task=app_with_task, _arguments=arguments)
    expected_key = f"task_invocation:{call.call_id}"

    # Verify only the invocation_id is stored
    stored_data = deterministic_executor.app.state_backend.get_workflow_data(
        deterministic_executor.workflow_identity, expected_key
    )

    assert stored_data is not None
    assert stored_data == result.invocation_id
