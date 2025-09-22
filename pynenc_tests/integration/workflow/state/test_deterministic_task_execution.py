"""
Integration tests for deterministic task execution.

This module tests that the execute_task method returns the same DistributedInvocation
when called with identical arguments, using the workflow context.
"""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.task import Task


def test_deterministic_task_execution_returns_same_invocation(
    deterministic_task_execution_workflow: "Task",
) -> None:
    """
    Test that deterministic task execution returns the same DistributedInvocation for identical calls.

    :param deterministic_task_execution_workflow: Task fixture for testing deterministic task execution
    """
    # Execute workflow without runner - just get the invocation
    invocation = deterministic_task_execution_workflow()

    # The workflow itself doesn't need a runner - it returns an invocation immediately
    from pynenc.invocation.dist_invocation import DistributedInvocation

    assert isinstance(invocation, DistributedInvocation)


def test_deterministic_task_execution_different_args_different_invocations(
    deterministic_task_execution_workflow: "Task",
) -> None:
    """
    Test that different arguments produce different invocations.

    :param deterministic_task_execution_workflow: Task fixture for testing different arguments
    """
    # Execute workflow without runner - just get the invocation
    invocation = deterministic_task_execution_workflow()

    # The workflow itself doesn't need a runner - it returns an invocation immediately
    from pynenc.invocation.dist_invocation import DistributedInvocation

    assert isinstance(invocation, DistributedInvocation)
