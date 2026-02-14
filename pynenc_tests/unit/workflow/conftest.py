"""
Shared fixtures for deterministic workflow tests.

Provides common test setup including app configuration, workflow identity,
and deterministic executor instances used across all deterministic test modules.
"""

from typing import TYPE_CHECKING

import pytest

from pynenc.workflow.workflow_identity import WorkflowIdentity

if TYPE_CHECKING:
    from pynenc.identifiers.invocation_id import InvocationId
    from pynenc.identifiers.task_id import TaskId


@pytest.fixture
def workflow_identity(task_id: "TaskId", inv_id: "InvocationId") -> WorkflowIdentity:
    """Create a test workflow identity with fixed ID for deterministic tests."""
    return WorkflowIdentity(
        workflow_id=inv_id,
        workflow_type=task_id,
        parent_workflow_id=None,
    )


@pytest.fixture
def other_workflow_identity(
    other_task_id: "TaskId", other_inv_id: "InvocationId"
) -> WorkflowIdentity:
    """Create a test workflow identity with fixed ID for deterministic tests."""
    return WorkflowIdentity(
        workflow_id=other_inv_id,
        workflow_type=other_task_id,
        parent_workflow_id=None,
    )
