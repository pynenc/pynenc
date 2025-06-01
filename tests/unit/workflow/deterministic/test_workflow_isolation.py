"""
Tests for workflow isolation and state backend integration.

This module tests that different workflows maintain isolated deterministic
sequences and that state backend persistence works correctly.
"""
from pynenc import Pynenc
from pynenc.workflow.deterministic import DeterministicExecutor
from pynenc.workflow.identity import WorkflowIdentity


def test_workflow_isolation(app: Pynenc) -> None:
    """Test that different workflows have isolated deterministic sequences."""
    # Create two different workflow identities
    workflow1 = WorkflowIdentity(
        workflow_task_id="workflow1",
        workflow_invocation_id="workflow1-invocation-123",
        parent_workflow=None,
    )
    workflow2 = WorkflowIdentity(
        workflow_task_id="workflow2",
        workflow_invocation_id="workflow2-invocation-456",
        parent_workflow=None,
    )

    executor1 = DeterministicExecutor(workflow1, app)
    executor2 = DeterministicExecutor(workflow2, app)

    # Generate sequences in both workflows
    randoms1 = [executor1.random() for _ in range(3)]
    randoms2 = [executor2.random() for _ in range(3)]

    # Sequences should be different between workflows
    assert randoms1 != randoms2

    # But each workflow should be internally consistent
    new_executor1 = DeterministicExecutor(workflow1, app)
    new_randoms1 = [new_executor1.random() for _ in range(3)]
    assert randoms1 == new_randoms1


def test_state_backend_persistence(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test integration with state backend for persistence."""
    # Generate some operations
    random_val = deterministic_executor.random()
    time_val = deterministic_executor.utc_now()
    uuid_val = deterministic_executor.uuid()

    # Create new executor with same workflow identity
    new_executor = DeterministicExecutor(
        deterministic_executor.workflow_identity, deterministic_executor.app
    )

    # Should get same values from state backend
    new_random = new_executor.random()
    new_time = new_executor.utc_now()
    new_uuid = new_executor.uuid()

    assert random_val == new_random
    assert time_val == new_time
    assert uuid_val == new_uuid


def test_state_backend_basic_operations(
    app: Pynenc, workflow_identity: WorkflowIdentity
) -> None:
    """Test that the state backend functions correctly."""
    # Direct test of state backend methods
    test_key = "test_key"
    test_value = "test_value"

    # Test set
    app.state_backend.set_workflow_deterministic_value(
        workflow_identity, test_key, test_value
    )

    # Test get
    retrieved = app.state_backend.get_workflow_deterministic_value(
        workflow_identity, test_key
    )
    assert retrieved == test_value

    # Test get non-existent returns None
    non_existent = app.state_backend.get_workflow_deterministic_value(
        workflow_identity, "non_existent"
    )
    assert non_existent is None
