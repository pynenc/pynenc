from pynenc import Pynenc
from pynenc.workflow.identity import WorkflowIdentity

config = {"runner_cls": "ThreadRunner"}
app = Pynenc(app_id="test_workflow_identity", config_values=config)


@app.task
def parent_task() -> int:
    # This will be the parent invocation
    return child_task().result


@app.task
def child_task() -> int:
    # Should inherit workflow from parent
    return new_sub_workflow().result


@app.task(force_new_workflow=True)
def new_sub_workflow() -> int:
    return 2


def test_workflow_identity_initialization(runner: None) -> None:
    """
    Test that every invocation is properly initialized with a workflow identity.
    """
    parent_inv = parent_task()
    assert parent_inv.result == 2
    assert parent_inv.workflow.workflow_task_id == parent_inv.task.task_id
    assert parent_inv.workflow.workflow_invocation_id == parent_inv.invocation_id
    assert parent_inv.workflow.parent_workflow is None
    childs_inv = list(app.orchestrator.get_existing_invocations(child_task))
    new_sub_inv = list(app.orchestrator.get_existing_invocations(new_sub_workflow))
    assert len(childs_inv) == 1
    child_inv = childs_inv[0]
    assert child_inv.workflow == parent_inv.workflow

    assert len(new_sub_inv) == 1
    new_sub_inv = new_sub_inv[0]
    assert new_sub_inv.workflow.workflow_task_id == new_sub_inv.task.task_id
    assert new_sub_inv.workflow.workflow_invocation_id == new_sub_inv.invocation_id
    assert new_sub_inv.workflow.parent_workflow == parent_inv.workflow


def test_workflow_identity_serialization() -> None:
    """
    Test that WorkflowIdentity can be properly serialized and deserialized.
    """
    # Create a simple workflow identity
    identity = WorkflowIdentity(
        workflow_task_id="test_task", workflow_invocation_id="test_invocation_id"
    )

    # Test basic properties
    assert identity.workflow_task_id == "test_task"
    assert identity.workflow_invocation_id == "test_invocation_id"
    assert identity.workflow_id == "test_invocation_id"
    assert identity.parent_workflow is None
    assert not identity.is_subworkflow

    # Serialize to JSON
    serialized = identity.to_json()
    assert isinstance(serialized, str)

    # Deserialize and verify
    restored = WorkflowIdentity.from_json(serialized)
    assert restored.workflow_task_id == identity.workflow_task_id
    assert restored.workflow_invocation_id == identity.workflow_invocation_id
    assert restored.parent_workflow is None
    assert not restored.is_subworkflow


@app.task(force_new_workflow=True)
def boundary_workflow_1() -> str:
    """A task that forces a new workflow boundary."""
    return boundary_workflow_1_task_2().result


@app.task
def boundary_workflow_1_task_2() -> str:
    """A task that should be part of boundary_workflow_1."""
    return "workflow boundary test"


@app.task
def workflow_entry_point() -> tuple[str, str]:
    """Entry point that calls both workflows in sequence."""
    # First workflow execution
    result1 = parent_task().result

    # Second workflow execution (with force_new_workflow)
    result2 = boundary_workflow_1().result

    return result1, result2


def test_subworkflow_boundary_with_flag(runner: None) -> None:
    """
    Test workflow boundaries with force_new_workflow flag.

    This test verifies that tasks marked with force_new_workflow=True
    create a new workflow context even when called from within another workflow.
    """
    # Execute a workflow that contains both sub-workflows
    entry_inv = workflow_entry_point()
    assert entry_inv.result == (2, "workflow boundary test")

    # Get all relevant invocations
    parent_invs = list(app.orchestrator.get_existing_invocations(parent_task))
    boundary1_invs = list(
        app.orchestrator.get_existing_invocations(boundary_workflow_1)
    )
    boundary1_task2_invs = list(
        app.orchestrator.get_existing_invocations(boundary_workflow_1_task_2)
    )

    assert len(parent_invs) == 1
    assert len(boundary1_invs) == 1
    assert len(boundary1_task2_invs) == 1

    parent_inv = parent_invs[0]
    boundary1_inv = boundary1_invs[0]
    boundary1_task2_invs = boundary1_task2_invs[0]

    # Parent task has no force_new_workflow flag, so it will inherit it from the main task
    assert parent_inv.workflow == entry_inv.workflow

    # Verify boundary_workflow_1 creates its own workflow (with force_new_workflow=True)
    assert boundary1_inv.workflow.parent_workflow == entry_inv.workflow

    # Verify boundary_workflow_1_task_2 shares boundary_workflow_1's workflow
    assert boundary1_task2_invs.workflow == boundary1_inv.workflow
