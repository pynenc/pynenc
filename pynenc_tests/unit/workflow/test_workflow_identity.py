from pynenc import Pynenc

config = {"app_id": "test_workflow_identity", "runner_cls": "ThreadRunner"}
app = Pynenc(config_values=config)


@app.workflow
def parent_task() -> int:
    # This will be the parent invocation
    return child_task().result


@app.task
def child_task() -> int:
    # Should inherit workflow from parent
    return new_sub_workflow().result


@app.workflow
def new_sub_workflow() -> int:
    return 2


def test_workflow_identity_initialization(runner: None) -> None:
    """
    Test that every invocation is properly initialized with a workflow identity.
    """
    parent_inv = parent_task()
    assert parent_inv.result == 2
    parent_workflow = parent_inv.workflow
    assert parent_workflow is not None
    assert parent_workflow.workflow_id == parent_inv.invocation_id
    assert parent_workflow.workflow_type == parent_inv.task.task_id
    assert parent_workflow.parent_workflow_id is None
    childs_inv_ids = list(app.orchestrator.get_existing_invocations(child_task))
    new_sub_inv_ids = list(app.orchestrator.get_existing_invocations(new_sub_workflow))
    assert len(childs_inv_ids) == 1
    child_inv = app.state_backend.get_invocation(childs_inv_ids[0])
    assert child_inv.workflow == parent_workflow

    assert len(new_sub_inv_ids) == 1
    new_sub_inv = app.state_backend.get_invocation(new_sub_inv_ids[0])
    sub_workflow = new_sub_inv.workflow
    assert sub_workflow is not None
    assert sub_workflow.workflow_type == new_sub_inv.task.task_id
    assert sub_workflow.workflow_id == new_sub_inv.invocation_id
    assert sub_workflow.parent_workflow_id == parent_workflow.workflow_id


@app.workflow
def boundary_workflow_1() -> str:
    """A task that forces a new workflow boundary."""
    return boundary_workflow_1_task_2().result


@app.task
def boundary_workflow_1_task_2() -> str:
    """A task that should be part of boundary_workflow_1."""
    return "workflow boundary test"


@app.workflow
def workflow_entry_point() -> tuple[str, str]:
    """Entry point that calls both workflows in sequence."""
    # First workflow execution
    result1 = parent_task().result

    # Second explicit sub-workflow execution
    result2 = boundary_workflow_1().result

    return result1, result2


def test_subworkflow_boundary_with_flag(runner: None) -> None:
    """
    Test workflow boundaries with explicit workflow tasks.

    This test verifies that tasks marked with @app.workflow
    create a new workflow context even when called from within another workflow.
    """
    # Execute a workflow that contains both sub-workflows
    entry_inv = workflow_entry_point()
    assert entry_inv.result == (2, "workflow boundary test")

    # Get all relevant invocations
    parent_inv_ids = list(app.orchestrator.get_existing_invocations(parent_task))
    boundary1_inv_ids = list(
        app.orchestrator.get_existing_invocations(boundary_workflow_1)
    )
    boundary1_task2_inv_ids = list(
        app.orchestrator.get_existing_invocations(boundary_workflow_1_task_2)
    )

    assert len(parent_inv_ids) == 1
    assert len(boundary1_inv_ids) == 1
    assert len(boundary1_task2_inv_ids) == 1

    parent_inv = app.state_backend.get_invocation(parent_inv_ids[0])
    boundary1_inv = app.state_backend.get_invocation(boundary1_inv_ids[0])
    boundary1_task2_inv = app.state_backend.get_invocation(boundary1_task2_inv_ids[0])
    entry_workflow = entry_inv.workflow
    parent_workflow = parent_inv.workflow
    boundary1_workflow = boundary1_inv.workflow

    assert entry_workflow is not None
    assert parent_workflow is not None
    assert boundary1_workflow is not None

    # Explicit workflow tasks called inside another workflow become sub-workflows.
    assert parent_workflow.parent_workflow_id == entry_workflow.workflow_id

    # Verify boundary_workflow_1 creates its own workflow.
    assert boundary1_workflow.parent_workflow_id == entry_workflow.workflow_id

    # Verify boundary_workflow_1_task_2 shares boundary_workflow_1's workflow
    assert boundary1_task2_inv.workflow == boundary1_workflow
