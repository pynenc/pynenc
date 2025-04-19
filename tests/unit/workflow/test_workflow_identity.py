from pynenc import Pynenc

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
