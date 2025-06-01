from pynenc import Pynenc

config = {"runner_cls": "ThreadRunner"}
app = Pynenc(app_id="test_workflow_identity", config_values=config)


@app.task
def standalone_task() -> str:
    """A task with no parent workflow."""
    return "standalone"


@app.task
def workflow_chain_1() -> str:
    """First task in a chain."""
    return workflow_chain_2().result


@app.task
def workflow_chain_2() -> str:
    """Second task in a chain."""
    return workflow_chain_3().result


@app.task
def workflow_chain_3() -> str:
    """Third task in a chain."""
    return "chain completed"


def test_standalone_task_creates_workflow(runner: None) -> None:
    """
    Test that a standalone task automatically creates its own workflow.
    """
    inv = standalone_task()
    assert inv.result == "standalone"

    # Verify workflow was created
    assert inv.workflow is not None
    assert inv.workflow.workflow_task_id == standalone_task.task_id
    assert inv.workflow.workflow_invocation_id == inv.invocation_id
    assert inv.workflow.parent_workflow is None


def test_workflow_inheritance_in_chain(runner: None) -> None:
    """
    Test that workflow identity is properly inherited through a chain of tasks.
    """
    # Execute a chain of tasks
    chain_inv = workflow_chain_1()
    assert chain_inv.result == "chain completed"

    # Get invocations for all tasks in the chain
    chain_1_inv = chain_inv
    chain_2_invs = list(app.orchestrator.get_existing_invocations(workflow_chain_2))
    chain_3_invs = list(app.orchestrator.get_existing_invocations(workflow_chain_3))

    assert len(chain_2_invs) == 1
    assert len(chain_3_invs) == 1

    chain_2_inv = chain_2_invs[0]
    chain_3_inv = chain_3_invs[0]

    # Verify all tasks share the same workflow identity
    assert chain_1_inv.workflow == chain_2_inv.workflow
    assert chain_2_inv.workflow == chain_3_inv.workflow

    # Verify workflow identity matches the first task (chain_1)
    assert chain_1_inv.workflow.workflow_task_id == workflow_chain_1.task_id
    assert chain_1_inv.workflow.workflow_invocation_id == chain_1_inv.invocation_id
