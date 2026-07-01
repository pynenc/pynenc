from typing import cast

import pytest

from pynenc import Pynenc
from pynenc.invocation import TaskInvocation, WorkflowInvocation
from pynenc.task import WorkflowTask
from pynenc.workflow import (
    DeterministicOperationScopeError,
    WorkflowMembershipError,
    WorkflowRootContext,
    WorkflowRootOps,
)

config = {"app_id": "test_workflow_root_api", "runner_cls": "ThreadRunner"}
app = Pynenc(config_values=config)
config_marked_app = Pynenc(
    config_values={
        "app_id": "test_config_marked_workflow_task",
        "runner_cls": "ThreadRunner",
        "is_workflow_task": True,
    }
)


@app.task
def child_updates_workflow_data() -> str:
    child_updates_workflow_data.wf.set_data("child_seen", True)
    return child_updates_workflow_data.wf.get_data("root_value")


@app.task
def child_attempts_root_uuid() -> str:
    return WorkflowRootOps(child_attempts_root_uuid).uuid()


@app.task
def standalone_reads_workflow_identity() -> str:
    return str(standalone_reads_workflow_identity.wf.identity.workflow_id)


@app.workflow
def root_workflow() -> dict[str, object]:
    root_workflow.wf.set_data("root_value", "shared")
    child_invocation = root_workflow.wf.root.execute_task(child_updates_workflow_data)
    return {
        "uuid": root_workflow.wf.root.uuid(),
        "random": root_workflow.wf.root.random(),
        "timestamp": root_workflow.wf.root.utc_now().isoformat(),
        "child_result": child_invocation.result,
        "child_seen": root_workflow.wf.get_data("child_seen"),
    }


@app.workflow
def root_with_invalid_child() -> None:
    _ = root_with_invalid_child.wf.root.execute_task(child_attempts_root_uuid).result


@config_marked_app.task
def config_marked_workflow() -> str:
    return cast(WorkflowTask, config_marked_workflow).wf.root.uuid()


def test_workflow_decorator_produces_workflow_task() -> None:
    assert isinstance(root_workflow, WorkflowTask)
    assert root_workflow.is_workflow_root_task is True
    assert child_updates_workflow_data.is_workflow_root_task is False


def test_internal_config_can_mark_task_as_workflow_escape_hatch() -> None:
    assert isinstance(config_marked_workflow, WorkflowTask)
    assert config_marked_workflow.is_workflow_root_task is True
    assert isinstance(config_marked_workflow.wf, WorkflowRootContext)


def test_root_ops_and_child_workflow_data_access(runner: None) -> None:
    invocation = root_workflow()
    result = invocation.result

    assert isinstance(invocation, WorkflowInvocation)
    assert result["child_result"] == "shared"
    assert result["child_seen"] is True
    assert isinstance(result["uuid"], str)
    assert isinstance(result["random"], float)
    assert isinstance(
        app.state_backend.get_invocation(invocation.invocation_id), WorkflowInvocation
    )


def test_child_root_ops_raise_scope_error(runner: None) -> None:
    invocation = root_with_invalid_child()

    with pytest.raises(DeterministicOperationScopeError):
        _ = invocation.result


def test_standalone_task_workflow_access_raises(runner: None) -> None:
    invocation = standalone_reads_workflow_identity()

    assert isinstance(invocation, TaskInvocation)
    with pytest.raises(WorkflowMembershipError):
        _ = invocation.result
