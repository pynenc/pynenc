from typing import TYPE_CHECKING

from pynenc.conf import config_task
from pynenc.invocation import DistributedInvocation, InvocationStatus
from tests.conftest import MockPynenc

if TYPE_CHECKING:
    from pynenc.task import Task


def test_no_concurrency_default(app: MockPynenc, task_sum: "Task") -> None:
    """Test that the orchestrator will return the invocation to run by default

    If there are no options:
     - The orchestrator will get the registered task
    """

    fake_running_invocation = task_sum(0, 0)
    trying_to_run_invocation = task_sum(1, 1)

    assert isinstance(fake_running_invocation, DistributedInvocation)
    app.orchestrator.set_invocation_status(
        fake_running_invocation, status=InvocationStatus.RUNNING
    )
    app.broker.retrieve_invocation_mock.side_effect = [trying_to_run_invocation, None]
    trying_to_run_invocation.task.conf.running_concurrency = (
        config_task.ConcurrencyControlType.DISABLED
    )
    invocations_to_run = list(
        app.orchestrator.get_invocations_to_run(max_num_invocations=1)
    )
    assert invocations_to_run == [trying_to_run_invocation]


def test_running_concurrency_type_task(app: MockPynenc, task_sum: "Task") -> None:
    """Test that if `task.conf.running_concurrency=ConcurrencyControlType.task` is set
    it will not return an invocation of the same task to run while there is another running
    """

    fake_running_invocation = task_sum(0, 0)
    trying_to_run_invocation = task_sum(1, 1)
    assert isinstance(fake_running_invocation, DistributedInvocation)
    app.orchestrator.set_invocation_status(
        fake_running_invocation, status=InvocationStatus.RUNNING
    )
    # First it will return the invocation tryin to run
    app.broker.retrieve_invocation_mock.side_effect = [trying_to_run_invocation, None]
    trying_to_run_invocation.task.conf.running_concurrency = (
        config_task.ConcurrencyControlType.TASK
    )
    invocations_to_run = list(
        app.orchestrator.get_invocations_to_run(max_num_invocations=10)
    )
    assert invocations_to_run == []
