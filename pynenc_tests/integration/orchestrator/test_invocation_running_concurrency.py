import os
from typing import TYPE_CHECKING

from pynenc.conf import config_task
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc_tests.util import create_test_logger

if TYPE_CHECKING:
    from pynenc.task import Task

logger = create_test_logger(__name__)


def test_no_concurrency_default(task_sum: "Task") -> None:
    """Test that the orchestrator will return the invocation to run by default

    If there are no options:
     - The orchestrator will get the registered task
    """
    app = task_sum.app

    fake_running_invocation = task_sum(0, 0)
    trying_to_run_invocation = task_sum(1, 1)

    assert isinstance(fake_running_invocation, DistributedInvocation)
    assert isinstance(trying_to_run_invocation, DistributedInvocation)

    app.orchestrator.register_new_invocations([fake_running_invocation])
    app.orchestrator.set_invocation_status(
        fake_running_invocation.invocation_id, status=InvocationStatus.RUNNING
    )
    app.broker.route_invocations([trying_to_run_invocation])
    trying_to_run_invocation.task.conf.running_concurrency = (
        config_task.ConcurrencyControlType.DISABLED
    )
    invocations_to_run = list(
        app.orchestrator.get_invocations_to_run(max_num_invocations=1)
    )
    assert invocations_to_run == [trying_to_run_invocation]


def test_running_concurrency_type_task(task_sum: "Task") -> None:
    """Test that if `task.conf.running_concurrency=ConcurrencyControlType.task` is set
    it will not return an invocation of the same task to run while there is another running
    """
    app = task_sum.app

    # We cannot directly change the config of the task to set running_concurrency to TASK
    # And that is because on serialization, we only store the options that are passed by parameter to the task
    # that means, the options that are not default
    #
    # In this case the tasks do not have any running_concurrency specify in its decorator
    # Therefore, when we serialize the tasks, we will not store any option
    #
    # When we run this tests, the broker will return a tasks, that was serialized without options
    # And will take the default
    #
    # So we change the environment variable to set the default to TASK
    # This way, when the task is deserialized, it will have running_concurrency=
    os.environ["PYNENC__RUNNING_CONCURRENCY"] = "task"

    fake_running_invocation = task_sum(0, 0)
    trying_to_run_invocation = task_sum(1, 1)

    assert isinstance(trying_to_run_invocation, DistributedInvocation)
    assert isinstance(fake_running_invocation, DistributedInvocation)

    app.orchestrator.register_new_invocations(
        [fake_running_invocation, trying_to_run_invocation]
    )
    app.orchestrator.set_invocation_status(
        fake_running_invocation.invocation_id, status=InvocationStatus.RUNNING
    )

    # First it will return the invocation tryin to run
    logger.info(f"Routing invocation {trying_to_run_invocation.invocation_id=}")
    # With concurrency control the task should not return
    app.broker.route_invocations([trying_to_run_invocation])
    invocations_to_run = list(
        app.orchestrator.get_invocations_to_run(max_num_invocations=10)
    )
    assert invocations_to_run == []
