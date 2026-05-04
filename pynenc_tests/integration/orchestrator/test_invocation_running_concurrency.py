from typing import TYPE_CHECKING


from pynenc.conf import config_task
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc.runner.runner_context import RunnerContext
from pynenc_tests.util import create_test_logger

if TYPE_CHECKING:
    from _pytest.monkeypatch import MonkeyPatch

    from pynenc.task import Task

logger = create_test_logger(__name__)


def test_no_concurrency_default(task_sum_io: "Task") -> None:
    """Test that the orchestrator will return the invocation to run by default

    If there are no options:
     - The orchestrator will get the registered task
    """
    app = task_sum_io.app

    fake_running_invocation = task_sum_io(0, 0)
    trying_to_run_invocation = task_sum_io(1, 1)

    assert isinstance(fake_running_invocation, DistributedInvocation)
    assert isinstance(trying_to_run_invocation, DistributedInvocation)

    app.orchestrator.register_new_invocations([fake_running_invocation])
    runner_ctx = RunnerContext.from_runner(app.runner)
    app.orchestrator.set_invocation_status(
        fake_running_invocation.invocation_id, InvocationStatus.PENDING, runner_ctx
    )
    app.orchestrator.set_invocation_status(
        fake_running_invocation.invocation_id, InvocationStatus.RUNNING, runner_ctx
    )
    app.broker.route_invocations([trying_to_run_invocation.invocation_id])
    trying_to_run_invocation.task.conf.running_concurrency = (
        config_task.ConcurrencyControlType.DISABLED
    )
    invocations_to_run = list(app.orchestrator.get_invocations_to_run(1, runner_ctx))
    assert invocations_to_run == [trying_to_run_invocation]


def test_running_concurrency_type_task(
    task_sum_io: "Task", monkeypatch: "MonkeyPatch"
) -> None:
    """Test that if `task.conf.running_concurrency=ConcurrencyControlType.task` is set
    it will not return an invocation of the same task to run while there is another running
    """
    app = task_sum_io.app
    runner_ctx = RunnerContext.from_runner(app.runner)
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
    monkeypatch.setenv("PYNENC__RUNNING_CONCURRENCY", "task")

    fake_running_invocation = task_sum_io(0, 0)
    trying_to_run_invocation = task_sum_io(1, 1)

    assert isinstance(trying_to_run_invocation, DistributedInvocation)
    assert isinstance(fake_running_invocation, DistributedInvocation)

    # Ensure the invocation/task has the desired running_concurrency explicitly.
    # Setting it on the invocation object avoids relying on environment timing
    # or fixture creation order.
    fake_running_invocation.task.conf.running_concurrency = (
        config_task.ConcurrencyControlType.TASK
    )
    trying_to_run_invocation.task.conf.running_concurrency = (
        config_task.ConcurrencyControlType.TASK
    )

    app.orchestrator.register_new_invocations(
        [fake_running_invocation, trying_to_run_invocation]
    )
    app.orchestrator.set_invocation_status(
        fake_running_invocation.invocation_id, InvocationStatus.PENDING, runner_ctx
    )
    app.orchestrator.set_invocation_status(
        fake_running_invocation.invocation_id, InvocationStatus.RUNNING, runner_ctx
    )

    # First it will return the invocation tryin to run
    logger.info(f"Routing invocation {trying_to_run_invocation.invocation_id=}")
    # With concurrency control the task should not return
    app.broker.route_invocations([trying_to_run_invocation.invocation_id])
    invocations_to_run = list(app.orchestrator.get_invocations_to_run(10, runner_ctx))
    assert invocations_to_run == []


def test_running_concurrency_keys_uses_key_arguments(task_key_arg_io: "Task") -> None:
    """KEYS running concurrency blocks only matching keys, not every task call."""
    app = task_key_arg_io.app
    runner_ctx = RunnerContext.from_runner(app.runner)

    task_key_arg_io.conf.running_concurrency = config_task.ConcurrencyControlType.KEYS
    task_key_arg_io.conf.key_arguments = ("key",)

    fake_running_invocation = task_key_arg_io("account-a", "fetch_profile")
    blocked_same_key_invocation = task_key_arg_io("account-a", "list_invoices")
    allowed_other_key_invocation = task_key_arg_io("account-b", "fetch_profile")

    assert isinstance(fake_running_invocation, DistributedInvocation)
    assert isinstance(blocked_same_key_invocation, DistributedInvocation)
    assert isinstance(allowed_other_key_invocation, DistributedInvocation)

    app.orchestrator.set_invocation_status(
        fake_running_invocation.invocation_id, InvocationStatus.PENDING, runner_ctx
    )
    app.orchestrator.set_invocation_status(
        fake_running_invocation.invocation_id, InvocationStatus.RUNNING, runner_ctx
    )

    app.broker.purge()
    app.broker.route_invocations(
        [
            blocked_same_key_invocation.invocation_id,
            allowed_other_key_invocation.invocation_id,
        ]
    )

    invocations_to_run = list(app.orchestrator.get_invocations_to_run(10, runner_ctx))

    assert blocked_same_key_invocation not in invocations_to_run
    assert invocations_to_run == [allowed_other_key_invocation]
