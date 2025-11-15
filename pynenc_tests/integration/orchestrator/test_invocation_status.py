import time
from typing import TYPE_CHECKING

import pytest

from pynenc.invocation import InvocationStatus
from pynenc.runner.runner_context import RunnerContext

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.invocation import DistributedInvocation
    from pynenc.task import Task


def check_status_and_get_elapsed_time(
    invocation_id: str, expected_status: InvocationStatus, app: "Pynenc"
) -> float:
    start_time = time.time()
    current_status = app.orchestrator.get_invocation_status(invocation_id)
    assert current_status == expected_status, (
        f"Expected status {expected_status}, but got {current_status}"
    )
    elapsed_time = time.time() - start_time
    return elapsed_time


# Thread runner do not need a cache, as everything is in memory already
FIRST_CALL_MIN_TIME = 0.001  # seconds


def test_get_status_performance(task_dummy_io: "Task") -> None:
    """Test that there are some cache mechanisms to improve performance of multiple calls to get same status."""
    app = task_dummy_io.app
    inv: DistributedInvocation = task_dummy_io()  # type: ignore

    app.orchestrator._register_new_invocations([inv])
    inv_id = inv.invocation_id
    runner_ctx = RunnerContext.from_runner(app.runner)
    app.orchestrator.set_invocation_status(inv_id, InvocationStatus.PENDING, runner_ctx)
    app.orchestrator.set_invocation_status(inv_id, InvocationStatus.RUNNING, runner_ctx)
    elapsed_times = []
    for i in range(10):
        elapsed_time = check_status_and_get_elapsed_time(
            inv_id, InvocationStatus.RUNNING, app
        )
        if i == 0 and elapsed_time < FIRST_CALL_MIN_TIME:
            pytest.skip(
                "First call is already fast; cache performance test is irrelevant."
            )
        elapsed_times.append(elapsed_time)
        time.sleep(0.01)  # Ensure some time passes between calls

    # Check that subsequent calls are significantly faster than the first call
    first_call_time = elapsed_times[0]
    subsequent_call_times = elapsed_times[1:]
    average_subsequent_time = sum(subsequent_call_times) / len(subsequent_call_times)
    assert average_subsequent_time < first_call_time, (
        f"Subsequent calls were not faster than the first call: "
        f"first call {first_call_time:.6f}s, average subsequent call {average_subsequent_time:.6f}s"
    )
