import threading
import time
from typing import TYPE_CHECKING, NamedTuple

import pytest

from pynenc.conf.config_task import ConcurrencyControlType
from pynenc.invocation import InvocationStatus
from pynenc.runner import ThreadRunner
from pynenc_tests.conftest import MockPynenc
from pynenc_tests.util import create_test_logger

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest

    from pynenc import Pynenc
    from pynenc.invocation import DistributedInvocation

mock_app = MockPynenc()
logger = create_test_logger(__name__)


class SleepResult(NamedTuple):
    start: float
    end: float


@mock_app.task(running_concurrency=ConcurrencyControlType.DISABLED)
def sleep_without_running_concurrency(seconds: float) -> SleepResult:
    start = time.time()
    time.sleep(seconds)
    return SleepResult(start=start, end=time.time())


@mock_app.task(
    running_concurrency=ConcurrencyControlType.TASK,
    reroute_on_concurrency_control=True,
)
def sleep_with_running_concurrency(seconds: float) -> SleepResult:
    start = time.time()
    time.sleep(seconds)
    return SleepResult(start=start, end=time.time())


@mock_app.task(
    running_concurrency=ConcurrencyControlType.TASK,
    reroute_on_concurrency_control=False,
)
def sleep_with_running_concurrency_no_reroute(seconds: float) -> SleepResult:
    start = time.time()
    time.sleep(seconds)
    return SleepResult(start=start, end=time.time())


def any_run_in_parallel(results: list[SleepResult]) -> bool:
    """Check if any tasks in the list ran in parallel (overlapping times)."""
    sorted_results = sorted(results, key=lambda x: x.start)
    for i in range(1, len(sorted_results)):
        if sorted_results[i].start < sorted_results[i - 1].end:
            logger.warning(f"Overlap: {sorted_results[i - 1]} and {sorted_results[i]}")
            return True  # Found an overlap, tasks ran in parallel
    return False  # No overlap found, tasks did not run in parallel


@pytest.fixture
def app(request: "FixtureRequest", app_instance: "Pynenc") -> "Pynenc":
    app = app_instance
    app.runner = ThreadRunner(app)
    app._tasks = mock_app._tasks
    sleep_without_running_concurrency.app = app
    sleep_with_running_concurrency.app = app
    sleep_with_running_concurrency_no_reroute.app = app
    app.purge()
    request.addfinalizer(app.purge)
    return app


def test_running_concurrency(app: "Pynenc") -> None:
    """checks that the concurrency control will prevent running multiple tasks at the same time"""
    # first purge any invocation pending from previous runs
    app.purge()

    # start a runner
    def run_in_thread() -> None:
        app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    # app.logger.info("check that without control runs in parallel")
    # no_control_invocations = [
    #     tasks.sleep_without_running_concurrency(0.1) for _ in range(10)
    # ]
    # no_control_results = [i.result for i in no_control_invocations]
    # if not any_run_in_parallel(no_control_results):
    #     raise AssertionError(f"Expected parallel execution, got {no_control_results}")

    logger.info("check that with control does not run in parallel")
    controlled_invocations = [sleep_with_running_concurrency(0.1) for _ in range(10)]
    controlled_results = [i.result for i in controlled_invocations]
    if any_run_in_parallel(controlled_results):
        raise AssertionError(f"Expected sequential execution, got {controlled_results}")

    # stop the runner
    app.runner.stop_runner_loop()
    thread.join()


def test_basic_running_concurrency_check(app: "Pynenc") -> None:
    """Basic concurency control check"""
    invocation1 = sleep_with_running_concurrency(0.5)
    owner_id = "test_owner"
    app.orchestrator._atomic_status_transition(
        invocation1.invocation_id, InvocationStatus.PENDING, owner_id
    )
    app.orchestrator._atomic_status_transition(
        invocation1.invocation_id, InvocationStatus.RUNNING, owner_id
    )
    invocation2: DistributedInvocation = sleep_with_running_concurrency(0.1)  # type: ignore
    assert (
        app.orchestrator.is_authorize_to_run_by_concurrency_control(invocation2)
        is False
    )


def test_running_concurrency_no_reroute(app: "Pynenc") -> None:
    """
    Test that concurrency control without reroute marks blocked invocations as CONCURRENCY_CONTROLLED_FINAL.
    """
    app.purge()

    # Start a runner
    def run_in_thread() -> None:
        app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    logger.info("Testing concurrency control without reroute")

    # Create multiple invocations
    invocations = [sleep_with_running_concurrency_no_reroute(0.3) for _ in range(5)]

    # Give runner time to process (task sleeps 0.3s + runner overhead)
    time.sleep(0.8)

    # Check statuses - only one should have run (SUCCESS), others should be CONCURRENCY_CONTROLLED_FINAL
    statuses = [
        app.orchestrator.get_invocation_status(inv.invocation_id) for inv in invocations
    ]

    success_count = sum(1 for s in statuses if s == InvocationStatus.SUCCESS)
    concurrency_final_count = sum(
        1 for s in statuses if s == InvocationStatus.CONCURRENCY_CONTROLLED_FINAL
    )

    logger.info(f"Statuses: {statuses}")
    logger.info(
        f"Success: {success_count}, Concurrency Final: {concurrency_final_count}"
    )

    assert success_count == 1, (
        f"Expected exactly 1 successful invocation, got {success_count}"
    )
    assert concurrency_final_count == 4, (
        f"Expected 4 concurrency-controlled-final invocations, got {concurrency_final_count}"
    )

    # Verify that blocked invocations have no result
    for inv, status in zip(invocations, statuses, strict=False):
        if status == InvocationStatus.CONCURRENCY_CONTROLLED_FINAL:
            with pytest.raises(KeyError):
                _ = inv.result

    # Stop the runner
    app.runner.stop_runner_loop()
    thread.join()
