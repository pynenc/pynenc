import threading
import time
from typing import TYPE_CHECKING, NamedTuple

import pytest

from pynenc.conf.config_task import ConcurrencyControlType
from pynenc.runner import ThreadRunner
from pynenc_tests.conftest import MockPynenc

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest

    from pynenc import Pynenc

mock_app = MockPynenc()


class SleepResult(NamedTuple):
    start: float
    end: float


@mock_app.task(running_concurrency=ConcurrencyControlType.DISABLED)
def sleep_without_running_concurrency(seconds: float) -> SleepResult:
    start = time.time()
    time.sleep(seconds)
    return SleepResult(start=start, end=time.time())


@mock_app.task(running_concurrency=ConcurrencyControlType.TASK)
def sleep_with_running_concurrency(seconds: float) -> SleepResult:
    start = time.time()
    time.sleep(seconds)
    return SleepResult(start=start, end=time.time())


def any_run_in_parallel(results: list[SleepResult]) -> bool:
    """Check if any tasks in the list ran in parallel (overlapping times)."""
    sorted_results = sorted(results, key=lambda x: x.start)
    for i in range(1, len(sorted_results)):
        if sorted_results[i].start < sorted_results[i - 1].end:
            mock_app.logger.warning(
                f"Overlap: {sorted_results[i - 1]} and {sorted_results[i]}"
            )
            return True  # Found an overlap, tasks ran in parallel
    return False  # No overlap found, tasks did not run in parallel


@pytest.fixture
def app(request: "FixtureRequest", app_instance: "Pynenc") -> "Pynenc":
    app = app_instance
    app.runner = ThreadRunner(app)
    app._tasks = mock_app._tasks
    sleep_without_running_concurrency.app = app
    sleep_with_running_concurrency.app = app
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

    app.logger.info("check that with control does not run in parallel")
    controlled_invocations = [sleep_with_running_concurrency(0.1) for _ in range(10)]
    controlled_results = [i.result for i in controlled_invocations]
    if any_run_in_parallel(controlled_results):
        raise AssertionError(f"Expected sequential execution, got {controlled_results}")

    # stop the runner
    app.runner.stop_runner_loop()
    thread.join()
