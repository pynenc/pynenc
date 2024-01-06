import os
import threading
from collections import Counter
from time import sleep, time
from unittest.mock import patch

import pytest

from pynenc import Task
from pynenc.exceptions import CycleDetectedError
from pynenc.invocation import DistributedInvocation, InvocationStatus


def test_task_execution(task_sum: Task) -> None:
    """Test the whole lifecycle of a task execution"""
    app = task_sum.app

    def run_in_thread() -> None:
        app.runner.run()

    invocation = task_sum(1, 2)
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    assert invocation.result == 3
    app.runner.stop_runner_loop()
    thread.join()


def test_task_retry(task_retry_once: Task) -> None:
    """Test that the task will retry if it raises a RetryError"""
    app = task_retry_once.app

    def run_in_thread() -> None:
        app.runner.run()

    invocation = task_retry_once()
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    assert invocation.result == 1
    app.runner.stop_runner_loop()
    thread.join()


def test_task_running_concurrency(task_sleep: Task) -> None:
    """Test the running concurrency functionalicity:
    - task_sleep has enabled running_concurrency=ConcurrencyControlType.TASK and will sleep x seconds
    """
    app = task_sleep.app

    def run_in_thread() -> None:
        app.runner.conf.min_parallel_slots = 2
        app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    fast_invocation_sleep_seconds = 0
    if app.conf.runner_cls == "ThreadRunner":
        slow_invocation_sleep_seconds = 0.25
    else:
        slow_invocation_sleep_seconds = 2
    max_fast_running_time = slow_invocation_sleep_seconds / 4

    assert slow_invocation_sleep_seconds > max_fast_running_time
    assert max_fast_running_time > fast_invocation_sleep_seconds

    #####################################################################################
    # CONTROL CHECK: fastes invocation should finish before a quarter of the slowest running time
    start_fast = time()
    # trigger invocation and ask wait for the result directly
    invocation = task_sleep(seconds=fast_invocation_sleep_seconds)
    assert invocation.result
    assert fast_invocation_sleep_seconds <= time() - start_fast < max_fast_running_time
    # check it was not rerouted in the history
    assert isinstance(invocation, DistributedInvocation)
    history = app.state_backend.get_history(invocation)
    statuses = {h.status for h in history}
    assert InvocationStatus.REROUTED not in statuses
    #####################################################################################

    #####################################################################################
    # CONCURRENCY CHECK: slow invocation will delay fast invocation running time
    # 1.- trigger slow invocation, it should run immediately (2 slots available in runner)
    start_slow_invocation = time()
    slow_invocation = task_sleep(seconds=slow_invocation_sleep_seconds)
    # 2.- wait a bit to ensure that the slow invocation is running
    sleep(max_fast_running_time)
    # 3.- trigger fast invocation, it should wait until slow invocation finishes
    start_fast_invocation = time()
    fast_invocation = task_sleep(seconds=fast_invocation_sleep_seconds)
    # 4.- wait for the invocations to finish and capture the running times
    slow_elapsed_time, fast_elapsed_time = 0.0, 0.0
    while not (slow_elapsed_time and fast_elapsed_time):
        if slow_invocation.status.is_final():
            slow_elapsed_time = time() - start_slow_invocation
        if fast_invocation.status.is_final():
            fast_elapsed_time = time() - start_fast_invocation
        sleep(0.1)
    # 5.- Check running times
    # slow invocation finish after sleep seconds
    assert slow_invocation_sleep_seconds < slow_elapsed_time
    # fast invocation took more than max_fast_running_time
    assert max_fast_running_time < fast_elapsed_time
    # 6.- check that only fast_invocation was rerouted in the history
    assert isinstance(slow_invocation, DistributedInvocation)
    assert isinstance(fast_invocation, DistributedInvocation)
    slow_history = app.state_backend.get_history(slow_invocation)
    fast_history = app.state_backend.get_history(fast_invocation)
    slow_statuses = {h.status for h in slow_history}
    fast_statuses = {h.status for h in fast_history}
    assert InvocationStatus.REROUTED in fast_statuses
    assert InvocationStatus.REROUTED not in slow_statuses
    #####################################################################################

    app.runner.stop_runner_loop()
    thread.join()


def test_parallel_execution(task_sum: Task) -> None:
    """Test the parallel execution functionalicity"""
    app = task_sum.app
    # app.app_id = app.app_id + "-test_parallel_execution"ase

    def run_in_thread() -> None:
        app.runner.run()

    invocation_group = task_sum.parallelize(
        ((1, 2), {"x": 3, "y": 4}, task_sum.args(5, y=6))
    )
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    assert Counter([3, 7, 11]) == Counter(invocation_group.results)
    app.runner.stop_runner_loop()
    thread.join()


def test_cycle_detection(task_cycle: Task) -> None:
    """Test that the execution will detect the cycle raising an exception"""
    app = task_cycle.app
    with patch.dict(
        os.environ,
        {
            "PYNENC__ORCHESTRATOR__CYCLE_CONTROL": "True",
        },
    ):

        def run_in_thread() -> None:
            app.runner.run()

        invocation = task_cycle()
        thread = threading.Thread(target=run_in_thread, daemon=True)
        thread.start()
        # TODO:
        # - if finish but does not go trough the cycle
        # - next check task cycle_start/cycle_end
        # - does keeping invocation_context in app root work? does it require a tree or references?
        with pytest.raises(CycleDetectedError) as exc_info:
            _ = invocation.result

        expected_error = (
            "A cycle was detected: Cycle detected:\n"
            "- tests.integration.apps.combinations.tasks.cycle_end()\n"
            "- tests.integration.apps.combinations.tasks.cycle_start()\n"
            "- back to tests.integration.apps.combinations.tasks.cycle_end()"
        )

        assert str(exc_info.value) == expected_error
        app.runner.stop_runner_loop()
        thread.join()


def test_raise_exception(task_raise_exception: Task) -> None:
    """Test that an exception is raised if the task raises an exception"""

    app = task_raise_exception.app

    def run_in_thread() -> None:
        app.runner.run()

    invocation = task_raise_exception()
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    with pytest.raises(ValueError):
        _ = invocation.result
    app.runner.stop_runner_loop()
    thread.join()


def test_sub_invocation_dependency(task_get_upper: Task) -> None:
    """Test when an invocation requires the result of another invocation"""

    app = task_get_upper.app

    def run_in_thread() -> None:
        app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    assert task_get_upper().result == "EXAMPLE"
    app.runner.stop_runner_loop()
    thread.join()


def test_avoid_direct_self_cycles(task_direct_cycle: Task) -> None:
    """Test that a cycle in the dependency graph is detected"""

    app = task_direct_cycle.app

    def run_in_thread() -> None:
        app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    # the request of invocation should work without problem,
    # as the cycle wasn't executed yet
    invocation = task_direct_cycle()
    with pytest.raises(CycleDetectedError) as exc_info:
        # however, when retrieving the result, an exception should be raised
        # because the function is calling itself
        _ = invocation.result

    expected_error = (
        "A cycle was detected: Cycle detected:\n"
        "- tests.integration.apps.combinations.tasks.direct_cycle()\n"
        "- back to tests.integration.apps.combinations.tasks.direct_cycle()"
    )
    assert str(exc_info.value) == expected_error
    app.runner.stop_runner_loop()
    thread.join()
