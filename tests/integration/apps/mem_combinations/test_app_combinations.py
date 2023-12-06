import threading
from collections import Counter

import pytest

from pynenc import Task
from pynenc.exceptions import CycleDetectedError


def test_task_execution(task_sum: Task) -> None:
    """Test the whole lifecycle of a task execution"""
    app = task_sum.app
    # app.app_id = app.app_id + "-test_task_execution"

    def run_in_thread() -> None:
        app.runner.run()

    invocation = task_sum(1, 2)
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    assert invocation.result == 3
    app.runner.stop_runner_loop()
    thread.join()


def test_parallel_execution(task_sum: Task) -> None:
    """Test the parallel execution functionalicity"""
    app = task_sum.app
    # app.app_id = app.app_id + "-test_parallel_execution"

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
    app.orchestrator.conf.cycle_control = True
    # app.app_id = app.app_id + "-test_cycle_detection"

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
        "- tests.integration.apps.mem_combinations.mem_combinations_tasks.cycle_end()\n"
        "- tests.integration.apps.mem_combinations.mem_combinations_tasks.cycle_start()\n"
        "- back to tests.integration.apps.mem_combinations.mem_combinations_tasks.cycle_end()"
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
        "- tests.integration.apps.mem_combinations.mem_combinations_tasks.direct_cycle()\n"
        "- back to tests.integration.apps.mem_combinations.mem_combinations_tasks.direct_cycle()"
    )
    assert str(exc_info.value) == expected_error
    app.runner.stop_runner_loop()
    thread.join()
