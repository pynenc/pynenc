import threading
from collections import Counter
from typing import TYPE_CHECKING, Any

import pytest

from pynenc import Pynenc
from pynenc.broker import RedisBroker
from pynenc.exceptions import CycleDetectedError
from pynenc.orchestrator import RedisOrchestrator
from pynenc.runner import ProcessRunner
from pynenc.serializer import JsonSerializer
from pynenc.state_backend import RedisStateBackend

if TYPE_CHECKING:
    from types import ModuleType

mp_app = Pynenc(app_id="test-process-runner")
mp_app.set_broker_cls(RedisBroker)
mp_app.set_orchestrator_cls(RedisOrchestrator)
mp_app.set_serializer_cls(JsonSerializer)
mp_app.set_state_backend_cls(RedisStateBackend)
mp_app.runner = ProcessRunner(mp_app)


def setup_module(module: "ModuleType") -> None:
    del module
    mp_app.purge()


def teardown_module(module: "ModuleType") -> None:
    del module
    mp_app.purge()


@mp_app.task
def sum(x: int, y: int) -> int:
    return x + y


@mp_app.task
def cycle_start() -> None:
    _ = cycle_end().result


@mp_app.task
def cycle_end() -> None:
    _ = cycle_start().result


@mp_app.task
def raise_exception() -> Any:
    raise ValueError("test")


@mp_app.task
def get_text() -> str:
    return "example"


@mp_app.task
def get_upper() -> str:
    return get_text().result.upper()


@mp_app.task
def direct_cycle() -> str:
    invocation = direct_cycle()
    return invocation.result.upper()


def test_task_execution() -> None:
    """Test the whole lifecycle of a task execution"""

    def run_in_thread() -> None:
        mp_app.runner.run()

    invocation = sum(1, 2)
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    assert invocation.result == 3
    mp_app.runner.stop_runner_loop()
    thread.join()


def test_parallel_execution() -> None:
    """Test the parallel execution functionalicity"""

    def run_in_thread() -> None:
        mp_app.runner.run()

    invocation_group = sum.parallelize(((1, 2), {"x": 3, "y": 4}, sum.args(5, y=6)))
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    assert Counter([3, 7, 11]) == Counter(invocation_group.results)
    mp_app.runner.stop_runner_loop()
    thread.join()


def test_cycle_detection() -> None:
    """Test that the execution will detect the cycle raising an exception"""

    def run_in_thread() -> None:
        mp_app.runner.run()

    invocation = cycle_start()
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
        "- test_app_redis_process_runner.cycle_end()\n"
        "- test_app_redis_process_runner.cycle_start()\n"
        "- back to test_app_redis_process_runner.cycle_end()"
    )

    assert str(exc_info.value) == expected_error
    mp_app.runner.stop_runner_loop()
    thread.join()


def test_raise_exception() -> None:
    """Test that an exception is raised if the task raises an exception"""

    def run_in_thread() -> None:
        mp_app.runner.run()

    invocation = raise_exception()
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    with pytest.raises(ValueError):
        _ = invocation.result
    mp_app.runner.stop_runner_loop()
    thread.join()


def test_sub_invocation_dependency() -> None:
    """Test when an invocation requires the result of another invocation"""

    def run_in_thread() -> None:
        mp_app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    assert get_upper().result == "EXAMPLE"
    mp_app.runner.stop_runner_loop()
    thread.join()


def test_avoid_direct_self_cycles() -> None:
    """Test that a cycle in the dependency graph is detected"""

    # raise NotImplementedError()

    def run_in_thread() -> None:
        mp_app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    # the request of invocation should work without problem,
    # as the cycle wasn't executed yet
    invocation = direct_cycle()
    with pytest.raises(CycleDetectedError) as exc_info:
        # however, when retrieving the result, an exception should be raised
        # because the function is calling itself
        _ = invocation.result

    expected_error = (
        "A cycle was detected: Cycle detected:\n"
        "- test_app_redis_process_runner.direct_cycle()\n"
        "- back to test_app_redis_process_runner.direct_cycle()"
    )
    assert str(exc_info.value) == expected_error
    mp_app.runner.stop_runner_loop()
    thread.join()
