from collections import Counter
import threading
from typing import TYPE_CHECKING, Iterator

import pytest

from pynenc import Pynenc
from pynenc.broker import RedisBroker
from pynenc.orchestrator import RedisOrchestrator
from pynenc.serializer import JsonSerializer
from pynenc.state_backend import RedisStateBackend
from pynenc.runner import ProcessRunner
from pynenc.exceptions import CycleDetectedError

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
    cycle_end().result


@mp_app.task
def cycle_end() -> None:
    cycle_start().result


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


@pytest.mark.test_id(name="parallel_exec")
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


@pytest.mark.test_id(name="cycle_detect")
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
    # with pytest.raises(CycleDetectedError) as exc_info:
    with pytest.raises(CycleDetectedError) as exc_info:
        invocation.result

    expected_error = (
        "A cycle was detected: Cycle detected:\n"
        "- test_app_redis_process_runner.cycle_end()\n"
        "- test_app_redis_process_runner.cycle_start()\n"
        "- back to test_app_redis_process_runner.cycle_end()"
    )

    assert str(exc_info.value) == expected_error
    mp_app.runner.stop_runner_loop()
    thread.join()


# def test_raise_exception(app: Pynenc) -> None:
#     """Test that an exception is raised if the task raises an exception"""

#     def run_in_thread() -> None:
#         app.runner.run()

#     @app.task
#     def raise_exception() -> Any:
#         raise Exception("test")

#     invocation = raise_exception()
#     thread = threading.Thread(target=run_in_thread, daemon=True)
#     thread.start()
#     with pytest.raises(Exception):
#         invocation.result
#     app.runner.stop_runner_loop()
#     thread.join()


# def test_mem_sub_invocation_dependency(app: Pynenc) -> None:
#     """Test when an invocation requires the result of another invocation"""

#     def run_in_thread() -> None:
#         app.runner.run()

#     @app.task
#     def get_text() -> str:
#         return "example"

#     @app.task
#     def get_upper() -> str:
#         return get_text().result.upper()

#     thread = threading.Thread(target=run_in_thread, daemon=True)
#     thread.start()
#     assert get_upper().result == "EXAMPLE"
#     app.runner.stop_runner_loop()
#     thread.join()


# def test_avoid_cycles(app: Pynenc) -> None:
#     """Test that a cycle in the dependency graph is detected"""

#     # raise NotImplementedError()

#     def run_in_thread() -> None:
#         app.runner.run()

#     @app.task
#     def get_upper() -> str:
#         invocation = get_upper()
#         return invocation.result.upper()

#     thread = threading.Thread(target=run_in_thread, daemon=True)
#     thread.start()
#     # the request of invocation should work without problem,
#     # as the cycle wasn't executed yet
#     invocation = get_upper()
#     with pytest.raises(CycleDetectedError) as exc_info:
#         # however, when retrieving the result, an exception should be raised
#         # because the function is calling itself
#         invocation.result

#     expected_error = (
#         "A cycle was detected: Cycle detected:\n"
#         "- test_mem_app.get_upper()\n"
#         "- back to test_mem_app.get_upper()"
#     )

#     assert str(exc_info.value) == expected_error

#     app.runner.stop_runner_loop()
#     thread.join()
