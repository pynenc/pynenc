import threading
from collections import Counter
from time import sleep, time
from typing import TYPE_CHECKING, Any

import pytest

from pynenc import Pynenc
from pynenc.broker import RedisBroker
from pynenc.conf import ConcurrencyControlType
from pynenc.exceptions import CycleDetectedError, RetryError
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc.orchestrator import RedisOrchestrator
from pynenc.runner import ProcessRunner
from pynenc.serializer import JsonSerializer
from pynenc.state_backend import RedisStateBackend

if TYPE_CHECKING:
    from types import ModuleType

mp_app = Pynenc(app_id="test-process-runner")
mp_app.conf.logging_level = "debug"
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


@mp_app.task(max_retries=2)
def retry_once() -> int:
    if retry_once.invocation.num_retries == 0:
        raise RetryError()
    return retry_once.invocation.num_retries


@mp_app.task(running_concurrency=ConcurrencyControlType.TASK)
def sleep_seconds(seconds: int) -> bool:
    sleep(seconds)
    return True


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


def test_task_retry() -> None:
    """Test that the task will retry if it raises a RetryError"""

    def run_in_thread() -> None:
        mp_app.runner.run()

    invocation = retry_once()
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    assert invocation.result == 1
    mp_app.runner.stop_runner_loop()
    thread.join()


def test_task_running_concurrency() -> None:
    """Test the running concurrency functionalicity:
    - task_sleep has enabled running_concurrency=ConcurrencyControlType.TASK and will sleep x seconds
    """

    def run_in_thread() -> None:
        mp_app.runner.conf.min_parallel_slots = 2
        mp_app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    fast_invocation_sleep_seconds = 0
    slow_invocation_sleep_seconds = 2
    max_fast_running_time = slow_invocation_sleep_seconds / 4

    assert slow_invocation_sleep_seconds > max_fast_running_time
    assert max_fast_running_time > fast_invocation_sleep_seconds

    #####################################################################################
    # CONTROL CHECK: fastes invocation should finish before a quarter of the slowest running time
    start_fast = time()
    # trigger invocation and ask wait for the result directly
    invocation = sleep_seconds(seconds=fast_invocation_sleep_seconds)
    assert invocation.result
    assert fast_invocation_sleep_seconds <= time() - start_fast < max_fast_running_time
    # check it was not rerouted in the history
    assert isinstance(invocation, DistributedInvocation)
    history = mp_app.state_backend.get_history(invocation)
    statuses = {h.status for h in history}
    assert InvocationStatus.REROUTED not in statuses
    #####################################################################################

    #####################################################################################
    # CONCURRENCY CHECK: slow invocation will delay fast invocation running time
    # 1.- trigger slow invocation, it should run immediately (2 slots available in runner)
    start_slow_invocation = time()
    slow_invocation = sleep_seconds(seconds=slow_invocation_sleep_seconds)
    # 2.- wait a bit to ensure that the slow invocation is running
    sleep(max_fast_running_time)
    # 3.- trigger fast invocation, it should wait until slow invocation finishes
    start_fast_invocation = time()
    fast_invocation = sleep_seconds(seconds=fast_invocation_sleep_seconds)
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
    slow_history = mp_app.state_backend.get_history(slow_invocation)
    fast_history = mp_app.state_backend.get_history(fast_invocation)
    slow_statuses = {h.status for h in slow_history}
    fast_statuses = {h.status for h in fast_history}
    assert InvocationStatus.REROUTED in fast_statuses
    assert InvocationStatus.REROUTED not in slow_statuses
    #####################################################################################

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
    # TODO: this is not working, the thread is not finishing
    # because is stuck in the managed dictionary of the ProcessRunner
    # list(self.wait_invocation.keys())
    # thread.join()


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
