import threading
from typing import Any

import pytest

from pynenc import Pynenc
from pynenc.exceptions import CycleDetectedError
from pynenc.runner import MemRunner
from pynenc.orchestrator.mem_orchestrator import MemOrchestrator


@pytest.fixture
def app() -> Pynenc:
    app = Pynenc()
    app.runner = MemRunner(app)
    return app


def test_mem_execution(app: Pynenc) -> None:
    """Test the whole lifecycle of a task execution"""

    def run_in_thread() -> None:
        app.runner.run()

    @app.task
    def sum_task(a: int, b: int) -> int:
        return a + b

    invocation = sum_task(1, 2)
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    assert invocation.result == 3
    app.runner.stop_runner_loop()
    thread.join()


def test_raise_exception(app: Pynenc) -> None:
    """Test that an exception is raised if the task raises an exception"""

    def run_in_thread() -> None:
        app.runner.run()

    @app.task
    def raise_exception() -> Any:
        raise Exception("test")

    invocation = raise_exception()
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    with pytest.raises(Exception):
        invocation.result
    app.runner.stop_runner_loop()
    thread.join()


def test_mem_sub_invocation_dependency(app: Pynenc) -> None:
    """Test when an invocation requires the result of another invocation"""

    def run_in_thread() -> None:
        app.runner.run()

    @app.task
    def get_text() -> str:
        return "example"

    @app.task
    def get_upper() -> str:
        return get_text().result.upper()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    assert get_upper().result == "EXAMPLE"
    app.runner.stop_runner_loop()
    thread.join()


def test_avoid_cycles(app: Pynenc) -> None:
    """Test that a cycle in the dependency graph is detected"""

    # raise NotImplementedError()

    def run_in_thread() -> None:
        app.runner.run()

    @app.task
    def get_upper() -> str:
        invocation = get_upper()
        return invocation.result.upper()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    # the request of invocation should work without problem,
    # as the cycle wasn't executed yet
    invocation = get_upper()
    with pytest.raises(CycleDetectedError) as exc_info:
        # however, when retrieving the result, an exception should be raised
        # because the function is calling itself
        invocation.result

    expected_error = (
        "A cycle was detected: Cycle detected:\n"
        "- test_mem_app.get_upper()\n"
        "- back to test_mem_app.get_upper()"
    )

    assert str(exc_info.value) == expected_error

    app.runner.stop_runner_loop()
    thread.join()
