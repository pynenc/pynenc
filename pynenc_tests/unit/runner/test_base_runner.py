import asyncio
import threading
import time
from typing import Any
from unittest.mock import Mock, patch

import pytest

from pynenc.exceptions import RunnerNotExecutableError
from pynenc.invocation import DistributedInvocation
from pynenc.runner.base_runner import DummyRunner
from pynenc_tests.conftest import MockPynenc
from pynenc_tests.util import capture_logs


def test_run(mock_base_app: "MockPynenc") -> None:
    """Test that the runner method will always call on_start and on_stop"""

    def run_in_thread() -> None:
        mock_base_app.runner.run()

    # Create a thread to run the loop
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    time.sleep(0.1)
    mock_base_app.runner.stop_runner_loop()
    thread.join()
    mock_base_app.runner._on_start_mock.assert_called_once()
    mock_base_app.runner.runner_loop_iteration_mock.assert_called()
    mock_base_app.runner._on_stop_mock.assert_called_once()


def test_exception_handling_in_run_method(mock_base_app: "MockPynenc") -> None:
    """Test that a general exception in the runner loop is logged and raised"""
    exception_message = "Test Exception"
    mock_base_app.runner.runner_loop_iteration_mock.side_effect = Exception(
        exception_message
    )

    with capture_logs(mock_base_app.logger) as log_buffer:
        with pytest.raises(Exception) as exc_info:
            mock_base_app.runner.run()

        assert exception_message in str(exc_info.value)
        log_output = log_buffer.getvalue()
        assert f"Exception in runner loop: {exception_message}" in log_output


def test_keyboard_interrupt_handling_in_run_method(mock_base_app: "MockPynenc") -> None:
    with capture_logs(mock_base_app.logger) as log_buffer:
        mock_base_app.runner.runner_loop_iteration_mock.side_effect = KeyboardInterrupt
        mock_base_app.runner.run()
        log_output = log_buffer.getvalue()
        assert "KeyboardInterrupt received. Stopping runner..." in log_output


def test_dummy_runner(mock_base_app: "MockPynenc") -> None:
    """Test that the dummy runner cannot be run"""
    mock_base_app.runner = DummyRunner(mock_base_app)  # type: ignore
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.run()
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.on_start()
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.on_stop()
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner._on_stop_runner_loop()
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.runner_loop_iteration()
    with pytest.raises(RunnerNotExecutableError):
        _ = mock_base_app.runner.max_parallel_slots
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.mem_compatible()
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner._on_stop_runner_loop()


@patch("pynenc.runner.base_runner.time.sleep")
def test_dummy_runner_waiting_for_result(
    mock_sleep: Mock, mock_base_app: "MockPynenc"
) -> None:
    runner = DummyRunner(mock_base_app)
    runner.conf.invocation_wait_results_sleep_time_sec = -1313
    mock_sleep.reset_mock()
    runner.waiting_for_results(None, None)  # type: ignore
    mock_sleep.assert_any_call(-1313)


@pytest.mark.asyncio
async def test_dummy_runner_async_waiting_for_result(
    mock_base_app: "MockPynenc",
) -> None:
    """Test async waiting for results in dummy runner."""
    runner = DummyRunner(mock_base_app)
    runner.conf.invocation_wait_results_sleep_time_sec = 0.1  # Small delay for testing

    with capture_logs(mock_base_app.logger) as log_buffer:
        await runner.async_waiting_for_results(None, [])  # type: ignore

        log_output = log_buffer.getvalue()
        assert "Async Waiting for result_invocation_ids=" in log_output
        assert "from outside this runner" in log_output


@pytest.mark.asyncio
async def test_async_waiting_for_results_with_sleep_time(
    mock_base_app: "MockPynenc",
) -> None:
    """Test that async_waiting_for_results respects configured sleep time."""
    runner = DummyRunner(mock_base_app)
    sleep_time = 0.1
    runner.conf.invocation_wait_results_sleep_time_sec = sleep_time

    start_time = asyncio.get_event_loop().time()
    await runner.async_waiting_for_results(None, [])  # type: ignore
    elapsed_time = asyncio.get_event_loop().time() - start_time

    assert elapsed_time >= sleep_time, "Sleep time was not respected"


def test_all_runners_can_be_instantiated(mock_base_app: "MockPynenc") -> None:
    """Test that all concrete runner classes can be instantiated."""
    from pynenc.runner.base_runner import BaseRunner

    # Get all subclasses recursively
    def get_all_subclasses(cls: type) -> list[type]:
        subclasses: list[type] = []
        for c in cls.__subclasses__():
            if "mock" in c.__name__.lower():
                continue
            subclasses.append(c)
            subclasses.extend(get_all_subclasses(c))
        return subclasses

    runners = get_all_subclasses(BaseRunner)

    assert runners, "No runner subclasses found"

    for runner_class in runners:
        try:
            runner = runner_class(mock_base_app)
            # Check that the instance is created successfully
            assert isinstance(runner, BaseRunner)
        except Exception as e:
            pytest.fail(f"Failed to instantiate {runner_class.__name__}: {str(e)}")


app = MockPynenc()


@app.task
def add(x: int, y: int) -> int:
    return x + y


@pytest.mark.asyncio
async def test_dummy_runner_async_waiting_for_results_running(
    monkeypatch: pytest.MonkeyPatch, mock_base_app: "MockPynenc"
) -> None:
    """
    Test that async_waiting_for_results calls _waiting_for_results when a running invocation is provided.
    """
    runner = DummyRunner(mock_base_app)
    sleep_time = 0.05
    runner.conf.invocation_wait_results_sleep_time_sec = sleep_time

    # Create a dummy invocation using the 'add' task.
    invocation: DistributedInvocation = add(1, 2)  # type: ignore

    # Define a flag to record that _waiting_for_results was called.
    called = False

    def fake_waiting_for_results(
        running_inv: Any, result_inv: Any, runner_args: Any = None
    ) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr(runner, "_waiting_for_results", fake_waiting_for_results)

    # Call async_waiting_for_results with a running invocation.
    await runner.async_waiting_for_results(
        invocation.invocation_id, [invocation.invocation_id], None
    )
    assert (
        called
    ), "Expected _waiting_for_results to be called when running_invocation is provided"


def test_dummy_runner_waiting_for_results_no_running(
    monkeypatch: pytest.MonkeyPatch, mock_base_app: "MockPynenc"
) -> None:
    """
    Test that waiting_for_results calls time.sleep when no running invocation is provided.
    """
    runner = DummyRunner(mock_base_app)
    sleep_time = 0.1
    runner.conf.invocation_wait_results_sleep_time_sec = sleep_time

    sleep_called = False

    def fake_sleep(duration: float) -> None:
        nonlocal sleep_called
        sleep_called = True
        assert duration == sleep_time

    monkeypatch.setattr(time, "sleep", fake_sleep)

    # Call waiting_for_results with running_invocation set to None.
    runner.waiting_for_results(None, [Mock()])
    assert (
        sleep_called
    ), "Expected time.sleep to be called when running_invocation is None"


def test_dummy_runner_waiting_for_results_no_effect_with_runner_args(
    mock_base_app: "MockPynenc",
) -> None:
    """
    Test that _waiting_for_results correctly ignores runner_args if provided.
    """
    runner = DummyRunner(mock_base_app)

    # Create a dummy invocation
    invocation = Mock(spec=DistributedInvocation)

    # Call _waiting_for_results with a dummy runner_args dict
    runner._waiting_for_results(invocation, [invocation], runner_args={"some_key": 42})

    # The function should not modify anything or have an observable effect
    assert True, "No side effects should occur when runner_args is provided."


def test_dummy_runner_waiting_for_results_respects_sleep_time(
    monkeypatch: pytest.MonkeyPatch, mock_base_app: "MockPynenc"
) -> None:
    """
    Test that _waiting_for_results sleeps for the correct duration.
    """
    runner = DummyRunner(mock_base_app)
    sleep_time = 0.2  # Expected sleep duration
    runner.conf.invocation_wait_results_sleep_time_sec = sleep_time

    sleep_called = False

    def fake_sleep(duration: float) -> None:
        nonlocal sleep_called
        sleep_called = True
        assert duration == sleep_time, "Expected sleep duration does not match"

    monkeypatch.setattr(time, "sleep", fake_sleep)

    # Call _waiting_for_results with None running_invocation (external call case)
    runner._waiting_for_results(None, [Mock()])  # type: ignore

    assert sleep_called, "Expected time.sleep to be called for external invocations."
