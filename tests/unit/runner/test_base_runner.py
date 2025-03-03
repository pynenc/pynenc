import threading
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest

from pynenc.exceptions import RunnerNotExecutableError
from pynenc.runner.base_runner import DummyRunner
from tests.util import capture_logs

if TYPE_CHECKING:
    from tests.conftest import MockPynenc


def test_run(mock_base_app: "MockPynenc") -> None:
    """Test that the runner method will always call on_start and on_stop"""

    def run_in_thread() -> None:
        mock_base_app.runner.run()

    # Create a thread to run the loop
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    mock_base_app.runner.stop_runner_loop()
    thread.join()
    mock_base_app.runner._on_start.assert_called_once()
    mock_base_app.runner.runner_loop_iteration.assert_called()
    mock_base_app.runner._on_stop.assert_called_once()


def test_exception_handling_in_run_method(mock_base_app: "MockPynenc") -> None:
    """Test that a general exception in the runner loop is logged and raised"""
    exception_message = "Test Exception"
    mock_base_app.runner.runner_loop_iteration.side_effect = Exception(
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
        mock_base_app.runner.runner_loop_iteration.side_effect = KeyboardInterrupt
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
