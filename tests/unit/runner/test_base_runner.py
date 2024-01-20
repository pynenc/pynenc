import threading
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest
from _pytest.logging import LogCaptureFixture

from pynenc.exceptions import RunnerNotExecutableError
from pynenc.runner.base_runner import DummyRunner

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


def test_keyboard_interrupt_handling_in_run_method(
    mock_base_app: "MockPynenc", caplog: LogCaptureFixture
) -> None:
    with caplog.at_level("WARNING"):
        mock_base_app.runner.runner_loop_iteration.side_effect = KeyboardInterrupt
        mock_base_app.runner.run()
        assert any(
            "KeyboardInterrupt received. Stopping runner..." in record.message
            for record in caplog.records
        )


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
