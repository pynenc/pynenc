import os
import signal
import threading
import time
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from pynenc.exceptions import RunnerNotExecutableError
from pynenc.runner.base_runner import DummyRunner

if TYPE_CHECKING:
    from pynenc.task import Task
    from pynenc.types import Params, Result
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


def test_dummy_runner(mock_base_app: "MockPynenc") -> None:
    """Test that the dummy runner cannot be run"""
    mock_base_app.runner = DummyRunner(mock_base_app)  # type: ignore
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.run()
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.on_start()
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.on_stop()
