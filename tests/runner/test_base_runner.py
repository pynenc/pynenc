import time
import pytest
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from pynenc.runner.base_runner import DummyRunner
from pynenc.exceptions import RunnerNotExecutableError

if TYPE_CHECKING:
    from tests.conftest import MockPynenc
    from pynenc.task import Task
    from pynenc.types import Params, Result


def test_run(mock_base_app: "MockPynenc") -> None:
    """Test that the runner method will always call on_start and on_stop"""
    mock_base_app.runner.run()
    mock_base_app.runner.on_start.assert_called_once()
    mock_base_app.runner.start_runner_loop.assert_called_once()
    mock_base_app.runner.on_stop.assert_called_once()


def test_dummy_runner(mock_base_app: "MockPynenc") -> None:
    """Test that the dummy runner cannot be run"""
    mock_base_app.runner = DummyRunner(mock_base_app)  # type: ignore
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.run()
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.on_start()
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.on_stop()
