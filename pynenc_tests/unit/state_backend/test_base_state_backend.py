import threading
import time
from typing import TYPE_CHECKING
from unittest.mock import create_autospec

import pytest

from pynenc.call import Call
from pynenc.conf.config_state_backend import ConfigStateBackend
from pynenc.exceptions import InvocationNotFoundError
from pynenc.invocation import (
    DistributedInvocation,
    InvocationStatus,
    InvocationStatusRecord,
)
from pynenc.runner import RunnerContext
from pynenc_tests.conftest import MockPynenc

if TYPE_CHECKING:
    from pynenc.task import Task
    from pynenc.types import Params, Result


mock_base_app = MockPynenc(app_id="pynenc_tests/unit/runner/test_base_runner.py")


def dummy() -> None: ...


@pytest.fixture
def dummy_task() -> "Task":
    return mock_base_app.task(dummy)


@pytest.fixture
def dummy_invocation(dummy_task: "Task") -> "DistributedInvocation":
    return DistributedInvocation(Call(dummy_task), None)


def test_add_history_non_blocking(
    dummy_invocation: "DistributedInvocation[Params, Result]",
) -> None:
    """Test that add_history is called in a non-blocking way."""
    mock_base_app.state_backend._add_histories.side_effect = lambda x, y: time.sleep(
        0.5
    )

    start_time = time.time()
    runner_ctx = RunnerContext(
        runner_cls="TestRunner",
        runner_id="test-runner",
        pid=12345,
        hostname="test-host",
    )
    mock_base_app.state_backend.add_histories(
        [dummy_invocation],
        status_record=InvocationStatusRecord(status=InvocationStatus.REGISTERED),
        runner_context=runner_ctx,
    )
    end_time = time.time()

    # check that our method returned control to the main thread almost instantly
    assert end_time - start_time < 0.5


def test_set_result_blocking(
    dummy_invocation: "DistributedInvocation[Params, Result]",
) -> None:
    """Test that _set_result is called in a blocking way"""

    mock_base_app.state_backend._set_result.side_effect = lambda inv, res: time.sleep(
        0.2
    )
    start_time = time.time()
    mock_base_app.state_backend.set_result(
        dummy_invocation.invocation_id, "dummy result"
    )
    end_time = time.time()

    # check that our method did block the main thread for about 0.2 seconds
    assert 0.2 < end_time - start_time


def test_get_invocation_exception() -> None:
    """Test that get invocation will raise an exception if doesn't exist"""
    mock_base_app.state_backend._get_invocation.return_value = None
    with pytest.raises(InvocationNotFoundError):
        mock_base_app.state_backend.get_invocation("x")


def test_conf_property() -> None:
    assert isinstance(mock_base_app.state_backend.conf, ConfigStateBackend)


def test_wait_for_all_async_operations() -> None:
    # Mock the threads for two dummy invocations
    mock_thread1 = create_autospec(threading.Thread)
    mock_thread2 = create_autospec(threading.Thread)
    mock_base_app.state_backend.invocation_threads = {
        "invocation1": [mock_thread1],
        "invocation2": [mock_thread2],
    }

    mock_base_app.state_backend.wait_for_all_async_operations()

    # Verify that join is called on all threads
    mock_thread1.join.assert_called_once()
    mock_thread2.join.assert_called_once()
