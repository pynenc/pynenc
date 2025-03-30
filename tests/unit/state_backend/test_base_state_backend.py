import threading
import time
from typing import TYPE_CHECKING
from unittest.mock import create_autospec

import pytest

from pynenc.conf.config_state_backend import ConfigStateBackend
from pynenc.exceptions import InvocationNotFoundError
from pynenc.invocation import DistributedInvocation, InvocationStatus

if TYPE_CHECKING:
    from pynenc.types import Params, Result
    from tests.conftest import MockPynenc


def test_add_history_non_blocking(
    mock_base_app: "MockPynenc",
    dummy_invocation: "DistributedInvocation[Params, Result]",
) -> None:
    """Test that add_history is called in a non-blocking way."""
    mock_base_app.state_backend._add_history_mock.side_effect = lambda x, y: time.sleep(
        0.5
    )

    start_time = time.time()
    mock_base_app.state_backend.add_history(
        dummy_invocation, status=InvocationStatus.REGISTERED
    )
    end_time = time.time()

    # check that our method returned control to the main thread almost instantly
    assert end_time - start_time < 0.5


def test_set_result_blocking(
    mock_base_app: "MockPynenc",
    dummy_invocation: "DistributedInvocation[Params, Result]",
) -> None:
    """Test that _set_result is called in a blocking way"""

    mock_base_app.state_backend._set_result_mock.side_effect = (
        lambda inv, res: time.sleep(0.2)
    )
    start_time = time.time()
    mock_base_app.state_backend.set_result(dummy_invocation, "dummy result")
    end_time = time.time()

    # check that our method did block the main thread for about 0.2 seconds
    assert 0.2 < end_time - start_time < 0.25


def test_get_invocation_exception(mock_base_app: "MockPynenc") -> None:
    """Test that get invocation will raise an exception if doesn't exist"""
    mock_base_app.state_backend._get_invocation_mock.return_value = None
    with pytest.raises(InvocationNotFoundError):
        mock_base_app.state_backend.get_invocation("x")


def test_conf_property(mock_base_app: "MockPynenc") -> None:
    assert isinstance(mock_base_app.state_backend.conf, ConfigStateBackend)


def test_wait_for_all_async_operations(mock_base_app: "MockPynenc") -> None:
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
