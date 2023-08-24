import time
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from pynenc.exceptions import InvocationNotFoundError
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc.state_backend.base_state_backend import (BaseStateBackend,
                                                     InvocationHistory)

if TYPE_CHECKING:
    from pynenc.task import Task
    from pynenc.types import Params, Result
    from tests.conftest import MockPynenc


def test_upsert_invocation_non_blocking(
    mock_base_app: "MockPynenc",
    dummy_invocation: "DistributedInvocation[Params, Result]",
) -> None:
    """Test that upsert_invocation is called in a non-blocking way."""
    mock_base_app.state_backend._upsert_invocation.side_effect = lambda x: time.sleep(
        0.5
    )

    start_time = time.time()
    mock_base_app.state_backend.upsert_invocation(dummy_invocation)
    end_time = time.time()

    # check that our method returned control to the main thread almost instantly
    assert end_time - start_time < 0.5


def test_add_history_non_blocking(
    mock_base_app: "MockPynenc",
    dummy_invocation: "DistributedInvocation[Params, Result]",
) -> None:
    """Test that add_history is called in a non-blocking way."""
    mock_base_app.state_backend._add_history.side_effect = lambda x, y: time.sleep(0.5)

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

    mock_base_app.state_backend._set_result.side_effect = lambda inv, res: time.sleep(
        0.2
    )
    start_time = time.time()
    mock_base_app.state_backend.set_result(dummy_invocation, "dummy result")
    end_time = time.time()

    # check that our method did block the main thread for about 0.2 seconds
    assert 0.2 < end_time - start_time < 0.25


def test_get_invocation_exception(mock_base_app: "MockPynenc") -> None:
    """Test that get invocation will raise an exception if doesn't exist"""
    mock_base_app.state_backend._get_invocation.return_value = None
    with pytest.raises(InvocationNotFoundError):
        mock_base_app.state_backend.get_invocation("x")
