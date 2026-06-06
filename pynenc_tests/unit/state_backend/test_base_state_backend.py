import time
from typing import TYPE_CHECKING

import pytest

from pynenc.call import Call
from pynenc.conf.config_state_backend import ConfigStateBackend
from pynenc.exceptions import InvocationNotFoundError
from pynenc.invocation import (
    DistributedInvocation,
)
from pynenc_tests.conftest import MockPynenc

if TYPE_CHECKING:
    from pynenc.task import Task
    from pynenc.types import Params, Result


mock_base_app = MockPynenc.with_id("pynenc_tests/unit/runner/test_base_runner.py")


def dummy() -> None: ...


@pytest.fixture
def dummy_task() -> "Task":
    return mock_base_app.task(dummy)


@pytest.fixture
def dummy_invocation(dummy_task: "Task") -> "DistributedInvocation":
    return DistributedInvocation.isolated(Call(dummy_task))


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
