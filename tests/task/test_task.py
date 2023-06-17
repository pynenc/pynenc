from unittest.mock import patch
from typing import Any
import os

import pytest
from _pytest.python import Metafunc

from pynenc import Pynenc, Task, TaskOptions
from pynenc.invocation import SynchronousInvocation, DistributedInvocation


@pytest.fixture
def app(request: pytest.FixtureRequest) -> Pynenc:
    return Pynenc()


def test_instanciate_task(app: Pynenc) -> None:
    """
    Test that the task decorator will transform the function in a Task instance
    """

    @app.task
    def add(x: int, y: int) -> int:
        return x + y

    assert isinstance(add, Task)


def test_instanciate_task_with_args(app: Pynenc) -> None:
    """
    Test that the decorator arguments exists in the Task instance
    """

    @app.task(profiling="any")
    def add(x: int, y: int) -> int:
        return x + y

    # I expect that function to become an instance of Task
    assert isinstance(add.options, TaskOptions)
    assert add.options.profiling == "any"


def test_sync_run_with_dev_mode_force_sync_invocation(app: Pynenc) -> None:
    """
    Test that the Task will return a SyncResult if DEV_MODE_FORCE_SYNC_TASK=True
    """

    @app.task
    def add(x: int, y: int) -> int:
        return x + y

    with patch.dict(os.environ, {"DEV_MODE_FORCE_SYNC_TASK": "True"}):
        result = add(1, 2)

    assert isinstance(result, SynchronousInvocation)
    assert result.value == 3


def test_async_invocation(app: Pynenc) -> None:
    """Test that the task will return an Async result"""

    @app.task
    def add(x: int, y: int) -> int:
        return x + y

    assert isinstance(add(1, 2), DistributedInvocation)


def test_extract_arguments_unpacking(app: Pynenc) -> None:
    """Test it will get args, kwargs from an unpacked function"""

    @app.task
    def f_unpacking(*args: Any, **kwargs: Any) -> None:
        """Does nothing"""

    result = f_unpacking("x", "y", z="z")
    assert result.arguments == {"args": ("x", "y"), "kwargs": {"z": "z"}}


def test_extract_arguments_named_regardless_call(app: Pynenc) -> None:
    """Test it will get args, kwargs from an unpacked function"""

    @app.task
    def dummy(arg0: Any, arg1: Any, arg2: Any, arg3: Any) -> None:
        """Does nothing"""

    expected = {"arg0": 0, "arg1": 1, "arg2": 2, "arg3": 3}
    result = dummy(0, 1, 2, 3)
    assert result.arguments == expected
    result = dummy(0, 1, arg2=2, arg3=3)
    assert result.arguments == expected
    result = dummy(arg0=0, arg1=1, arg2=2, arg3=3)
    assert result.arguments == expected
