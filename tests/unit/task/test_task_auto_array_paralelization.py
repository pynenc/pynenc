from unittest.mock import patch
from typing import Any
import os

import pytest

from pynenc import Pynenc, Task, TaskOptions
from pynenc.invocation import SynchronousInvocationGroup, DistributedInvocationGroup


@pytest.fixture
def app() -> Pynenc:
    return Pynenc()


def test_sync_invocation_group(app: Pynenc) -> None:
    """
    Test that the Task will return a SyncResult if DEV_MODE_FORCE_SYNC_TASK=True
    """

    @app.task
    def add(x: int, y: int) -> int:
        return x + y

    with patch.dict(os.environ, {"DEV_MODE_FORCE_SYNC_TASK": "True"}):
        invocation_group = add.parallelize([(1, 1), add.args(1, 2), {"x": 2, "y": 3}])

    assert isinstance(invocation_group, SynchronousInvocationGroup)
    assert list(invocation_group.results) == [2, 3, 5]


def test_async_invocation(app: Pynenc) -> None:
    """Test that the task will return an Async result"""

    @app.task
    def add(x: int, y: int) -> int:
        return x + y

    invocation_group = add.parallelize([(1, 1), add.args(1, 2), {"x": 2, "y": 3}])

    assert isinstance(invocation_group, DistributedInvocationGroup)


def test_extract_arguments_unpacking(app: Pynenc) -> None:
    """Test it will get args, kwargs from an unpacked function"""

    @app.task
    def f_unpacking(*args: Any, **kwargs: Any) -> None:
        """Does nothing"""

    invocation_group = f_unpacking.parallelize(
        [
            (1, 2, 3),  # call with tuple of positional arguments
            {"arg_3": "z"},  # call with dict of keyword arguments
            f_unpacking.args("x", "y", arg_3="z"),  # call with Arguments instance
        ]
    )
    # call with tuple of positional arguments:
    assert invocation_group.invocations[0].arguments.kwargs == {
        "args": (1, 2, 3),
        "kwargs": {},
    }
    # call with dict of keyword arguments:
    assert invocation_group.invocations[1].arguments.kwargs == {
        "args": tuple(),
        "kwargs": {"arg_3": "z"},
    }
    # call with Arguments instance:
    assert invocation_group.invocations[2].arguments.kwargs == {
        "args": ("x", "y"),
        "kwargs": {"arg_3": "z"},
    }


def test_extract_arguments_named_regardless_call(app: Pynenc) -> None:
    """Test it will get args, kwargs from an unpacked function"""

    @app.task
    def dummy(arg0: Any, arg1: Any, arg2: Any, arg3: Any) -> None:
        """Does nothing"""

    expected = {"arg0": 0, "arg1": 1, "arg2": 2, "arg3": 3}
    invocation_group = dummy.parallelize(
        [
            (0, 1, 2, 3),
            dummy.args(0, 1, arg2=2, arg3=3),
            {"arg0": 0, "arg1": 1, "arg2": 2, "arg3": 3},
        ]
    )
    for invocation in invocation_group.invocations:
        assert invocation.arguments.kwargs == expected
