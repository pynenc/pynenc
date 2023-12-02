import os
from typing import Any
from unittest.mock import patch

from pynenc import Pynenc, Task, TaskOptions
from pynenc.invocation import DistributedInvocation, SynchronousInvocation

app = Pynenc()


@app.task
def add(x: int, y: int) -> int:
    return x + y


def test_instanciate_task() -> None:
    """
    Test that the task decorator will transform the function in a Task instance
    """
    assert isinstance(add, Task)


@app.task(profiling="any")
def add_profiled(x: int, y: int) -> int:
    return x + y


def test_instanciate_task_with_args() -> None:
    """
    Test that the decorator arguments exists in the Task instance
    """
    # I expect that function to become an instance of Task
    assert isinstance(add_profiled.options, TaskOptions)
    assert add_profiled.options.profiling == "any"


def test_sync_run_with_dev_mode_force_sync_invocation() -> None:
    """
    Test that the Task will return a SyncResult if PYNENC_DEV_MODE_FORCE_SYNC_TASKS=True
    """
    with patch.dict(os.environ, {"PYNENC__DEV_MODE_FORCE_SYNC_TASKS": "True"}):
        add.app = Pynenc()  # re-instantiate the app, config os.environ is cached
        invocation = add(1, 2)

    assert isinstance(invocation, SynchronousInvocation)
    assert invocation.result == 3


def test_async_invocation() -> None:
    """Test that the task will return an Async result"""
    with patch.dict(os.environ, {"PYNENC__DEV_MODE_FORCE_SYNC_TASKS": ""}):
        add.app = Pynenc()  # re-instantiate the app, config os.environ is cached
        invocation = add(1, 2)
    assert isinstance(invocation, DistributedInvocation)


@app.task
def f_unpacking(*args: Any, **kwargs: Any) -> None:
    """Does nothing"""


def test_extract_arguments_unpacking() -> None:
    """Test it will get args, kwargs from an unpacked function"""
    invocation = f_unpacking("x", "y", z="z")
    assert invocation.arguments.kwargs == {"args": ("x", "y"), "kwargs": {"z": "z"}}


@app.task
def dummy(arg0: Any, arg1: Any, arg2: Any, arg3: Any) -> None:
    """Does nothing"""


def test_extract_arguments_named_regardless_call() -> None:
    """Test it will get args, kwargs from an unpacked function"""
    expected = {"arg0": 0, "arg1": 1, "arg2": 2, "arg3": 3}
    invocation = dummy(0, 1, 2, 3)
    assert invocation.arguments.kwargs == expected
    invocation = dummy(0, 1, arg2=2, arg3=3)
    assert invocation.arguments.kwargs == expected
    invocation = dummy(arg0=0, arg1=1, arg2=2, arg3=3)
    assert invocation.arguments.kwargs == expected
