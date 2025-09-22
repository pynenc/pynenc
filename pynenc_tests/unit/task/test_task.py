import os
from typing import Any
from unittest.mock import patch

import pytest

from pynenc import Pynenc, Task
from pynenc.conf.config_task import ConfigTask
from pynenc.exceptions import RetryError
from pynenc.invocation import ConcurrentInvocation, DistributedInvocation

app = Pynenc(app_id="test_task")


@app.task
def add(x: int, y: int) -> int:
    return x + y


def test_instanciate_task() -> None:
    """
    Test that the task decorator will transform the function in a Task instance
    """
    assert isinstance(add, Task)


@app.task(max_retries=2)
def add_with_max_retries(x: int, y: int) -> int:
    return x + y


def test_instanciate_task_with_args() -> None:
    """
    Test that the decorator arguments exists in the Task instance
    """
    # I expect that function to become an instance of Task
    assert isinstance(add_with_max_retries.conf, ConfigTask)
    assert add_with_max_retries.conf.max_retries == 2


def test_sync_run_with_dev_mode_force_conc_invocation() -> None:
    """
    Test that the Task will return a SyncResult if PYNENC_DEV_MODE_FORCE_SYNC_TASKS=True
    """
    with patch.dict(os.environ, {"PYNENC__DEV_MODE_FORCE_SYNC_TASKS": "True"}):
        add.app = Pynenc()  # re-instantiate the app, config os.environ is cached
        invocation = add(1, 2)

    assert isinstance(invocation, ConcurrentInvocation)
    assert invocation.result == 3


def test_aconc_invocation() -> None:
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


@app.task(max_retries=1)
def retry_once() -> int:
    if retry_once.invocation.num_retries == 0:
        raise RetryError()
    return retry_once.invocation.num_retries


def test_sync_run_retry() -> None:
    """
    Test that the Task will retry once for synchronous invocation
    """
    with patch.dict(os.environ, {"PYNENC__DEV_MODE_FORCE_SYNC_TASKS": "True"}):
        retry_once.app = Pynenc(app_id="test_sync_run_retry")
        invocation = retry_once()

    assert isinstance(invocation, ConcurrentInvocation)
    assert retry_once.conf.max_retries == 1
    assert invocation.num_retries == 0
    assert invocation.result == 1
    assert invocation.num_retries == 1


@app.task
def example_task() -> None:
    pass


def test_task_invocation_property_error() -> None:
    """
    Test that accessing the invocation property of a task before it has been invoked
    raises a RuntimeError.
    """
    with pytest.raises(RuntimeError) as exc_info:
        _ = example_task.invocation

    assert "Task has not been invoked yet" in str(exc_info.value)


def another_task() -> None:
    pass


def test_retriable_exceptions() -> None:
    """Test that the task will contain RetryError by default as a retriable exception"""
    task = app.task(another_task)
    task.conf.retry_for = ()
    assert task.retriable_exceptions == (RetryError,)

    task = app.task(another_task)
    task.conf.retry_for = (RetryError,)
    assert task.retriable_exceptions == (RetryError,)

    task = app.task(another_task)
    task.conf.retry_for = (RuntimeError,)
    assert task.retriable_exceptions == (RuntimeError, RetryError)


def test_task_str() -> None:
    assert str(example_task) == f"Task(func={example_task.func.__name__})"


def test_task_repr() -> None:
    assert repr(example_task) == str(example_task)


def test_task_getstate() -> None:
    """Test that __getstate__ correctly serializes the task instance."""
    task = example_task  # Use an existing task for testing

    # Expected state should contain:
    # - The associated Pynenc app
    # - The serialized task JSON
    expected_state = {
        "app": task.app,
        "task_json": task.to_json(),
    }

    assert task.__getstate__() == expected_state
