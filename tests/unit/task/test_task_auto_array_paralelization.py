from typing import Any

import pytest

from pynenc import Pynenc
from pynenc.invocation import ConcurrentInvocationGroup, DistributedInvocationGroup

_app = Pynenc()


@_app.task
def add(x: int, y: int) -> int:
    return x + y


@_app.task
def f_unpacking(*args: Any, **kwargs: Any) -> None:
    """Does nothing"""


@_app.task
def dummy(arg0: Any, arg1: Any, arg2: Any, arg3: Any) -> None:
    """Does nothing"""


@pytest.fixture
def app() -> Pynenc:
    # refresh the app for every test
    app = Pynenc()
    add.app = app
    f_unpacking.app = app
    dummy.app = app
    return app


def test_task_only_module_level(app: Pynenc) -> None:
    """Test that a task will crash when trying to decorate a sub-function"""

    with pytest.raises(ValueError):

        @app.task
        def add(x: int, y: int) -> int:
            return x + y


def test_conc_invocation_group(app: Pynenc) -> None:
    """
    Test that the Task will return a SyncResult if dev_mode_force_sync_tasks=True
    """
    add.app = Pynenc(
        config_values={"dev_mode_force_sync_tasks": True}
    )  # re-instantiate the app, config os.environ is cached
    # app.conf.dev_mode_force_sync_tasks = True
    invocation_group = add.parallelize([(1, 1), add.args(1, 2), {"x": 2, "y": 3}])
    assert isinstance(invocation_group, ConcurrentInvocationGroup)
    assert list(invocation_group.results) == [2, 3, 5]


def test_aconc_invocation(app: Pynenc) -> None:
    """Test that the task will return an Async result"""
    app.conf.dev_mode_force_sync_tasks = False
    invocation_group = add.parallelize([(1, 1), add.args(1, 2), {"x": 2, "y": 3}])
    assert isinstance(invocation_group, DistributedInvocationGroup)


def test_extract_arguments_unpacking(app: Pynenc) -> None:
    """Test it will get args, kwargs from an unpacked function"""
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
        "args": (),
        "kwargs": {"arg_3": "z"},
    }
    # call with Arguments instance:
    assert invocation_group.invocations[2].arguments.kwargs == {
        "args": ("x", "y"),
        "kwargs": {"arg_3": "z"},
    }


def test_extract_arguments_named_regardless_call(app: Pynenc) -> None:
    """Test it will get args, kwargs from an unpacked function"""

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
