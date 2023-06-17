from unittest.mock import MagicMock
from typing import TYPE_CHECKING
from functools import cached_property

import pytest

from pynenc import Pynenc
from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
from pynenc.broker.base_broker import BaseBroker
from pynenc.invocation import DistributedInvocation, ReusedInvocation
from pynenc import exceptions as exc
from pynenc import conf


if TYPE_CHECKING:
    from _pytest.python import Metafunc
    from _pytest.fixtures import FixtureRequest


def pytest_generate_tests(metafunc: "Metafunc") -> None:
    subclasses = [
        c for c in BaseOrchestrator.__subclasses__() if "mock" not in c.__name__.lower()
    ]
    if "app" in metafunc.fixturenames:
        metafunc.parametrize("app", subclasses, indirect=True)


class MockBroker(BaseBroker):
    route_invocation = MagicMock()


class MockPynenc(Pynenc):
    @cached_property
    def broker(self) -> MockBroker:
        return MockBroker(self)


@pytest.fixture
def app(request: "FixtureRequest") -> MockPynenc:
    app = MockPynenc()
    app.set_orchestrator_cls(request.param)
    return app


def test_route_default(app: MockPynenc) -> None:
    """Test that the orchestrator will route the task by default

    If there are no options:
     - The orchestrator will forward the task to the broker
     - The broker should return a new Invocation and report the change of status to the orchestrator
    """

    @app.task
    def add(x: int, y: int) -> int:
        return x + y

    actual_invocations = []
    for i in range(2):
        actual_invocations.append(add(i, i))
        assert isinstance(actual_invocations[-1], DistributedInvocation)
        app.broker.route_invocation.assert_called_once()
        app.broker.route_invocation.reset_mock()
    # test that app.broker.route_invocation (MockBroker.route_invocation) has been called
    _iter = app.orchestrator.get_existing_invocations(task=add)
    stored_invocations = list(_iter)
    assert actual_invocations == stored_invocations


def test_single_invocation_raising(app: MockPynenc) -> None:
    """Test the option `task.options.single_invocation=SingleInvocation`
    In that case will only route the task if do not exists a Registered instance
    It can only exists one pending instance for the task

    With on_diff_args_raise=True will raise an exception if the task arguments differ
    """

    @app.task(single_invocation=conf.SingleInvocation(on_diff_args_raise=True))
    def add(arg: str) -> str:
        return arg

    # Get existing invocation doesn't find any pending match
    first_invocation = add("0")
    # We are calling but the previous invocation is still at REGISTERED status
    # So the new invocation cannot run
    # But we cannot return the first_invocation because the arguments doesn't match
    # So it will return an exception with the different arguments
    # The user of the library should handle this (or add ignore option in Pynenc)
    with pytest.raises(exc.SingleInvocationWithDifferentArgumentsError) as excinfo:
        _ = add("1")
    assert excinfo.value.task == add
    assert excinfo.value.existing_invocation == first_invocation
    assert excinfo.value.call_arguments == {"arg": "1"}
    # Trying with same arguments
    next_invocation = add("0")
    assert isinstance(first_invocation, DistributedInvocation)
    assert isinstance(next_invocation, ReusedInvocation)
    assert first_invocation.invocation_id == next_invocation.invocation_id
    assert first_invocation.arguments == next_invocation.arguments
    assert first_invocation.arguments["arg"] == "0"
    assert next_invocation.diff_arg is None


def test_single_invocation_not_raising(app: MockPynenc) -> None:
    """Test the option `task.options.single_invocation=SingleInvocation`
    In that case will only route the task if do not exists a Registered instance
    It can only exists one pending instance for the task

    With on_diff_args_raise=False will return an invocation with the diff_args
    """

    @app.task(single_invocation=conf.SingleInvocation(on_diff_args_raise=False))
    def add(arg: str) -> str:
        return arg

    # Get existing invocation doesn't find any pending match
    first_invocation = add("0")
    # In this test case, it will find the previous invocation with different arguments
    # But on_diff_args_raise is False, so it will return the Reused invocation
    # specifying previous invocation arguments and diff_args on the current call
    next_invocation = add("1")
    assert isinstance(first_invocation, DistributedInvocation)
    assert isinstance(next_invocation, ReusedInvocation)
    assert first_invocation.invocation_id == next_invocation.invocation_id
    assert first_invocation.arguments == next_invocation.arguments
    assert first_invocation.arguments["arg"] == "0"
    assert isinstance(next_invocation.diff_arg, dict)
    assert next_invocation.diff_arg["arg"] == "1"


def test_single_invocation_arguments(app: MockPynenc) -> None:
    """Test the option `task.options.single_invocation=SingleInvocationPerArguments`
    In that case will only route the task if do not exists a Registered instance with the same arguments

    In this case on_diff_args_raise is not necessary
    """

    @app.task(single_invocation=conf.SingleInvocationPerArguments())
    def add(arg0: str, arg1: str) -> str:
        return f"{arg0=},{arg1=}"

    # Get existing invocation doesn't find any pending match
    inv_ab = add("a", "b")
    inv_cd = add("c", "d")
    assert inv_ab.invocation_id != inv_cd.invocation_id
    assert inv_ab.invocation_id == add("a", "b").invocation_id
    assert inv_cd.invocation_id == add("c", "d").invocation_id


def test_single_invocation_keys_raising(app: MockPynenc) -> None:
    """Test the option `task.options.single_invocation=SingleInvocationPerArguments`
    In that case will only route the task if do not exists a Registered instance with the same key arguments

    With on_diff_args_raise=True will raise an exception if the task arguments differ
    """

    @app.task(
        single_invocation=conf.SingleInvocationPerKeyArguments(
            ["key"], on_diff_args_raise=True
        )
    )
    def add(key: str, arg: str) -> str:
        return f"{key}:{arg}"

    # Get existing invocation doesn't find any pending match
    inv_k0 = add("key0", "a")
    inv_k1 = add("key1", "a")
    assert inv_k0.invocation_id != inv_k1.invocation_id
    # Finds invocation with same key but different arguments -> Exception
    #
    with pytest.raises(exc.SingleInvocationWithDifferentArgumentsError) as excinfo:
        _ = add("key0", "b")
    assert excinfo.value.task == add
    assert excinfo.value.existing_invocation == inv_k0
    assert excinfo.value.call_arguments == {"key": "key0", "arg": "b"}

    assert inv_k0.invocation_id == add("key0", "a").invocation_id
    assert inv_k1.invocation_id == add("key1", "a").invocation_id


def test_single_invocation_keys_not_raising(app: MockPynenc) -> None:
    """Test the option `task.options.single_invocation=SingleInvocationPerArguments`
    In that case will only route the task if do not exists a Registered instance with the same key arguments

    With on_diff_args_raise=False will return an invocation with the diff_args
    """

    @app.task(
        single_invocation=conf.SingleInvocationPerKeyArguments(
            ["key"], on_diff_args_raise=False
        )
    )
    def add(key: str, arg: str) -> str:
        return f"{key}:{arg}"

    # Get existing invocation doesn't find any pending match
    inv_k0 = add("key0", "a")
    inv_k1 = add("key1", "a")
    assert inv_k0.invocation_id != inv_k1.invocation_id
    # Finds invocation with same key but different arguments -> Exception
    #
    next_inv_k0 = add("key0", "b")
    next_inv_k1 = add("key1", "b")
    assert isinstance(next_inv_k0, ReusedInvocation)
    assert isinstance(next_inv_k1, ReusedInvocation)
    # Reuse invocations
    assert inv_k0.invocation_id == next_inv_k0.invocation_id
    assert inv_k1.invocation_id == next_inv_k1.invocation_id
    assert inv_k0.arguments == next_inv_k0.arguments
    assert inv_k1.arguments == next_inv_k1.arguments
    # with diff_args specified for the differences
    assert isinstance(next_inv_k0.diff_arg, dict)
    assert isinstance(next_inv_k1.diff_arg, dict)
    # keys are the same
    assert inv_k0.arguments["key"] == next_inv_k0.diff_arg["key"]
    assert inv_k1.arguments["key"] == next_inv_k1.diff_arg["key"]
    # not key arguments differ
    assert inv_k0.arguments["arg"] != next_inv_k0.diff_arg["arg"]
    assert inv_k1.arguments["arg"] != next_inv_k1.diff_arg["arg"]
