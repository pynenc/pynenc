from typing import TYPE_CHECKING

import pytest

from pynenc.arguments import Arguments
from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
from pynenc.invocation import DistributedInvocation, ReusedInvocation
from pynenc import exceptions as exc
from pynenc import conf
from tests.unit.conftest import MockPynenc


if TYPE_CHECKING:
    from _pytest.python import Metafunc
    from _pytest.fixtures import FixtureRequest


def pytest_generate_tests(metafunc: "Metafunc") -> None:
    subclasses = [
        c for c in BaseOrchestrator.__subclasses__() if "mock" not in c.__name__.lower()
    ]
    if "app" in metafunc.fixturenames:
        metafunc.parametrize("app", subclasses, indirect=True)


@pytest.fixture
def app(request: "FixtureRequest") -> MockPynenc:
    app = MockPynenc()
    app.orchestrator = request.param(app)
    return app


def test_route_default(app: MockPynenc) -> None:
    """Test that the orchestrator will route the task by default

    If there are no options:
     - The orchestrator will forward the task to the broker
     - The broker should return a new Invocation and report the change of status to the orchestrator
    """

    @app.task
    def dummy(x: int, y: int) -> int:
        return x + y

    actual_invocations = []
    for i in range(2):
        actual_invocations.append(dummy(i, i))
        assert isinstance(actual_invocations[-1], DistributedInvocation)
        app.broker._route_invocation.assert_called_once()
        app.broker._route_invocation.reset_mock()
    # test that app.broker.route_invocation (MockBroker.route_invocation) has been called
    _iter = app.orchestrator.get_existing_invocations(task=dummy)
    stored_invocations = list(_iter)
    assert actual_invocations == stored_invocations


def test_single_invocation_raising(app: MockPynenc) -> None:
    """Test the option `task.options.single_invocation=SingleInvocation`
    In that case will only route the task if do not exists a Registered instance
    It can only exists one pending instance for the task

    With on_diff_args_raise=True will raise an exception if the task arguments differ
    """

    @app.task(single_invocation=conf.SingleInvocation(on_diff_args_raise=True))
    def dummy(arg: str) -> str:
        return arg

    # Get existing invocation doesn't find any pending match
    first_invocation = dummy("0")
    # We are calling but the previous invocation is still at REGISTERED status
    # So the new invocation cannot run
    # But we cannot return the first_invocation because the arguments doesn't match
    # So it will return an exception with the different arguments
    # The user of the library should handle this (or add ignore option in Pynenc)
    with pytest.raises(exc.SingleInvocationWithDifferentArgumentsError) as excinfo:
        _ = dummy("1")
    assert excinfo.value.task_id == dummy.task_id
    assert excinfo.value.existing_invocation_id == first_invocation.invocation_id
    assert excinfo.value.diff == (
        "==============================\n"
        "Differences for test_subclasses_single_invocation.dummy:\n"
        "==============================\n"
        "  * Original: {'arg': '0'}\n"
        "  * Updated: {'arg': '1'}\n"
        "------------------------------\n"
        "  * Changes: \n"
        "    - arg: 0 -> 1\n"
        "=============================="
    )
    # Trying with same arguments
    next_invocation = dummy("0")
    assert isinstance(first_invocation, DistributedInvocation)
    assert isinstance(next_invocation, ReusedInvocation)
    assert first_invocation.invocation_id == next_invocation.invocation_id
    assert first_invocation.arguments == next_invocation.arguments
    assert first_invocation.arguments.kwargs["arg"] == "0"
    assert next_invocation.diff_arg is None


def test_single_invocation_not_raising(app: MockPynenc) -> None:
    """Test the option `task.options.single_invocation=SingleInvocation`
    In that case will only route the task if do not exists a Registered instance
    It can only exists one pending instance for the task

    With on_diff_args_raise=False will return an invocation with the diff_args
    """

    @app.task(single_invocation=conf.SingleInvocation(on_diff_args_raise=False))
    def dummy(arg: str) -> str:
        return arg

    # Get existing invocation doesn't find any pending match
    first_invocation = dummy("0")
    # In this test case, it will find the previous invocation with different arguments
    # But on_diff_args_raise is False, so it will return the Reused invocation
    # specifying previous invocation arguments and diff_args on the current call
    next_invocation = dummy("1")
    assert isinstance(first_invocation, DistributedInvocation)
    assert isinstance(next_invocation, ReusedInvocation)
    assert first_invocation.invocation_id == next_invocation.invocation_id
    assert first_invocation.arguments == next_invocation.arguments
    assert first_invocation.arguments.kwargs["arg"] == "0"
    assert isinstance(next_invocation.diff_arg, Arguments)
    assert next_invocation.diff_arg.kwargs["arg"] == "1"


def test_single_invocation_arguments(app: MockPynenc) -> None:
    """Test the option `task.options.single_invocation=SingleInvocationPerArguments`
    In that case will only route the task if do not exists a Registered instance with the same arguments

    In this case on_diff_args_raise is not necessary
    """

    @app.task(single_invocation=conf.SingleInvocationPerArguments())
    def dummy(arg0: str, arg1: str) -> str:
        return f"{arg0=},{arg1=}"

    # Get existing invocation doesn't find any pending match
    inv_ab = dummy("a", "b")
    inv_cd = dummy("c", "d")
    assert inv_ab.invocation_id != inv_cd.invocation_id
    assert inv_ab.invocation_id == dummy("a", "b").invocation_id
    assert inv_cd.invocation_id == dummy("c", "d").invocation_id


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
    def dummy(key: str, arg: str) -> str:
        return f"{key}:{arg}"

    # Get existing invocation doesn't find any pending match
    inv_k0 = dummy("key0", "a")
    inv_k1 = dummy("key1", "a")
    assert inv_k0.invocation_id != inv_k1.invocation_id
    # Finds invocation with same key but different arguments -> Exception
    #
    with pytest.raises(exc.SingleInvocationWithDifferentArgumentsError) as excinfo:
        _ = dummy("key0", "b")
    assert excinfo.value.task_id == dummy.task_id
    assert excinfo.value.existing_invocation_id == inv_k0.invocation_id
    assert excinfo.value.diff == (
        "==============================\n"
        "Differences for test_subclasses_single_invocation.dummy:\n"
        "==============================\n"
        "  * Original: {'key': 'key0', 'arg': 'a'}\n"
        "  * Updated: {'key': 'key0', 'arg': 'b'}\n"
        "------------------------------\n"
        "  * Changes: \n"
        "    - arg: a -> b\n"
        "=============================="
    )

    assert inv_k0.invocation_id == dummy("key0", "a").invocation_id
    assert inv_k1.invocation_id == dummy("key1", "a").invocation_id


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
    def dummy(key: str, arg: str) -> str:
        return f"{key}:{arg}"

    # Get existing invocation doesn't find any pending match
    inv_k0 = dummy("key0", "a")
    inv_k1 = dummy("key1", "a")
    assert inv_k0.invocation_id != inv_k1.invocation_id
    # Finds invocation with same key but different arguments -> Exception
    #
    next_inv_k0 = dummy("key0", "b")
    next_inv_k1 = dummy("key1", "b")
    assert isinstance(next_inv_k0, ReusedInvocation)
    assert isinstance(next_inv_k1, ReusedInvocation)
    # Reuse invocations
    assert inv_k0.invocation_id == next_inv_k0.invocation_id
    assert inv_k1.invocation_id == next_inv_k1.invocation_id
    assert inv_k0.arguments == next_inv_k0.arguments
    assert inv_k1.arguments == next_inv_k1.arguments
    # with diff_args specified for the differences
    assert isinstance(next_inv_k0.diff_arg, Arguments)
    assert isinstance(next_inv_k1.diff_arg, Arguments)
    # keys are the same
    assert inv_k0.arguments.kwargs["key"] == next_inv_k0.diff_arg.kwargs["key"]
    assert inv_k1.arguments.kwargs["key"] == next_inv_k1.diff_arg.kwargs["key"]
    # not key arguments differ
    assert inv_k0.arguments.kwargs["arg"] != next_inv_k0.diff_arg.kwargs["arg"]
    assert inv_k1.arguments.kwargs["arg"] != next_inv_k1.diff_arg.kwargs["arg"]
