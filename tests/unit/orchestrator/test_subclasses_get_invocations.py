from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
from pynenc.invocation import DistributedInvocation, InvocationStatus
from tests.unit.conftest import MockPynenc


if TYPE_CHECKING:
    from _pytest.python import Metafunc
    from _pytest.fixtures import FixtureRequest
    from pynenc.task import Task


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


@dataclass
class Vars:
    task: "Task"
    inv1: DistributedInvocation
    inv2: DistributedInvocation
    inv3: DistributedInvocation
    expected_ids: set[str]


@pytest.fixture
def test_vars(app: MockPynenc) -> Vars:
    """Test the implementation of abstract methods:
    set_invocation_status, get_existing_invocations
    """

    @app.task
    def dummy(arg0: str, arg1: str) -> str:
        return f"{arg0}:{arg1}"

    inv1: DistributedInvocation = DistributedInvocation(
        Call(dummy, Arguments({"arg0": "a", "arg1": "a"})), None
    )
    inv2: DistributedInvocation = DistributedInvocation(
        Call(dummy, Arguments({"arg0": "a", "arg1": "b"})), None
    )
    inv3: DistributedInvocation = DistributedInvocation(
        Call(dummy, Arguments({"arg0": "a", "arg1": "a"})), None
    )
    app.orchestrator.set_invocation_status(inv1, status=InvocationStatus.REGISTERED)
    app.orchestrator.set_invocation_status(inv2, status=InvocationStatus.SUCCESS)
    app.orchestrator.set_invocation_status(inv3, status=InvocationStatus.SUCCESS)
    expected_ids = {inv1.invocation_id, inv2.invocation_id, inv3.invocation_id}
    return Vars(dummy, inv1, inv2, inv3, expected_ids)


def test_get_all_invocations(app: MockPynenc, test_vars: Vars) -> None:
    """Test get without filters"""

    invocations = list(app.orchestrator.get_existing_invocations(test_vars.task))
    invocations_ids = set(i.invocation_id for i in invocations)
    assert invocations_ids == test_vars.expected_ids


def test_get_by_arguments(app: MockPynenc, test_vars: Vars) -> None:
    """Test filter by arguments"""
    # argument arg0:a is the same for both
    invocations = list(
        app.orchestrator.get_existing_invocations(test_vars.task, {"arg0": '"a"'})
    )
    invocations_ids = set(i.invocation_id for i in invocations)
    assert invocations_ids == test_vars.expected_ids
    # argument arg1:a is only valid for inv1
    invocations = list(
        app.orchestrator.get_existing_invocations(test_vars.task, {"arg1": '"b"'})
    )
    assert len(invocations) == 1
    assert invocations[0].invocation_id == test_vars.inv2.invocation_id
    # argument without any invocation
    invocations = list(
        app.orchestrator.get_existing_invocations(test_vars.task, {"arg1": '"x"'})
    )
    assert len(invocations) == 0


def test_get_by_status(app: MockPynenc, test_vars: Vars) -> None:
    """Test filter by status"""
    invocations = list(
        app.orchestrator.get_existing_invocations(
            test_vars.task, status=InvocationStatus.REGISTERED
        )
    )
    assert len(invocations) == 1
    assert invocations[0].invocation_id == test_vars.inv1.invocation_id
    invocations = list(
        app.orchestrator.get_existing_invocations(
            test_vars.task, status=InvocationStatus.SUCCESS
        )
    )
    assert len(invocations) == 2
    invocations_ids = set(i.invocation_id for i in invocations)
    assert invocations_ids == {
        test_vars.inv2.invocation_id,
        test_vars.inv3.invocation_id,
    }


def test_get_mix(app: MockPynenc, test_vars: Vars) -> None:
    """Test mixed filter (status and arguments)"""
    # The only way of getting just one invocation is combining filters
    # - arg1: a         --> inv1 and inv3
    # - status: SUCCESS --> inv2 and inv3
    # The only filtered invocation should be inv3
    invocations = list(
        app.orchestrator.get_existing_invocations(
            test_vars.task, {"arg1": '"a"'}, status=InvocationStatus.SUCCESS
        )
    )
    assert len(invocations) == 1
    assert invocations[0].invocation_id == test_vars.inv3.invocation_id
