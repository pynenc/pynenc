from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.state_backend import BaseStateBackend, InvocationHistory
from pynenc.invocation import DistributedInvocation, InvocationStatus
from tests.unit.conftest import MockPynenc


if TYPE_CHECKING:
    from _pytest.python import Metafunc
    from _pytest.fixtures import FixtureRequest
    from pynenc.task import Task
    from pynenc.types import Params, Result


def pytest_generate_tests(metafunc: "Metafunc") -> None:
    subclasses = [
        c for c in BaseStateBackend.__subclasses__() if "mock" not in c.__name__.lower()
    ]
    if "app" in metafunc.fixturenames:
        metafunc.parametrize("app", subclasses, indirect=True)


@pytest.fixture
def app(request: "FixtureRequest") -> MockPynenc:
    app = MockPynenc()
    app.state_backend = request.param(app)
    return app


@pytest.fixture
def invocation(app: MockPynenc) -> "DistributedInvocation[Params, Result]":
    @app.task
    def dummy() -> None:
        ...

    return DistributedInvocation(Call(dummy), None)


def test_store_invocation(
    app: MockPynenc, invocation: "DistributedInvocation[Params, Result]"
) -> None:
    """Test that it will store and retrieve an invocation"""
    app.state_backend.upsert_invocation(invocation)
    retrieved_invocation = app.state_backend.get_invocation(invocation.invocation_id)
    assert invocation == retrieved_invocation


def test_store_history_status(
    app: MockPynenc, invocation: "DistributedInvocation[Params, Result]"
) -> None:
    """Test that it will store and retrieve the status change history"""

    def _check_history(
        invocation_id: str,
        expected_statuses: list[InvocationStatus],
        history: list[InvocationHistory],
    ) -> None:
        assert len(history) == len(expected_statuses)
        prev_datetime = datetime.min
        for expected_status, inv_hist in zip(expected_statuses, history):
            assert inv_hist.timestamp > prev_datetime
            assert inv_hist.invocation_id == invocation_id
            assert inv_hist.status == expected_status
            prev_datetime = inv_hist.timestamp

    assert [] == app.state_backend.get_history(invocation)
    app.state_backend.add_history(invocation, status=InvocationStatus.REGISTERED)
    _check_history(
        invocation.invocation_id,
        [InvocationStatus.REGISTERED],
        app.state_backend.get_history(invocation),
    )
    app.state_backend.add_history(invocation, status=InvocationStatus.RUNNING)
    _check_history(
        invocation.invocation_id,
        [InvocationStatus.REGISTERED, InvocationStatus.RUNNING],
        app.state_backend.get_history(invocation),
    )


def test_store_result(
    app: MockPynenc, invocation: "DistributedInvocation[Params, Result]"
) -> None:
    """Test that it will store and retrieve a task result"""
    app.state_backend.upsert_invocation(invocation)
    app.state_backend.set_result(invocation, result="res_x")
    assert "res_x" == app.state_backend.get_result(invocation)
