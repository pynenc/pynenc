from typing import TYPE_CHECKING

import pytest

from pynenc.broker.base_broker import BaseBroker
from pynenc.call import Call
from pynenc.invocation import DistributedInvocation
from tests.conftest import MockPynenc

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from _pytest.python import Metafunc


base_app = MockPynenc()


@base_app.task
def dummy() -> None:
    ...


def pytest_generate_tests(metafunc: "Metafunc") -> None:
    subclasses = [
        c for c in BaseBroker.__subclasses__() if "mock" not in c.__name__.lower()
    ]

    if "app" in metafunc.fixturenames:
        metafunc.parametrize("app", subclasses, indirect=True)


@pytest.fixture
def app(request: "FixtureRequest") -> MockPynenc:
    app = MockPynenc()
    app.broker = request.param(app)
    app.broker.purge()
    return app


@pytest.fixture
def call(app: MockPynenc) -> "Call":
    dummy.app = app
    return Call(dummy)


def test_routing(app: MockPynenc, call: "Call") -> None:
    """Test that it routes and retrieve all the invocations"""
    inv1: DistributedInvocation = app.broker.route_call(call)
    inv2: DistributedInvocation = DistributedInvocation(call, None)
    expected_ids = {inv1.invocation_id, inv2.invocation_id}
    app.broker.route_invocation(inv2)
    assert app.broker.count_invocations() == 2
    assert (retrieved_inv_a := app.broker.retrieve_invocation())
    assert (retrieved_inv_b := app.broker.retrieve_invocation())
    assert retrieved_inv_a != retrieved_inv_b
    assert expected_ids == {
        retrieved_inv_a.invocation_id,
        retrieved_inv_b.invocation_id,
    }
