from typing import TYPE_CHECKING

from pynenc.call import Call
from pynenc.invocation import DistributedInvocation
from pynenc_tests.conftest import MockPynenc

if TYPE_CHECKING:
    from pynenc import Pynenc


base_app = MockPynenc()


@base_app.task
def dummy() -> None: ...


def test_routing(app_instance: "Pynenc") -> None:
    """Test that it routes and retrieve all the invocations"""
    app = app_instance
    dummy.app = app
    call: Call = Call(dummy)
    inv1: DistributedInvocation = DistributedInvocation.isolated(call)
    app.state_backend.upsert_invocations([inv1])
    app.orchestrator.route_invocation(inv1)
    inv2: DistributedInvocation = DistributedInvocation.isolated(call)
    app.state_backend.upsert_invocations([inv2])
    expected_ids = {inv1.invocation_id, inv2.invocation_id}
    app.orchestrator.route_invocation(inv2)
    assert app.broker.count_invocations() == 2
    assert (retrieved_inv_a_id := app.broker.retrieve_invocation())
    assert (retrieved_inv_b_id := app.broker.retrieve_invocation())
    assert retrieved_inv_a_id != retrieved_inv_b_id
    assert expected_ids == {retrieved_inv_a_id, retrieved_inv_b_id}


def test_broker_purge(app_instance: "Pynenc") -> None:
    """Test that purge removes all invocations"""
    app = app_instance
    dummy.app = app
    call: Call = Call(dummy)
    inv1: DistributedInvocation = DistributedInvocation.isolated(call)
    app.state_backend.upsert_invocations([inv1])
    app.orchestrator.route_invocation(inv1)
    inv2: DistributedInvocation = DistributedInvocation.isolated(call)
    app.state_backend.upsert_invocations([inv2])
    app.orchestrator.route_invocation(inv2)
    assert app.broker.count_invocations() == 2
    app.broker.purge()
    assert app.broker.count_invocations() == 0
