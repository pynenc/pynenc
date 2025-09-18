from typing import TYPE_CHECKING

from pynenc.invocation import DistributedInvocation

if TYPE_CHECKING:
    pass


def test_get_an_increase_retries(dummy_invocation: "DistributedInvocation") -> None:
    app = dummy_invocation.app
    inv_id = dummy_invocation.invocation_id
    app.orchestrator._register_new_invocations([dummy_invocation])
    assert app.orchestrator.get_invocation_retries(inv_id) == 0
    app.orchestrator.increment_invocation_retries(inv_id)
    assert app.orchestrator.get_invocation_retries(inv_id) == 1
