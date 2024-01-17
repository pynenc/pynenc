from typing import TYPE_CHECKING

from pynenc.invocation import DistributedInvocation

if TYPE_CHECKING:
    pass


def test_get_an_increase_retries(dummy_invocation: "DistributedInvocation") -> None:
    app = dummy_invocation.app
    assert app.orchestrator.get_invocation_retries(dummy_invocation) == 0
    app.orchestrator.increment_invocation_retries(dummy_invocation)
    assert app.orchestrator.get_invocation_retries(dummy_invocation) == 1
