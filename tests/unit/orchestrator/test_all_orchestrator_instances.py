from typing import TYPE_CHECKING

from pynenc.invocation import InvocationStatus
from tests.conftest import MockPynenc

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.invocation import DistributedInvocation

app = MockPynenc()


@app.task
def dummy_task() -> None:
    pass


def test_retry_count(app_instance: "Pynenc") -> None:
    """Test that the retry count functionality works for both orchestrators."""
    app = app_instance
    inv: "DistributedInvocation" = dummy_task()  # type: ignore
    app.orchestrator._register_new_invocations([inv])
    assert app.orchestrator.get_invocation_retries(inv.invocation_id) == 0
    app.orchestrator.increment_invocation_retries(inv.invocation_id)
    assert app.orchestrator.get_invocation_retries(inv.invocation_id) == 1
    app.orchestrator.purge()
    assert app.orchestrator.get_invocation_retries(inv.invocation_id) == 0


def test_set_pending_status(app_instance: "Pynenc") -> None:
    # Setup
    inv: "DistributedInvocation" = dummy_task()  # type: ignore
    app_instance.orchestrator._register_new_invocations([inv])
    invocation_id = inv.invocation_id
    # Set pending status can only be called on an invocation with some other status
    app_instance.orchestrator._set_invocation_status(
        invocation_id, InvocationStatus.RUNNING
    )

    # Test
    app_instance.orchestrator._set_invocation_pending_status(invocation_id)

    # Assertions
    assert (
        app_instance.orchestrator.get_invocation_status(invocation_id)
        == InvocationStatus.PENDING
    )
    assert (
        app_instance.orchestrator.get_invocation_pending_timer(invocation_id)
        is not None
    )


def test_registering_an_invocation_set_register_status(app_instance: "Pynenc") -> None:
    inv: "DistributedInvocation" = dummy_task()  # type: ignore
    app_instance.orchestrator._register_new_invocations([inv])
    assert (
        app_instance.orchestrator.get_invocation_status(inv.invocation_id)
        == InvocationStatus.REGISTERED
    )
