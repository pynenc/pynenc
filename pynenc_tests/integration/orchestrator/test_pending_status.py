from time import sleep
from typing import TYPE_CHECKING

from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc.runner.runner_context import RunnerContext

if TYPE_CHECKING:
    pass


def test_pending_status_expiration(
    dummy_invocation_io: "DistributedInvocation",
) -> None:
    """Test that PENDING status expires and gets recovered to REROUTED."""
    app = dummy_invocation_io.app
    app.orchestrator.register_new_invocations([dummy_invocation_io])
    runner_ctx = RunnerContext.from_runner(app.runner)

    assert (
        app.orchestrator.get_invocation_status(dummy_invocation_io.invocation_id)
        == InvocationStatus.REGISTERED
    )

    # Configure short expiration time for testing
    app.conf.max_pending_seconds = 0.1
    app.orchestrator.set_invocation_status(
        dummy_invocation_io.invocation_id, InvocationStatus.PENDING, runner_ctx
    )

    # Check immediately - should still be PENDING
    assert (
        app.orchestrator.get_invocation_status(dummy_invocation_io.invocation_id)
        == InvocationStatus.PENDING
    )

    # Wait for pending to expire
    sleep(app.conf.max_pending_seconds * 2)

    # Run recovery service to recover expired PENDING invocations
    app.orchestrator.invocation_recovery_service(runner_ctx)

    # Should be REROUTED after recovery service
    assert (
        app.orchestrator.get_invocation_status(dummy_invocation_io.invocation_id)
        == InvocationStatus.REROUTED
    )
