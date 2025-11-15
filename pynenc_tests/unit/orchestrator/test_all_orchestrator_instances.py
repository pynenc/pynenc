from typing import TYPE_CHECKING

import pytest

from pynenc.exceptions import (
    InvocationStatusTransitionError,
    InvocationStatusOwnershipError,
)
from pynenc.invocation import InvocationStatus
from pynenc_tests.conftest import MockPynenc

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
    inv: DistributedInvocation = dummy_task()  # type: ignore
    app.orchestrator._register_new_invocations([inv])
    assert app.orchestrator.get_invocation_retries(inv.invocation_id) == 0
    app.orchestrator.increment_invocation_retries(inv.invocation_id)
    assert app.orchestrator.get_invocation_retries(inv.invocation_id) == 1
    app.orchestrator.purge()
    assert app.orchestrator.get_invocation_retries(inv.invocation_id) == 0


def test_registering_an_invocation_set_register_status(app_instance: "Pynenc") -> None:
    inv: DistributedInvocation = dummy_task()  # type: ignore
    app_instance.orchestrator._register_new_invocations([inv])
    assert (
        app_instance.orchestrator.get_invocation_status(inv.invocation_id)
        == InvocationStatus.REGISTERED
    )


def test_cannot_change_final_status(app_instance: "Pynenc") -> None:
    """Check that any attempt to change status from a final state raises InvocationOnFinalStatusError."""
    inv: DistributedInvocation = dummy_task()  # type: ignore
    app_instance.orchestrator._register_new_invocations([inv])
    invocation_id = inv.invocation_id
    owner_id = "some_unique_owner_id"

    # We should follow a valid status transition path to reach a final status
    app_instance.orchestrator._atomic_status_transition(
        invocation_id, InvocationStatus.PENDING, owner_id
    )
    app_instance.orchestrator._atomic_status_transition(
        invocation_id, InvocationStatus.RUNNING, owner_id
    )

    # Attempting to change ownership by another owner should raise InvocationStatusOwnershipError
    with pytest.raises(InvocationStatusOwnershipError) as exc_info:
        app_instance.orchestrator._atomic_status_transition(
            invocation_id, InvocationStatus.SUCCESS, "some_other_owner_id"
        )
        assert exc_info.value.from_status == InvocationStatus.RUNNING
        assert exc_info.value.to_status == InvocationStatus.SUCCESS
        assert exc_info.value.current_owner == owner_id
        assert exc_info.value.attempted_owner == "some_other_owner_id"

    app_instance.orchestrator._atomic_status_transition(
        invocation_id, InvocationStatus.SUCCESS, owner_id
    )

    # It's not possible to transition from a final status to any other status
    for status in InvocationStatus:
        with pytest.raises(InvocationStatusTransitionError) as exc_info:
            app_instance.orchestrator._atomic_status_transition(
                invocation_id, status, owner_id
            )
            assert exc_info.value.from_status == InvocationStatus.SUCCESS
            assert exc_info.value.to_status == status
