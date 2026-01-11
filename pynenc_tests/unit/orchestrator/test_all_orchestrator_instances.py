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


@app.task
def another_task(value: int) -> int:
    return value * 2


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
    runner_id = "some_unique_runner_id"

    # We should follow a valid status transition path to reach a final status
    app_instance.orchestrator._atomic_status_transition(
        invocation_id, InvocationStatus.PENDING, runner_id
    )
    app_instance.orchestrator._atomic_status_transition(
        invocation_id, InvocationStatus.RUNNING, runner_id
    )

    # Attempting to change ownership by another owner should raise InvocationStatusOwnershipError
    with pytest.raises(InvocationStatusOwnershipError) as exc_info:
        app_instance.orchestrator._atomic_status_transition(
            invocation_id, InvocationStatus.SUCCESS, "some_other_runner_id"
        )
        assert exc_info.value.from_status == InvocationStatus.RUNNING
        assert exc_info.value.to_status == InvocationStatus.SUCCESS
        assert exc_info.value.current_owner == runner_id
        assert exc_info.value.attempted_owner == "some_other_runner_id"

    app_instance.orchestrator._atomic_status_transition(
        invocation_id, InvocationStatus.SUCCESS, runner_id
    )

    # It's not possible to transition from a final status to any other status
    for status in InvocationStatus:
        with pytest.raises(InvocationStatusTransitionError) as exc_info:
            app_instance.orchestrator._atomic_status_transition(
                invocation_id, status, runner_id
            )
            assert exc_info.value.from_status == InvocationStatus.SUCCESS
            assert exc_info.value.to_status == status


# ################################################################################### #
# PAGINATION TESTS
# ################################################################################### #


def test_get_invocation_ids_paginated_returns_limited_results(
    app_instance: "Pynenc",
) -> None:
    """Test that paginated query respects limit parameter."""
    dummy_task.app = app_instance
    another_task.app = app_instance

    # Create multiple invocations
    invocations = []
    for i in range(5):
        inv: DistributedInvocation = another_task(i)  # type: ignore
        invocations.append(inv)

    app_instance.orchestrator._register_new_invocations(invocations)

    # Request only 3 invocations
    result = app_instance.orchestrator.get_invocation_ids_paginated(limit=3)
    assert len(result) == 3

    # Request all invocations
    result_all = app_instance.orchestrator.get_invocation_ids_paginated(limit=10)
    assert len(result_all) == 5


def test_get_invocation_ids_paginated_respects_offset(
    app_instance: "Pynenc",
) -> None:
    """Test that paginated query respects offset parameter."""
    another_task.app = app_instance

    # Create multiple invocations
    invocations = []
    for i in range(5):
        inv: DistributedInvocation = another_task(i)  # type: ignore
        invocations.append(inv)

    app_instance.orchestrator._register_new_invocations(invocations)

    # Get first page
    page1 = app_instance.orchestrator.get_invocation_ids_paginated(limit=2, offset=0)
    assert len(page1) == 2

    # Get second page
    page2 = app_instance.orchestrator.get_invocation_ids_paginated(limit=2, offset=2)
    assert len(page2) == 2

    # Pages should have different invocations
    assert set(page1).isdisjoint(set(page2))

    # Get third page (should have only 1 item)
    page3 = app_instance.orchestrator.get_invocation_ids_paginated(limit=2, offset=4)
    assert len(page3) == 1


def test_get_invocation_ids_paginated_filters_by_task_id(
    app_instance: "Pynenc",
) -> None:
    """Test that paginated query filters by task_id."""
    dummy_task.app = app_instance
    another_task.app = app_instance

    # Create invocations from different tasks
    inv1: DistributedInvocation = dummy_task()  # type: ignore
    inv2: DistributedInvocation = another_task(1)  # type: ignore
    inv3: DistributedInvocation = another_task(2)  # type: ignore

    app_instance.orchestrator._register_new_invocations([inv1, inv2, inv3])

    # Filter by another_task's task_id
    result = app_instance.orchestrator.get_invocation_ids_paginated(
        task_id=another_task.task_id,
        limit=10,
    )
    assert len(result) == 2
    assert inv1.invocation_id not in result
    assert inv2.invocation_id in result
    assert inv3.invocation_id in result


def test_get_invocation_ids_paginated_filters_by_status(
    app_instance: "Pynenc",
) -> None:
    """Test that paginated query filters by status."""
    another_task.app = app_instance

    # Create multiple invocations
    inv1: DistributedInvocation = another_task(1)  # type: ignore
    inv2: DistributedInvocation = another_task(2)  # type: ignore
    inv3: DistributedInvocation = another_task(3)  # type: ignore

    app_instance.orchestrator._register_new_invocations([inv1, inv2, inv3])

    # Transition one to PENDING and one to SUCCESS
    runner_id = "test_owner"
    app_instance.orchestrator._atomic_status_transition(
        inv1.invocation_id, InvocationStatus.PENDING, runner_id
    )
    app_instance.orchestrator._atomic_status_transition(
        inv2.invocation_id, InvocationStatus.PENDING, runner_id
    )
    app_instance.orchestrator._atomic_status_transition(
        inv2.invocation_id, InvocationStatus.RUNNING, runner_id
    )
    app_instance.orchestrator._atomic_status_transition(
        inv2.invocation_id, InvocationStatus.SUCCESS, runner_id
    )

    # Filter by REGISTERED status (should only include inv3)
    registered = app_instance.orchestrator.get_invocation_ids_paginated(
        statuses=[InvocationStatus.REGISTERED],
        limit=10,
    )
    assert len(registered) == 1
    assert inv3.invocation_id in registered

    # Filter by SUCCESS status
    success = app_instance.orchestrator.get_invocation_ids_paginated(
        statuses=[InvocationStatus.SUCCESS],
        limit=10,
    )
    assert len(success) == 1
    assert inv2.invocation_id in success


def test_count_invocations_returns_correct_count(app_instance: "Pynenc") -> None:
    """Test that count_invocations returns accurate counts."""
    another_task.app = app_instance

    # Initially should be 0
    count = app_instance.orchestrator.count_invocations()
    assert count == 0

    # Create invocations
    invocations = []
    for i in range(5):
        inv: DistributedInvocation = another_task(i)  # type: ignore
        invocations.append(inv)

    app_instance.orchestrator._register_new_invocations(invocations)

    # Should now be 5
    count = app_instance.orchestrator.count_invocations()
    assert count == 5


def test_count_invocations_filters_by_task_id(app_instance: "Pynenc") -> None:
    """Test that count_invocations filters by task_id."""
    dummy_task.app = app_instance
    another_task.app = app_instance

    inv1: DistributedInvocation = dummy_task()  # type: ignore
    inv2: DistributedInvocation = another_task(1)  # type: ignore
    inv3: DistributedInvocation = another_task(2)  # type: ignore

    app_instance.orchestrator._register_new_invocations([inv1, inv2, inv3])

    count_another = app_instance.orchestrator.count_invocations(
        task_id=another_task.task_id
    )
    assert count_another == 2

    count_dummy = app_instance.orchestrator.count_invocations(
        task_id=dummy_task.task_id
    )
    assert count_dummy == 1


def test_count_invocations_filters_by_status(app_instance: "Pynenc") -> None:
    """Test that count_invocations filters by status."""
    another_task.app = app_instance

    inv1: DistributedInvocation = another_task(1)  # type: ignore
    inv2: DistributedInvocation = another_task(2)  # type: ignore

    app_instance.orchestrator._register_new_invocations([inv1, inv2])

    # Transition inv1 to SUCCESS
    runner_id = "test_owner"
    app_instance.orchestrator._atomic_status_transition(
        inv1.invocation_id, InvocationStatus.PENDING, runner_id
    )
    app_instance.orchestrator._atomic_status_transition(
        inv1.invocation_id, InvocationStatus.RUNNING, runner_id
    )
    app_instance.orchestrator._atomic_status_transition(
        inv1.invocation_id, InvocationStatus.SUCCESS, runner_id
    )

    # Count by status
    registered_count = app_instance.orchestrator.count_invocations(
        statuses=[InvocationStatus.REGISTERED]
    )
    assert registered_count == 1

    success_count = app_instance.orchestrator.count_invocations(
        statuses=[InvocationStatus.SUCCESS]
    )
    assert success_count == 1

    # Count multiple statuses
    all_final = app_instance.orchestrator.count_invocations(
        statuses=[InvocationStatus.SUCCESS, InvocationStatus.FAILED]
    )
    assert all_final == 1


def test_auto_purge(app_instance: "Pynenc") -> None:
    """Test that it will auto purge invocations in final state"""
    app = app_instance
    # Ensure the module-level task is bound to the test app instance
    dummy_task.app = app
    inv: DistributedInvocation = dummy_task()  # type: ignore
    app.orchestrator.register_new_invocations([inv])

    def get_invocation_id() -> str | None:
        invs = list(app.orchestrator.get_existing_invocations(task=inv.task))
        assert len(invs) <= 1
        return invs[0] if invs else None

    assert get_invocation_id() == inv.invocation_id
    # we mark the inv1 to auto_purge it
    # but the auto_purge is set to 24 hours
    app.orchestrator.conf.auto_final_invocation_purge_hours = 24.0
    app.orchestrator.set_up_invocation_auto_purge(inv.invocation_id)
    # auto_purge should not purge it
    app.orchestrator.auto_purge()
    assert get_invocation_id() == inv.invocation_id
    # if we change auto_purge to 0
    app.orchestrator.conf.auto_final_invocation_purge_hours = 0.0
    # auto_purge should purge it
    app.orchestrator.auto_purge()
    assert get_invocation_id() is None
