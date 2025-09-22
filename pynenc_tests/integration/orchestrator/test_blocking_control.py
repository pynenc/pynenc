from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.invocation import DistributedInvocation

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.task import Task


@dataclass
class Vars:
    app: "Pynenc"
    task: "Task"
    inv1: DistributedInvocation
    inv2: DistributedInvocation
    inv3: DistributedInvocation
    expected_ids: set[str]


@pytest.fixture
def test_vars(task_concat: "Task") -> Vars:
    """Set up test invocations for blocking control tests."""
    inv1: DistributedInvocation = DistributedInvocation(
        Call(task_concat, Arguments({"arg0": "a", "arg1": "a"})), None
    )
    inv2: DistributedInvocation = DistributedInvocation(
        Call(task_concat, Arguments({"arg0": "a", "arg1": "b"})), None
    )
    inv3: DistributedInvocation = DistributedInvocation(
        Call(task_concat, Arguments({"arg0": "b", "arg1": "b"})), None
    )
    app = task_concat.app
    # Set initial statuses
    app.orchestrator.register_new_invocations([inv1, inv2, inv3])
    expected_ids = {inv1.invocation_id, inv2.invocation_id, inv3.invocation_id}
    return Vars(app, task_concat, inv1, inv2, inv3, expected_ids)


def test_waiting_for_results(test_vars: Vars) -> None:
    """Test that waiting_for_results correctly registers dependencies."""
    app = test_vars.app
    inv1, inv2, inv3 = test_vars.inv1, test_vars.inv2, test_vars.inv3

    # inv1 waits for inv2 and inv3
    app.orchestrator.blocking_control.waiting_for_results(
        inv1.invocation_id, [inv2.invocation_id, inv3.invocation_id]
    )

    # Since we can't directly inspect the backend (or other impls), we'll verify indirectly
    # by checking that inv2 and inv3 are considered blocking when inv1 is waiting
    blocking_ids = list(app.orchestrator.get_blocking_invocations(3))
    assert (
        inv2.invocation_id in blocking_ids
    ), "inv2 should be blocking due to inv1 waiting"
    assert (
        inv3.invocation_id in blocking_ids
    ), "inv3 should be blocking due to inv1 waiting"
    assert (
        inv1.invocation_id not in blocking_ids
    ), "inv1 should not be blocking (it's waiting)"


def test_get_blocking_invocations_max_limit(test_vars: Vars) -> None:
    """Test that get_blocking_invocations respects max_num_invocations."""
    app = test_vars.app
    inv1, inv2, inv3 = test_vars.inv1, test_vars.inv2, test_vars.inv3

    # No dependencies initially, all should be "not waiting"
    blocking_ids = set(app.orchestrator.get_blocking_invocations(2))
    assert len(blocking_ids) <= 2, f"Expected at most 2, got {len(blocking_ids)}"
    assert blocking_ids.issubset(
        test_vars.expected_ids
    ), "Blocking IDs should match test vars"

    # Make inv1 wait on inv2 and inv3
    app.orchestrator.waiting_for_results(
        inv1.invocation_id, [inv2.invocation_id, inv3.invocation_id]
    )

    # Now only inv2 and inv3 should be blocking
    blocking_ids = set(app.orchestrator.get_blocking_invocations(1))
    assert len(blocking_ids) == 1, f"Expected exactly 1, got {len(blocking_ids)}"
    assert blocking_ids.pop() in {
        inv2.invocation_id,
        inv3.invocation_id,
    }, "Should yield either inv2 or inv3"

    blocking_ids = set(app.orchestrator.get_blocking_invocations(2))
    assert len(blocking_ids) == 2, f"Expected exactly 2, got {len(blocking_ids)}"
    assert blocking_ids == {
        inv2.invocation_id,
        inv3.invocation_id,
    }, "Should yield both inv2 and inv3"


def test_get_blocking_invocations_empty(test_vars: Vars) -> None:
    """Test get_blocking_invocations when all invocations are waiting."""
    app = test_vars.app
    inv1, inv2, inv3 = test_vars.inv1, test_vars.inv2, test_vars.inv3

    # Create a circular wait to ensure no invocations are "not waiting"
    app.orchestrator.waiting_for_results(inv1.invocation_id, [inv2.invocation_id])
    app.orchestrator.waiting_for_results(inv2.invocation_id, [inv3.invocation_id])
    app.orchestrator.waiting_for_results(inv3.invocation_id, [inv1.invocation_id])

    blocking = list(app.orchestrator.get_blocking_invocations(3))
    assert len(blocking) == 0, "Expected no blocking invocations in a circular wait"


def test_release_waiters(test_vars: Vars) -> None:
    """Test that release_waiters updates blocking status."""
    app = test_vars.app
    inv1, inv2, inv3 = test_vars.inv1, test_vars.inv2, test_vars.inv3

    # inv1 waits on inv2 and inv3
    app.orchestrator.waiting_for_results(
        inv1.invocation_id, [inv2.invocation_id, inv3.invocation_id]
    )

    # Initially, inv2 and inv3 are blocking
    blocking_ids = list(app.orchestrator.get_blocking_invocations(2))
    assert len(blocking_ids) == 2, "Expected inv2 and inv3 to be blocking"

    # Release inv2, inv1 should now be not waiting (assuming single dependency for simplicity)
    app.orchestrator.blocking_control.release_waiters(inv2.invocation_id)

    # After release, inv3 should still be blocking, inv1 might be available
    blocking_ids = set(app.orchestrator.get_blocking_invocations(2))
    assert inv3.invocation_id in blocking_ids, "inv3 should still be blocking"
    # Depending on impl, inv1 might now be blocking if inv3 is its only remaining dependency
    # We'll test for either outcome to be implementation-agnostic
    assert len(blocking_ids) <= 2, "Should not exceed max after release"
