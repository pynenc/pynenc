import concurrent.futures
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.exceptions import PendingInvocationLockError
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc.orchestrator import MemOrchestrator

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
def test_vars_gi(task_concat_io: "Task") -> Vars:
    """Test the implementation of abstract methods:
    set_invocation_status, get_existing_invocations
    """
    inv1: DistributedInvocation = DistributedInvocation(
        Call(task_concat_io, Arguments({"arg0": "a", "arg1": "a"})), None
    )
    inv2: DistributedInvocation = DistributedInvocation(
        Call(task_concat_io, Arguments({"arg0": "a", "arg1": "b"})), None
    )
    inv3: DistributedInvocation = DistributedInvocation(
        Call(task_concat_io, Arguments({"arg0": "a", "arg1": "a"})), None
    )
    app = task_concat_io.app
    app.orchestrator.register_new_invocations([inv1, inv2, inv3])
    expected_ids = {inv1.invocation_id, inv2.invocation_id, inv3.invocation_id}
    return Vars(app, task_concat_io, inv1, inv2, inv3, expected_ids)


def test_get_all_invocations(test_vars_gi: Vars) -> None:
    """Test get without filters"""

    app = test_vars_gi.task.app
    invocations_ids = set(app.orchestrator.get_existing_invocations(test_vars_gi.task))
    assert invocations_ids == test_vars_gi.expected_ids


def test_get_by_arguments(test_vars_gi: Vars) -> None:
    """Test filter by arguments"""
    app = test_vars_gi.app
    # we only store arguments on routing if the task has concurrency control enabled
    # we register the arguments manually for the test
    app.orchestrator.index_arguments_for_concurrency_control(test_vars_gi.inv1)
    app.orchestrator.index_arguments_for_concurrency_control(test_vars_gi.inv2)
    app.orchestrator.index_arguments_for_concurrency_control(test_vars_gi.inv3)

    # argument arg0:a is the same for both
    invocations_ids = set(
        app.orchestrator.get_existing_invocations(test_vars_gi.task, {"arg0": '"a"'})
    )
    assert invocations_ids == test_vars_gi.expected_ids
    # argument arg1:a is only valid for inv1
    invocations_ids = set(
        app.orchestrator.get_existing_invocations(test_vars_gi.task, {"arg1": '"b"'})
    )
    assert len(invocations_ids) == 1
    assert invocations_ids == {test_vars_gi.inv2.invocation_id}
    # argument without any invocation
    invocations = list(
        app.orchestrator.get_existing_invocations(test_vars_gi.task, {"arg1": '"x"'})
    )
    assert len(invocations) == 0


def test_get_by_status(test_vars_gi: Vars) -> None:
    """Test filter by status"""
    app = test_vars_gi.app
    invocations_ids = set(
        app.orchestrator.get_existing_invocations(
            test_vars_gi.task, statuses=[InvocationStatus.REGISTERED]
        )
    )
    assert len(invocations_ids) == 3
    app.orchestrator.set_invocation_status(
        test_vars_gi.inv2.invocation_id, status=InvocationStatus.SUCCESS
    )
    app.orchestrator.set_invocation_status(
        test_vars_gi.inv3.invocation_id, status=InvocationStatus.SUCCESS
    )
    invocation_ids = set(
        app.orchestrator.get_existing_invocations(
            test_vars_gi.task, statuses=[InvocationStatus.SUCCESS]
        )
    )
    assert len(invocation_ids) == 2
    assert invocation_ids == {
        test_vars_gi.inv2.invocation_id,
        test_vars_gi.inv3.invocation_id,
    }


def test_get_mix(test_vars_gi: Vars) -> None:
    """Test mixed filter (status and arguments)"""
    # The only way of getting just one invocation is combining filters
    # - arg1: a         --> inv1 and inv3
    # - status: SUCCESS --> inv2 and inv3
    # The only filtered invocation should be inv3
    app = test_vars_gi.app
    # we only store arguments on routing if the task has concurrency control enabled
    # we register the arguments manually for the test
    app.orchestrator.index_arguments_for_concurrency_control(test_vars_gi.inv3)
    invocation_ids = set(
        app.orchestrator.get_existing_invocations(test_vars_gi.task, {"arg1": '"a"'})
    )
    assert len(invocation_ids) == 1
    assert invocation_ids == {test_vars_gi.inv3.invocation_id}
    assert invocation_ids == set(
        app.orchestrator.get_existing_invocations(
            test_vars_gi.task, {"arg1": '"a"'}, statuses=[test_vars_gi.inv3.status]
        )
    )


def test_set_invocation_pending_status(test_vars_gi: Vars) -> None:
    """test that set pending cannot be called in an already pending invocation"""
    app = test_vars_gi.app
    if not isinstance(app.orchestrator, MemOrchestrator):
        pytest.skip("Only for MemOrchestrator")
    app.conf.max_pending_seconds = 10
    app.orchestrator._set_invocation_pending_status(test_vars_gi.inv1.invocation_id)
    with pytest.raises(PendingInvocationLockError):
        app.orchestrator._set_invocation_pending_status(test_vars_gi.inv1.invocation_id)


def test_set_invocation_pending_status_atomicity(test_vars_gi: Vars) -> None:
    """Check that on multiple set_pending calls only one can succeed"""
    app = test_vars_gi.app
    if not isinstance(app.orchestrator, MemOrchestrator):
        pytest.skip("Only for MemOrchestrator")

    app = test_vars_gi.app
    app.conf.max_pending_seconds = 10

    # Define a function to run in a separate thread
    def run_set_invocation_pending_status(app: "Pynenc") -> None:
        app.orchestrator._set_invocation_pending_status(test_vars_gi.inv1.invocation_id)

    # Run set_invocation_pending_status concurrently in two threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(run_set_invocation_pending_status, app)
        future2 = executor.submit(run_set_invocation_pending_status, app)
        with pytest.raises(PendingInvocationLockError):
            future1.result()
            future2.result()


def test_get_invocations_to_run_atomicity(task_concat_io: "Task") -> None:
    """test that getting task to run and set up pending is atomic
    otherwise workers could get several times the same invocation
    """

    # Define a function to run in a separate thread
    def run_get_invocations_to_run(_app: "Pynenc") -> list[DistributedInvocation]:
        return list(_app.orchestrator.get_invocations_to_run(1))

    app = task_concat_io.app
    # max pending seconds to a value that will not affect the tests
    task_concat_io.app.conf.max_pending_seconds = 10
    attempts = 5
    for _ in range(attempts):
        # broker should not return the same invocation twice
        # Create a new invocation each time to do multiple attemps
        inv: DistributedInvocation = DistributedInvocation(
            Call(task_concat_io, Arguments({"arg0": "a", "arg1": "a"})), None
        )
        task_concat_io.app.orchestrator.register_new_invocations([inv])
        # Run get_invocations_to_run concurrently in two threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future1 = executor.submit(run_get_invocations_to_run, app)
            future2 = executor.submit(run_get_invocations_to_run, app)
        # Check that never returns the same invocation
        res1 = future1.result()
        res2 = future2.result()
        assert res1 != res2


def test_get_invocations_to_run_max_limit(test_vars_gi: Vars) -> None:
    """
    Test that get_invocations_to_run respects max_num_invocations, yielding at most that many invocations,
    even if multiple blocking invocations are available.
    """
    app = test_vars_gi.app

    # Broker will return invocations in order
    app.orchestrator.route_call(test_vars_gi.inv1.call)
    app.orchestrator.route_call(test_vars_gi.inv2.call)
    app.orchestrator.route_call(test_vars_gi.inv3.call)

    # Notify that inv1 is waiting in inv2 and inv3
    app.orchestrator.waiting_for_results(
        test_vars_gi.inv1.invocation_id,
        [test_vars_gi.inv2.invocation_id, test_vars_gi.inv3.invocation_id],
    )

    # Get one invocation to run
    invocations = list(app.orchestrator.get_invocations_to_run(1))

    # Check only 1 invocation retrieved
    assert len(invocations) == 1, f"Expected 1 invocation, got {len(invocations)}"

    # Check that the returned invocation is one of the blocking ones
    returned_invocation = invocations[0]
    if returned_invocation.invocation_id == test_vars_gi.inv2.invocation_id:
        returned_blocking_invocation = test_vars_gi.inv2
        other_blocking_invocation = test_vars_gi.inv3
    elif returned_invocation.invocation_id == test_vars_gi.inv3.invocation_id:
        returned_blocking_invocation = test_vars_gi.inv3
        other_blocking_invocation = test_vars_gi.inv2
    else:
        pytest.fail(
            f"Returned inv1, that is blocked by inv2 and inv3 : {returned_invocation.invocation_id}"
        )

    # Verify the returned blocking invocation is PENDING and the other is REGISTERED
    assert (
        app.orchestrator.get_invocation_status(
            returned_blocking_invocation.invocation_id
        )
        == InvocationStatus.PENDING
    )
    assert (
        app.orchestrator.get_invocation_status(other_blocking_invocation.invocation_id)
        == InvocationStatus.REGISTERED
    )
