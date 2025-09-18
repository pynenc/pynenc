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
def test_vars(task_concat: "Task") -> Vars:
    """Test the implementation of abstract methods:
    set_invocation_status, get_existing_invocations
    """
    inv1: DistributedInvocation = DistributedInvocation(
        Call(task_concat, Arguments({"arg0": "a", "arg1": "a"})), None
    )
    inv2: DistributedInvocation = DistributedInvocation(
        Call(task_concat, Arguments({"arg0": "a", "arg1": "b"})), None
    )
    inv3: DistributedInvocation = DistributedInvocation(
        Call(task_concat, Arguments({"arg0": "a", "arg1": "a"})), None
    )
    app = task_concat.app
    app.orchestrator.register_new_invocations([inv1, inv2, inv3])
    expected_ids = {inv1.invocation_id, inv2.invocation_id, inv3.invocation_id}
    return Vars(app, task_concat, inv1, inv2, inv3, expected_ids)


def test_get_all_invocations(test_vars: Vars) -> None:
    """Test get without filters"""

    app = test_vars.task.app
    invocations_ids = set(app.orchestrator.get_existing_invocations(test_vars.task))
    assert invocations_ids == test_vars.expected_ids


def test_get_by_arguments(test_vars: Vars) -> None:
    """Test filter by arguments"""
    app = test_vars.app
    # we only store arguments on routing if the task has concurrency control enabled
    # we register the arguments manually for the test
    app.orchestrator.index_arguments_for_concurrency_control(test_vars.inv1)
    app.orchestrator.index_arguments_for_concurrency_control(test_vars.inv2)
    app.orchestrator.index_arguments_for_concurrency_control(test_vars.inv3)

    # argument arg0:a is the same for both
    invocations_ids = set(
        app.orchestrator.get_existing_invocations(test_vars.task, {"arg0": '"a"'})
    )
    assert invocations_ids == test_vars.expected_ids
    # argument arg1:a is only valid for inv1
    invocations_ids = set(
        app.orchestrator.get_existing_invocations(test_vars.task, {"arg1": '"b"'})
    )
    assert len(invocations_ids) == 1
    assert invocations_ids == {test_vars.inv2.invocation_id}
    # argument without any invocation
    invocations = list(
        app.orchestrator.get_existing_invocations(test_vars.task, {"arg1": '"x"'})
    )
    assert len(invocations) == 0


def test_get_by_status(test_vars: Vars) -> None:
    """Test filter by status"""
    app = test_vars.app
    invocations_ids = set(
        app.orchestrator.get_existing_invocations(
            test_vars.task, statuses=[InvocationStatus.REGISTERED]
        )
    )
    assert len(invocations_ids) == 3
    app.orchestrator.set_invocation_status(
        test_vars.inv2.invocation_id, status=InvocationStatus.SUCCESS
    )
    app.orchestrator.set_invocation_status(
        test_vars.inv3.invocation_id, status=InvocationStatus.SUCCESS
    )
    invocation_ids = set(
        app.orchestrator.get_existing_invocations(
            test_vars.task, statuses=[InvocationStatus.SUCCESS]
        )
    )
    assert len(invocation_ids) == 2
    assert invocation_ids == {
        test_vars.inv2.invocation_id,
        test_vars.inv3.invocation_id,
    }


def test_get_mix(test_vars: Vars) -> None:
    """Test mixed filter (status and arguments)"""
    # The only way of getting just one invocation is combining filters
    # - arg1: a         --> inv1 and inv3
    # - status: SUCCESS --> inv2 and inv3
    # The only filtered invocation should be inv3
    app = test_vars.app
    # we only store arguments on routing if the task has concurrency control enabled
    # we register the arguments manually for the test
    app.orchestrator.index_arguments_for_concurrency_control(test_vars.inv3)
    invocation_ids = set(
        app.orchestrator.get_existing_invocations(test_vars.task, {"arg1": '"a"'})
    )
    assert len(invocation_ids) == 1
    assert invocation_ids == {test_vars.inv3.invocation_id}
    assert invocation_ids == set(
        app.orchestrator.get_existing_invocations(
            test_vars.task, {"arg1": '"a"'}, statuses=[test_vars.inv3.status]
        )
    )


def test_set_invocation_pending_status(test_vars: Vars) -> None:
    """test that set pending cannot be called in an already pending invocation"""
    app = test_vars.app
    if not isinstance(app.orchestrator, MemOrchestrator):
        pytest.skip("Only for MemOrchestrator")
    app.conf.max_pending_seconds = 10
    app.orchestrator._set_invocation_pending_status(test_vars.inv1.invocation_id)
    with pytest.raises(PendingInvocationLockError):
        app.orchestrator._set_invocation_pending_status(test_vars.inv1.invocation_id)


def test_set_invocation_pending_status_atomicity(test_vars: Vars) -> None:
    """Check that on multiple set_pending calls only one can succeed"""
    app = test_vars.app
    if not isinstance(app.orchestrator, MemOrchestrator):
        pytest.skip("Only for MemOrchestrator")

    app = test_vars.app
    app.conf.max_pending_seconds = 10

    # Define a function to run in a separate thread
    def run_set_invocation_pending_status(app: "Pynenc") -> None:
        app.orchestrator._set_invocation_pending_status(test_vars.inv1.invocation_id)

    # Run set_invocation_pending_status concurrently in two threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(run_set_invocation_pending_status, app)
        future2 = executor.submit(run_set_invocation_pending_status, app)
        with pytest.raises(PendingInvocationLockError):
            future1.result()
            future2.result()


def test_get_invocations_to_run_atomicity(task_concat: "Task") -> None:
    """test that getting task to run and set up pending is atomic
    otherwise workers could get several times the same invocation
    """

    # Define a function to run in a separate thread
    def run_get_invocations_to_run(_app: "Pynenc") -> list[DistributedInvocation]:
        return list(_app.orchestrator.get_invocations_to_run(1))

    app = task_concat.app
    # max pending seconds to a value that will not affect the tests
    task_concat.app.conf.max_pending_seconds = 10
    attempts = 5
    for _ in range(attempts):
        # broker should not return the same invocation twice
        # Create a new invocation each time to do multiple attemps
        inv: DistributedInvocation = DistributedInvocation(
            Call(task_concat, Arguments({"arg0": "a", "arg1": "a"})), None
        )
        task_concat.app.orchestrator.register_new_invocations([inv])
        # Run get_invocations_to_run concurrently in two threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future1 = executor.submit(run_get_invocations_to_run, app)
            future2 = executor.submit(run_get_invocations_to_run, app)
        # Check that never returns the same invocation
        res1 = future1.result()
        res2 = future2.result()
        assert res1 != res2


def test_get_invocations_to_run_max_limit(test_vars: Vars) -> None:
    """
    Test that get_invocations_to_run respects max_num_invocations, yielding at most that many invocations,
    even if multiple blocking invocations are available.
    """
    app = test_vars.app

    # Broker will return invocations in order
    app.orchestrator.route_call(test_vars.inv1.call)
    app.orchestrator.route_call(test_vars.inv2.call)
    app.orchestrator.route_call(test_vars.inv3.call)

    # Notify that inv1 is waiting in inv2 and inv3
    app.orchestrator.waiting_for_results(
        test_vars.inv1.invocation_id,
        [test_vars.inv2.invocation_id, test_vars.inv3.invocation_id],
    )

    # Get one invocation to run
    invocations = list(app.orchestrator.get_invocations_to_run(1))

    # Check only 1 invocation retrieved
    assert len(invocations) == 1, f"Expected 1 invocation, got {len(invocations)}"

    # Check the invocation is from the blocking ones
    assert invocations[0].invocation_id in {
        test_vars.inv2.invocation_id,
        test_vars.inv3.invocation_id,
    }

    # Verify only one got set to PENDING
    pending_count = sum(
        1
        for inv in [test_vars.inv1, test_vars.inv2]
        if app.orchestrator.get_invocation_status(inv.invocation_id)
        == InvocationStatus.PENDING
    )
    assert pending_count == 1, f"Expected 1 invocation PENDING, got {pending_count}"
