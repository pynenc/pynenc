import concurrent.futures
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc.exceptions import PendingInvocationLockError


if TYPE_CHECKING:
    from pynenc.task import Task
    from pynenc import Pynenc


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
    app.orchestrator.set_invocation_status(inv1, status=InvocationStatus.REGISTERED)
    app.orchestrator.set_invocation_status(inv2, status=InvocationStatus.SUCCESS)
    app.orchestrator.set_invocation_status(inv3, status=InvocationStatus.SUCCESS)
    expected_ids = {inv1.invocation_id, inv2.invocation_id, inv3.invocation_id}
    return Vars(app, task_concat, inv1, inv2, inv3, expected_ids)


def test_get_all_invocations(test_vars: Vars) -> None:
    """Test get without filters"""

    app = test_vars.task.app
    invocations = list(app.orchestrator.get_existing_invocations(test_vars.task))
    invocations_ids = set(i.invocation_id for i in invocations)
    assert invocations_ids == test_vars.expected_ids


def test_get_by_arguments(test_vars: Vars) -> None:
    """Test filter by arguments"""
    # argument arg0:a is the same for both
    app = test_vars.app
    invocations = list(
        app.orchestrator.get_existing_invocations(test_vars.task, {"arg0": '"a"'})
    )
    invocations_ids = set(i.invocation_id for i in invocations)
    assert invocations_ids == test_vars.expected_ids
    # argument arg1:a is only valid for inv1
    invocations = list(
        app.orchestrator.get_existing_invocations(test_vars.task, {"arg1": '"b"'})
    )
    assert len(invocations) == 1
    assert invocations[0].invocation_id == test_vars.inv2.invocation_id
    # argument without any invocation
    invocations = list(
        app.orchestrator.get_existing_invocations(test_vars.task, {"arg1": '"x"'})
    )
    assert len(invocations) == 0


def test_get_by_status(test_vars: Vars) -> None:
    """Test filter by status"""
    app = test_vars.app
    invocations = list(
        app.orchestrator.get_existing_invocations(
            test_vars.task, status=InvocationStatus.REGISTERED
        )
    )
    assert len(invocations) == 1
    assert invocations[0].invocation_id == test_vars.inv1.invocation_id
    invocations = list(
        app.orchestrator.get_existing_invocations(
            test_vars.task, status=InvocationStatus.SUCCESS
        )
    )
    assert len(invocations) == 2
    invocations_ids = set(i.invocation_id for i in invocations)
    assert invocations_ids == {
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
    invocations = list(
        app.orchestrator.get_existing_invocations(
            test_vars.task, {"arg1": '"a"'}, status=InvocationStatus.SUCCESS
        )
    )
    assert len(invocations) == 1
    assert invocations[0].invocation_id == test_vars.inv3.invocation_id


def test_set_invocation_pending_status(test_vars: Vars) -> None:
    """test that set pending cannot be called in an already pending invocation"""
    app = test_vars.app
    test_vars.app.orchestrator.set_invocation_status(
        test_vars.inv1, InvocationStatus.REGISTERED
    )
    app.conf.max_pending_seconds = 10
    app.orchestrator._set_invocation_pending_status(test_vars.inv1)
    with pytest.raises(PendingInvocationLockError):
        app.orchestrator._set_invocation_pending_status(test_vars.inv1)


def test_set_invocation_pending_status_atomicity(test_vars: Vars) -> None:
    """Check that on multiple set_pending calls only one can succeed"""
    app = test_vars.app
    test_vars.app.orchestrator.set_invocation_status(
        test_vars.inv1, InvocationStatus.REGISTERED
    )
    app.conf.max_pending_seconds = 10

    # Define a function to run in a separate thread
    def run_set_invocation_pending_status(app: "Pynenc") -> None:
        app.orchestrator._set_invocation_pending_status(test_vars.inv1)

    # Run set_invocation_pending_status concurrently in two threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future1 = executor.submit(run_set_invocation_pending_status, app)
        future2 = executor.submit(run_set_invocation_pending_status, app)
        with pytest.raises(Exception):
            future1.result()
            future2.result()


def test_get_invocations_to_run_atomicity(test_vars: Vars) -> None:
    """test that getting task to run and set up pending is atomic
    otherwise workers could get several times the same invocation
    """

    # Define a function to run in a separate thread
    def run_get_invocations_to_run(_app: "Pynenc") -> list[DistributedInvocation]:
        return list(_app.orchestrator.get_invocations_to_run(1))

    app = test_vars.app
    # max pending seconds to a value that will not affect the tests
    test_vars.inv3.app.conf.max_pending_seconds = 10
    attempts = 5
    for _ in range(attempts):
        # broker should not return the same invocation twice
        # but we may get a blocking invocatio and immediately after the same
        # invocation from the broker
        app.broker.retrieve_invocation.side_effect = [  # type: ignore
            test_vars.inv1,
            test_vars.inv1,
            test_vars.inv1,
            None,  # broker will stop returning invocations
        ]
        # Reset inv1 status
        test_vars.app.orchestrator.set_invocation_status(
            test_vars.inv1, InvocationStatus.REGISTERED
        )
        # Run get_invocations_to_run concurrently in two threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future1 = executor.submit(run_get_invocations_to_run, app)
            future2 = executor.submit(run_get_invocations_to_run, app)
        # Check that never returns the same invocation
        assert future1.result() != future2.result()
