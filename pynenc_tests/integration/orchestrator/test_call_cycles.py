from dataclasses import dataclass
from time import sleep
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.conf.config_pynenc import ArgumentPrintMode
from pynenc.exceptions import CycleDetectedError
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc.runner import DummyRunner, RunnerContext

if TYPE_CHECKING:
    from collections.abc import Generator

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
def test_vars(task_mirror: "Task") -> Vars:
    """Test the implementation of abstract methods:
    set_invocation_status, get_existing_invocations
    """
    inv1: DistributedInvocation = DistributedInvocation(
        Call(task_mirror, Arguments({"arg": "a"})), None
    )
    inv2: DistributedInvocation = DistributedInvocation(
        Call(task_mirror, Arguments({"arg": "b"})), None
    )
    inv3: DistributedInvocation = DistributedInvocation(
        Call(task_mirror, Arguments({"arg": "c"})), None
    )
    app = task_mirror.app
    # app.orchestrator.set_invocation_status(inv1, status=InvocationStatus.REGISTERED)
    # app.orchestrator.set_invocation_status(inv2, status=InvocationStatus.SUCCESS)
    # app.orchestrator.set_invocation_status(inv3, status=InvocationStatus.SUCCESS)
    expected_ids = {inv1.invocation_id, inv2.invocation_id, inv3.invocation_id}
    return Vars(app, task_mirror, inv1, inv2, inv3, expected_ids)


@pytest.fixture
def mock_register_task_run() -> "Generator[MagicMock, None, None]":
    """
    Mock workflow registration for tests to prevent crashes.

    This fixture patches the workflow context's register_task_run method
    to prevent crashes when calling set_invocation_run directly.

    :yield: The mocked register_task_run method
    """
    # Create a mock workflow context with register_task_run method
    mock_context = MagicMock()
    mock_context.register_task_run = MagicMock()

    # Patch the property to return our mock context
    with patch(
        "pynenc.invocation.dist_invocation.DistributedInvocation.wf",
        new_callable=PropertyMock,
        return_value=mock_context,
    ):
        yield mock_context.register_task_run


def test_causes_cycles(test_vars: Vars, mock_register_task_run: MagicMock) -> None:
    """Test that it will raise an exception on cycles"""
    test_vars.app.conf.print_arguments = True
    test_vars.app.conf.argument_print_mode = ArgumentPrintMode.FULL

    test_vars.app.orchestrator.register_new_invocations(
        [test_vars.inv1, test_vars.inv2]
    )
    runner_context = RunnerContext.from_runner(DummyRunner(test_vars.app))
    test_vars.app.orchestrator.set_invocation_run(
        test_vars.inv1, test_vars.inv2, runner_context
    )
    test_vars.app.orchestrator.set_invocation_run(
        test_vars.inv2, test_vars.inv3, runner_context
    )
    with pytest.raises(CycleDetectedError) as exc_info:
        test_vars.app.orchestrator.add_call_and_check_cycles(
            test_vars.inv3, test_vars.inv1
        )

    expected_error = (
        "A cycle was detected: Cycle detected:\n"
        "- Call(task=pynenc_tests.integration.orchestrator.orchestrator_tasks.dummy_mirror, arguments={arg:c})\n"
        "- Call(task=pynenc_tests.integration.orchestrator.orchestrator_tasks.dummy_mirror, arguments={arg:a})\n"
        "- Call(task=pynenc_tests.integration.orchestrator.orchestrator_tasks.dummy_mirror, arguments={arg:b})\n"
        "- back to Call(task=pynenc_tests.integration.orchestrator.orchestrator_tasks.dummy_mirror, arguments={arg:c})"
    )

    assert str(exc_info.value) == expected_error


def test_clean_up_cycles(test_vars: Vars, mock_register_task_run: MagicMock) -> None:
    """Test that it will clean up cycles"""
    test_vars.app.orchestrator.register_new_invocations(
        [test_vars.inv1, test_vars.inv2, test_vars.inv3]
    )
    runner_context = RunnerContext.from_runner(DummyRunner(test_vars.app))
    test_vars.app.orchestrator.set_invocation_run(
        test_vars.inv1, test_vars.inv2, runner_context
    )
    test_vars.app.orchestrator.set_invocation_run(
        test_vars.inv2, test_vars.inv3, runner_context
    )
    # it should avoid the cycle between inv1 -> inv2 -> inv3 -> inv1
    with pytest.raises(CycleDetectedError):
        test_vars.app.orchestrator.add_call_and_check_cycles(
            test_vars.inv3, test_vars.inv1
        )
    # if inv2 finished, and gets cleaned up
    test_vars.app.orchestrator.set_invocations_status(
        [test_vars.inv2.invocation_id], InvocationStatus.SUCCESS
    )
    test_vars.app.orchestrator.clean_up_invocation_cycles(test_vars.inv2.invocation_id)
    # it should not raise an exception
    test_vars.app.orchestrator.add_call_and_check_cycles(test_vars.inv3, test_vars.inv1)


def test_get_waiting_for_results(test_vars: Vars) -> None:
    """Test that it will return the invocation waiting for the result"""
    # test_vars.app.broker = MemBroker(test_vars.app)
    test_vars.app.orchestrator.register_new_invocations(
        [test_vars.inv1, test_vars.inv2, test_vars.inv3]
    )
    # add waiting for result
    test_vars.app.orchestrator.waiting_for_results(
        test_vars.inv1.invocation_id, [test_vars.inv2.invocation_id]
    )
    test_vars.app.orchestrator.waiting_for_results(
        test_vars.inv2.invocation_id, [test_vars.inv3.invocation_id]
    )
    test_vars.app.orchestrator.waiting_for_results(
        test_vars.inv1.invocation_id, [test_vars.inv3.invocation_id]
    )
    # get invocations to run
    inv_to_run = list(test_vars.app.orchestrator.get_blocking_invocations(3))
    # should get invocations that are not waiting in anybody
    # the order will be by age, the oldes invocation that are not waiting, first
    assert inv_to_run == [test_vars.inv3.invocation_id]


def test_avoid_getting_always_same_invocations(test_vars: Vars) -> None:
    """If we have 1 blocking invocation, and 10 workers requesting 1 invocation
    we should avoid that all these workers get the same invocation
    """
    test_vars.app.orchestrator.register_new_invocations(
        [test_vars.inv1, test_vars.inv2, test_vars.inv3]
    )
    test_vars.app.orchestrator.waiting_for_results(
        test_vars.inv1.invocation_id, [test_vars.inv3.invocation_id]
    )
    test_vars.inv3.app.conf.max_pending_seconds = 10
    # when we call get_blocking_invocations it will get inv3 as its blocking inv3
    # this call does not change the status of inv3
    blocking_inv = list(test_vars.app.orchestrator.get_blocking_invocations(1))
    # when we instead call get_invocation_to_run it will get inv3 as its blocking inv3
    # but this call will change the status of inv3 to pending
    inv_to_run = list(test_vars.app.orchestrator.get_invocations_to_run(1))
    inv_to_run_ids = [inv.invocation_id for inv in inv_to_run]
    sleep(0.1)  # sleep as the pending status is async
    assert blocking_inv == inv_to_run_ids == [test_vars.inv3.invocation_id]


def test_clean_up_blocker(test_vars: Vars) -> None:
    test_vars.app.orchestrator.register_new_invocations(
        [test_vars.inv1, test_vars.inv2, test_vars.inv3]
    )
    # add waiting for result (inv1 -> inv2 -> inv3)
    test_vars.app.orchestrator.waiting_for_results(
        test_vars.inv1.invocation_id, [test_vars.inv2.invocation_id]
    )
    test_vars.app.orchestrator.waiting_for_results(
        test_vars.inv2.invocation_id, [test_vars.inv3.invocation_id]
    )
    # get invocations to run
    # we try to get 3, but only inv3 is not waiting in anybody else
    inv_to_run = list(test_vars.app.orchestrator.get_blocking_invocations(3))
    assert inv_to_run == [test_vars.inv3.invocation_id]
    # now, after inv3 succeed, we remove it from the blockers
    test_vars.app.orchestrator.release_waiters(test_vars.inv3.invocation_id)
    # it should return inv2
    inv_to_run = list(test_vars.app.orchestrator.get_blocking_invocations(3))
    assert inv_to_run == [test_vars.inv2.invocation_id]
    # if inv2 succeed, and it is removed from the blockers
    test_vars.app.orchestrator.release_waiters(test_vars.inv2.invocation_id)
    # nothing should remove, as inv1 is not blocking anybody
    # the runner will get invocations from the broker
    inv_to_run = list(test_vars.app.orchestrator.get_blocking_invocations(3))
    assert inv_to_run == []


def test_auto_purge(test_vars: Vars) -> None:
    """Test that it will auto purge invocations in final state"""
    test_vars.app.orchestrator.register_new_invocations([test_vars.inv1])

    def get_invocations() -> list[str]:
        return list(
            test_vars.app.orchestrator.get_existing_invocations(
                task=test_vars.inv1.task
            )
        )

    assert get_invocations() == [test_vars.inv1.invocation_id]
    # we mark the inv1 to auto_purge it
    # but the auto_purge is set to 24 hours
    test_vars.app.orchestrator.conf.auto_final_invocation_purge_hours = 24.0
    test_vars.app.orchestrator.set_up_invocation_auto_purge(
        test_vars.inv1.invocation_id
    )
    # auto_purge should not purge it
    test_vars.app.orchestrator.auto_purge()
    assert get_invocations() == [test_vars.inv1.invocation_id]
    # if we change auto_purge to 0
    test_vars.app.orchestrator.conf.auto_final_invocation_purge_hours = 0.0
    # auto_purge should purge it
    test_vars.app.orchestrator.auto_purge()
    assert get_invocations() == []


def test_config_cycle_control(
    test_vars: Vars, mock_register_task_run: MagicMock
) -> None:
    test_vars.app.orchestrator.conf.cycle_control = True
    test_vars.app.orchestrator.register_new_invocations(
        [test_vars.inv1, test_vars.inv2, test_vars.inv3]
    )
    runner_context = RunnerContext.from_runner(DummyRunner(test_vars.app))
    test_vars.app.orchestrator.set_invocation_run(
        test_vars.inv1, test_vars.inv2, runner_context
    )
    test_vars.app.orchestrator.set_invocation_run(
        test_vars.inv2, test_vars.inv3, runner_context
    )
    # it should avoid the cycle between inv1 -> inv2 -> inv3 -> inv1

    with pytest.raises(CycleDetectedError):
        test_vars.app.orchestrator.add_call_and_check_cycles(
            test_vars.inv3, test_vars.inv1
        )
        print(test_vars.app)
    # test it will not check for cycles when cycle_control is disabled
    test_vars.app.orchestrator.conf.cycle_control = False
    test_vars.app.orchestrator.add_call_and_check_cycles(test_vars.inv3, test_vars.inv1)


def test_config_blocking_control(test_vars: Vars) -> None:
    test_vars.app.orchestrator.conf.blocking_control = True
    test_vars.app.orchestrator.register_new_invocations(
        [test_vars.inv1, test_vars.inv2, test_vars.inv3]
    )
    # add waiting for result
    test_vars.app.orchestrator.waiting_for_results(
        test_vars.inv1.invocation_id, [test_vars.inv2.invocation_id]
    )
    # test_vars.app.orchestrator.waiting_for_results(test_vars.inv2, [test_vars.inv3])
    # get invocations to run

    inv_to_run = list(test_vars.app.orchestrator.get_blocking_invocations(3))
    inv_to_run_ids = list(inv_to_run)
    same_inv = list(test_vars.app.orchestrator.get_blocking_invocations(3))
    same_inv_ids = list(same_inv)
    assert inv_to_run_ids == same_inv_ids == [test_vars.inv2.invocation_id]
    # test it will not check for blocking invocations when blocking_control is disabled
    test_vars.app.orchestrator.conf.blocking_control = False
    inv_to_run = list(test_vars.app.orchestrator.get_blocking_invocations(3))
    assert inv_to_run == []
