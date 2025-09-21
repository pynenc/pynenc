from typing import TYPE_CHECKING

import pytest

from pynenc import exceptions as exc
from pynenc.arguments import Arguments
from pynenc.conf import config_task
from pynenc.invocation import DistributedInvocation, ReusedInvocation

if TYPE_CHECKING:
    from pynenc.task import Task


def test_route_default(task_sum: "Task") -> None:
    """Test that the orchestrator will route the task by default

    If there are no options:
     - The orchestrator will forward the task to the broker
     - The broker should return a new Invocation and report the change of status to the orchestrator
    """
    app = task_sum.app
    actual_invocations = [task_sum(i, i).invocation_id for i in range(2)]
    # test that app.broker.route_invocation (MemBroker.route_invocation) has been called
    stored_invocations = list(app.orchestrator.get_existing_invocations(task=task_sum))
    assert set(actual_invocations) == set(stored_invocations)


def test_task_concurrency_control_raising(task_mirror: "Task") -> None:
    """Test the option `task.conf.registration_concurrency=ConcurrencyControlType.task`
    In that case will only route the task if do not exists a Registered instance
    It can only exists one pending instance for the task

    With on_diff_args_raise=True will raise an exception if the task arguments differ
    """
    task_mirror.conf.registration_concurrency = config_task.ConcurrencyControlType.TASK
    task_mirror.conf.on_diff_non_key_args_raise = True

    # Get existing invocation doesn't find any pending match
    first_invocation = task_mirror("0")
    # We are calling but the previous invocation is still at REGISTERED status
    # So the new invocation cannot run
    # But we cannot return the first_invocation because the arguments doesn't match
    # So it will return an exception with the different arguments
    # The user of the library should handle this (or add ignore option in Pynenc)
    with pytest.raises(exc.InvocationConcurrencyWithDifferentArgumentsError) as excinfo:
        _ = task_mirror("1")
    assert excinfo.value.task_id == task_mirror.task_id
    assert excinfo.value.existing_invocation_id == first_invocation.invocation_id
    assert excinfo.value.diff == (
        "==============================\n"
        "Differences for pynenc_tests.integration.orchestrator.orchestrator_tasks.dummy_mirror:\n"
        "==============================\n"
        "  * Original: {'arg': '0'}\n"
        "  * Updated: {'arg': '1'}\n"
        "------------------------------\n"
        "  * Changes: \n"
        "    - arg: 0 -> 1\n"
        "=============================="
    )
    # Trying with same arguments
    next_invocation = task_mirror("0")
    assert isinstance(first_invocation, DistributedInvocation)
    assert isinstance(next_invocation, ReusedInvocation)
    assert first_invocation.invocation_id == next_invocation.invocation_id
    assert first_invocation.arguments == next_invocation.arguments
    assert first_invocation.arguments.kwargs["arg"] == "0"
    assert next_invocation.diff_arg is None


def test_registration_concurrency_not_raising(task_mirror: "Task") -> None:
    """Test the option `task.options.registration_concurrency=InvocationConcurrency`
    In that case will only route the task if do not exists a Registered instance
    It can only exists one pending instance for the task

    With on_diff_args_raise=False will return an invocation with the diff_args
    """
    task_mirror.conf.registration_concurrency = config_task.ConcurrencyControlType.TASK
    task_mirror.conf.on_diff_non_key_args_raise = False

    # Get existing invocation doesn't find any pending match
    first_invocation = task_mirror("0")
    # In this test case, it will find the previous invocation with different arguments
    # But on_diff_args_raise is False, so it will return the Reused invocation
    # specifying previous invocation arguments and diff_args on the current call
    next_invocation = task_mirror("1")
    assert isinstance(first_invocation, DistributedInvocation)
    assert isinstance(next_invocation, ReusedInvocation)
    assert first_invocation.invocation_id == next_invocation.invocation_id
    assert first_invocation.arguments == next_invocation.arguments
    assert first_invocation.arguments.kwargs["arg"] == "0"
    assert isinstance(next_invocation.diff_arg, Arguments)
    assert next_invocation.diff_arg.kwargs["arg"] == "1"


def test_registration_concurrency_arguments(task_concat: "Task") -> None:
    """Test the option `task.options.registration_concurrency=InvocationConcurrencyPerArguments`
    In that case will only route the task if do not exists a Registered instance with the same arguments

    In this case on_diff_args_raise is not necessary
    (raise an exception if is not routing because exists an instance with different arguments)
    """
    task_concat.conf.registration_concurrency = (
        config_task.ConcurrencyControlType.ARGUMENTS
    )
    # Get existing invocation doesn't find any pending match
    inv_ab = task_concat("a", "b")
    inv_cd = task_concat("c", "d")
    assert inv_ab.invocation_id != inv_cd.invocation_id
    assert inv_ab.invocation_id == task_concat("a", "b").invocation_id
    assert inv_cd.invocation_id == task_concat("c", "d").invocation_id


def test_registration_concurrency_keys_raising(task_key_arg: "Task") -> None:
    """Test the option `task.options.registration_concurrency=InvocationConcurrencyPerArguments`
    In that case will only route the task if do not exists a Registered instance with the same key arguments

    With on_diff_args_raise=True will raise an exception if the task arguments differ
    """
    task_key_arg.conf.registration_concurrency = config_task.ConcurrencyControlType.KEYS
    task_key_arg.conf.key_arguments = ("key",)
    task_key_arg.conf.on_diff_non_key_args_raise = True
    # Get existing invocation doesn't find any pending match
    inv_k0 = task_key_arg("key0", "a")
    inv_k1 = task_key_arg("key1", "a")
    assert inv_k0.invocation_id != inv_k1.invocation_id
    # Finds invocation with same key but different arguments -> Exception
    #
    with pytest.raises(exc.InvocationConcurrencyWithDifferentArgumentsError) as excinfo:
        _ = task_key_arg("key0", "b")
    assert excinfo.value.task_id == task_key_arg.task_id
    assert excinfo.value.existing_invocation_id == inv_k0.invocation_id
    assert excinfo.value.diff == (
        "==============================\n"
        "Differences for pynenc_tests.integration.orchestrator.orchestrator_tasks.dummy_key_arg:\n"
        "==============================\n"
        "  * Original: {'key': 'key0', 'arg': 'a'}\n"
        "  * Updated: {'key': 'key0', 'arg': 'b'}\n"
        "------------------------------\n"
        "  * Changes: \n"
        "    - arg: a -> b\n"
        "=============================="
    )

    assert inv_k0.invocation_id == task_key_arg("key0", "a").invocation_id
    assert inv_k1.invocation_id == task_key_arg("key1", "a").invocation_id


def test_single_invocation_keys_not_raising(task_key_arg: "Task") -> None:
    """Test the option `task.options.single_invocation=InvocationConcurrencyPerArguments`
    In that case will only route the task if do not exists a Registered instance with the same key arguments

    With on_diff_args_raise=False will return an invocation with the diff_args
    """
    task_key_arg.conf.registration_concurrency = config_task.ConcurrencyControlType.KEYS
    task_key_arg.conf.key_arguments = ("key",)
    task_key_arg.conf.on_diff_non_key_args_raise = False
    # Get existing invocation doesn't find any pending match
    inv_k0 = task_key_arg("key0", "a")
    inv_k1 = task_key_arg("key1", "a")
    assert inv_k0.invocation_id != inv_k1.invocation_id
    # Finds invocation with same key but different arguments -> Exception
    #
    next_inv_k0 = task_key_arg("key0", "b")
    next_inv_k1 = task_key_arg("key1", "b")
    assert isinstance(next_inv_k0, ReusedInvocation)
    assert isinstance(next_inv_k1, ReusedInvocation)
    # Reuse invocations
    assert inv_k0.invocation_id == next_inv_k0.invocation_id
    assert inv_k1.invocation_id == next_inv_k1.invocation_id
    assert inv_k0.arguments == next_inv_k0.arguments
    assert inv_k1.arguments == next_inv_k1.arguments
    # with diff_args specified for the differences
    assert isinstance(next_inv_k0.diff_arg, Arguments)
    assert isinstance(next_inv_k1.diff_arg, Arguments)
    # keys are the same
    assert inv_k0.arguments.kwargs["key"] == next_inv_k0.diff_arg.kwargs["key"]
    assert inv_k1.arguments.kwargs["key"] == next_inv_k1.diff_arg.kwargs["key"]
    # not key arguments differ
    assert inv_k0.arguments.kwargs["arg"] != next_inv_k0.diff_arg.kwargs["arg"]
    assert inv_k1.arguments.kwargs["arg"] != next_inv_k1.diff_arg.kwargs["arg"]
