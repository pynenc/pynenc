from unittest.mock import Mock

from pynenc.invocation import ConcurrentInvocation, ConcurrentInvocationGroup

mock_call = Mock()
mock_task = Mock()


# Tests for BaseInvocation
def test_base_invocation_instantiation() -> None:
    invocation: ConcurrentInvocation = ConcurrentInvocation(call=mock_call)
    assert invocation.call == mock_call


def test_base_invocation_properties() -> None:
    invocation: ConcurrentInvocation = ConcurrentInvocation(call=mock_call)
    assert invocation.app == mock_call.app
    assert invocation.call_id == mock_call.call_id
    assert invocation.task == mock_call.task
    assert invocation.arguments == mock_call.arguments
    assert invocation.serialized_arguments == mock_call.serialized_arguments


def test_invocation_id_is_unique() -> None:
    """Test that invocation ids are unique per each invocation"""
    invocation: ConcurrentInvocation = ConcurrentInvocation(call=mock_call)
    assert invocation.invocation_id != mock_call.call_id
    assert (
        invocation.invocation_id != ConcurrentInvocation(call=mock_call).invocation_id
    )


def test_base_invocation_str_repr() -> None:
    invocation: ConcurrentInvocation = ConcurrentInvocation(call=mock_call)
    str_repr = str(invocation)
    repr_repr = repr(invocation)
    assert str_repr == repr_repr
    assert invocation.invocation_id in str_repr


def test_base_invocation_equality() -> None:
    invocation: ConcurrentInvocation = ConcurrentInvocation(call=mock_call)
    other_invocation: ConcurrentInvocation = ConcurrentInvocation(call=mock_call)
    assert invocation != other_invocation
    assert invocation == invocation
    assert invocation != "not an invocation"


# Tests for BaseInvocationGroup
def test_base_invocation_group_instantiation() -> None:
    group: ConcurrentInvocationGroup = ConcurrentInvocationGroup(
        task=mock_task, invocations=[ConcurrentInvocation(call=mock_call)]
    )
    assert group.task == mock_task
    assert len(group.invocations) == 1


def test_base_invocation_group_properties() -> None:
    group: ConcurrentInvocationGroup = ConcurrentInvocationGroup(
        task=mock_task, invocations=[ConcurrentInvocation(call=mock_call)]
    )
    assert group.app == mock_task.app


def test_base_invocation_group_iter() -> None:
    invocations: list[ConcurrentInvocation] = [
        ConcurrentInvocation(call=mock_call) for _ in range(2)
    ]
    concrete_invocation_group: ConcurrentInvocationGroup = ConcurrentInvocationGroup(
        task=mock_task, invocations=invocations
    )
    assert invocations == list(concrete_invocation_group)
