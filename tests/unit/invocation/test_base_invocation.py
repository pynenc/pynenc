from typing import Any
from unittest.mock import Mock

from pynenc.invocation import BaseInvocation, BaseInvocationGroup

mock_call = Mock()
mock_task = Mock()


# Concrete classes for testing
class ConcreteInvocation(BaseInvocation):
    def to_json(self) -> Any:
        raise NotImplementedError("Not implemented")

    @classmethod
    def from_json(cls, app: Any, serialized: Any) -> Any:
        del app, serialized
        raise NotImplementedError("Not implemented")

    @property
    def status(self) -> Any:
        raise NotImplementedError("Not implemented")

    @property
    def result(self) -> Any:
        raise NotImplementedError("Not implemented")

    def async_result(self) -> Any:
        raise NotImplementedError("Not implemented")

    @property
    def num_retries(self) -> Any:
        raise NotImplementedError("Not implemented")


class ConcreteInvocationGroup(BaseInvocationGroup):
    @property
    def results(self) -> Any:
        raise NotImplementedError("Not implemented")

    async def async_results(self) -> Any:
        if False:
            yield  # type: ignore
        raise NotImplementedError("Not implemented")


# Tests for BaseInvocation
def test_base_invocation_instantiation() -> None:
    invocation = ConcreteInvocation(call=mock_call)
    assert invocation.call == mock_call


def test_base_invocation_properties() -> None:
    invocation = ConcreteInvocation(call=mock_call)
    assert invocation.app == mock_call.app
    assert invocation.call_id == mock_call.call_id
    assert invocation.task == mock_call.task
    assert invocation.arguments == mock_call.arguments
    assert invocation.serialized_arguments == mock_call.serialized_arguments


def test_invocation_id_is_unique() -> None:
    """Test that invocation ids are unique per each invocation"""
    invocation = ConcreteInvocation(call=mock_call)
    assert invocation.invocation_id != mock_call.call_id
    assert invocation.invocation_id != ConcreteInvocation(call=mock_call).invocation_id


def test_base_invocation_str_repr() -> None:
    invocation = ConcreteInvocation(call=mock_call)
    str_repr = str(invocation)
    repr_repr = repr(invocation)
    assert str_repr == repr_repr
    assert invocation.invocation_id in str_repr


def test_base_invocation_equality() -> None:
    invocation = ConcreteInvocation(call=mock_call)
    other_invocation = ConcreteInvocation(call=mock_call)
    assert invocation != other_invocation
    assert invocation == invocation
    assert invocation != "not an invocation"


# Tests for BaseInvocationGroup
def test_base_invocation_group_instantiation() -> None:
    group = ConcreteInvocationGroup(
        task=mock_task, invocations=[ConcreteInvocation(call=mock_call)]
    )
    assert group.task == mock_task
    assert len(group.invocations) == 1


def test_base_invocation_group_properties() -> None:
    group = ConcreteInvocationGroup(
        task=mock_task, invocations=[ConcreteInvocation(call=mock_call)]
    )
    assert group.app == mock_task.app


def test_base_invocation_group_iter() -> None:
    invocations = [ConcreteInvocation(call=mock_call) for _ in range(2)]
    concrete_invocation_group = ConcreteInvocationGroup(
        task=mock_task, invocations=invocations
    )
    assert invocations == list(concrete_invocation_group)
