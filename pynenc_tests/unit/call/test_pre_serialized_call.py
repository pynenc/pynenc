from typing import TYPE_CHECKING

import pytest

from pynenc import PynencBuilder
from pynenc.call import PreSerializedCall

if TYPE_CHECKING:
    pass


app = PynencBuilder().serializer_pickle().build()


@app.task
def task_mult(x: int, y: str) -> str:
    return y * x


@pytest.fixture
def all_arg_keys() -> set[str]:
    return {"common_arg1", "common_arg2", "unique_arg1", "unique_arg2"}


@pytest.fixture
def pre_serialized_call(all_arg_keys: set[str]) -> PreSerializedCall:
    """Helper to create a PreSerializedCall with consistent parameters"""
    other_args = {"unique_arg1": "unique1", "unique_arg2": 42}
    common_args = {
        "common_arg1": "value1",
        "common_arg2": "value2",
    }
    common_serialized_args = {
        "common_arg1": "serialized:value1",
        "common_arg2": "serialized:value2",
    }
    # Sanity check to ensure our test data is consistent
    assert set(common_args.keys()) == set(common_serialized_args.keys())
    assert all_arg_keys == set(common_serialized_args.keys()).union(other_args.keys())

    return PreSerializedCall(
        task=task_mult,
        other_args=other_args,
        common_args=common_args,
        common_serialized_args=common_serialized_args,
    )


def test_preserialized_call_contains_all_args(
    pre_serialized_call: PreSerializedCall, all_arg_keys: set[str]
) -> None:
    """Test that the PreSerializedCall contains all expected argument keys in its various properties."""
    call = pre_serialized_call
    assert set(call.serialized_arguments.keys()) == all_arg_keys
    assert set(call.arguments.kwargs.keys()) == all_arg_keys
    assert set(call.serialized_arguments.keys()) == all_arg_keys
    assert set(call.to_dto().serialized_arguments.keys()) == all_arg_keys
    assert call.arg_keys == all_arg_keys


def test_preserialized_call_arguments_values(
    pre_serialized_call: PreSerializedCall,
) -> None:
    """Test that the arguments property deserializes correctly."""
    # Get deserialized arguments
    kwargs = pre_serialized_call.arguments.kwargs

    # Verify all arguments were correctly deserialized
    assert kwargs["common_arg1"] == "value1"
    assert kwargs["common_arg2"] == "value2"
    assert kwargs["unique_arg1"] == "unique1"
    assert kwargs["unique_arg2"] == 42


def test_preserialized_call_serialized_arguments_values(
    pre_serialized_call: PreSerializedCall,
) -> None:
    """Test that the serialized_arguments property contains the correct serialized values."""
    serialized_args = pre_serialized_call.serialized_arguments

    # Verify common args are from common_serialized_args
    assert serialized_args["common_arg1"] == "serialized:value1"
    assert serialized_args["common_arg2"] == "serialized:value2"

    # Verify unique args are correctly serialized
    assert serialized_args[
        "unique_arg1"
    ] == pre_serialized_call.app.serializer.serialize("unique1")
    assert serialized_args[
        "unique_arg2"
    ] == pre_serialized_call.app.serializer.serialize(42)
