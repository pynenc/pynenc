from typing import TYPE_CHECKING, Any

import pytest

from pynenc import PynencBuilder
from pynenc.arguments import Arguments
from pynenc.call import PreSerializedCall
from pynenc.serializer import PickleSerializer

if TYPE_CHECKING:
    pass


app = PynencBuilder().serializer_pickle().build()


@app.task
def task_mult(x: int, y: str) -> str:
    return y * x


# Helper function to create a PreSerializedCall instance
def create_pre_serialized_call(
    pre_serialized_args: dict[str, str],
    other_args: dict[str, Any],
) -> PreSerializedCall:
    """Helper to create a PreSerializedCall with consistent parameters"""
    return PreSerializedCall(
        task=task_mult,
        other_args=other_args,
        pre_serialized_args=pre_serialized_args,
    )


# Test functions remain the same...
def test_preserialized_call_creation() -> None:
    """Test that PreSerializedCall is created properly with pre-serialized arguments."""
    # Create call with pre-serialized and unique arguments
    pre_serialized_args = {
        "common_arg1": "serialized:value1",
        "common_arg2": "serialized:value2",
    }
    other_args = {"unique_arg1": "unique1", "unique_arg2": 42}

    call = create_pre_serialized_call(pre_serialized_args, other_args)

    # Check that the call properties are set correctly
    assert call.task == task_mult
    assert call.other_args == other_args
    assert call.pre_serialized_args == pre_serialized_args


def test_preserialized_call_serialized_arguments() -> None:
    """Test that serialized_arguments combines both pre-serialized and other arguments."""
    pre_serialized_args = {
        "common_arg1": PickleSerializer.serialize("value1"),
        "common_arg2": PickleSerializer.serialize("value1"),
    }
    other_args = {"unique_arg1": "unique1", "unique_arg2": 42}

    call = create_pre_serialized_call(pre_serialized_args, other_args)

    # Get serialized arguments
    serialized = call.serialized_arguments

    # Check pre-serialized args were preserved
    assert serialized["common_arg1"] == pre_serialized_args["common_arg1"]
    assert serialized["common_arg2"] == pre_serialized_args["common_arg2"]

    # Check other args were serialized
    assert serialized["unique_arg1"] == PickleSerializer.serialize("unique1")
    assert serialized["unique_arg2"] == PickleSerializer.serialize(42)


def test_preserialized_call_arguments() -> None:
    """Test that the arguments property deserializes correctly."""
    pre_serialized_args = {
        "common_arg1": PickleSerializer.serialize("value1"),
        "common_arg2": PickleSerializer.serialize("value2"),
    }
    other_args = {"unique_arg1": "unique1", "unique_arg2": 42}

    call = create_pre_serialized_call(pre_serialized_args, other_args)

    # Get deserialized arguments
    args = call.arguments

    # Verify all arguments were correctly deserialized
    assert args.kwargs["common_arg1"] == "value1"
    assert args.kwargs["common_arg2"] == "value2"
    assert args.kwargs["unique_arg1"] == "unique1"
    assert args.kwargs["unique_arg2"] == 42


def test_preserialized_call_getstate_setstate() -> None:
    """Test that __getstate__ and __setstate__ work correctly."""
    pre_serialized_args = {
        "common_arg1": PickleSerializer.serialize("value1"),
        "common_arg2": PickleSerializer.serialize("value2"),
    }
    other_args = {"unique_arg1": "unique1", "unique_arg2": 42}

    call = create_pre_serialized_call(pre_serialized_args, other_args)

    # Get state
    state = call.__getstate__()

    # Verify state contains expected keys
    assert "task" in state
    assert "other_args" in state
    assert "pre_serialized_args" in state

    # Verify state content
    assert state["task"] == task_mult
    assert "unique_arg1" in state["other_args"]
    assert "unique_arg2" in state["other_args"]
    assert state["pre_serialized_args"] == pre_serialized_args

    # Create a new call and set state
    new_call: PreSerializedCall = PreSerializedCall(task=task_mult)
    new_call.__setstate__(state)

    # Verify new call has the same properties
    assert new_call.task == call.task
    assert new_call.other_args["unique_arg1"] == "unique1"
    assert new_call.other_args["unique_arg2"] == 42
    assert new_call.pre_serialized_args == call.pre_serialized_args


def test_preserialized_call_with_arguments_object_fails() -> None:
    """Test that using an Arguments object instead of a dict for other_args fails."""
    pre_serialized_args = {"common_arg1": PickleSerializer.serialize("value1")}
    other_args = Arguments(
        {"unique_arg1": "unique1"}
    )  # This is wrong - should be a dict

    call = create_pre_serialized_call(pre_serialized_args, other_args)  # type: ignore

    # This should raise a TypeError or AttributeError when we try to get state
    with pytest.raises((TypeError, AttributeError)):
        call.__getstate__()


def test_preserialized_call_to_regular_call() -> None:
    """Test converting PreSerializedCall to a regular Call."""
    pre_serialized_args = {
        "common_arg1": PickleSerializer.serialize("value1"),
        "common_arg2": PickleSerializer.serialize("value2"),
    }
    other_args = {"unique_arg1": "unique1", "unique_arg2": 42}

    call = create_pre_serialized_call(pre_serialized_args, other_args)

    # Access the regular call
    regular_call = call.call

    # Verify regular call has same arguments
    assert "common_arg1" in regular_call.arguments.kwargs
    assert "common_arg2" in regular_call.arguments.kwargs
    assert "unique_arg1" in regular_call.arguments.kwargs
    assert "unique_arg2" in regular_call.arguments.kwargs

    # Verify values were correctly deserialized
    assert regular_call.arguments.kwargs["common_arg1"] == "value1"
    assert regular_call.arguments.kwargs["common_arg2"] == "value2"
    assert regular_call.arguments.kwargs["unique_arg1"] == "unique1"
    assert regular_call.arguments.kwargs["unique_arg2"] == 42


def test_preserialized_call_unsupported_methods() -> None:
    """Test that unsupported methods raise NotImplementedError."""
    pre_serialized_args = {"arg2": PickleSerializer.serialize("val2")}
    other_args = {"arg1": "val1"}

    call = create_pre_serialized_call(pre_serialized_args, other_args)

    # Test hash
    with pytest.raises(NotImplementedError):
        hash(call)

    # Test serialized_args_for_concurrency_check
    with pytest.raises(NotImplementedError):
        _ = call.serialized_args_for_concurrency_check

    # Test from_json
    with pytest.raises(NotImplementedError):
        PreSerializedCall.from_json(app, "{}")
