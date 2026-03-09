from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from pynenc.client_data_store import MemClientDataStore
from pynenc.call import Call, CallId
from pynenc.conf.config_task import ConcurrencyControlType
from pynenc.serializer.constants import ReservedKeys
from pynenc.identifiers.task_id import TaskId

if TYPE_CHECKING:
    pass


@pytest.fixture
def mock_task(call_id: "CallId") -> MagicMock:
    task = MagicMock()
    task.task_id = call_id.task_id
    task.app = MagicMock()
    return task


@pytest.fixture
def mock_arguments(call_id: "CallId") -> MagicMock:
    arguments = MagicMock()
    arguments.kwargs = {"arg1": "value1", "arg2": 2}
    arguments.args_id = call_id.args_id
    return arguments


@pytest.fixture
def call(mock_task: MagicMock, mock_arguments: MagicMock) -> Call:
    return Call(task=mock_task, arguments=mock_arguments)


def test_call_id(call: Call) -> None:
    """Test that call_id is based in task_id and args_id"""
    assert call.task.task_id == call.call_id.task_id
    assert call.args_id == call.call_id.args_id


def test_serialized_arguments_with_caching(call: Call) -> None:
    """Test that large arguments are cached while small ones are directly serialized."""
    # Setup
    call.app.config_values = None
    call.app.config_filepath = None
    call.app.client_data_store = MemClientDataStore(call.app)  # type: ignore[misc]

    # Create arguments of different sizes
    call.arguments.kwargs = {
        "small_arg": "small",
        "large_arg": "x"
        * (
            call.app.client_data_store.conf.min_size_to_cache + 100
        ),  # Exceeds cache threshold
    }

    # Setup serializer to return input unchanged
    call.app.serializer.serialize = lambda obj: obj  # type: ignore

    # Get serialized arguments
    serialized = call.serialized_arguments

    # Small argument should not be cached
    assert serialized["small_arg"] == "small"
    assert not call.app.client_data_store.is_reference(serialized["small_arg"])
    # Large argument should contain an arg cache key
    assert ReservedKeys.CLIENT_DATA.value in serialized["large_arg"]

    # Large argument should be cached
    assert call.app.client_data_store.is_reference(serialized["large_arg"])
    # Verify the large value was stored in cache
    cached_value = call.app.client_data_store._retrieve(serialized["large_arg"])
    assert cached_value == "x" * (
        call.app.client_data_store.conf.min_size_to_cache + 100
    )


def test_equality_and_hash(mock_task: MagicMock, mock_arguments: MagicMock) -> None:
    call1: Call = Call(task=mock_task, arguments=mock_arguments)
    call2: Call = Call(task=mock_task, arguments=mock_arguments)
    # Ensure deterministic args_id for equality/hash (mock Arguments may not
    # provide the behavior Call expects), set private _args_id from fixture.
    call1._args_id = mock_arguments.args_id
    call2._args_id = mock_arguments.args_id
    assert call1 == call2
    assert hash(call1) == hash(call2)

    different_task = MagicMock()
    different_task.task_id = TaskId("different_task_module", "different_func_name")
    call3: Call = Call(task=different_task, arguments=mock_arguments)
    call3._args_id = mock_arguments.args_id
    assert call1 != call3
    assert hash(call1) != hash(call3)

    different_arguments = MagicMock()
    different_arguments.args_id = "different_args"
    call4: Call = Call(task=mock_task, arguments=different_arguments)
    call4._args_id = different_arguments.args_id
    assert call1 != call4
    assert hash(call1) != hash(call4)

    assert call1 != "not a call"


@pytest.mark.parametrize(
    "concurrency_type",
    [
        ConcurrencyControlType.DISABLED,
        ConcurrencyControlType.TASK,
        "UNKNOWN_TYPE",  # Test for unhandled concurrency type
    ],
)
def test_serialized_args_for_concurrency_check_returns_none(
    mock_task: MagicMock,
    mock_arguments: MagicMock,
    concurrency_type: ConcurrencyControlType,
) -> None:
    """Test that serialized_args_for_concurrency_check returns None for DISABLED and TASK types."""
    # Configure mock task
    mock_task.conf.registration_concurrency = concurrency_type
    mock_task.conf.key_arguments = []

    call: Call = Call(task=mock_task, arguments=mock_arguments)

    assert call.serialized_args_for_concurrency_check is None


# def test_call_getstate(mock_task: MagicMock, mock_arguments: MagicMock) -> None:
#     """Test that __getstate__ correctly serializes the call object."""
#     call: Call = Call(task=mock_task, arguments=mock_arguments)

#     # Expected state should contain the task and the serialized arguments
#     expected_state = {
#         "task": mock_task,
#         "arguments": call.serialized_arguments,
#     }

#     assert call.__getstate__() == expected_state
