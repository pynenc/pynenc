from unittest.mock import MagicMock

import pytest

from pynenc.arg_cache import DisabledArgCache, MemArgCache
from pynenc.call import Call
from pynenc.conf.config_task import ConcurrencyControlType
from pynenc.serializer.constants import ReservedKeys


@pytest.fixture
def mock_task() -> MagicMock:
    task = MagicMock()
    task.task_id = "test_task"
    task.app = MagicMock()
    task.app.runner.cache = {}
    task.to_json.return_value = '{"task_id": "' + task.task_id + '"}'
    return task


@pytest.fixture
def mock_arguments() -> MagicMock:
    arguments = MagicMock()
    arguments.kwargs = {"arg1": "value1", "arg2": 2}
    arguments.args_id = "args_" + "_".join(
        f"{k}_{v}" for k, v in arguments.kwargs.items()
    )
    return arguments


def test_call_id(mock_task: MagicMock, mock_arguments: MagicMock) -> None:
    """Test that call_id is based in task_id and args_id"""
    call: Call = Call(task=mock_task, arguments=mock_arguments)
    assert mock_task.task_id in call.call_id
    assert mock_arguments.args_id in call.call_id


def test_serialized_arguments(mock_task: MagicMock, mock_arguments: MagicMock) -> None:
    """Test that it will call the serializer for each argument"""
    call: Call = Call(task=mock_task, arguments=mock_arguments)
    call.app.arg_cache = DisabledArgCache(call.app)
    call.app.serializer.serialize = lambda obj: obj  # type: ignore
    assert call.serialized_arguments == mock_arguments.kwargs
    call: Call = Call(task=mock_task, arguments=mock_arguments)
    call.app.serializer.serialize = lambda obj: "0"  # type: ignore
    assert call.serialized_arguments == {k: "0" for k in mock_arguments.kwargs}  # type: ignore


def test_serialized_arguments_with_caching(
    mock_task: MagicMock, mock_arguments: MagicMock
) -> None:
    """Test that large arguments are cached while small ones are directly serialized."""
    # Setup
    call: Call = Call(task=mock_task, arguments=mock_arguments)
    call.app.config_values = None
    call.app.config_filepath = None
    call.app.arg_cache = MemArgCache(call.app)

    # Create arguments of different sizes
    mock_arguments.kwargs = {
        "small_arg": "small",
        "large_arg": "x"
        * (call.app.arg_cache.conf.min_size_to_cache + 100),  # Exceeds cache threshold
    }

    # Setup serializer to return input unchanged
    call.app.serializer.serialize = lambda obj: obj  # type: ignore

    # Get serialized arguments
    serialized = call.serialized_arguments

    # Small argument should not be cached
    assert serialized["small_arg"] == "small"
    assert not call.app.arg_cache.is_cache_key(serialized["small_arg"])
    # Large argument should contain an arg cache key
    assert ReservedKeys.ARG_CACHE.value in serialized["large_arg"]

    # Large argument should be cached
    assert call.app.arg_cache.is_cache_key(serialized["large_arg"])
    # Verify the large value was stored in cache
    cached_value = call.app.arg_cache._retrieve(serialized["large_arg"])
    assert cached_value == "x" * (call.app.arg_cache.conf.min_size_to_cache + 100)


def test_equality_and_hash(mock_task: MagicMock, mock_arguments: MagicMock) -> None:
    mock_task.task_id = "test_task"
    mock_arguments.args_id = "test_args"
    call1: Call = Call(task=mock_task, arguments=mock_arguments)
    call2: Call = Call(task=mock_task, arguments=mock_arguments)
    assert call1 == call2
    assert hash(call1) == hash(call2)

    different_task = MagicMock()
    different_task.task_id = "different_task"
    call3: Call = Call(task=different_task, arguments=mock_arguments)
    assert call1 != call3
    assert hash(call1) != hash(call3)

    different_arguments = MagicMock()
    different_arguments.args_id = "different_args"
    call4: Call = Call(task=mock_task, arguments=different_arguments)
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
