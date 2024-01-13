from unittest.mock import MagicMock

import pytest

from pynenc.call import Call


@pytest.fixture
def mock_task() -> MagicMock:
    task = MagicMock()
    task.task_id = "test_task"
    task.app = MagicMock()
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
    call.app.serializer.serialize = lambda obj: obj  # type: ignore
    assert call.serialized_arguments == mock_arguments.kwargs
    call: Call = Call(task=mock_task, arguments=mock_arguments)
    call.app.serializer.serialize = lambda obj: "0"  # type: ignore
    assert call.serialized_arguments == {k: 0 for k in mock_arguments.kwargs}  # type: ignore


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
