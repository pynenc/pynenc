import json
from typing import TYPE_CHECKING

from pynenc.exceptions import TaskError


if TYPE_CHECKING:
    from pynenc.identifiers.task_id import TaskId


def test_task_error_initialization(task_id: "TaskId") -> None:
    error = TaskError(task_id, "An error occurred")
    assert error.task_id == task_id
    assert error.message == "An error occurred"


def test_task_error_string_representation(task_id: "TaskId") -> None:
    error = TaskError(task_id, "An error occurred")
    assert str(error) == f"TaskError({task_id}): An error occurred"
    error_no_message = TaskError(task_id)
    assert str(error_no_message) == f"TaskError({task_id})"


def test_task_error_to_json_dict(task_id: "TaskId") -> None:
    error = TaskError(task_id, "An error occurred")
    expected_dict = {"task_id_key": task_id.key, "message": "An error occurred"}
    assert error._to_json_dict() == expected_dict


def test_task_error_from_json_dict(task_id: "TaskId") -> None:
    json_dict = {"task_id_key": task_id.key, "message": "An error occurred"}
    error = TaskError._from_json_dict(json_dict)
    assert isinstance(error, TaskError)
    assert error.task_id == task_id
    assert error.message == "An error occurred"


def test_task_error_to_json(task_id: "TaskId") -> None:
    error = TaskError(task_id, "An error occurred")
    expected_json = json.dumps(
        {"task_id_key": task_id.key, "message": "An error occurred"}
    )
    assert error.to_json() == expected_json


def test_task_error_from_json(task_id: "TaskId") -> None:
    serialized = json.dumps(
        {"task_id_key": task_id.key, "message": "An error occurred"}
    )
    error = TaskError.from_json("TaskError", serialized)
    assert isinstance(error, TaskError)
    assert error.task_id == task_id
    assert error.message == "An error occurred"
