import json

from pynenc.exceptions import TaskError


def test_task_error_initialization() -> None:
    error = TaskError("task123", "An error occurred")
    assert error.task_id == "task123"
    assert error.message == "An error occurred"


def test_task_error_string_representation() -> None:
    error = TaskError("task123", "An error occurred")
    assert str(error) == "TaskError(task123): An error occurred"
    error_no_message = TaskError("task123")
    assert str(error_no_message) == "TaskError(task123)"


def test_task_error_to_json_dict() -> None:
    error = TaskError("task123", "An error occurred")
    expected_dict = {"task_id": "task123", "message": "An error occurred"}
    assert error._to_json_dict() == expected_dict


def test_task_error_from_json_dict() -> None:
    json_dict = {"task_id": "task123", "message": "An error occurred"}
    error = TaskError._from_json_dict(json_dict)
    assert isinstance(error, TaskError)
    assert error.task_id == "task123"
    assert error.message == "An error occurred"


def test_task_error_to_json() -> None:
    error = TaskError("task123", "An error occurred")
    expected_json = json.dumps({"task_id": "task123", "message": "An error occurred"})
    assert error.to_json() == expected_json


def test_task_error_from_json() -> None:
    serialized = json.dumps({"task_id": "task123", "message": "An error occurred"})
    error = TaskError.from_json("TaskError", serialized)
    assert isinstance(error, TaskError)
    assert error.task_id == "task123"
    assert error.message == "An error occurred"
