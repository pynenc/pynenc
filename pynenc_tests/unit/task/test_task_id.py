import pytest

from pynenc import Pynenc, Task

app = Pynenc()


@app.task
def add(x: int, y: int) -> int:
    return x + y


def mock_func() -> None:
    pass


def test_task_id() -> None:
    """
    Test that the task id is set correctly on task registration
    """
    assert add.task_id == "test_task_id.add"
    *module, func_name = add.task_id.split(".")
    assert add.func.__module__ == ".".join(module)
    assert add.func.__name__ == func_name


def test_deserialize_task() -> None:
    """
    Test that a task can be deserialized from a serialized task
    """
    serialized_task = add.to_json()
    assert serialized_task == '{"task_id": "test_task_id.add", "options": "{}"}'
    # Deserialize the task
    deserialized_task = Task.from_json(app, serialized_task)
    # Check if the deserialized task is the same as the original task
    assert deserialized_task.task_id == add.task_id
    assert deserialized_task.func == add.func


def test_error_on_main() -> None:
    """Test that the task will raise an error is it is defined in a module with __main__ name"""
    mock_func.__module__ = "__main__"
    with pytest.raises(RuntimeError) as exc_info:
        app.task(mock_func)
    assert (
        str(exc_info.value)
        == "Cannot create a task from a function in the __main__ module"
    )
