import pytest

from pynenc import Pynenc
from pynenc.identifiers.task_id import TaskId

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
    expected_task_id = TaskId("test_task_id", "add")
    assert add.task_id == expected_task_id
    assert add.func.__module__ == expected_task_id.module
    assert add.func.__name__ == expected_task_id.func_name


def test_pickle_task() -> None:
    """
    Test that a task can be serialized and deserialized using pickle (__getstate__/__setstate__)
    """
    import pickle

    # Serialize the task using pickle
    serialized_task = pickle.dumps(add)

    # Deserialize the task
    deserialized_task = pickle.loads(serialized_task)

    # Check if the deserialized task is the same as the original task
    assert deserialized_task.task_id == add.task_id
    assert deserialized_task.func == add.func
    assert deserialized_task.app.app_id == add.app.app_id
    assert deserialized_task.options == add.options


def test_error_on_main() -> None:
    """Test that the task will raise an error is it is defined in a module with __main__ name"""
    mock_func.__module__ = "__main__"
    with pytest.raises(RuntimeError) as exc_info:
        app.task(mock_func)
    assert (
        str(exc_info.value)
        == "Cannot create a task from a function in the __main__ module"
    )
