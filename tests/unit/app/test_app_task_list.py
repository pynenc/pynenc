import pytest

from pynenc import Pynenc

app = Pynenc(app_id="task_registry_test")


# Define and register tasks
@app.task
def task1(x: int, y: int) -> int:
    return x + y


@app.task
def task2(x: int) -> int:
    return x * 2


def test_task_registry() -> None:
    """Test that tasks are properly registered in the app's task registry."""

    # Check that tasks are in the registry
    assert len(app.tasks) == 2
    assert task1.task_id in app.tasks
    assert task2.task_id in app.tasks

    # Check that get_task works correctly
    assert app.get_task(task1.task_id) is task1
    assert app.get_task(task2.task_id) is task2
    assert app.get_task("nonexistent_task") is None


app_ser = Pynenc(app_id="serialization_test")


@app_ser.task
def sample_task(x: int) -> int:
    return x * 2


def test_task_registry_serialization() -> None:
    """Test that the task registry is properly serialized and deserialized."""

    pytest.skip("Disable task serialization for now")  # TODO

    # Get the state that would be serialized
    state = app_ser.__getstate__()

    # Verify it contains the tasks
    assert "tasks" in state
    assert sample_task.task_id in state["tasks"]

    # Create a new app and restore the state
    new_app = Pynenc()
    new_app.__setstate__(state)

    # Verify the tasks are restored
    assert len(new_app.tasks) == 1
    assert sample_task.task_id in new_app.tasks

    # The task reference should work but the object is not the same
    # as task instances are recreated during deserialization
    restored_task = new_app.get_task(sample_task.task_id)
    assert restored_task is not None
    assert restored_task.task_id == sample_task.task_id
