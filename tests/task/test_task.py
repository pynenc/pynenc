import pytest

from pynenc import Pynenc
from pynenc.task import Task


@pytest.fixture
def app() -> Pynenc:
    return Pynenc(task_cls=Task)


def test_instanciate_task_with_specific_args(app: Pynenc) -> None:
    """
    Test that the decorator arguments exists in the Task instance
    """

    @app.task(sub_task_option=3)
    def add(x: int, y: int) -> int:
        return x + y

    # I expect that function to become an instance of Task
    assert isinstance(add.options, Task.options_cls)
    assert add.options.sub_task_option == 3
