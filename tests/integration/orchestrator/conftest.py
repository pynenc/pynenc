from typing import TYPE_CHECKING

import pytest

from pynenc.call import Call
from pynenc.invocation import DistributedInvocation
from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
from tests import util
from tests.conftest import MockPynenc

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from _pytest.python import Metafunc

    from pynenc.task import Task


def pytest_generate_tests(metafunc: "Metafunc") -> None:
    subclasses = [
        c for c in BaseOrchestrator.__subclasses__() if "mock" not in c.__name__.lower()
    ]
    if "app" in metafunc.fixturenames:
        metafunc.parametrize("app", subclasses, indirect=True)


@pytest.fixture
def app(request: "FixtureRequest") -> MockPynenc:
    test_module, test_name = util.get_module_name(request)
    app = MockPynenc(app_id=f"{test_module}.{test_name}")
    app.orchestrator = request.param(app)
    app.purge()
    request.addfinalizer(app.purge)
    return app


mock_app = MockPynenc()


@mock_app.task
def dummy_task() -> None:
    ...


@mock_app.task
def dummy_sum(x: int, y: int) -> int:
    return x + y


@mock_app.task
def dummy_concat(arg0: str, arg1: str) -> str:
    return f"{arg0}:{arg1}"


@mock_app.task
def dummy_mirror(arg: str) -> str:
    return arg


@mock_app.task
def dummy_key_arg(key: str, arg: str) -> str:
    return f"{key}:{arg}"


@pytest.fixture
def task_dummy(app: MockPynenc) -> "Task":
    dummy_task.app = app
    return dummy_task


@pytest.fixture
def task_sum(app: MockPynenc) -> "Task":
    dummy_sum.app = app
    return dummy_sum


@pytest.fixture
def task_concat(app: MockPynenc) -> "Task":
    dummy_concat.app = app
    return dummy_concat


@pytest.fixture
def task_mirror(app: MockPynenc) -> "Task":
    dummy_mirror.app = app
    return dummy_mirror


@pytest.fixture
def task_key_arg(app: MockPynenc) -> "Task":
    dummy_key_arg.app = app
    return dummy_key_arg


@pytest.fixture
def dummy_invocation(task_dummy: "Task") -> "DistributedInvocation":
    return DistributedInvocation(Call(task_dummy), None)
