from collections import namedtuple
import hashlib
from itertools import product
from typing import TYPE_CHECKING, Optional
import uuid

import pytest

from pynenc import Pynenc
from pynenc.broker.base_broker import BaseBroker
from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
from pynenc.runner.base_runner import BaseRunner
from pynenc.serializer.base_serializer import BaseSerializer
from pynenc.state_backend.base_state_backend import BaseStateBackend
from tests.conftest import MockPynenc

if TYPE_CHECKING:
    from _pytest.python import Metafunc
    from _pytest.fixtures import FixtureRequest
    from pynenc.task import Task


AppComponents = namedtuple(
    "AppComponents",
    [
        "broker",
        "orchestrator",
        "runner",
        "serializer",
        "state_backend",
    ],
)


def get_combination_id(combination: AppComponents) -> str:
    return (
        f"run.{combination.runner.__name__.replace('Runner', '')}-"
        f"brk.{combination.broker.__name__.replace('Broker', '')}-"
        f"orc.{combination.orchestrator.__name__.replace('Orchestrator', '')}-"
        f"sbk.{combination.state_backend.__name__.replace('StateBackend', '')}-"
        f"ser.{combination.serializer.__name__.replace('Serializer', '')}"
    )


def pytest_generate_tests(metafunc: "Metafunc") -> None:
    def get_subclasses(cls: type, mem_cls: Optional[bool] = None) -> list[type]:
        subclasses = []
        for c in cls.__subclasses__():
            if "mock" in c.__name__.lower() or c.__name__.startswith("Dummy"):
                continue
            if mem_cls is not None and mem_cls != c.__name__.startswith("Mem"):
                continue
            if c.__name__.startswith("Process"):
                continue
            subclasses.append(c)
        return subclasses

    if "app" in metafunc.fixturenames:
        # mem runners can run with any combination of components (including memory components)
        mem_combinations = map(
            lambda x: AppComponents(*x),
            product(
                get_subclasses(BaseBroker),
                get_subclasses(BaseOrchestrator),
                get_subclasses(BaseRunner, mem_cls=True),
                get_subclasses(BaseSerializer),
                get_subclasses(BaseStateBackend),
            ),
        )
        # If the runner is not a memory runner, it cannot be used with memory components
        not_mem_combinations = map(
            lambda x: AppComponents(*x),
            product(
                get_subclasses(BaseBroker, mem_cls=False),
                get_subclasses(BaseOrchestrator, mem_cls=False),
                get_subclasses(BaseRunner, mem_cls=False),
                get_subclasses(BaseSerializer, mem_cls=False),
                get_subclasses(BaseStateBackend, mem_cls=False),
            ),
        )
        combinations = list(mem_combinations) + list(not_mem_combinations)
        ids = list(map(get_combination_id, combinations))
        metafunc.parametrize("app", combinations, ids=ids, indirect=True)


def get_unique_id() -> str:
    _id = uuid.uuid4()
    return hashlib.sha256(_id.bytes).hexdigest()[:8]


@pytest.fixture
def app(request: "FixtureRequest") -> Pynenc:
    components: AppComponents = request.param
    test_name = request.node.name.replace("[", "(").replace("]", ")")
    test_module = request.node.module.__name__
    app = Pynenc(app_id=f"{test_module}.{test_name}")
    app.set_broker_cls(components.broker)
    app.set_orchestrator_cls(components.orchestrator)
    app.set_serializer_cls(components.serializer)
    app.set_state_backend_cls(components.state_backend)
    app.runner = components.runner(app)
    # purge before and after each test
    app.purge()
    request.addfinalizer(app.purge)
    return app


mock_app = MockPynenc()


@mock_app.task
def sum(x: int, y: int) -> int:
    return x + y


@pytest.fixture(scope="function")
def task_sum(app: Pynenc) -> "Task":
    sum.app = app
    return sum


@mock_app.task
def cycle_start() -> None:
    cycle_end().result


@mock_app.task
def cycle_end() -> None:
    cycle_start().result


@pytest.fixture(scope="function")
def task_cycle(app: Pynenc) -> "Task":
    # this replacing the app of the task works in multithreading

    # but not in multi processing runner,
    # the process start from scratch and reference the function
    # with the mocked decorator
    cycle_start.app = app
    cycle_end.app = app
    return cycle_start
