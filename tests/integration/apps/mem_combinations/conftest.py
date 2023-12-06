from collections import namedtuple
from itertools import product
from typing import TYPE_CHECKING, Optional

import pytest

from pynenc import Pynenc
from pynenc.broker.base_broker import BaseBroker
from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
from pynenc.runner.base_runner import BaseRunner
from pynenc.serializer.base_serializer import BaseSerializer
from pynenc.state_backend.base_state_backend import BaseStateBackend
from tests import util
from tests.integration.apps.mem_combinations import mem_combinations_tasks as tasks

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from _pytest.python import Metafunc

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
        mem_combinations = (
            AppComponents(*x)
            for x in product(
                get_subclasses(BaseBroker),
                get_subclasses(BaseOrchestrator),
                get_subclasses(BaseRunner, mem_cls=True),
                get_subclasses(BaseSerializer),
                get_subclasses(BaseStateBackend),
            )
        )

        # If the runner is not a memory runner, it cannot be used with memory components
        not_mem_combinations = (
            AppComponents(*x)
            for x in product(
                get_subclasses(BaseBroker, mem_cls=False),
                get_subclasses(BaseOrchestrator, mem_cls=False),
                get_subclasses(BaseRunner, mem_cls=False),
                get_subclasses(BaseSerializer, mem_cls=False),
                get_subclasses(BaseStateBackend, mem_cls=False),
            )
        )
        combinations = list(mem_combinations) + list(not_mem_combinations)
        ids = list(map(get_combination_id, combinations))
        metafunc.parametrize("app", combinations, ids=ids, indirect=True)


@pytest.fixture
def app(request: "FixtureRequest") -> Pynenc:
    components: AppComponents = request.param
    test_module, test_name = util.get_module_name(request)
    app = Pynenc(
        app_id=f"{test_module}.{test_name}", config_values={"logging_level": "debug"}
    )
    app.set_broker_cls(components.broker)
    app.set_orchestrator_cls(components.orchestrator)
    app.set_serializer_cls(components.serializer)
    app.set_state_backend_cls(components.state_backend)
    app.runner = components.runner(app)
    # purge before and after each test
    app.purge()
    request.addfinalizer(app.purge)
    return app


@pytest.fixture(scope="function")
def task_cycle(app: Pynenc) -> "Task":
    # this replacing the app of the task works in multithreading

    # but not in multi processing runner,
    # the process start from scratch and reference the function
    # with the mocked decorator
    tasks.cycle_start.app = app
    tasks.cycle_end.app = app
    return tasks.cycle_start


@pytest.fixture(scope="function")
def task_raise_exception(app: Pynenc) -> "Task":
    tasks.raise_exception.app = app
    return tasks.raise_exception


@pytest.fixture(scope="function")
def task_sum(app: Pynenc) -> "Task":
    tasks.sum.app = app
    return tasks.sum


@pytest.fixture(scope="function")
def task_get_text(app: Pynenc) -> "Task":
    tasks.get_text.app = app
    return tasks.get_text


@pytest.fixture(scope="function")
def task_get_upper(app: Pynenc) -> "Task":
    tasks.get_text.app = app
    tasks.get_upper.app = app
    return tasks.get_upper


@pytest.fixture(scope="function")
def task_direct_cycle(app: Pynenc) -> "Task":
    tasks.direct_cycle.app = app
    return tasks.direct_cycle
