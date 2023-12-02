from collections import namedtuple
from itertools import product
from typing import TYPE_CHECKING, Any, Optional

import pytest

from pynenc import Pynenc
from pynenc.broker.base_broker import BaseBroker
from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
from pynenc.runner.base_runner import BaseRunner
from pynenc.serializer.base_serializer import BaseSerializer
from pynenc.state_backend.base_state_backend import BaseStateBackend
from tests import util
from tests.conftest import MockPynenc

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
    _ = cycle_end().result


@mock_app.task
def cycle_end() -> None:
    _ = cycle_start().result


@pytest.fixture(scope="function")
def task_cycle(app: Pynenc) -> "Task":
    # this replacing the app of the task works in multithreading

    # but not in multi processing runner,
    # the process start from scratch and reference the function
    # with the mocked decorator
    cycle_start.app = app
    cycle_end.app = app
    return cycle_start


@mock_app.task
def raise_exception() -> Any:
    raise ValueError("test")


@pytest.fixture(scope="function")
def task_raise_exception(app: Pynenc) -> "Task":
    raise_exception.app = app
    return raise_exception


@mock_app.task
def get_text() -> str:
    return "example"


@mock_app.task
def get_upper() -> str:
    return get_text().result.upper()


@pytest.fixture(scope="function")
def task_get_text(app: Pynenc) -> "Task":
    get_text.app = app
    return get_text


@pytest.fixture(scope="function")
def task_get_upper(app: Pynenc) -> "Task":
    get_text.app = app
    get_upper.app = app
    return get_upper


@mock_app.task
def direct_cycle() -> str:
    invocation = direct_cycle()
    return invocation.result.upper()


@pytest.fixture(scope="function")
def task_direct_cycle(app: Pynenc) -> "Task":
    direct_cycle.app = app
    return direct_cycle
