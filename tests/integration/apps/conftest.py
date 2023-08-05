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
from tests.conftest import MockPynenc
from pynenc.call import Call
from pynenc.invocation import DistributedInvocation

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


def pytest_generate_tests(metafunc: "Metafunc") -> None:
    def get_subclasses(cls: type, mem_cls: Optional[bool] = None) -> list[type]:
        subclasses = []
        for c in cls.__subclasses__():
            if "mock" in c.__name__.lower() or c.__name__.startswith("Dummy"):
                continue
            if mem_cls is not None and mem_cls != c.__name__.startswith("Mem"):
                continue
            subclasses.append(c)
        return subclasses

    def get_combination_id(combination: AppComponents) -> str:
        return (
            f"run:{combination.runner.__name__.replace('Runner', '')},"
            f"brk:{combination.broker.__name__.replace('Broker', '')},"
            f"orc:{combination.orchestrator.__name__.replace('Orchestrator', '')},"
            f"sbk:{combination.state_backend.__name__.replace('StateBackend', '')},"
            f"ser:{combination.serializer.__name__.replace('Serializer', '')}"
        )

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


@pytest.fixture
def app(request: "FixtureRequest") -> Pynenc:
    app = Pynenc()
    components: AppComponents = request.param
    app.set_broker_cls(components.broker)
    app.set_orchestrator_cls(components.orchestrator)
    app.set_serializer_cls(components.serializer)
    app.set_state_backend_cls(components.state_backend)
    app.runner = components.runner(app)
    app.broker.purge()
    app.orchestrator.purge()
    app.state_backend.purge()
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
def task_dummy(app: Pynenc) -> "Task":
    dummy_task.app = app
    return dummy_task


@pytest.fixture
def task_sum(app: Pynenc) -> "Task":
    dummy_sum.app = app
    return dummy_sum


@pytest.fixture
def task_concat(app: Pynenc) -> "Task":
    dummy_concat.app = app
    return dummy_concat


@pytest.fixture
def task_mirror(app: Pynenc) -> "Task":
    dummy_mirror.app = app
    return dummy_mirror


@pytest.fixture
def task_key_arg(app: Pynenc) -> "Task":
    dummy_key_arg.app = app
    return dummy_key_arg


@pytest.fixture
def dummy_invocation(task_dummy: "Task") -> "DistributedInvocation":
    return DistributedInvocation(Call(task_dummy), None)
