import logging
import os
import signal
import subprocess
import time
from collections import namedtuple
from collections.abc import Generator
from itertools import product
from typing import TYPE_CHECKING, Any, Optional

import pytest
from _pytest.monkeypatch import MonkeyPatch

from pynenc import Pynenc

# Import all concrete implementations so they're available to __subclasses__()
from pynenc.arg_cache import *  # noqa: F403, F401
from pynenc.arg_cache.base_arg_cache import BaseArgCache
from pynenc.broker import *  # noqa: F403, F401
from pynenc.broker.base_broker import BaseBroker
from pynenc.orchestrator import *  # noqa: F403, F401
from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
from pynenc.runner import *  # noqa: F403, F401
from pynenc.runner.base_runner import BaseRunner
from pynenc.serializer import *  # noqa: F403, F401
from pynenc.serializer.base_serializer import BaseSerializer
from pynenc.state_backend import *  # noqa: F403, F401
from pynenc.state_backend.base_state_backend import BaseStateBackend
from pynenc_tests import util
from pynenc_tests.integration.apps.combinations import tasks, tasks_async

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from _pytest.python import Metafunc

    from pynenc.task import Task


def pytest_configure(config: Any) -> None:
    # Set up debug logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@pytest.fixture(scope="function", autouse=True)
def measure_test_time(request: "FixtureRequest") -> Generator:
    """Automatically measure and report the time taken by each test."""
    test_name = request.node.name
    logging.info(f"TIMER: Test {test_name} starting")
    start_time = time.time()

    yield

    elapsed = time.time() - start_time
    logging.info(f"TIMER: Test {test_name} took {elapsed:.3f} seconds")


AppComponents = namedtuple(
    "AppComponents",
    [
        "arg_cache",
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
        f"ser.{combination.serializer.__name__.replace('Serializer', '')}-"
        f"arg.{combination.arg_cache.__name__.replace('ArgCache', '')}"
    )


def pytest_generate_tests(metafunc: "Metafunc") -> None:
    def get_subclasses(cls: type, mem_cls: Optional[bool] = None) -> list[type]:
        """Get all subclasses recursively."""

        def _get_all_subclasses(cls: type) -> set[type]:
            """Recursively get all subclasses."""
            subclasses = set()
            for subclass in cls.__subclasses__():
                subclasses.add(subclass)
                subclasses.update(_get_all_subclasses(subclass))
            return subclasses

        all_subclasses = _get_all_subclasses(cls)
        subclasses: list[type] = []

        for c in all_subclasses:
            if "mock" in c.__name__.lower() or c.__name__.startswith("Dummy"):
                continue

            # Special case: All serializers are memory compatible and work with both
            # memory and non-memory setups, so include them regardless of mem_cls
            if issubclass(c, BaseSerializer):
                subclasses.append(c)
                continue

            # For other classes, use the "Mem" and "SharedMemory" prefix conventions
            # SharedMemory classes are also memory compatible (for testing process runners)
            is_mem_compatible = c.__name__.startswith("Mem") or c.__name__.startswith(
                "SharedMemory"
            )
            if mem_cls is not None and mem_cls != is_mem_compatible:
                continue
            # if c.__name__.startswith("Process"):
            #     continue
            subclasses.append(c)
        return subclasses

    def get_runners(mem_compatible: bool) -> list[type[BaseRunner]]:
        return [
            r
            for r in get_subclasses(BaseRunner)
            if r.mem_compatible() == mem_compatible  # type: ignore
        ]

    if "app" in metafunc.fixturenames:
        # Memory-compatible runners with pure Mem components only (no mixing with SharedMemory)
        mem_combinations = (
            AppComponents(*x)
            for x in product(
                [
                    c
                    for c in get_subclasses(BaseArgCache, mem_cls=True)
                    if c.__name__.startswith("Mem")
                ],
                [
                    c
                    for c in get_subclasses(BaseBroker, mem_cls=True)
                    if c.__name__.startswith("Mem")
                ],
                [
                    c
                    for c in get_subclasses(BaseOrchestrator, mem_cls=True)
                    if c.__name__.startswith("Mem")
                ],
                get_runners(mem_compatible=True),
                get_subclasses(BaseSerializer),
                [
                    c
                    for c in get_subclasses(BaseStateBackend, mem_cls=True)
                    if c.__name__.startswith("Mem")
                ],
            )
        )
        # Process-compatible runners with pure SQLite components only
        sqlite_combinations = (
            AppComponents(*x)
            for x in product(
                [
                    c
                    for c in get_subclasses(BaseArgCache)
                    if c.__name__.startswith("SQLite")
                ],
                [
                    c
                    for c in get_subclasses(BaseBroker)
                    if c.__name__.startswith("SQLite")
                ],
                [
                    c
                    for c in get_subclasses(BaseOrchestrator)
                    if c.__name__.startswith("SQLite")
                ],
                # Call it with all the classes, including the memory exclusive runners (ThreadRunner)
                # get_runners(mem_compatible=False),
                get_subclasses(BaseRunner),
                get_subclasses(BaseSerializer),
                [
                    c
                    for c in get_subclasses(BaseStateBackend)
                    if c.__name__.startswith("SQLite")
                ],
            )
        )

        combinations = list(mem_combinations) + list(sqlite_combinations)
        ids = list(map(get_combination_id, combinations))
        metafunc.parametrize("app", combinations, ids=ids, indirect=True)


def _cleanup_multiprocessing_children() -> None:
    """Kill lingering Python multiprocessing child processes (spawn/resource_tracker) before app init."""
    try:
        # List all python processes
        result = subprocess.run(
            ["ps", "aux"], capture_output=True, text=True, check=True
        )
        for line in result.stdout.splitlines():
            if "python" in line and (
                "multiprocessing.spawn" in line
                or "multiprocessing.resource_tracker" in line
            ):
                parts = line.split()
                pid = int(parts[1])
                try:
                    # Send SIGKILL to the process
                    os.kill(pid, signal.SIGKILL)
                    logging.info(f"Killed lingering multiprocessing process {pid}")
                except Exception as e:
                    logging.warning(f"Failed to kill process {pid}: {e}")
    except Exception as e:
        logging.warning(f"Failed to cleanup multiprocessing children: {e}")


@pytest.fixture
def app(
    request: "FixtureRequest", monkeypatch: MonkeyPatch, temp_sqlite_db_path: str
) -> Pynenc:
    _cleanup_multiprocessing_children()
    components: AppComponents = request.param
    test_module, test_name = util.get_module_name(request)
    monkeypatch.setenv("PYNENC__APP_ID", f"{test_module}.{test_name}")
    monkeypatch.setenv("PYNENC__ARG_CACHE_CLS", components.arg_cache.__name__)
    monkeypatch.setenv("PYNENC__ORCHESTRATOR_CLS", components.orchestrator.__name__)
    monkeypatch.setenv("PYNENC__BROKER_CLS", components.broker.__name__)
    monkeypatch.setenv("PYNENC__SERIALIZER_CLS", components.serializer.__name__)
    monkeypatch.setenv("PYNENC__STATE_BACKEND_CLS", components.state_backend.__name__)
    monkeypatch.setenv("PYNENC__RUNNER_CLS", components.runner.__name__)
    monkeypatch.setenv("PYNENC__LOGGING_LEVEL", "debug")
    monkeypatch.setenv("PYNENC__ORCHESTRATOR__CYCLE_CONTROL", "True")
    monkeypatch.setenv("PYNENC__PRINT_ARGUMENTS", "False")

    # Set shared SQLite database path for SQLite components
    if any(
        cls.__name__.startswith("SQLite")
        for cls in [
            components.arg_cache,
            components.broker,
            components.orchestrator,
            components.state_backend,
        ]
    ):
        monkeypatch.setenv("PYNENC__SQLITE_DB_PATH", temp_sqlite_db_path)

    app = Pynenc()
    app.purge()
    request.addfinalizer(app.purge)
    return app


def replace_tasks_app(app: Pynenc) -> None:
    """Replace the .app attribute for all tasks in tasks and tasks_async modules."""
    for mod in [tasks, tasks_async]:
        for attr in dir(mod):
            obj = getattr(mod, attr)
            # Only update if it's a Task and has .app
            if hasattr(obj, "app"):
                obj.app = app


@pytest.fixture(scope="function")
def task_cycle(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks.cycle_start


@pytest.fixture(scope="function")
def task_raise_exception(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks.raise_exception


@pytest.fixture(scope="function")
def task_sum(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks.sum_task


@pytest.fixture(scope="function")
def task_get_text(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks.get_text


@pytest.fixture(scope="function")
def task_get_upper(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks.get_upper


@pytest.fixture(scope="function")
def task_direct_cycle(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks.direct_cycle


@pytest.fixture(scope="function")
def task_retry_once(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks.retry_once


@pytest.fixture(scope="function")
def task_sleep(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks.sleep_seconds


@pytest.fixture(scope="function")
def task_cpu_intensive_no_conc(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks.cpu_intensive_no_conc


@pytest.fixture(scope="function")
def task_distribute_cpu_work(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks.distribute_cpu_work


@pytest.fixture(scope="function")
def task_async_add(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks_async.async_add


@pytest.fixture(scope="function")
def task_async_get_text(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks_async.async_get_text


@pytest.fixture(scope="function")
def task_async_get_upper(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks_async.async_get_upper


@pytest.fixture(scope="function")
def task_async_fail(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks_async.async_fail


@pytest.fixture(scope="function")
def task_async_sleep(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks_async.async_sleep_seconds


@pytest.fixture(scope="function")
def task_process_large_shared_arg(app: Pynenc) -> "Task":
    replace_tasks_app(app)
    return tasks.process_large_shared_arg
