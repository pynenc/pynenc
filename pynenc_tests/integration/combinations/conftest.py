import logging
import multiprocessing
from collections.abc import Generator
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from _pytest.monkeypatch import MonkeyPatch

from pynenc import Pynenc
from pynenc.broker import MemBroker, SQLiteBroker
from pynenc.client_data_store import MemClientDataStore, SQLiteClientDataStore
from pynenc.orchestrator import MemOrchestrator, SQLiteOrchestrator
from pynenc.runner import *  # noqa: F403, F401
from pynenc.serializer import *  # noqa: F403, F401
from pynenc.serializer.base_serializer import BaseSerializer
from pynenc.state_backend import MemStateBackend, SQLiteStateBackend
from pynenc.trigger import MemTrigger, SQLiteTrigger
from pynenc.util.subclasses import get_all_subclasses
from pynenc_tests import util
from pynenc_tests.integration.combinations import tasks, tasks_async
from pynenc_tests.util.multiprocesses import _cleanup_multiprocessing_children
from pynenc_tests.util.subclasses import get_runner_subclasses

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest

    from pynenc.task import Task


# Replace namedtuple with a typed frozen dataclass that exposes combination_id
@dataclass(frozen=True)
class AppComponents:
    client_data_store: type
    broker: type
    orchestrator: type
    state_backend: type
    trigger: type
    serializer: type
    runner: type

    @property
    def backend_type(self) -> str:
        """Determine backend type from component classes."""
        return "SQLite" if "SQLite" in self.state_backend.__name__ else "Mem"

    @property
    def combination_id(self) -> str:
        """Compute a stable identifier for the component combination in columnar format."""
        backend = self.backend_type
        runner = self.runner.__name__.replace("Runner", "")
        serializer = self.serializer.__name__.replace("Serializer", "")
        return f"{backend} {runner} {serializer}"


# Define component class tuples for cleaner code
MEM_CLASSES = (
    MemClientDataStore,
    MemBroker,
    MemOrchestrator,
    MemStateBackend,
    MemTrigger,
)
SQLITE_CLASSES = (
    SQLiteClientDataStore,
    SQLiteBroker,
    SQLiteOrchestrator,
    SQLiteStateBackend,
    SQLiteTrigger,
)


def build_test_combinations() -> list[AppComponents]:
    """
    Build component combinations for testing, pairing runners with serializers (one per runner, cycling serializers).

    This ensures deterministic test IDs and reliable test discovery, with reduced test count.

    :return: List of component combinations, one per runner
    """
    # Sort to ensure same fixture params and ids across runs
    runners = sorted(get_runner_subclasses(), key=lambda cls: cls.__name__)
    serializers = sorted(
        get_all_subclasses(BaseSerializer),  # type: ignore
        key=lambda cls: cls.__name__,
    )

    combinations: list[AppComponents] = []
    for i, runner_cls in enumerate(runners):
        serializer_cls = serializers[i % len(serializers)]
        if runner_cls.mem_compatible():
            combinations.append(AppComponents(*MEM_CLASSES, serializer_cls, runner_cls))
        combinations.append(AppComponents(*SQLITE_CLASSES, serializer_cls, runner_cls))
    return combinations


@pytest.fixture(
    params=build_test_combinations(),
    ids=lambda comp: comp.combination_id,
)
def app_combination_instance(
    request: "FixtureRequest", monkeypatch: MonkeyPatch, temp_sqlite_db_path: str
) -> Pynenc:
    """
    Parametrized app fixture that builds a Pynenc instance for each combination.

    Plugin authors: override this fixture in your project-level conftest if you want
    to build the Pynenc instance differently (for example using testcontainers).
    When this fixture is parametrized by pytest, `request.param` will be an
    AppComponents instance describing the desired combination.

    Usage notes for plugin authors:
    - The components are provided in `components: AppComponents = request.param`.
    - You can inspect `components.serializer`, `components.runner`, etc. and
      instantiate/configure Pynenc as you prefer (PynencBuilder, testcontainers, etc.).
    - If you need to avoid the default environment-variable driven builder below,
      simply construct and return your Pynenc instance here. Pytest will still
      parametrize tests using the `test_combinations` list described above.

    :param request: FixtureRequest provided by pytest
    :param monkeypatch: MonkeyPatch fixture for environment changes
    :param temp_sqlite_db_path: Path to a temporary sqlite db provided by tests
    :return: Pynenc - an initialized Pynenc instance configured for the combination
    """
    components: AppComponents = request.param
    test_module, test_name = util.get_module_name(request)
    monkeypatch.setenv("PYNENC__APP_ID", f"{test_module}.{test_name}")
    monkeypatch.setenv(
        "PYNENC__CLIENT_DATA_STORE_CLS", components.client_data_store.__name__
    )
    monkeypatch.setenv("PYNENC__ORCHESTRATOR_CLS", components.orchestrator.__name__)
    monkeypatch.setenv("PYNENC__BROKER_CLS", components.broker.__name__)
    monkeypatch.setenv("PYNENC__SERIALIZER_CLS", components.serializer.__name__)
    monkeypatch.setenv("PYNENC__STATE_BACKEND_CLS", components.state_backend.__name__)
    monkeypatch.setenv("PYNENC__TRIGGER_CLS", components.trigger.__name__)
    monkeypatch.setenv("PYNENC__RUNNER_CLS", components.runner.__name__)
    monkeypatch.setenv("PYNENC__LOGGING_LEVEL", "debug")
    monkeypatch.setenv("PYNENC__PRINT_ARGUMENTS", "False")

    # Set shared SQLite database path for SQLite components
    if any(
        cls.__name__.startswith("SQLite")
        for cls in [
            components.client_data_store,
            components.broker,
            components.orchestrator,
            components.state_backend,
        ]
    ):
        monkeypatch.setenv("PYNENC__SQLITE_DB_PATH", temp_sqlite_db_path)

    return Pynenc()


@pytest.fixture
def app(app_combination_instance: Pynenc) -> Generator[Pynenc, None, None]:
    """
    Compatibility wrapper so existing tests can still request `app`.

    :param Pynenc app_combination_instance: The parametrized app instance created by the core fixture.
    :return: Yields the Pynenc instance for the test and performs post-test cleanup.
    """
    yield app_combination_instance

    try:
        # Signal the runner to stop (idempotent if already stopped by the test)
        app_combination_instance.runner.stop_runner_loop()
    except Exception as e:
        logging.warning("Compatibility fixture failed to stop runner after test: %s", e)

    try:
        # Wait for the runner's run() loop to fully exit before dropping tables.
        # Without this, background threads may still be mid-iteration and hit
        # 'no such table' after purge() drops the schema.
        if not app_combination_instance.runner.wait_until_stopped(timeout=3.0):
            logging.warning(
                "Runner did not stop within timeout; proceeding with purge anyway"
            )
    except Exception as e:
        logging.warning(
            "Compatibility fixture failed while waiting for runner stop: %s", e
        )

    try:
        # Gracefully terminate and join any remaining multiprocessing children
        # spawned by process-based runners before brute-force cleanup.
        for child in multiprocessing.active_children():
            child.terminate()
        for child in multiprocessing.active_children():
            child.join(timeout=5)
    except Exception as e:
        logging.warning("Compatibility fixture failed to join active children: %s", e)

    try:
        _cleanup_multiprocessing_children()
    except Exception as e:
        logging.warning(
            "Compatibility fixture failed to cleanup multiprocessing children after test: %s",
            e,
        )

    try:
        app_combination_instance.purge()
    except Exception as e:
        logging.warning("Compatibility fixture failed to purge app after test: %s", e)


def replace_tasks_app(app: Pynenc) -> None:
    """Replace the .app attribute for all tasks in tasks and tasks_async modules.

    Also clears the cached `conf` property so tasks pick up the new app's config.
    """
    for mod in [tasks, tasks_async]:
        for attr in dir(mod):
            obj = getattr(mod, attr)
            # Only update if it's a Task and has .app
            if hasattr(obj, "app"):
                obj.app = app
                # Reset conf so it's re-computed with new app's config_values
                obj._conf = None


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
