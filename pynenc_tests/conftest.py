"""
Pytest configuration and automated mocks for Pynenc testing.

Key components:
- MockPynenc: Automated mock app with dynamic dependency mocking (only abstract methods)
- runner: Fixture to run app.runner in a thread
- temp_sqlite_db_path: Fixture for temporary SQLite DB
- app_instance: Parametrized fixture for memory/sqlite backends
- check_all_status_transitions: Helper to assert no invalid status transitions in history
"""

import itertools
import os
import tempfile
import threading
import time
from collections.abc import Generator
from logging import Logger
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pynenc import Pynenc, PynencBuilder
from pynenc.broker.base_broker import BaseBroker
from pynenc.client_data_store.base_client_data_store import BaseClientDataStore
from pynenc.identifiers.call_id import CallId
from pynenc.identifiers.invocation_id import InvocationId
from pynenc.identifiers.task_id import TaskId
from pynenc.orchestrator.base_orchestrator import BaseBlockingControl, BaseOrchestrator
from pynenc.runner.base_runner import BaseRunner
from pynenc.state_backend.base_state_backend import BaseStateBackend
from pynenc_tests.util.log import create_test_logger

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest


logger: Logger = create_test_logger("conftest")


def check_all_status_transitions(app: Pynenc) -> None:
    """
    Validate that every recorded status transition for all invocations is allowed
    by the state machine defined in ``pynenc.invocation.status``.

    Iterates all invocations stored in the orchestrator, loads their history from
    the state backend, sorts entries by timestamp, and asserts that each consecutive
    pair constitutes a valid transition.  Any violations are collected and reported
    together so the full picture is visible in a single failure.

    :param Pynenc app: The app instance whose history to validate.
    :raises AssertionError: If one or more invalid status transitions are found.
    """
    from pynenc.invocation.status import validate_transition
    from pynenc.exceptions import InvocationStatusTransitionError

    violations: list[str] = []
    batch_size = 100
    offset = 0
    total = app.orchestrator.count_invocations()

    while offset < total:
        inv_ids = app.orchestrator.get_invocation_ids_paginated(
            limit=batch_size, offset=offset
        )
        for inv_id in inv_ids:
            history = app.state_backend.get_history(inv_id)
            sorted_history = sorted(history, key=lambda h: h.timestamp)
            for i in range(1, len(sorted_history)):
                from_status = sorted_history[i - 1].status_record.status
                to_status = sorted_history[i].status_record.status
                try:
                    validate_transition(from_status, to_status)
                except InvocationStatusTransitionError as exc:
                    ts = sorted_history[i].timestamp.isoformat()
                    violations.append(
                        f"  invocation {inv_id}: {from_status} -> {to_status}"
                        f" at {ts} — {exc}"
                    )
        offset += batch_size

    if violations:
        joined = "\n".join(violations)
        raise AssertionError(
            f"Found {len(violations)} invalid status transition(s):\n{joined}"
        )


def patch_abstract_methods(cls: type) -> type:
    """Create a subclass with abstract methods mocked."""
    abstract_methods: set[str] = getattr(cls, "__abstractmethods__", set())
    attrs: dict[str, Any] = {}

    for name in abstract_methods:
        func = getattr(cls, name, None)
        if func and getattr(func, "__code__", None) and func.__code__.co_flags & 0x80:
            attrs[name] = AsyncMock()
        else:
            attrs[name] = MagicMock()

    patched_cls = type(f"Patched{cls.__name__}", (cls,), attrs)
    patched_cls._patched_methods = list(attrs.keys())  # type: ignore
    return patched_cls


@pytest.fixture(autouse=True)
def reset_mock_classes() -> Generator[None, None, None]:
    """Reset all mocked methods and multiton state around each test for isolation.

    Also clears ``Pynenc._instances`` so that pickle roundtrips in one test
    cannot return a stale (already-configured) instance to a later test.
    Tests that explicitly rely on the multiton (e.g. runner isolation tests)
    register their own instances within the test body, so clearing here is safe.
    """

    mock_classes: list[type] = [
        MockBroker,
        MockBlockingControl,
        MockBaseOrchestrator,
        MockStateBackend,
        MockClientDataStore,
        MockRunner,
    ]

    def _reset_all() -> None:
        for mock_cls in mock_classes:
            for method_name in getattr(mock_cls, "_patched_methods", []):
                getattr(mock_cls, method_name).reset_mock(
                    return_value=True,
                    side_effect=True,
                )
        # Prevent multiton cross-test pollution: __setstate__ (called during
        # same-process pickle.loads) registers instances here; later tests with
        # the same app_id would get the stale instance with a cached _conf.
        Pynenc._instances.clear()

    # Clear leftovers from previous tests before this one starts.
    _reset_all()
    yield
    # Also clear state after the test for safety.
    _reset_all()


MockBroker = patch_abstract_methods(BaseBroker)  # type: ignore[misc]
MockBlockingControl = patch_abstract_methods(BaseBlockingControl)  # type: ignore[misc]
MockBaseOrchestrator = patch_abstract_methods(BaseOrchestrator)  # type: ignore[misc]
MockStateBackend = patch_abstract_methods(BaseStateBackend)  # type: ignore[misc]
MockClientDataStore = patch_abstract_methods(BaseClientDataStore)  # type: ignore[misc]
MockRunner = patch_abstract_methods(BaseRunner)  # type: ignore[misc]

# Monotonically increasing counter to give each MockPynenc a unique app_id,
# preventing the multiton from returning a stale instance across tests.
_mock_pynenc_counter = itertools.count(1)


class MockPynenc(Pynenc):
    """
    Automated mock Pynenc app for testing.

    Only abstract methods of dependencies are mocked, concrete logic is preserved.
    This allows tests to use real invocation objects and await their methods.

    :param dict[str, Any] | None config_values: Configuration values (include ``app_id`` key)
    """

    def __init__(self, config_values: dict[str, Any] | None = None) -> None:
        if config_values is None:
            config_values = {"app_id": f"mock_pynenc_{next(_mock_pynenc_counter)}"}
        elif "app_id" not in config_values:
            config_values = {
                **config_values,
                "app_id": f"mock_pynenc_{next(_mock_pynenc_counter)}",
            }
        super().__init__(config_values=config_values)
        self._runner_instance = MockRunner(self)  # type: ignore[misc]

    @classmethod
    def with_id(cls, app_id: str) -> "MockPynenc":
        """Convenience constructor to create a MockPynenc with a specific app_id."""
        return cls(config_values={"app_id": app_id})

    def _reset_cached_components(self) -> None:
        """Reset cached components with mock implementations."""
        super()._reset_cached_components()
        self._broker = MockBroker(self)  # type: ignore[misc]
        self._orchestrator = MockBaseOrchestrator(self)  # type: ignore[misc]
        self._state_backend = MockStateBackend(self)  # type: ignore[misc]
        self._client_data_store = MockClientDataStore(self)  # type: ignore[misc]
        self._trigger = MagicMock()  # type: ignore[misc]

    # Override component properties to return Any so mypy allows
    # mock-specific attribute access (.return_value, .side_effect, etc.)
    # in tests without scattered type: ignore comments.
    @property  # type: ignore[override]
    def orchestrator(self) -> Any:
        return self._orchestrator

    @property  # type: ignore[override]
    def broker(self) -> Any:
        return self._broker

    @property  # type: ignore[override]
    def state_backend(self) -> Any:
        return self._state_backend

    @property  # type: ignore[override]
    def client_data_store(self) -> Any:
        return self._client_data_store

    @client_data_store.setter
    def client_data_store(self, value: Any) -> None:
        self._client_data_store = value

    @property  # type: ignore[override]
    def trigger(self) -> Any:
        return self._trigger

    @property  # type: ignore[override]
    def serializer(self) -> Any:
        return self._serializer

    @property
    def runner(self) -> Any:  # type: ignore[override]
        return self._runner_instance

    @runner.setter
    def runner(self, runner_instance: Any) -> None:
        self._runner_instance = runner_instance


@pytest.fixture(scope="function")
def runner(request: "FixtureRequest") -> Generator[None, None, None]:
    """
    Start the runner in a separate thread for each test.

    This fixture starts the runner thread, waits for it to initialize,
    yields control back to the test, then stops the runner and purges
    app data when the test completes.

    :param request: The pytest request object to access the calling module
    :yield: None
    """
    app = request.module.app

    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.2)
    logger.info("Runner thread started")

    yield

    app.logger.info("Stopping runner thread...")
    app.runner.stop_runner_loop()
    # runner_thread.join()
    logger.info("Thread join completed")

    logger.info("Purging app data...")
    app.purge()


@pytest.fixture(scope="function")
def temp_sqlite_db_path() -> Generator[str, None, None]:
    """
    Provides a temporary SQLite database path for testing.

    :return: Path to a temporary SQLite database file
    """
    fd, path = tempfile.mkstemp(suffix=".db")
    logger.info(f"Created temporary SQLite DB at: {path}")
    os.close(fd)
    try:
        yield path
    finally:
        if os.path.exists(path):
            logger.info(f"Removing temporary SQLite DB at: {path}")
            os.remove(path)


@pytest.fixture(params=["memory", "sqlite"], scope="function")
def app_instance(request: "FixtureRequest", temp_sqlite_db_path: str) -> Pynenc:
    """
    Parametrized fixture that provides a Pynenc app instance for both memory and SQLite backends.

    Runs each test twice: once with an in-memory backend, once with a SQLite backend.

    :param request: pytest fixture request object
    :param str temp_sqlite_db_path: pytest fixture for temporary SQLite database path
    :return: Pynenc app instance
    :rtype: Pynenc
    """
    if request.param == "memory":
        return PynencBuilder().memory().build()
    elif request.param == "sqlite":
        return PynencBuilder().sqlite(sqlite_db_path=str(temp_sqlite_db_path)).build()
    else:
        raise ValueError(f"Unknown app backend: {request.param}")


@pytest.fixture
def task_id() -> TaskId:
    """Fixture for a sample TaskId."""
    return TaskId("some_module", "some_func")


@pytest.fixture
def other_task_id() -> TaskId:
    """Fixture for a different sample TaskId."""
    return TaskId("some_other_module", "some_other_func")


@pytest.fixture
def call_id(task_id: TaskId) -> CallId:
    """Fixture for a sample CallId."""
    return CallId(task_id=task_id, args_id="some_args_id")


@pytest.fixture
def other_call_id(other_task_id: TaskId) -> CallId:
    """Fixture for a different sample CallId."""
    return CallId(task_id=other_task_id, args_id="some_other_args_id")


@pytest.fixture
def inv_id() -> InvocationId:
    """Fixture for a sample InvocationId."""
    return InvocationId("some_inv_id")


@pytest.fixture
def other_inv_id() -> InvocationId:
    """Fixture for a different sample InvocationId."""
    return InvocationId("some_other_inv_id")
