"""
Pytest configuration and automated mocks for Pynenc testing.

Key components:
- MockPynenc: Automated mock app with dynamic dependency mocking (only abstract methods)
- runner: Fixture to run app.runner in a thread
- temp_sqlite_db_path: Fixture for temporary SQLite DB
- app_instance: Parametrized fixture for memory/sqlite backends
"""

import os
import tempfile
import threading
import time
from collections.abc import Generator
from functools import cached_property
from logging import Logger
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pynenc import Pynenc, PynencBuilder
from pynenc.arg_cache.base_arg_cache import BaseArgCache
from pynenc.broker.base_broker import BaseBroker
from pynenc.orchestrator.base_orchestrator import BaseBlockingControl, BaseOrchestrator
from pynenc.runner.base_runner import BaseRunner
from pynenc.state_backend.base_state_backend import BaseStateBackend
from pynenc_tests.util.log import create_test_logger

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest


logger: Logger = create_test_logger("conftest")


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
    """Reset all mocked methods between tests."""
    yield

    for mock_cls in [
        MockBroker,
        MockBlockingControl,
        MockBaseOrchestrator,
        MockStateBackend,
        MockArgCache,
        MockRunner,
    ]:
        for method_name in getattr(mock_cls, "_patched_methods", []):
            getattr(mock_cls, method_name).reset_mock()


MockBroker = patch_abstract_methods(BaseBroker)  # type: ignore[misc]
MockBlockingControl = patch_abstract_methods(BaseBlockingControl)  # type: ignore[misc]
MockBaseOrchestrator = patch_abstract_methods(BaseOrchestrator)  # type: ignore[misc]
MockStateBackend = patch_abstract_methods(BaseStateBackend)  # type: ignore[misc]
MockArgCache = patch_abstract_methods(BaseArgCache)  # type: ignore[misc]
MockRunner = patch_abstract_methods(BaseRunner)  # type: ignore[misc]


class MockPynenc(Pynenc):
    """
    Automated mock Pynenc app for testing.

    Only abstract methods of dependencies are mocked, concrete logic is preserved.
    This allows tests to use real invocation objects and await their methods.

    :param str app_id: Application identifier
    """

    def __init__(self, app_id: str = "mock_pynenc") -> None:
        super().__init__(app_id=app_id)
        self._runner_instance = MockRunner(self)  # type: ignore[misc]
        # Ensure runner.cache is always a real dict for all tests
        self._runner_instance.cache = {}  # type: ignore

    @cached_property
    def broker(self):  # type: ignore
        return MockBroker(self)  # type: ignore[misc]

    @cached_property
    def orchestrator(self):  # type: ignore
        return MockBaseOrchestrator(self)  # type: ignore[misc]

    @cached_property
    def state_backend(self):  # type: ignore
        return MockStateBackend(self)  # type: ignore[misc]

    @cached_property
    def arg_cache(self):  # type: ignore
        return MockArgCache(self)  # type: ignore[misc]

    @property
    def runner(self):  # type: ignore
        return self._runner_instance

    @runner.setter
    def runner(self, runner_instance) -> None:  # type: ignore[no-untyped-def,override]
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
    runner_thread.join(timeout=1)
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
