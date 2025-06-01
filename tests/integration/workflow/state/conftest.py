"""
Shared test fixtures for workflow state integration tests.

This module provides fixtures for testing workflow data persistence and
deterministic operations across different state backend implementations,
following the same pattern as app combinations integration tests.
"""
from typing import TYPE_CHECKING

import pytest
from _pytest.monkeypatch import MonkeyPatch

from pynenc import Pynenc
from pynenc.state_backend.base_state_backend import BaseStateBackend
from tests import util
from tests.integration.workflow.state import tasks_data, tasks_deterministic

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from _pytest.python import Metafunc

    from pynenc.task import Task


def pytest_generate_tests(metafunc: "Metafunc") -> None:
    """
    Generate test parameters for different state backend implementations.

    :param metafunc: Pytest metafunc object for test parametrization
    """
    subclasses = [
        c for c in BaseStateBackend.__subclasses__() if "mock" not in c.__name__.lower()
    ]
    if "app" in metafunc.fixturenames:
        metafunc.parametrize("app", subclasses, indirect=True)


@pytest.fixture
def app(request: "FixtureRequest", monkeypatch: MonkeyPatch) -> Pynenc:
    """
    Create test app with specified state backend implementation.

    :param request: Pytest request fixture
    :param monkeypatch: Pytest monkeypatch fixture for environment variables
    :return: Configured Pynenc app instance with cleanup
    """
    state_backend_cls = request.param
    test_module, test_name = util.get_module_name(request)

    # Set environment variables following app combinations pattern
    monkeypatch.setenv("PYNENC__APP_ID", f"{test_module}.{test_name}")
    monkeypatch.setenv("PYNENC__STATE_BACKEND_CLS", state_backend_cls.__name__)
    monkeypatch.setenv("PYNENC__RUNNER_CLS", "ThreadRunner")
    monkeypatch.setenv("PYNENC__LOGGING_LEVEL", "info")

    app = Pynenc()
    app.purge()
    request.addfinalizer(app.purge)
    return app


# Data task fixtures
@pytest.fixture
def workflow_data_test_runner(app: Pynenc) -> "Task":
    """Create workflow data test runner task."""
    tasks_data.workflow_data_test_runner.app = app
    return tasks_data.workflow_data_test_runner


@pytest.fixture
def multi_counter_workflow(app: Pynenc) -> "Task":
    """Create multi-counter workflow task."""
    tasks_data.multi_counter_workflow.app = app
    return tasks_data.multi_counter_workflow


@pytest.fixture
def isolated_workflow_test(app: Pynenc) -> "Task":
    """Create isolated workflow test task."""
    tasks_data.isolated_workflow_test.app = app
    return tasks_data.isolated_workflow_test


@pytest.fixture
def counter_workflow(app: Pynenc) -> "Task":
    """Create counter workflow task."""
    tasks_data.counter_workflow.app = app
    return tasks_data.counter_workflow


# Deterministic task fixtures
@pytest.fixture
def deterministic_random_workflow(app: Pynenc) -> "Task":
    """Create deterministic random workflow task."""
    tasks_deterministic.deterministic_random_workflow.app = app
    return tasks_deterministic.deterministic_random_workflow


@pytest.fixture
def deterministic_time_workflow(app: Pynenc) -> "Task":
    """Create deterministic time workflow task."""
    tasks_deterministic.deterministic_time_workflow.app = app
    return tasks_deterministic.deterministic_time_workflow


@pytest.fixture
def deterministic_uuid_workflow(app: Pynenc) -> "Task":
    """Create deterministic UUID workflow task."""
    tasks_deterministic.deterministic_uuid_workflow.app = app
    return tasks_deterministic.deterministic_uuid_workflow


@pytest.fixture
def deterministic_mixed_workflow(app: Pynenc) -> "Task":
    """Create deterministic mixed operations workflow task."""
    tasks_deterministic.deterministic_mixed_workflow.app = app
    return tasks_deterministic.deterministic_mixed_workflow


@pytest.fixture
def deterministic_task_execution_workflow(app: Pynenc) -> "Task":
    """Create deterministic task execution workflow task."""
    tasks_deterministic.deterministic_task_execution_workflow.app = app
    return tasks_deterministic.deterministic_task_execution_workflow


@pytest.fixture
def simple_add_task(app: Pynenc) -> "Task":
    """Create simple add task for deterministic execution tests."""
    tasks_deterministic.simple_add_task.app = app
    return tasks_deterministic.simple_add_task
