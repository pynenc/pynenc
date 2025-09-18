"""
Shared test fixtures for workflow state integration tests.

This module provides fixtures for testing workflow data persistence and
deterministic operations across different state backend implementations,
following the same pattern as app combinations integration tests.
"""
from typing import TYPE_CHECKING

import pytest

from pynenc.runner import ThreadRunner
from tests.integration.workflow.state import tasks_data, tasks_deterministic

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.task import Task


@pytest.fixture
def app(app_instance: "Pynenc") -> "Pynenc":
    """
    Create test app with specified state backend implementation.

    :param request: Pytest request fixture
    :param monkeypatch: Pytest monkeypatch fixture for environment variables
    :return: Configured "Pynenc" app instance with cleanup
    """
    app_instance.purge()
    app_instance.runner = ThreadRunner(app_instance)
    return app_instance


# Data task fixtures
@pytest.fixture
def workflow_data_test_runner(app: "Pynenc") -> "Task":
    """Create workflow data test runner task."""
    tasks_data.workflow_data_test_runner.app = app
    return tasks_data.workflow_data_test_runner


@pytest.fixture
def multi_counter_workflow(app: "Pynenc") -> "Task":
    """Create multi-counter workflow task."""
    tasks_data.multi_counter_workflow.app = app
    return tasks_data.multi_counter_workflow


@pytest.fixture
def isolated_workflow_test(app: "Pynenc") -> "Task":
    """Create isolated workflow test task."""
    tasks_data.isolated_workflow_test.app = app
    return tasks_data.isolated_workflow_test


@pytest.fixture
def counter_workflow(app: "Pynenc") -> "Task":
    """Create counter workflow task."""
    tasks_data.counter_workflow.app = app
    return tasks_data.counter_workflow


# Deterministic task fixtures
@pytest.fixture
def deterministic_random_workflow(app: "Pynenc") -> "Task":
    """Create deterministic random workflow task."""
    tasks_deterministic.deterministic_random_workflow.app = app
    return tasks_deterministic.deterministic_random_workflow


@pytest.fixture
def deterministic_time_workflow(app: "Pynenc") -> "Task":
    """Create deterministic time workflow task."""
    tasks_deterministic.deterministic_time_workflow.app = app
    return tasks_deterministic.deterministic_time_workflow


@pytest.fixture
def deterministic_uuid_workflow(app: "Pynenc") -> "Task":
    """Create deterministic UUID workflow task."""
    tasks_deterministic.deterministic_uuid_workflow.app = app
    return tasks_deterministic.deterministic_uuid_workflow


@pytest.fixture
def deterministic_mixed_workflow(app: "Pynenc") -> "Task":
    """Create deterministic mixed operations workflow task."""
    tasks_deterministic.deterministic_mixed_workflow.app = app
    return tasks_deterministic.deterministic_mixed_workflow


@pytest.fixture
def deterministic_task_execution_workflow(app: "Pynenc") -> "Task":
    """Create deterministic task execution workflow task."""
    tasks_deterministic.deterministic_task_execution_workflow.app = app
    return tasks_deterministic.deterministic_task_execution_workflow


@pytest.fixture
def simple_add_task(app: "Pynenc") -> "Task":
    """Create simple add task for deterministic execution tests."""
    tasks_deterministic.simple_add_task.app = app
    return tasks_deterministic.simple_add_task
