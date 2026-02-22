"""
Tests for Task.from_id resolution.

This module tests that Task.from_id correctly resolves tasks in various scenarios:
- Tasks registered dynamically via app.task(func) call
- Tasks decorated at module level with @app.task
- Tasks decorated at module level with @app.direct_task

These tests reproduce the ValueError that occurs when a runner starts fresh
and tries to resolve a task that was not loaded into the app's task registry.

Key components:
- test_from_id_should_resolve_dynamically_registered_task: Reproduces the conftest/fixture pattern
- test_from_id_should_resolve_module_level_task: Tests @app.task module-level decoration
- test_from_id_should_resolve_module_level_direct_task: Tests @app.direct_task resolution
- test_from_id_should_resolve_pynenc_task_wrapper: Tests wrapper objects with pynenc_task attr
"""

from typing import TYPE_CHECKING


from pynenc import Pynenc
from pynenc.identifiers.task_id import TaskId
from pynenc.task import Task

if TYPE_CHECKING:
    pass


# Module-level function NOT decorated — simulates dynamic registration
# (e.g., the conftest pattern where a fixture calls app.task(func))
def add_func(a: int, b: int) -> int:
    """Simple addition function for testing."""
    return a + b


def test_from_id_should_resolve_dynamically_registered_task() -> None:
    """Test that from_id resolves a task registered via app.task(func) call.

    This reproduces the exact error from the deterministic executor test:
    the function is defined at module level but NOT decorated with @app.task,
    so _get_from_task_id finds the plain function, not a Task instance.
    The task IS registered in app._tasks via the app.task() call.
    """
    config = {
        "runner_cls": "ThreadRunner",
        "state_backend_cls": "MemStateBackend",
        "broker_cls": "MemBroker",
        "orchestrator_cls": "MemOrchestrator",
        "client_data_store_cls": "MemClientDataStore",
    }
    app = Pynenc(app_id="test_get_tasks", config_values=config)
    task = app.task(add_func)

    # from_id should resolve the task from app._tasks
    resolved = Task.from_id(app, task.task_id)
    assert resolved.task_id == task.task_id
    assert resolved.func == add_func


def test_from_id_should_resolve_module_level_task_from_different_app() -> None:
    """Test that from_id resolves a @app.task decorated function from a different app.

    When a module defines a task at module level with @app.task, the module attribute
    IS a Task instance. A different app should be able to resolve it via from_id
    by importing the module and finding the Task.
    """
    config = {
        "runner_cls": "ThreadRunner",
        "state_backend_cls": "MemStateBackend",
        "broker_cls": "MemBroker",
        "orchestrator_cls": "MemOrchestrator",
        "client_data_store_cls": "MemClientDataStore",
    }
    new_app = Pynenc(app_id="test_resolve_task", config_values=config)

    # Task defined in the helper module with @helper_app.task
    task_id = TaskId("pynenc_tests.unit.task.tasks_get_tasks", "helper_task_func")

    # This should work: _get_from_task_id finds a Task instance at module level
    resolved = Task.from_id(new_app, task_id)
    assert resolved.task_id == task_id
    assert resolved.app is new_app


def test_from_id_should_resolve_module_level_direct_task_from_different_app() -> None:
    """Test that from_id resolves a @app.direct_task decorated function.

    This reproduces the client error: when a function is decorated with
    @app.direct_task, the module attribute is a wrapper function (not a Task).
    The Task is only registered in the decorating app's _tasks, not accessible
    via getattr on the module. A different app calling from_id fails because
    it finds the wrapper, not a Task.

    This is the root cause of the client error where _get_from_task_id returns
    a non-Task object (like celery_app.tasks.StatusBase).
    """
    config = {
        "runner_cls": "ThreadRunner",
        "state_backend_cls": "MemStateBackend",
        "broker_cls": "MemBroker",
        "orchestrator_cls": "MemOrchestrator",
        "client_data_store_cls": "MemClientDataStore",
    }
    new_app = Pynenc(app_id="test_resolve_direct", config_values=config)

    # Task defined in the helper module with @helper_app.direct_task
    task_id = TaskId("pynenc_tests.unit.task.tasks_get_tasks", "helper_direct_func")

    # This should work but currently fails:
    # _get_from_task_id finds the sync_wrapper, not a Task instance
    resolved = Task.from_id(new_app, task_id)
    assert resolved.task_id == task_id
    assert resolved.app is new_app


def test_from_id_should_resolve_pynenc_task_wrapper() -> None:
    """Test that from_id resolves a wrapper object with a pynenc_task attribute.

    This reproduces the Celery migration pattern where the module-level name
    is a StatusBase-like wrapper that holds a reference to the underlying Task
    via a ``pynenc_task`` attribute.  Task.from_id should extract the inner Task
    and bind it to the requesting app.
    """
    config = {
        "runner_cls": "ThreadRunner",
        "state_backend_cls": "MemStateBackend",
        "broker_cls": "MemBroker",
        "orchestrator_cls": "MemOrchestrator",
        "client_data_store_cls": "MemClientDataStore",
    }
    new_app = Pynenc(app_id="test_resolve_wrapper", config_values=config)

    # The helper module attribute "helper_wrapped_func" is a _TaskWrapper,
    # not a Task.  Its .pynenc_task attribute IS a Task.
    task_id = TaskId("pynenc_tests.unit.task.tasks_get_tasks", "helper_wrapped_func")

    resolved = Task.from_id(new_app, task_id)
    assert resolved.task_id == task_id
    assert resolved.app is new_app
