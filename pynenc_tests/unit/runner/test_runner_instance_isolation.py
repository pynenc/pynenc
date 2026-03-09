"""
Tests for runner instance isolation across multiple app instances.

Verifies that when multiple Pynenc apps coexist (e.g. with different runner types),
task resolution via Task.from_id does not cause cross-contamination of runner
instances on shared module-level app objects.

Key components:
- Task.from_id must bind tasks to the given app, not the module-level app
- ProcessRunner must not mutate invocation.app.runner
- Pickling/unpickling must preserve correct runner type via config_values
"""

import pickle
from typing import TYPE_CHECKING


from pynenc import Pynenc
from pynenc.builder import PynencBuilder
from pynenc.runner.thread_runner import ThreadRunner
from pynenc.task import Task

if TYPE_CHECKING:
    pass

# ── Module-level app and tasks (simulates a real user module) ──────

_module_app = PynencBuilder().memory().thread_runner().app_id("isolation-test").build()


@_module_app.task
def sample_task(x: int) -> int:
    """Simple task for isolation testing."""
    return x * 2


@_module_app.task
def another_task(y: str) -> str:
    """Second task for multi-lookup testing."""
    return y.upper()


# ── Tests ──────────────────────────────────────────────────────────


def test_from_id_returns_task_bound_to_given_app() -> None:
    """Task resolved via from_id should reference the given app, not the module-level app."""
    other_app = PynencBuilder().memory().thread_runner().app_id("other-app").build()

    resolved = Task.from_id(other_app, sample_task.task_id)

    assert resolved.app is other_app
    assert resolved.app is not _module_app


def test_from_id_caches_in_app_tasks() -> None:
    """Once resolved, the Task should be cached in app._tasks for future lookups."""
    other_app = PynencBuilder().memory().thread_runner().app_id("cache-test").build()

    resolved_first = Task.from_id(other_app, sample_task.task_id)
    resolved_second = Task.from_id(other_app, sample_task.task_id)

    assert resolved_first is resolved_second
    assert sample_task.task_id in other_app._tasks


def test_from_id_preserves_function_reference() -> None:
    """The resolved Task must use the same underlying function."""
    other_app = PynencBuilder().memory().thread_runner().app_id("func-test").build()

    resolved = Task.from_id(other_app, sample_task.task_id)

    assert resolved.func is sample_task.func


def test_from_id_independent_across_apps() -> None:
    """Two different apps resolving the same task_id get independent Task objects."""
    app_a = PynencBuilder().memory().thread_runner().app_id("app-a").build()
    app_b = PynencBuilder().memory().thread_runner().app_id("app-b").build()

    task_a = Task.from_id(app_a, sample_task.task_id)
    task_b = Task.from_id(app_b, sample_task.task_id)

    assert task_a is not task_b
    assert task_a.app is app_a
    assert task_b.app is app_b


def test_setting_runner_on_one_app_does_not_affect_another() -> None:
    """Mutating runner on app_a must not leak to app_b."""
    app_a = PynencBuilder().memory().thread_runner().app_id("runner-a").build()
    app_b = PynencBuilder().memory().thread_runner().app_id("runner-b").build()

    thread_runner_a = ThreadRunner(app_a)
    # app_a.runner is now thread_runner_a (set in BaseRunner.__init__)

    assert app_a._runner_instance is thread_runner_a
    # app_b must be unaffected
    assert app_b._runner_instance is not thread_runner_a


def test_task_from_id_runner_does_not_cross_contaminate() -> None:
    """Accessing runner on a Task resolved via from_id uses the correct app."""
    app_a = PynencBuilder().memory().thread_runner().app_id("iso-a").build()
    app_b = PynencBuilder().memory().thread_runner().app_id("iso-b").build()

    thread_runner_a = ThreadRunner(app_a)

    # Resolve the task on app_b (which has NO explicit runner set)
    task_b = Task.from_id(app_b, sample_task.task_id)

    # task_b.app should be app_b, not app_a
    assert task_b.app is app_b
    assert task_b.app._runner_instance is not thread_runner_a


def test_module_level_app_unaffected_by_from_id() -> None:
    """Resolving tasks via from_id on other apps must not mutate the module-level app."""
    original_runner = _module_app._runner_instance

    other_app = PynencBuilder().memory().thread_runner().app_id("no-mutate").build()
    ThreadRunner(other_app)
    Task.from_id(other_app, sample_task.task_id)

    # Module-level app should be completely unaffected
    assert _module_app._runner_instance is original_runner


def test_pickle_roundtrip_preserves_runner_cls() -> None:
    """After pickle roundtrip, the app's conf.runner_cls should match the original."""
    app = PynencBuilder().memory().thread_runner().app_id("pickle-test").build()

    pickled = pickle.dumps(app)
    restored: Pynenc = pickle.loads(pickled)

    assert restored.conf.runner_cls == "ThreadRunner"


def test_pickle_roundtrip_with_process_runner_config() -> None:
    """ProcessRunner config survives pickle roundtrip."""
    app = Pynenc(config_values={"runner_cls": "ProcessRunner"})

    pickled = pickle.dumps(app)
    restored: Pynenc = pickle.loads(pickled)

    assert restored.conf.runner_cls == "ProcessRunner"


def test_pickle_does_not_carry_runner_instance() -> None:
    """The runner instance itself is NOT pickled — only config determines the type."""
    app = PynencBuilder().memory().thread_runner().app_id("no-inst").build()
    ThreadRunner(app)
    assert app._runner_instance is not None

    pickled = pickle.dumps(app)
    restored: Pynenc = pickle.loads(pickled)

    # Runner instance should NOT survive pickling
    assert restored._runner_instance is None


def test_task_setstate_binds_to_unpickled_app() -> None:
    """When a Task is unpickled, its .app should be the unpickled app, not the module-level one."""
    app = PynencBuilder().memory().thread_runner().app_id("task-pickle").build()
    task = Task.from_id(app, sample_task.task_id)

    pickled = pickle.dumps(task)
    restored_task: Task = pickle.loads(pickled)

    # The restored task's app should be the unpickled version, not _module_app
    assert restored_task.app is not _module_app
    assert restored_task.app.app_id == "task-pickle"
