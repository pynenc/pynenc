"""
Integration test for app_id isolation across all persistent backends.

Verifies that two apps sharing the same backing store (same SQLite file)
with different app_ids have fully isolated data: invocations, broker
queues, and purge operations do not leak across app boundaries.

Uses the ``app_instance`` fixture so plugins can reuse this test
via ``all_tests.py`` with their own backends.
"""

import threading
from typing import TYPE_CHECKING

from pynenc import Pynenc
from pynenc.runner import ThreadRunner
from pynenc_tests.conftest import MockPynenc

if TYPE_CHECKING:
    pass

mock_app = MockPynenc()


@mock_app.task
def get_app_id_a() -> str:
    return get_app_id_a.app.app_id


@mock_app.task
def get_app_id_b() -> str:
    return get_app_id_b.app.app_id


def _build_sibling_app(app: Pynenc, sibling_app_id: str) -> Pynenc:
    """Build a second app sharing the same backend but with a different app_id."""
    cfg = dict(app.config_values) if app.config_values else {}
    cfg["app_id"] = sibling_app_id
    sibling = Pynenc(config_values=cfg)
    sibling.runner = ThreadRunner(sibling)
    return sibling


def test_isolation_should_execute_tasks_independently(
    app_instance: Pynenc,
) -> None:
    """Two apps sharing the same backend must execute tasks in complete isolation."""
    app_a = app_instance
    app_a.runner = ThreadRunner(app_a)
    app_b = _build_sibling_app(app_a, "sibling_exec")

    get_app_id_a.app = app_a
    get_app_id_b.app = app_b

    inv_a = get_app_id_a()
    inv_b = get_app_id_b()

    thread_a = threading.Thread(target=app_a.runner.run, daemon=True)
    thread_b = threading.Thread(target=app_b.runner.run, daemon=True)
    thread_a.start()
    thread_b.start()

    try:
        assert inv_a.result == app_a.app_id
        assert inv_b.result == app_b.app_id

        # Each app only sees its own invocations
        ids_a = list(app_a.orchestrator.get_task_invocation_ids(get_app_id_a.task_id))
        ids_b = list(app_b.orchestrator.get_task_invocation_ids(get_app_id_b.task_id))
        assert len(ids_a) == 1
        assert len(ids_b) == 1

        # Cross-check: neither sees the other's data
        assert (
            len(list(app_a.orchestrator.get_task_invocation_ids(get_app_id_b.task_id)))
            == 0
        )
        assert (
            len(list(app_b.orchestrator.get_task_invocation_ids(get_app_id_a.task_id)))
            == 0
        )
    finally:
        app_a.runner.stop_runner_loop()
        app_b.runner.stop_runner_loop()
        app_a.purge()
        app_b.purge()


def test_isolation_purge_should_not_affect_sibling_results(
    app_instance: Pynenc,
) -> None:
    """Purging one app after execution must not destroy the sibling's results."""
    app_a = app_instance
    app_a.runner = ThreadRunner(app_a)
    app_b = _build_sibling_app(app_a, "sibling_purge_exec")

    get_app_id_a.app = app_a
    get_app_id_b.app = app_b

    inv_a = get_app_id_a()
    inv_b = get_app_id_b()

    thread_a = threading.Thread(target=app_a.runner.run, daemon=True)
    thread_b = threading.Thread(target=app_b.runner.run, daemon=True)
    thread_a.start()
    thread_b.start()

    try:
        assert inv_a.result == app_a.app_id
        assert inv_b.result == app_b.app_id

        app_a.runner.stop_runner_loop()

        # Purge app_a completely
        app_a.purge()

        # app_a is empty
        assert app_a.broker.count_invocations() == 0
        assert (
            len(list(app_a.orchestrator.get_task_invocation_ids(get_app_id_a.task_id)))
            == 0
        )

        # app_b still has its data
        assert (
            len(list(app_b.orchestrator.get_task_invocation_ids(get_app_id_b.task_id)))
            == 1
        )
    finally:
        app_b.runner.stop_runner_loop()
        app_a.purge()
        app_b.purge()
