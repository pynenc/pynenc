"""
Tests for app_id isolation across all persistent backends.

Uses the ``app_instance`` fixture (memory + sqlite) so plugins can reuse
this test via ``all_tests.py`` with their own backends.
"""

from pynenc import Pynenc
from pynenc_tests.conftest import MockPynenc


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
    return Pynenc(config_values=cfg)


def test_app_id_isolation_should_separate_data_between_apps(
    app_instance: Pynenc,
) -> None:
    """Two apps sharing the same backend must not see each other's data."""
    app_a = app_instance
    app_b = _build_sibling_app(app_a, "sibling_isolation")

    get_app_id_a.app = app_a
    get_app_id_b.app = app_b

    try:
        # Create one invocation in each app
        get_app_id_a()
        get_app_id_b()

        # Each app sees only its own
        assert (
            len(list(app_a.orchestrator.get_task_invocation_ids(get_app_id_a.task_id)))
            == 1
        )
        assert (
            len(list(app_b.orchestrator.get_task_invocation_ids(get_app_id_b.task_id)))
            == 1
        )

        # Cross-check: neither sees the other's data
        assert (
            len(list(app_a.orchestrator.get_task_invocation_ids(get_app_id_b.task_id)))
            == 0
        )
        assert (
            len(list(app_b.orchestrator.get_task_invocation_ids(get_app_id_a.task_id)))
            == 0
        )

        assert app_a.broker.count_invocations() >= 1
        assert app_b.broker.count_invocations() >= 1
    finally:
        app_a.purge()
        app_b.purge()


def test_app_id_purge_should_not_affect_sibling_app(
    app_instance: Pynenc,
) -> None:
    """Purging one app must leave the sibling's data intact."""
    app_a = app_instance
    app_b = _build_sibling_app(app_a, "sibling_purge")

    get_app_id_a.app = app_a
    get_app_id_b.app = app_b

    try:
        get_app_id_a()
        get_app_id_b()

        # Purge app_a
        app_a.purge()

        # app_a is empty
        assert (
            len(list(app_a.orchestrator.get_task_invocation_ids(get_app_id_a.task_id)))
            == 0
        )
        assert app_a.broker.count_invocations() == 0

        # app_b still intact
        assert (
            len(list(app_b.orchestrator.get_task_invocation_ids(get_app_id_b.task_id)))
            == 1
        )
        assert app_b.broker.count_invocations() >= 1
    finally:
        app_a.purge()
        app_b.purge()
