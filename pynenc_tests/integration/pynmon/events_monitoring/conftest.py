"""Shared helpers for focused Pynmon event-monitoring integration tests."""

from __future__ import annotations

import os
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

import pytest

from pynenc.builder import PynencBuilder
from pynenc.runner.thread_runner import ThreadRunner
from pynenc_tests.conftest import (
    check_all_status_transitions,
    check_no_atomic_service_overlap,
)

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest

    from pynenc import Pynenc
    from pynenc.trigger.monitoring import EventRecord
    from pynenc_tests.integration.pynmon.conftest import PynmonClient


def build_monitoring_app(app_id: str) -> tuple[Pynenc, str]:
    """Build a small SQLite-backed app for one event-monitoring module."""
    temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    temp_db_path = temp_db.name
    temp_db.close()
    app = (
        PynencBuilder()
        .sqlite(temp_db_path)
        .thread_runner()
        .runner_tuning(
            runner_loop_sleep_time_sec=0.01,
            invocation_wait_results_sleep_time_sec=0.01,
        )
        .app_id(app_id)
        .build()
    )
    app.conf.atomic_service_interval_minutes = 0.01
    app.conf.atomic_service_check_interval_minutes = 0.005
    app.conf.atomic_service_spread_margin_minutes = 0.0
    app.conf.atomic_service_max_start_slot_fraction = 1.0
    return app, temp_db_path


@pytest.fixture(scope="module", autouse=True)
def cleanup_monitoring_app_db(request: FixtureRequest) -> Iterator[None]:
    """Remove the temp SQLite database owned by the importing module."""
    yield
    temp_db_path = getattr(request.module, "TEMP_DB_PATH", None)
    if temp_db_path and os.path.exists(temp_db_path):
        try:
            os.unlink(temp_db_path)
        except OSError:
            pass


class EventMonitoringHarness:
    """Small runtime helper for tests that need real runners and Pynmon."""

    def __init__(self, app: Pynenc) -> None:
        self.app = app
        self._lock = threading.RLock()

    @contextmanager
    def running(self, runner_count: int = 1) -> Iterator[None]:
        """Purge/register/start runners, then stop and validate transitions."""
        self.app.purge()
        self.app.register_deferred_triggers()
        runners = self._start_runners(runner_count)
        try:
            yield
        finally:
            for runner in runners:
                runner.stop_runner_loop()
            check_all_status_transitions(self.app)
            check_no_atomic_service_overlap(self.app)

    def emit_event(self, event_code: str, payload: dict[str, Any]) -> str:
        """Emit an event through the normal trigger API."""
        with self._lock:
            return self.app.trigger.emit_event(event_code, payload)

    def wait_for_event_count(
        self, event_code: str, expected_count: int = 1, timeout_seconds: float = 20
    ) -> None:
        """Wait until real runners have produced the expected event count."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            count = self.app.trigger.count_events(event_code=event_code)
            if count >= expected_count:
                return
            time.sleep(0.02)
        count = self.app.trigger.count_events(event_code=event_code)
        raise AssertionError(
            f"Timed out waiting for {expected_count} {event_code} events (got {count})"
        )

    def event_by_code(self, event_code: str) -> EventRecord:
        """Return the newest event for ``event_code``."""
        events = self.app.trigger.get_events(event_code=event_code, limit=1)
        if not events:
            raise AssertionError(f"Expected event not found: {event_code}")
        return events[0]

    def _start_runners(self, runner_count: int) -> list[ThreadRunner]:
        runners: list[ThreadRunner] = []
        for _index in range(runner_count):
            runner = ThreadRunner(self.app)
            thread = threading.Thread(target=runner.run, daemon=True)
            thread.start()
            runners.append(runner)
        return runners


class PynmonEventAssertions:
    """Assertions shared by the small event-monitoring HTTP tests."""

    def event_list_contains(self, pynmon_client: PynmonClient, event_code: str) -> None:
        response = pynmon_client.get(f"/events?event_code={event_code}&page_size=10")
        assert response.status_code == 200
        assert event_code in response.text

    def invocation_triggered_by_context(
        self,
        pynmon_client: PynmonClient,
        invocation_id: str,
        context_type: str,
    ) -> dict[str, Any]:
        response = pynmon_client.get(f"/invocations/{invocation_id}/api")
        assert response.status_code == 200
        triggered_by = response.json()["triggered_by"]
        assert triggered_by is not None
        context_types = {p["context_type"] for p in triggered_by["participants"]}
        assert context_type in context_types
        return triggered_by


@pytest.fixture
def event_monitor() -> PynmonEventAssertions:
    """Expose Pynmon event-monitoring HTTP assertions."""
    return PynmonEventAssertions()
