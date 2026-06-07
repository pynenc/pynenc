"""
Unit tests for pynmon runners view.

Tests runner monitoring endpoints including overview, detail, and atomic service timeline.
"""

from datetime import UTC, datetime, timedelta
import re
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from pynenc.orchestrator.atomic_service import ActiveRunnerInfo
from pynenc.orchestrator.atomic_service import AtomicServiceRun
from pynenc.runner.runner_context import RunnerContext
from pynenc.trigger.monitoring import TriggerRunRecord
from pynenc_tests.conftest import MockPynenc
from pynmon.app import app as pynmon_app
from pynmon.app import setup_routes
from pynmon.views.atomic_service import _get_running_atomic_service_runner

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from pynenc import Pynenc

# Module level app setup
mock_app = MockPynenc()


def _record_atomic_service_execution(
    app: "Pynenc",
    runner_id: str,
    atomic_service_run_id: str,
    start: datetime,
    end: datetime,
    *,
    status_name: str = "COMPLETED",
) -> None:
    """Record an atomic-service execution for timeline fixtures."""
    from pynenc.orchestrator.atomic_service import AtomicServiceExecutionStatus

    atomic_service_run = AtomicServiceRun(
        runner_id=runner_id,
        atomic_service_run_id=atomic_service_run_id,
        started_at=start,
    )
    app.orchestrator.register_runner_heartbeats(
        [runner_id], can_run_atomic_service=True
    )
    app.orchestrator.record_atomic_service_execution_start(atomic_service_run, start)
    app.orchestrator.finalize_atomic_service_execution(
        atomic_service_run,
        end,
        AtomicServiceExecutionStatus[status_name],
    )


def test_running_atomic_service_runner_uses_latest_unfinished_claim(
    app_runners: "Pynenc",
) -> None:
    """Pynmon shows the most recent unfinished atomic-service claim."""
    from pynenc.orchestrator.atomic_service import AtomicServiceExecutionStatus

    now = datetime.now(UTC)
    older_start = now - timedelta(minutes=2)
    latest_start = now - timedelta(seconds=30)
    older = AtomicServiceRun(
        runner_id="older-running",
        atomic_service_run_id="run-older",
        started_at=older_start,
    )
    latest = AtomicServiceRun(
        runner_id="latest-running",
        atomic_service_run_id="run-latest",
        started_at=latest_start,
    )
    app_runners.orchestrator.register_runner_heartbeats(
        ["older-running", "latest-running"],
        can_run_atomic_service=True,
    )
    app_runners.orchestrator.record_atomic_service_execution_start(older, older_start)
    app_runners.orchestrator.finalize_atomic_service_execution(
        older,
        older_start + timedelta(seconds=1),
        AtomicServiceExecutionStatus.COMPLETED,
    )
    app_runners.orchestrator.record_atomic_service_execution_start(latest, latest_start)

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance", return_value=app_runners
        ),
    ):
        result = _get_running_atomic_service_runner(app_runners)

    assert result is not None
    assert result["runner_id"] == "latest-running"


@mock_app.task
def sample_task(x: int) -> int:
    """Simple test task."""
    return x * 2


@pytest.fixture
def app_runners(request: "FixtureRequest", app_instance: "Pynenc") -> "Pynenc":
    """Fixture providing a configured Pynenc app for runners tests."""
    app = app_instance
    app._tasks = mock_app._tasks
    sample_task.app = app
    app.purge()
    request.addfinalizer(app.purge)
    return app


@pytest.fixture
def mock_active_runners() -> list[ActiveRunnerInfo]:
    """Create mock active runner info."""
    now = datetime.now(UTC)
    return [
        ActiveRunnerInfo(
            runner_id="runner-1",
            creation_time=now,
            last_heartbeat=now,
            allow_to_run_atomic_service=True,
        ),
        ActiveRunnerInfo(
            runner_id="runner-2",
            creation_time=now,
            last_heartbeat=now,
            allow_to_run_atomic_service=False,
        ),
    ]


@pytest.fixture
def mock_runner_contexts() -> list[RunnerContext]:
    """Create mock runner contexts."""
    return [
        RunnerContext(
            runner_cls="ThreadRunner",
            runner_id="runner-1",
            hostname="test-host",
            pid=1234,
            thread_id=5678,
        ),
        RunnerContext(
            runner_cls="ProcessRunner",
            runner_id="runner-2",
            hostname="test-host-2",
            pid=9999,
            thread_id=1111,
        ),
    ]


# ################################################################################### #
# RUNNERS OVERVIEW TESTS
# ################################################################################### #


def test_runners_overview_shows_active_runners(
    app_runners: "Pynenc",
    mock_active_runners: list[ActiveRunnerInfo],
    mock_runner_contexts: list[RunnerContext],
) -> None:
    """Test that runners overview displays active runner information."""
    setup_routes()

    # Store runner contexts in state backend
    for ctx in mock_runner_contexts:
        app_runners.state_backend.store_runner_context(ctx)

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance",
            return_value=app_runners,
        ),
        patch.object(
            app_runners.orchestrator,
            "get_active_runners",
            return_value=mock_active_runners,
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

        content = response.text
        assert "Active Runners" in content
        assert "runner-1" in content or "ThreadRunner" in content


def test_runners_overview_shows_statistics(
    app_runners: "Pynenc",
    mock_active_runners: list[ActiveRunnerInfo],
    mock_runner_contexts: list[RunnerContext],
) -> None:
    """Test that runners overview displays correct statistics."""
    setup_routes()

    for ctx in mock_runner_contexts:
        app_runners.state_backend.store_runner_context(ctx)

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance",
            return_value=app_runners,
        ),
        patch.object(
            app_runners.orchestrator,
            "get_active_runners",
            return_value=mock_active_runners,
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/")

        assert response.status_code == 200
        content = response.text

        # Check statistics are displayed
        assert "Total Runners" in content
        assert "Atomic Eligible" in content


def test_runners_refresh_endpoint(
    app_runners: "Pynenc",
    mock_active_runners: list[ActiveRunnerInfo],
    mock_runner_contexts: list[RunnerContext],
) -> None:
    """Test that runners refresh endpoint returns updated data."""
    setup_routes()

    for ctx in mock_runner_contexts:
        app_runners.state_backend.store_runner_context(ctx)

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance",
            return_value=app_runners,
        ),
        patch.object(
            app_runners.orchestrator,
            "get_active_runners",
            return_value=mock_active_runners,
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/refresh")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]


# ################################################################################### #
# RUNNER DETAIL TESTS
# ################################################################################### #


def test_runner_detail_shows_context(
    app_runners: "Pynenc",
    mock_active_runners: list[ActiveRunnerInfo],
    mock_runner_contexts: list[RunnerContext],
) -> None:
    """Test that runner detail page shows runner context information."""
    setup_routes()

    for ctx in mock_runner_contexts:
        app_runners.state_backend.store_runner_context(ctx)

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance",
            return_value=app_runners,
        ),
        patch.object(
            app_runners.orchestrator,
            "get_active_runners",
            return_value=mock_active_runners,
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/runner-1")

        assert response.status_code == 200
        content = response.text

        # Check context information is displayed
        assert "Runner Context" in content
        assert "ThreadRunner" in content or "runner-1" in content
        assert "test-host" in content or "Hostname" in content


def test_runner_detail_shows_heartbeat(
    app_runners: "Pynenc",
    mock_active_runners: list[ActiveRunnerInfo],
    mock_runner_contexts: list[RunnerContext],
) -> None:
    """Test that runner detail page shows heartbeat status."""
    setup_routes()

    for ctx in mock_runner_contexts:
        app_runners.state_backend.store_runner_context(ctx)

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch.object(
            app_runners.orchestrator,
            "get_active_runners",
            return_value=mock_active_runners,
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/runner-1")

        assert response.status_code == 200
        content = response.text

        # Check heartbeat information
        assert "Heartbeat Status" in content


def test_runner_detail_not_found(app_runners: "Pynenc") -> None:
    """Test that requesting non-existent runner returns 404."""
    setup_routes()

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch.object(app_runners.orchestrator, "get_active_runners", return_value=[]),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/nonexistent-runner")

        assert response.status_code == 404


# ################################################################################### #
# ATOMIC SERVICE TIMELINE TESTS
# ################################################################################### #


def test_atomic_service_timeline_shows_executions(
    app_runners: "Pynenc",
    mock_active_runners: list[ActiveRunnerInfo],
) -> None:
    """Test that atomic service timeline displays execution history."""
    setup_routes()
    start = datetime.now(UTC) - timedelta(milliseconds=5)
    _record_atomic_service_execution(
        app_runners,
        "runner-1",
        "as-runner-1",
        start,
        start + timedelta(milliseconds=2),
    )

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance",
            return_value=app_runners,
        ),
        patch.object(
            app_runners.orchestrator,
            "get_active_runners",
            return_value=mock_active_runners,
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/atomic-service/timeline")

        assert response.status_code == 200
        content = response.text

        # Check timeline elements
        assert "Atomic Service Timeline" in content
        assert "Timeline" in content
        assert "/invocations/timeline?" in content
        assert "show_atomic_service=1" in content


def test_atomic_service_timeline_shows_recorded_history(
    app_runners: "Pynenc",
) -> None:
    """The atomic-service page lists more than the active runner's latest window."""
    setup_routes()
    start = datetime.now(UTC) - timedelta(seconds=60)
    app_runners.orchestrator.register_runner_heartbeats(
        ["runner-history"], can_run_atomic_service=True
    )
    _record_atomic_service_execution(
        app_runners,
        "runner-history",
        "as-history-1",
        start,
        start + timedelta(milliseconds=2),
    )
    _record_atomic_service_execution(
        app_runners,
        "runner-history",
        "as-history-2",
        start + timedelta(seconds=30),
        start + timedelta(seconds=30, milliseconds=3),
    )

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance", return_value=app_runners
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/atomic-service/timeline")

    assert response.status_code == 200
    content = response.text
    assert content.count("/invocations/timeline?") == 2
    assert "runner-history" in content


def test_atomic_service_timeline_background_uses_actual_period(
    app_runners: "Pynenc",
) -> None:
    """Tiny runs near the right edge must not be widened into fake overlaps."""
    setup_routes()
    # Use relative offsets so execution times never fall outside the retention window.
    base = datetime.now(UTC) - timedelta(minutes=5)
    older = base
    first_start = base + timedelta(seconds=59, microseconds=695000)
    first_end = base + timedelta(seconds=59, microseconds=706000)
    second_start = base + timedelta(seconds=60, microseconds=91000)
    second_end = base + timedelta(seconds=60, microseconds=125000)
    app_runners.conf.atomic_service_execution_retention_minutes = 60.0 * 48
    app_runners.orchestrator.register_runner_heartbeats(
        ["older", "first", "second"], can_run_atomic_service=True
    )
    _record_atomic_service_execution(
        app_runners,
        "older",
        "as-period-older",
        older,
        older + timedelta(milliseconds=1),
    )
    _record_atomic_service_execution(
        app_runners, "first", "as-period-first", first_start, first_end
    )
    _record_atomic_service_execution(
        app_runners, "second", "as-period-second", second_start, second_end
    )

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance", return_value=app_runners
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/atomic-service/timeline")

    assert response.status_code == 200
    period_styles = [
        (float(start), float(end))
        for start, end in re.findall(
            r"--period-start: ([0-9.]+)%; --period-end: ([0-9.]+)%",
            response.text,
        )
    ]
    assert len(period_styles) == 3
    second_period, first_period, _older_period = period_styles
    assert first_period[1] < second_period[0]
    assert first_period != (99.0, 100.0)
    assert second_period != (99.0, 100.0)


def test_atomic_service_timeline_warns_when_service_is_running(
    app_runners: "Pynenc",
) -> None:
    """The timeline warns when an active runner has no atomic-service end time."""
    setup_routes()
    now = datetime.now(UTC)
    active_runners = [
        ActiveRunnerInfo(
            runner_id="runner-running-service",
            creation_time=now,
            last_heartbeat=now,
            allow_to_run_atomic_service=True,
        )
    ]
    running_run = AtomicServiceRun(
        runner_id="runner-running-service",
        atomic_service_run_id="run-in-progress",
        started_at=now,
    )
    app_runners.orchestrator.register_runner_heartbeats(
        ["runner-running-service"], can_run_atomic_service=True
    )
    app_runners.orchestrator.record_atomic_service_execution_start(running_run, now)

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance",
            return_value=app_runners,
        ),
        patch.object(
            app_runners.orchestrator,
            "get_active_runners",
            return_value=active_runners,
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/atomic-service/timeline")

    assert response.status_code == 200
    content = response.text
    assert "Atomic service in progress." in content
    assert "runner-running-service" in content


def test_atomic_service_timeline_empty_state(app_runners: "Pynenc") -> None:
    """Test atomic service timeline with no execution history."""
    setup_routes()

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance",
            return_value=app_runners,
        ),
        patch.object(app_runners.orchestrator, "get_active_runners", return_value=[]),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/atomic-service/timeline")

        assert response.status_code == 200
        content = response.text

        # Should show empty state message
        assert "Atomic Service Timeline" in content


def test_atomic_service_timeline_filters_by_runner_id(
    app_runners: "Pynenc",
) -> None:
    """The ``runner_id`` URL filter restricts the rendered rows."""
    setup_routes()
    now = datetime.now(UTC)
    app_runners.orchestrator.register_runner_heartbeats(
        ["runner-keep", "runner-skip"], can_run_atomic_service=True
    )
    keep_start = now - timedelta(seconds=20)
    skip_start = now - timedelta(seconds=10)
    _record_atomic_service_execution(
        app_runners,
        "runner-keep",
        "as-keep",
        keep_start,
        keep_start + timedelta(milliseconds=2),
    )
    _record_atomic_service_execution(
        app_runners,
        "runner-skip",
        "as-skip",
        skip_start,
        skip_start + timedelta(milliseconds=2),
    )

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance", return_value=app_runners
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/atomic-service/timeline?runner_id=runner-keep")

    assert response.status_code == 200
    content = response.text
    assert content.count("/invocations/timeline?") == 1
    assert "runner-keep" in content


def test_atomic_service_timeline_filters_by_min_duration(
    app_runners: "Pynenc",
) -> None:
    """The ``min_duration_seconds`` URL filter drops short executions."""
    setup_routes()
    now = datetime.now(UTC)
    app_runners.orchestrator.register_runner_heartbeats(
        ["runner-short", "runner-long"], can_run_atomic_service=True
    )
    short_start = now - timedelta(seconds=20)
    long_start = now - timedelta(seconds=10)
    _record_atomic_service_execution(
        app_runners,
        "runner-short",
        "as-short",
        short_start,
        short_start + timedelta(milliseconds=5),
    )
    _record_atomic_service_execution(
        app_runners,
        "runner-long",
        "as-long",
        long_start,
        long_start + timedelta(seconds=1, milliseconds=500),
    )

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance", return_value=app_runners
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/atomic-service/timeline?min_duration_seconds=1")

    assert response.status_code == 200
    content = response.text
    assert content.count("/invocations/timeline?") == 1
    assert "runner-long" in content


def test_atomic_service_timeline_filters_by_atomic_service_id(
    app_runners: "Pynenc",
) -> None:
    """The atomic-service id filter narrows the rendered execution rows."""
    setup_routes()
    now = datetime.now(UTC)
    app_runners.orchestrator.register_runner_heartbeats(
        ["runner-filter-as"], can_run_atomic_service=True
    )
    find_start = now - timedelta(seconds=20)
    other_start = now - timedelta(seconds=10)
    _record_atomic_service_execution(
        app_runners,
        "runner-filter-as",
        "as-find-me",
        find_start,
        find_start + timedelta(milliseconds=2),
    )
    _record_atomic_service_execution(
        app_runners,
        "runner-filter-as",
        "as-other",
        other_start,
        other_start + timedelta(milliseconds=2),
    )

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance", return_value=app_runners
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get(
            "/runners/atomic-service/timeline?atomic_service_run_id=find-me"
        )

    assert response.status_code == 200
    content = response.text
    assert content.count("/invocations/timeline?") == 1
    assert "as-find-me" in content
    assert "as-other" not in content


def test_atomic_service_timeline_row_shade_uses_status_color(
    app_runners: "Pynenc",
) -> None:
    """The row stripe class follows execution status, not a generic run color."""
    setup_routes()
    now = datetime.now(UTC)
    app_runners.orchestrator.register_runner_heartbeats(
        ["runner-status-shades"], can_run_atomic_service=True
    )
    statuses = ("COMPLETED", "ABANDONED", "BLOCKED")
    for index, status_name in enumerate(statuses):
        start = now - timedelta(seconds=30 - index)
        _record_atomic_service_execution(
            app_runners,
            "runner-status-shades",
            f"as-status-{status_name.lower()}",
            start,
            start + timedelta(milliseconds=2),
            status_name=status_name,
        )

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance", return_value=app_runners
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/atomic-service/timeline")

    assert response.status_code == 200
    content = response.text
    assert "atomic-service-period-row-status-completed" in content
    assert "atomic-service-period-row-status-abandoned" in content
    assert "atomic-service-period-row-status-blocked" in content


def test_atomic_service_timeline_paginates_retained_window(
    app_runners: "Pynenc",
) -> None:
    """The timeline renders pages instead of a fixed latest-only slice."""
    setup_routes()
    base = datetime.now(UTC) - timedelta(minutes=10)
    runner_id = "runner-paginated-as"
    app_runners.orchestrator.register_runner_heartbeats(
        [runner_id], can_run_atomic_service=True
    )
    for index in range(105):
        start = base + timedelta(seconds=index)
        _record_atomic_service_execution(
            app_runners,
            runner_id,
            f"as-page-{index:03d}",
            start,
            start + timedelta(milliseconds=2),
        )

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance", return_value=app_runners
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/atomic-service/timeline?page_size=100&page=2")

    assert response.status_code == 200
    content = response.text
    assert content.count("/invocations/timeline?") == 5
    assert "as-page-000" in content
    assert "as-page-004" in content
    assert "as-page-104" not in content
    assert "Page 2 / 2" in content


def test_atomic_service_timeline_selected_run_opens_its_page(
    app_runners: "Pynenc",
) -> None:
    """A selected atomic-service id is highlighted on its natural page."""
    setup_routes()
    base = datetime.now(UTC) - timedelta(minutes=10)
    runner_id = "runner-selected-as"
    selected_run_id = "as-selected-page-002"
    app_runners.orchestrator.register_runner_heartbeats(
        [runner_id], can_run_atomic_service=True
    )
    for index in range(105):
        start = base + timedelta(seconds=index)
        run_id = selected_run_id if index == 2 else f"as-selected-page-{index:03d}"
        _record_atomic_service_execution(
            app_runners,
            runner_id,
            run_id,
            start,
            start + timedelta(milliseconds=2),
        )

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance", return_value=app_runners
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get(
            "/runners/atomic-service/timeline?"
            f"selected_atomic_service_run_id={selected_run_id}&page_size=100"
        )

    assert response.status_code == 200
    content = response.text
    assert selected_run_id in content
    assert "atomic-service-period-row-selected" in content
    assert "selected" in content
    assert "as-selected-page-104" not in content
    assert "Page 2 / 2" in content


def test_runner_detail_includes_atomic_service_history(
    app_runners: "Pynenc",
) -> None:
    """The runner detail page lists recent atomic-service executions for that runner."""
    setup_routes()
    now = datetime.now(UTC)
    runner_id = "runner-detail-history"
    app_runners.orchestrator.register_runner_heartbeats(
        [runner_id], can_run_atomic_service=True
    )
    older_start = now - timedelta(seconds=20)
    newer_start = now - timedelta(seconds=5)
    _record_atomic_service_execution(
        app_runners,
        runner_id,
        "as-detail-old",
        older_start,
        older_start + timedelta(milliseconds=4),
    )
    _record_atomic_service_execution(
        app_runners,
        runner_id,
        "as-detail-new",
        newer_start,
        newer_start + timedelta(milliseconds=6),
    )

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch(
            "pynmon.views.atomic_service.get_pynenc_instance", return_value=app_runners
        ),
    ):
        client = TestClient(pynmon_app)
        response = client.get(f"/runners/{runner_id}")

    assert response.status_code == 200
    content = response.text
    assert "Recent Atomic Service Runs" in content
    # Two history rows, each rendering a timeline link.
    assert content.count("/invocations/timeline?") >= 2
    # The "View full timeline" link should preselect this runner_id.
    assert f"/runners/atomic-service/timeline?runner_id={runner_id}" in content


def test_atomic_service_run_detail_shows_recorded_execution_and_trigger_refs(
    app_runners: "Pynenc",
) -> None:
    """The AS run detail page links the recorded window to trigger runs."""
    setup_routes()
    now = datetime.now(UTC)
    runner_id = "runner-as-detail"
    atomic_service_run_id = "as-run-recorded"
    trigger_run_id = "trigger-run-for-as"
    app_runners.orchestrator.register_runner_heartbeats(
        [runner_id], can_run_atomic_service=True
    )
    _record_atomic_service_execution(
        app_runners,
        runner_id,
        atomic_service_run_id,
        now - timedelta(seconds=2),
        now - timedelta(seconds=1),
    )
    app_runners.trigger.store_trigger_run(
        TriggerRunRecord(
            trigger_run_id=trigger_run_id,
            trigger_id="trigger-as-detail",
            task_id_key="tests.sample_task",
            logic_value="AND",
            claimed_at=now - timedelta(seconds=2),
            executed_at=now - timedelta(seconds=1),
            atomic_service_run_id=atomic_service_run_id,
            atomic_service_runner_id=runner_id,
        )
    )

    with patch(
        "pynmon.views.atomic_service.get_pynenc_instance", return_value=app_runners
    ):
        client = TestClient(pynmon_app)
        response = client.get(f"/runners/atomic-service/runs/{atomic_service_run_id}")

    assert response.status_code == 200
    content = response.text
    assert "Atomic Service Run" in content
    assert atomic_service_run_id in content
    assert runner_id in content
    assert trigger_run_id in content
    assert "recorded" in content
    assert "/invocations/timeline?" in content


def test_atomic_service_run_detail_shows_trigger_reference_without_history(
    app_runners: "Pynenc",
) -> None:
    """AS ids that only exist on trigger-run records still have a detail page.

    Execution-record retention is now coupled to trigger-run references via
    the orchestrator's purge protection, so this scenario only arises when
    an execution record genuinely never existed. The detail page should
    still render with the trigger-run list and an "unavailable" badge.
    """
    setup_routes()
    now = datetime.now(UTC)
    runner_id = "test-trigger-runner"
    atomic_service_run_id = "as-run-reference-only"
    trigger_run_id = "trigger-run-reference-only"
    app_runners.trigger.store_trigger_run(
        TriggerRunRecord(
            trigger_run_id=trigger_run_id,
            trigger_id="trigger-reference-only",
            task_id_key="tests.sample_task",
            logic_value="AND",
            claimed_at=now - timedelta(milliseconds=5),
            executed_at=now,
            atomic_service_run_id=atomic_service_run_id,
            atomic_service_runner_id=runner_id,
        )
    )

    with patch(
        "pynmon.views.atomic_service.get_pynenc_instance", return_value=app_runners
    ):
        client = TestClient(pynmon_app)
        response = client.get(f"/runners/atomic-service/runs/{atomic_service_run_id}")

    assert response.status_code == 200
    content = response.text
    assert "unavailable" in content
    assert atomic_service_run_id in content
    assert runner_id in content
    assert trigger_run_id in content
