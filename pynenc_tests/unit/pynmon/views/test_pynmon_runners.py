"""
Unit tests for pynmon runners view.

Tests runner monitoring endpoints including overview, detail, and atomic service timeline.
"""

import pytest

pytest.importorskip("fastapi", reason="pynmon tests require monitor dependencies")
pytest.importorskip("jinja2", reason="pynmon tests require monitor dependencies")

# All imports below must come after pytest.importorskip calls
# ruff: noqa: E402

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import patch

from fastapi.testclient import TestClient

from pynenc.orchestrator.atomic_service import ActiveRunnerInfo
from pynenc.runner.runner_context import RunnerContext
from pynenc_tests.conftest import MockPynenc
from pynmon.app import app as pynmon_app
from pynmon.app import setup_routes

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from pynenc import Pynenc

# Module level app setup
mock_app = MockPynenc(app_id="test-runners-app")


@mock_app.task
def sample_task(x: int) -> int:
    """Simple test task."""
    return x * 2


@pytest.fixture
def app_runners(request: "FixtureRequest", app_instance: "Pynenc") -> "Pynenc":
    """Fixture providing a configured Pynenc app for runners tests."""
    app = app_instance
    app._app_id = mock_app.app_id
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
            last_service_start=now,
            last_service_end=now,
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
        assert "test-runners-app" in content
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

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
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


def test_atomic_service_timeline_empty_state(app_runners: "Pynenc") -> None:
    """Test atomic service timeline with no execution history."""
    setup_routes()

    with (
        patch("pynmon.views.runners.get_pynenc_instance", return_value=app_runners),
        patch.object(app_runners.orchestrator, "get_active_runners", return_value=[]),
    ):
        client = TestClient(pynmon_app)
        response = client.get("/runners/atomic-service/timeline")

        assert response.status_code == 200
        content = response.text

        # Should show empty state message
        assert "Atomic Service Timeline" in content
