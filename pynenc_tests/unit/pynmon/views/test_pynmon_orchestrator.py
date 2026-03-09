"""
Unit tests for pynmon orchestrator view.

Tests orchestrator monitoring endpoints including overview, refresh, and auto-purge.
"""

import pytest

pytest.importorskip("fastapi", reason="pynmon tests require monitor dependencies")
pytest.importorskip("jinja2", reason="pynmon tests require monitor dependencies")

# All imports below must come after pytest.importorskip calls
# ruff: noqa: E402

from typing import TYPE_CHECKING
from unittest.mock import patch

from fastapi.testclient import TestClient

from pynenc_tests.conftest import MockPynenc
from pynmon.app import app as pynmon_app
from pynmon.app import setup_routes

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest

    from pynenc import Pynenc

# Module level app and task setup
mock_app = MockPynenc()


@mock_app.task
def sample_task(x: int) -> int:
    """Simple test task."""
    return x * 2


@mock_app.task
def another_task(msg: str) -> str:
    """Another test task."""
    return f"processed: {msg}"


@pytest.fixture
def app_orchestrator(request: "FixtureRequest", app_instance: "Pynenc") -> "Pynenc":
    """Fixture providing a configured Pynenc app for orchestrator tests."""
    app = app_instance
    app._tasks = mock_app._tasks
    sample_task.app = app
    another_task.app = app
    app.purge()
    request.addfinalizer(app.purge)
    return app


# ################################################################################### #
# ORCHESTRATOR OVERVIEW TESTS
# ################################################################################### #


def test_orchestrator_overview_shows_info(app_orchestrator: "Pynenc") -> None:
    """Test that orchestrator overview displays orchestrator information."""
    setup_routes()

    with patch(
        "pynmon.views.orchestrator.get_pynenc_instance", return_value=app_orchestrator
    ):
        client = TestClient(pynmon_app)
        response = client.get("/orchestrator/")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

        content = response.text
        assert app_orchestrator.orchestrator.__class__.__name__ in content


def test_orchestrator_overview_shows_status_counts(app_orchestrator: "Pynenc") -> None:
    """Test that orchestrator overview shows invocation status counts."""
    # Create an invocation to have some status counts
    sample_task(5)

    setup_routes()

    with patch(
        "pynmon.views.orchestrator.get_pynenc_instance", return_value=app_orchestrator
    ):
        client = TestClient(pynmon_app)
        response = client.get("/orchestrator/")

        assert response.status_code == 200
        content = response.text

        # Should show status names
        assert "PENDING" in content or "pending" in content.lower()


def test_orchestrator_overview_shows_active_runners(
    app_orchestrator: "Pynenc",
) -> None:
    """Test that orchestrator overview shows active runner count."""
    setup_routes()

    with patch(
        "pynmon.views.orchestrator.get_pynenc_instance", return_value=app_orchestrator
    ):
        client = TestClient(pynmon_app)
        response = client.get("/orchestrator/")

        assert response.status_code == 200
        # Page should render successfully
        assert "Orchestrator" in response.text or "orchestrator" in response.text


# ################################################################################### #
# REFRESH ENDPOINT TESTS
# ################################################################################### #


def test_orchestrator_refresh_returns_partial(app_orchestrator: "Pynenc") -> None:
    """Test that refresh endpoint returns partial HTML for HTMX."""
    setup_routes()

    with patch(
        "pynmon.views.orchestrator.get_pynenc_instance", return_value=app_orchestrator
    ):
        client = TestClient(pynmon_app)
        response = client.get("/orchestrator/refresh")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]


def test_orchestrator_refresh_with_invocations(app_orchestrator: "Pynenc") -> None:
    """Test refresh endpoint with existing invocations."""
    # Create invocations
    sample_task(10)
    another_task("test")

    setup_routes()

    with patch(
        "pynmon.views.orchestrator.get_pynenc_instance", return_value=app_orchestrator
    ):
        client = TestClient(pynmon_app)
        response = client.get("/orchestrator/refresh")

        assert response.status_code == 200


# ################################################################################### #
# AUTO-PURGE ENDPOINT TESTS
# ################################################################################### #


def test_orchestrator_auto_purge_success(app_orchestrator: "Pynenc") -> None:
    """Test that auto-purge endpoint returns success response."""
    setup_routes()

    with patch(
        "pynmon.views.orchestrator.get_pynenc_instance", return_value=app_orchestrator
    ):
        client = TestClient(pynmon_app)
        response = client.post("/orchestrator/auto-purge")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "purge" in data["message"].lower()
