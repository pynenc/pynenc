"""
Unit tests for pynmon state backend view.

Tests state backend monitoring endpoints including overview and purge.
"""

import pytest

pytest.importorskip("fastapi", reason="pynmon tests require monitor dependencies")
pytest.importorskip("jinja2", reason="pynmon tests require monitor dependencies")

# All imports below must come after pytest.importorskip calls
# ruff: noqa: E402

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from pynenc_tests.conftest import MockPynenc
from pynmon.app import app as pynmon_app
from pynmon.app import setup_routes

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest

    from pynenc import Pynenc

# Module level app setup
mock_app = MockPynenc(app_id="test-state-backend-app")


@mock_app.task
def state_task(value: int) -> int:
    """Test task for state backend tests."""
    return value * 2


@pytest.fixture
def app_state_backend(request: "FixtureRequest", app_instance: "Pynenc") -> "Pynenc":
    """Fixture providing a configured Pynenc app for state backend tests."""
    app = app_instance
    app._app_id = mock_app.app_id
    app._tasks = mock_app._tasks
    state_task.app = app
    app.purge()
    request.addfinalizer(app.purge)
    return app


# ################################################################################### #
# STATE BACKEND OVERVIEW TESTS
# ################################################################################### #


def test_state_backend_overview_shows_info(app_state_backend: "Pynenc") -> None:
    """Test that state backend overview displays backend information."""
    setup_routes()

    with patch(
        "pynmon.views.state_backend.get_pynenc_instance", return_value=app_state_backend
    ):
        client = TestClient(pynmon_app)
        response = client.get("/state-backend/")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

        content = response.text
        assert "test-state-backend-app" in content
        assert app_state_backend.state_backend.__class__.__name__ in content


def test_state_backend_overview_shows_type(app_state_backend: "Pynenc") -> None:
    """Test that state backend overview displays the backend type."""
    setup_routes()

    with patch(
        "pynmon.views.state_backend.get_pynenc_instance", return_value=app_state_backend
    ):
        client = TestClient(pynmon_app)
        response = client.get("/state-backend/")

        assert response.status_code == 200
        # Should show the backend type name
        assert app_state_backend.state_backend.__class__.__name__ in response.text


def test_state_backend_overview_handles_error() -> None:
    """Test that state backend overview handles errors gracefully."""
    setup_routes()

    mock_app_error = MagicMock()
    mock_app_error.state_backend = MagicMock()
    mock_app_error.state_backend.__class__.__name__ = "MockStateBackend"
    # Make something raise an error during template rendering
    type(mock_app_error).app_id = property(
        lambda self: (_ for _ in ()).throw(RuntimeError("Backend error"))
    )

    with patch(
        "pynmon.views.state_backend.get_pynenc_instance", return_value=mock_app_error
    ):
        client = TestClient(pynmon_app, raise_server_exceptions=False)
        response = client.get("/state-backend/")

        # Should return error template
        assert response.status_code == 500
        assert "error" in response.text.lower()


# ################################################################################### #
# STATE BACKEND PURGE TESTS
# ################################################################################### #


def test_state_backend_purge_success(app_state_backend: "Pynenc") -> None:
    """Test that purge endpoint returns success response."""
    setup_routes()

    with patch(
        "pynmon.views.state_backend.get_pynenc_instance", return_value=app_state_backend
    ):
        client = TestClient(pynmon_app)
        response = client.post("/state-backend/purge")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "purge" in data["message"].lower()
