"""
Unit tests for pynmon arg cache view.

Tests argument cache monitoring endpoints including overview and purge.
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
mock_app = MockPynenc()


@mock_app.task
def cache_task(data: str) -> str:
    """Test task for arg cache tests."""
    return f"processed: {data}"


@pytest.fixture
def app_client_data(request: "FixtureRequest", app_instance: "Pynenc") -> "Pynenc":
    """Fixture providing a configured Pynenc app for arg cache tests."""
    app = app_instance
    app._tasks = mock_app._tasks
    cache_task.app = app
    app.purge()
    request.addfinalizer(app.purge)
    return app


# ################################################################################### #
# ARG CACHE OVERVIEW TESTS
# ################################################################################### #


def test_client_data_overview_shows_info(app_client_data: "Pynenc") -> None:
    """Test that arg cache overview displays cache information."""
    setup_routes()

    with patch(
        "pynmon.views.client_data_store.get_pynenc_instance",
        return_value=app_client_data,
    ):
        client = TestClient(pynmon_app)
        response = client.get("/client-data-store/")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

        content = response.text
        assert app_client_data.client_data_store.__class__.__name__ in content


def test_client_data_overview_shows_config(app_client_data: "Pynenc") -> None:
    """Test that arg cache overview displays configuration values."""
    setup_routes()

    with patch(
        "pynmon.views.client_data_store.get_pynenc_instance",
        return_value=app_client_data,
    ):
        client = TestClient(pynmon_app)
        response = client.get("/client-data-store/")

        assert response.status_code == 200
        content = response.text

        # Should show configuration info (template uses human-readable labels)
        assert "Min Size to Externalize" in content or "Local LRU Cache" in content


def test_client_data_overview_handles_error() -> None:
    """Test that arg cache overview handles errors gracefully."""
    setup_routes()

    mock_app_error = MagicMock()
    mock_app_error.client_data_store = MagicMock()
    mock_app_error.client_data_store.__class__.__name__ = "MockClientDataStore"
    mock_app_error.client_data_store.conf.min_size_to_cache = 100
    mock_app_error.client_data_store.conf.local_cache_size = 50
    # Make something raise an error during template rendering
    type(mock_app_error).app_id = property(
        lambda self: (_ for _ in ()).throw(RuntimeError("App error"))
    )

    with patch(
        "pynmon.views.client_data_store.get_pynenc_instance",
        return_value=mock_app_error,
    ):
        client = TestClient(pynmon_app, raise_server_exceptions=False)
        response = client.get("/client-data-store/")

        # Should return error template
        assert response.status_code == 500
        assert "error" in response.text.lower()


# ################################################################################### #
# ARG CACHE PURGE TESTS
# ################################################################################### #


def test_client_data_purge_success(app_client_data: "Pynenc") -> None:
    """Test that purge endpoint returns success response."""
    setup_routes()

    with patch(
        "pynmon.views.client_data_store.get_pynenc_instance",
        return_value=app_client_data,
    ):
        client = TestClient(pynmon_app)
        response = client.post(
            "/client-data-store/purge", headers={"origin": "http://testserver"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "purge" in data["message"].lower()
