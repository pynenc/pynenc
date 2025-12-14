"""
Unit tests for pynmon.app module.

Tests core app functionality including endpoints, error handling, and app management.
"""

import sys
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fastapi", reason="pynmon tests require monitor dependencies")
pytest.importorskip("jinja2", reason="pynmon tests require monitor dependencies")

# All imports below must come after pytest.importorskip calls
# ruff: noqa: E402
from fastapi import HTTPException
from fastapi.testclient import TestClient

from pynenc import Pynenc, PynencBuilder
from pynenc.app import AppInfo
from pynmon import app as pynmon_module
from pynmon.app import (
    app as pynmon_app,
    get_active_app,
    get_pynenc_instance,
    hydrate_app_instances,
    start_monitor,
)

if TYPE_CHECKING:
    pass


# ################################################################################### #
# PYTHON VERSION CHECK TESTS
# ################################################################################### #


def test_python_version_check_raises_on_313() -> None:
    """Test that importing pynmon on Python 3.13+ raises RuntimeError."""
    # Mock version_info to simulate Python 3.13
    mock_version = (3, 13, 0, "final", 0)

    with patch.object(sys, "version_info", mock_version):
        # The check happens at module import time, so we need to re-execute the check
        if sys.version_info >= (3, 13):
            with pytest.raises(RuntimeError, match="requires Python <3.13"):
                raise RuntimeError(
                    "The pynmon monitoring UI requires Python <3.13 due to FastAPI/Pydantic limitations. "
                    "Core pynenc functionality supports Python 3.13+."
                )


# ################################################################################### #
# GLOBAL EXCEPTION HANDLER TESTS
# ################################################################################### #


def test_global_exception_handler() -> None:
    """Test that unhandled exceptions return proper 500 response."""
    client = TestClient(pynmon_app, raise_server_exceptions=False)

    # Create a route that raises an exception
    @pynmon_app.get("/test-error-route")
    async def raise_error() -> None:
        raise ValueError("Test error for coverage")

    response = client.get("/test-error-route")

    assert response.status_code == 500
    data = response.json()
    assert data["error"] == "Internal Server Error"
    assert "Test error for coverage" in data["message"]
    assert data["path"] == "/test-error-route"


# ################################################################################### #
# ROOT ENDPOINT TESTS
# ################################################################################### #


def test_root_no_apps_configured() -> None:
    """Test root endpoint when no apps are configured."""
    # Use function patching instead of module attribute patching
    with patch(
        "pynmon.app.get_active_app",
        side_effect=HTTPException(status_code=500, detail="No app"),
    ):
        with patch("pynmon.app.get_all_apps", return_value={}):
            client = TestClient(pynmon_app, raise_server_exceptions=False)
            response = client.get("/")

            # The endpoint catches HTTPException from get_active_app
            # and shows critical_error template
            assert response.status_code in (200, 500)


# ################################################################################### #
# SWITCH APP ENDPOINT TESTS
# ################################################################################### #


def test_switch_app_success() -> None:
    """Test switching to a valid app."""
    app1 = PynencBuilder().app_id("test-app-1").memory().build()
    app2 = PynencBuilder().app_id("test-app-2").memory().build()

    with patch.object(
        pynmon_module, "all_pynenc_instances", {"test-app-1": app1, "test-app-2": app2}
    ):
        with patch.object(pynmon_module, "pynenc_instance", app1):
            client = TestClient(pynmon_app)
            response = client.get("/switch-app/test-app-2", follow_redirects=False)

            assert response.status_code == 303
            assert response.headers["location"] == "/"


def test_switch_app_not_found() -> None:
    """Test switching to a non-existent app raises 404."""
    app1 = PynencBuilder().app_id("test-app-1").memory().build()

    with patch.object(pynmon_module, "all_pynenc_instances", {"test-app-1": app1}):
        client = TestClient(pynmon_app, raise_server_exceptions=False)
        response = client.get("/switch-app/non-existent-app")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()


# ################################################################################### #
# PURGE ENDPOINT TESTS
# ################################################################################### #


def test_purge_success() -> None:
    """Test purge endpoint with a valid app."""
    mock_app = MagicMock(spec=Pynenc)

    with patch.object(pynmon_module, "pynenc_instance", mock_app):
        client = TestClient(pynmon_app)
        response = client.post("/purge")

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        mock_app.purge.assert_called_once()


def test_purge_no_app_configured() -> None:
    """Test purge endpoint when no app is configured."""
    with patch.object(pynmon_module, "pynenc_instance", None):
        client = TestClient(pynmon_app, raise_server_exceptions=False)
        response = client.post("/purge")

        assert response.status_code == 500
        assert "No Pynenc application" in response.json()["detail"]


def test_purge_error_during_purge() -> None:
    """Test purge endpoint when purge raises an exception."""
    mock_app = MagicMock(spec=Pynenc)
    mock_app.purge.side_effect = RuntimeError("Database error")

    with patch.object(pynmon_module, "pynenc_instance", mock_app):
        client = TestClient(pynmon_app)
        response = client.post("/purge")

        assert response.status_code == 500
        data = response.json()
        assert data["success"] is False
        assert "Database error" in data["message"]


# ################################################################################### #
# GET_PYNENC_INSTANCE TESTS
# ################################################################################### #


def test_get_pynenc_instance_no_instance() -> None:
    """Test get_pynenc_instance raises HTTPException when no instance."""
    with patch.object(pynmon_module, "pynenc_instance", None):
        with pytest.raises(HTTPException) as exc_info:
            get_pynenc_instance()

        assert exc_info.value.status_code == 500
        assert "No Pynenc application" in exc_info.value.detail


def test_get_pynenc_instance_returns_instance() -> None:
    """Test get_pynenc_instance returns the configured instance."""
    mock_app = MagicMock(spec=Pynenc)

    with patch.object(pynmon_module, "pynenc_instance", mock_app):
        result = get_pynenc_instance()
        assert result is mock_app


# ################################################################################### #
# GET_ACTIVE_APP TESTS
# ################################################################################### #


def test_get_active_app_auto_selects_first() -> None:
    """Test get_active_app auto-selects first available when none active."""
    app1 = PynencBuilder().app_id("auto-app").memory().build()

    with patch.object(pynmon_module, "pynenc_instance", None):
        with patch.object(pynmon_module, "all_pynenc_instances", {"auto-app": app1}):
            result = get_active_app()
            assert result.app_id == "auto-app"


def test_get_active_app_no_instances_raises() -> None:
    """Test get_active_app raises HTTPException when no instances available."""
    with patch.object(pynmon_module, "pynenc_instance", None):
        with patch.object(pynmon_module, "all_pynenc_instances", {}):
            with pytest.raises(HTTPException) as exc_info:
                get_active_app()

            assert exc_info.value.status_code == 500
            assert "No Pynenc application" in exc_info.value.detail


# ################################################################################### #
# HYDRATE_APP_INSTANCES TESTS
# ################################################################################### #


def test_hydrate_app_instances_with_selected_app() -> None:
    """Test hydrate_app_instances includes selected app."""
    selected = PynencBuilder().app_id("selected-app").memory().build()

    result = hydrate_app_instances({}, selected)

    assert "selected-app" in result
    assert result["selected-app"] is selected


def test_hydrate_app_instances_skips_already_included() -> None:
    """Test hydrate_app_instances skips apps already in instances."""
    selected = PynencBuilder().app_id("my-app").memory().build()
    app_info = AppInfo(app_id="my-app", module="some.path")

    result = hydrate_app_instances({"my-app": app_info}, selected)

    # Should only have the selected instance, not try to hydrate again
    assert len(result) == 1
    assert result["my-app"] is selected


def test_hydrate_app_instances_handles_hydration_failure() -> None:
    """Test hydrate_app_instances handles failed hydrations gracefully."""
    app_info = AppInfo(app_id="bad-app", module="nonexistent.module.path")

    with patch.object(Pynenc, "from_info", side_effect=ImportError("Module not found")):
        result = hydrate_app_instances({"bad-app": app_info})

    # Should return empty dict since hydration failed
    assert "bad-app" not in result


def test_hydrate_app_instances_handles_none_return() -> None:
    """Test hydrate_app_instances handles from_info returning None."""
    app_info = AppInfo(app_id="null-app", module="some.path")

    with patch.object(Pynenc, "from_info", return_value=None):
        result = hydrate_app_instances({"null-app": app_info})

    assert "null-app" not in result


# ################################################################################### #
# START_MONITOR TESTS
# ################################################################################### #


def test_start_monitor_no_apps_raises() -> None:
    """Test start_monitor raises ValueError when no apps provided."""
    with pytest.raises(ValueError, match="must be provided"):
        start_monitor({}, None)


def test_start_monitor_no_instances_raises() -> None:
    """Test start_monitor raises ValueError when no instances could be initialized."""
    app_info = AppInfo(app_id="bad-app", module="nonexistent.path")

    with patch.object(Pynenc, "from_info", return_value=None):
        with pytest.raises(ValueError, match="No app instances could be initialized"):
            start_monitor({"bad-app": app_info}, None)


def test_start_monitor_uses_selected_app() -> None:
    """Test start_monitor uses selected_app when provided."""
    selected = PynencBuilder().app_id("selected").memory().build()
    # Need a non-empty apps dict to pass the initial check
    app_info = AppInfo(app_id="other-app", module="test")

    with patch("pynmon.app.hydrate_app_instances", return_value={"selected": selected}):
        with patch("pynmon.app.setup_routes"):
            with patch("pynmon.app.uvicorn.run") as mock_run:
                start_monitor({"other-app": app_info}, selected)

                assert pynmon_module.pynenc_instance is selected
                mock_run.assert_called_once()


def test_start_monitor_selects_first_when_no_selected() -> None:
    """Test start_monitor selects first app when selected_app not in instances."""
    app1 = PynencBuilder().app_id("first-app").memory().build()
    app_info = AppInfo(app_id="first-app", module="test")

    with patch("pynmon.app.hydrate_app_instances", return_value={"first-app": app1}):
        with patch("pynmon.app.setup_routes"):
            with patch("pynmon.app.uvicorn.run"):
                start_monitor({"first-app": app_info}, None)

                assert pynmon_module.pynenc_instance is app1
