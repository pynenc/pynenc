"""
Minimal tests for pynmon home view.

Tests basic functionality using real in-memory Pynenc app.
"""

# Skip all pynmon tests if monitor dependencies are not available
import pytest

pytest.importorskip("fastapi", reason="pynmon tests require monitor dependencies")
pytest.importorskip("jinja2", reason="pynmon tests require monitor dependencies")

# All imports below must come after pytest.importorskip calls
# ruff: noqa: E402

from unittest.mock import patch

from fastapi.testclient import TestClient

from pynenc import Pynenc
from pynmon.app import app as pynmon_app


def test_home_displays_app_info() -> None:
    """Test that home page displays basic app information."""
    # Create real in-memory Pynenc app
    test_app = Pynenc(app_id="test-home-app")

    # Patch pynmon to use our test app
    with patch("pynmon.app.get_active_app", return_value=test_app):
        with patch("pynmon.app.get_all_apps", return_value={"test-home-app": test_app}):
            client = TestClient(pynmon_app)
            response = client.get("/")

            assert response.status_code == 200
            assert "text/html" in response.headers["content-type"]

            # Check that app info is in the response
            content = response.text
            assert "test-home-app" in content


def test_home_displays_multiple_apps() -> None:
    """Test that home page can display multiple apps."""
    # Create multiple real in-memory Pynenc apps
    app1 = Pynenc(app_id="app-one")
    app2 = Pynenc(app_id="app-two")

    all_apps = {"app-one": app1, "app-two": app2}

    # Patch pynmon to use our test apps
    with patch("pynmon.app.get_active_app", return_value=app1):
        with patch("pynmon.app.get_all_apps", return_value=all_apps):
            client = TestClient(pynmon_app)
            response = client.get("/")

            assert response.status_code == 200

            # Check that both apps are mentioned in the response
            content = response.text
            assert "app-one" in content
            assert "app-two" in content
