"""
Regression tests for Starlette 1.x TemplateResponse compatibility.

Starlette 1.x removed the deprecated (name, context_dict) calling convention
for TemplateResponse. Pynmon must use the request-first API:
  TemplateResponse(request, name, context={...})
which is supported by both Starlette 0.28+ and 1.x.

These tests verify that pynmon views render without TypeError on any
supported Starlette version.
"""

import pytest

pytest.importorskip("fastapi", reason="pynmon tests require monitor dependencies")
pytest.importorskip("jinja2", reason="pynmon tests require monitor dependencies")

# All imports below must come after pytest.importorskip calls
# ruff: noqa: E402
import warnings
from typing import TYPE_CHECKING
from unittest.mock import patch

from fastapi.testclient import TestClient

from pynmon.app import app as pynmon_app, setup_routes

setup_routes()

if TYPE_CHECKING:
    from pynenc import Pynenc


def test_home_route_returns_200_no_unhashable_dict(
    app_instance: "Pynenc",
) -> None:
    """GET / returns 200 without TypeError: unhashable type: dict."""
    with patch("pynmon.views.home.get_active_app", return_value=app_instance):
        with patch(
            "pynmon.views.home.get_all_apps",
            return_value={app_instance.app_id: app_instance},
        ):
            client = TestClient(pynmon_app, raise_server_exceptions=True)
            response = client.get("/")

            assert response.status_code == 200
            assert "text/html" in response.headers["content-type"]


def test_home_page_renders_expected_html_content(
    app_instance: "Pynenc",
) -> None:
    """Home page renders HTML with expected dashboard title and app info."""
    with patch("pynmon.views.home.get_active_app", return_value=app_instance):
        with patch(
            "pynmon.views.home.get_all_apps",
            return_value={app_instance.app_id: app_instance},
        ):
            client = TestClient(pynmon_app, raise_server_exceptions=True)
            response = client.get("/")

            content = response.text
            assert "<html" in content
            assert app_instance.app_id in content


def test_template_response_uses_request_first_api(
    app_instance: "Pynenc",
) -> None:
    """Verify no Starlette DeprecationWarning for old-style TemplateResponse."""
    with patch("pynmon.views.home.get_active_app", return_value=app_instance):
        with patch(
            "pynmon.views.home.get_all_apps",
            return_value={app_instance.app_id: app_instance},
        ):
            client = TestClient(pynmon_app, raise_server_exceptions=True)
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                response = client.get("/")

            assert response.status_code == 200
            starlette_deprecations = [
                w
                for w in caught
                if issubclass(w.category, DeprecationWarning)
                and "TemplateResponse" in str(w.message)
            ]
            assert starlette_deprecations == [], (
                f"Old-style TemplateResponse detected: {starlette_deprecations}"
            )
