"""
Fixtures for pynmon integration tests.

This module provides fixtures for testing pynmon with real backend.
Each test module defines its own app and tasks. The fixtures here start
runners and set up pynmon clients that use the app from the test module.

Note: Pynmon requires Python <3.13 due to FastAPI/Pydantic v2 dependencies.
"""

from logging import Logger
import os
import socket
import sys
import threading
import time
from typing import TYPE_CHECKING

import pytest

from pynenc_tests.util.log import create_test_logger

# Skip all pynmon tests on Python 3.13+
if sys.version_info >= (3, 13):
    pytest.skip(
        "Pynmon tests skipped: requires Python <3.13 (FastAPI/Pydantic limitation)",
        allow_module_level=True,
    )

pytest.importorskip("fastapi", reason="pynmon tests require monitor dependencies")
pytest.importorskip("jinja2", reason="pynmon tests require monitor dependencies")

# All imports below must come after pytest.importorskip calls
# ruff: noqa: E402

if TYPE_CHECKING:
    from collections.abc import Generator
    from typing import Any

    from _pytest.fixtures import FixtureRequest

    from pynenc import Pynenc

import requests
import uvicorn

from pynmon.app import _uvicorn_log_config
from pynmon.app import all_pynenc_instances
from pynmon.app import app as pynmon_app
from pynmon.app import pynenc_instance

logger: Logger = create_test_logger("conftest")


class PynmonClient:
    """HTTP client for making requests to the actual pynmon server."""

    def __init__(self, base_url: str) -> None:
        """
        Initialize the pynmon client.

        :param str base_url: Base URL for the pynmon server
        """
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def get(self, path: str) -> requests.Response:
        """
        Make a GET request to the pynmon server.

        :param str path: URL path to request
        :return: HTTP response
        """
        url = f"{self.base_url}{path}"
        return self.session.get(url)

    def post(self, path: str, **kwargs: "Any") -> requests.Response:
        """
        Make a POST request to the pynmon server.

        :param str path: URL path to request
        :param kwargs: Additional arguments for the POST request
        :return: HTTP response
        """
        url = f"{self.base_url}{path}"
        return self.session.post(url, **kwargs)


@pytest.fixture
def pynmon_client(pynmon_server: str) -> "Generator[PynmonClient, None, None]":
    """
    Create an HTTP client for making requests to the actual pynmon server.

    Uses the real server started by pynmon_server fixture to make HTTP requests.
    This ensures all tests use the actual server for browser-accessible debugging.

    :param str pynmon_server: The server URL from pynmon_server fixture
    :return: HTTP client configured to use the server
    """
    client = PynmonClient(pynmon_server)
    yield client


def get_free_port() -> int:
    """
    Find a free port on localhost.

    :return: Available port number
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture
def pynmon_server(request: "FixtureRequest") -> "Generator[str, None, None]":
    """
    Start a real HTTP server for pynmon that can be accessed in a browser.

    This fixture starts an actual uvicorn server in a separate thread,
    making pynmon accessible for debugging purposes.
    All tests use this server to ensure browser accessibility.

    :param FixtureRequest request: The pytest request object to access the calling module
    :return: The server URL
    """
    # Get the app from the test module
    test_app: Pynenc = request.module.app

    # Store original global state
    original_all_instances = all_pynenc_instances.copy()
    original_instance = pynenc_instance

    # Set up pynmon global variables
    all_pynenc_instances.clear()
    all_pynenc_instances[test_app.app_id] = test_app

    # Set the active instance
    import pynmon.app

    pynmon.app.pynenc_instance = test_app

    # Ensure routes are set up
    pynmon.app.setup_routes()

    port = get_free_port()
    logger.warning(f"Starting pynmon server on port {port}")

    # Configure uvicorn server
    config = uvicorn.Config(
        app=pynmon_app,
        host="127.0.0.1",
        port=port,
        log_level="info",
        access_log=True,
        log_config=_uvicorn_log_config(),
        loop="asyncio",
    )
    server = uvicorn.Server(config)

    # Track server startup errors
    server_error: Exception | None = None

    # Start server in separate thread
    def start_server() -> None:
        nonlocal server_error
        try:
            import asyncio

            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Increase recursion limit for CI environments
            import sys

            old_limit = sys.getrecursionlimit()
            sys.setrecursionlimit(max(old_limit, 3000))

            try:
                loop.run_until_complete(server.serve())
            finally:
                sys.setrecursionlimit(old_limit)
        except Exception as e:
            server_error = e
            logger.error(f"Server startup failed: {e}", exc_info=True)

    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()

    # Wait for server to start and verify it's responding
    max_retries = 20  # Increased from 10 for slower CI
    for i in range(max_retries):
        # Check if server thread encountered an error
        if server_error is not None:
            raise RuntimeError(
                f"Server failed during startup: {server_error}"
            ) from server_error

        try:
            time.sleep(1)  # Increased from 0.5 for CI
            response = requests.get(f"http://127.0.0.1:{port}/", timeout=5)
            if response.status_code == 200:
                logger.info(f"Server started successfully on port {port}")
                break
        except requests.exceptions.RequestException as e:
            logger.debug(f"Attempt {i + 1}/{max_retries}: Server not ready - {e}")
            if i == max_retries - 1:
                error_msg = f"Server failed to start on port {port} after {max_retries} attempts"
                if server_error:
                    error_msg += f"\nStartup error: {server_error}"
                raise RuntimeError(error_msg) from e
            continue

    server_url = f"http://localhost:{port}"
    # Use logger (not print) so messages appear immediately via live log
    # (pytest buffers print() and only shows it after the test completes)
    logger.warning(f"\n🌐 Pynmon server started at: {server_url}")
    logger.warning("   You can access the pynmon interface in your browser!")

    # Check if we should keep the server running for debugging
    # Priority: module-level KEEP_ALIVE constant, then environment variable
    module_keep_alive = getattr(request.module, "KEEP_ALIVE", 0)
    env_keep_alive = os.getenv("PYNMON_KEEP_ALIVE", "").lower() in ("1", "true", "yes")
    keep_alive = module_keep_alive == 1 or env_keep_alive

    if keep_alive:
        if module_keep_alive == 1:
            logger.warning(
                "   🔒 Module KEEP_ALIVE=1 - server will stay running after tests!"
            )
        else:
            logger.warning(
                "   🔒 PYNMON_KEEP_ALIVE is set - server will stay running after tests!"
            )
        logger.warning("   Press Ctrl+C to stop the server when done debugging.")

    try:
        yield server_url
    finally:
        # Check if we should keep the server running
        if keep_alive:
            logger.warning(f"\n🔒 Keeping server running at: {server_url}")
            logger.warning("   The server will continue running for debugging.")
            logger.warning("   Press Ctrl+C to stop when done.")
            try:
                # Keep the server alive indefinitely
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n👋 Stopping server...")

        # Stop the server
        if hasattr(server, "should_exit"):
            server.should_exit = True

        # Restore original global state
        all_pynenc_instances.clear()
        all_pynenc_instances.update(original_all_instances)
        pynmon.app.pynenc_instance = original_instance
