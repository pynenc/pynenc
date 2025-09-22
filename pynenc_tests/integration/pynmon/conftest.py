"""
Fixtures for pynmon integration tests.

This module provides fixtures for testing pynmon with real backend.
Each test module defines its own app and tasks. The fixtures here start
runners and set up pynmon clients that use the app from the test module.
"""

# Skip all pynmon integration tests if monitor dependencies are not available
import pytest

pytest.importorskip("fastapi", reason="pynmon tests require monitor dependencies")
pytest.importorskip("jinja2", reason="pynmon tests require monitor dependencies")

# All imports below must come after pytest.importorskip calls
# ruff: noqa: E402

import os
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any

import requests
import uvicorn

from pynmon.app import all_pynenc_instances
from pynmon.app import app as pynmon_app
from pynmon.app import pynenc_instance

if TYPE_CHECKING:
    from collections.abc import Generator

    from _pytest.fixtures import FixtureRequest

    from pynenc import Pynenc


class PynmonClient:
    """HTTP client for making requests to the actual pynmon server."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def get(self, path: str) -> requests.Response:
        """Make a GET request to the pynmon server."""
        url = f"{self.base_url}{path}"
        return self.session.get(url)

    def post(self, path: str, **kwargs: "Any") -> requests.Response:
        """Make a POST request to the pynmon server."""
        url = f"{self.base_url}{path}"
        return self.session.post(url, **kwargs)


@pytest.fixture
def pynmon_client(pynmon_server: str) -> "Generator[PynmonClient, None, None]":
    """
    Create an HTTP client for making requests to the actual pynmon server.

    Uses the real server started by pynmon_server fixture to make HTTP requests.
    This ensures all tests use the actual server for browser-accessible debugging.

    :param pynmon_server: The server URL from pynmon_server fixture
    :return: HTTP client configured to use the server
    """
    client = PynmonClient(pynmon_server)
    yield client


@pytest.fixture
def pynmon_server(request: "FixtureRequest") -> "Generator[str, None, None]":
    """
    Start a real HTTP server for pynmon that can be accessed in a browser.

    This fixture starts an actual uvicorn server in a separate thread,
    making pynmon accessible at http://localhost:8081 for debugging purposes.
    All tests use this server to ensure browser accessibility.

    :param request: The pytest request object to access the calling module
    :return: The server URL (http://localhost:8081)
    """
    # Get the app from the test module
    test_app: "Pynenc" = request.module.app

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

    # Find an available port starting from 8081
    import socket

    port = 8081
    while port < 8090:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("localhost", port)) != 0:
                break
        port += 1

    # Configure uvicorn server
    config = uvicorn.Config(
        app=pynmon_app,
        host="127.0.0.1",
        port=port,
        log_level="info",
        access_log=False,
        loop="asyncio",
    )
    server = uvicorn.Server(config)

    # Start server in separate thread
    def start_server() -> None:
        import asyncio

        # Create new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(server.serve())

    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()

    # Wait for server to start and verify it's responding
    max_retries = 10
    for i in range(max_retries):
        try:
            time.sleep(0.5)
            response = requests.get(f"http://127.0.0.1:{port}/", timeout=5)
            if response.status_code == 200:
                break
        except requests.exceptions.RequestException:
            if i == max_retries - 1:
                raise RuntimeError(f"Server failed to start on port {port}") from None
            continue

    server_url = f"http://localhost:{port}"
    print(f"\n🌐 Pynmon server started at: {server_url}")
    print("   You can access the pynmon interface in your browser!")

    # Check if we should keep the server running for debugging
    # Priority: module-level KEEP_ALIVE constant, then environment variable
    module_keep_alive = getattr(request.module, "KEEP_ALIVE", 0)
    env_keep_alive = os.getenv("PYNMON_KEEP_ALIVE", "").lower() in ("1", "true", "yes")
    keep_alive = module_keep_alive == 1 or env_keep_alive

    if keep_alive:
        if module_keep_alive == 1:
            print("   🔒 Module KEEP_ALIVE=1 - server will stay running after tests!")
        else:
            print(
                "   🔒 PYNMON_KEEP_ALIVE is set - server will stay running after tests!"
            )
        print("   Press Ctrl+C to stop the server when done debugging.")

    try:
        yield server_url
    finally:
        # Check if we should keep the server running
        if keep_alive:
            print(f"\n🔒 Keeping server running at: {server_url}")
            print("   The server will continue running for debugging.")
            print("   Press Ctrl+C to stop when done.")
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
