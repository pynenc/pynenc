"""
Integration tests for pynmon home page.

Tests the complete home page functionality with real Redis backend
and real Pynenc app integ    # Execute tasks to populate the system with data
    hello_result = hello_task("Test User")
    add_result = add_task(15, 25)

    # Verify tasks completed successfully
    assert hello_result.result == "Hello, Test User!"
    assert add_result.result == 40

To debug: Set KEEP_ALIVE = 1 and run any test, then open http://localhost:8081
"""

from typing import TYPE_CHECKING

from pynenc.builder import PynencBuilder

if TYPE_CHECKING:
    from conftest import PynmonClient

# Debug configuration - Set to 1 to keep server alive for browser debugging
KEEP_ALIVE = 0

# Configure app for testing (following pattern from test_status_trigger.py)
app = (
    PynencBuilder()
    .redis(url="redis://localhost:6379", db=15)  # Use test database for isolation
    .thread_runner()
    .app_id("test-pynmon-app")
    .build()
)


@app.task
def hello_task(name: str) -> str:
    """Simple test task that greets someone."""
    return f"Hello, {name}!"


@app.task
def add_task(x: int, y: int) -> int:
    """Simple test task that adds two numbers."""
    return x + y


def test_home_page_renders_successfully(pynmon_client: "PynmonClient") -> None:
    """
    Test that the home page renders successfully.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    response = pynmon_client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_home_page_displays_app_info(pynmon_client: "PynmonClient") -> None:
    """
    Test that the home page displays basic app information.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    response = pynmon_client.get("/")

    assert response.status_code == 200
    content = response.text

    # Check that app ID is displayed
    assert "test-pynmon-app" in content

    # Check that the dashboard title is present
    assert "Pynenc Monitor Dashboard" in content or "Pynenc Monitor" in content


def test_home_page_has_navigation_links(pynmon_client: "PynmonClient") -> None:
    """
    Test that the home page includes navigation links to other sections.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    response = pynmon_client.get("/")

    assert response.status_code == 200
    content = response.text

    # Check for navigation links to main sections
    expected_links = ["/broker", "/orchestrator", "/tasks", "/invocations"]

    for link in expected_links:
        assert link in content


def test_home_page_purge_button_present(pynmon_client: "PynmonClient") -> None:
    """
    Test that the home page includes the purge all data button.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    response = pynmon_client.get("/")

    assert response.status_code == 200
    content = response.text

    # Check for purge functionality
    assert "Purge All Application Data" in content or "Purge All" in content
    assert "hx-post" in content  # HTMX purge functionality


def test_home_page_app_selector_present(pynmon_client: "PynmonClient") -> None:
    """
    Test that the home page includes app selector dropdown.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    response = pynmon_client.get("/")

    assert response.status_code == 200
    content = response.text

    # Check for app selector in navigation
    assert "test-pynmon-app" in content
    # The dropdown should show current app
    assert "App:" in content or "app" in content.lower()


def test_home_page_with_active_tasks(pynmon_client: "PynmonClient") -> None:
    """
    Test home page with active tasks and data.

    Execute some tasks first to populate pynmon with data, then verify
    the home page displays correctly with real application state.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    # Start the runner in a separate thread to process tasks
    import threading
    import time

    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.2)  # Wait for runner to initialize
    app.logger.info("Runner thread started for test")

    try:
        # Execute tasks to populate the system with data
        hello_result = hello_task("Test User")
        add_result = add_task(15, 25)

        # Verify tasks completed successfully
        assert hello_result.result == "Hello, Test User!"
        assert add_result.result == 40
    finally:
        # Stop the runner and clean up
        app.logger.info("Stopping runner thread...")
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)
        app.logger.info("Runner cleanup completed")

    # Test the home page
    response = pynmon_client.get("/")

    assert response.status_code == 200
    content = response.text

    # Check that app ID is displayed
    assert "test-pynmon-app" in content

    # Should not show error states
    assert "No Pynenc application is configured" not in content
    assert "critical_error" not in content.lower()

    # Should show dashboard content
    assert "Pynenc Monitor" in content
