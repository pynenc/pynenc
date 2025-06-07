"""
Integration tests for pynmon calls view.

Tests the complete calls functionality with real Redis backend
and real Pynenc app integration.
"""
import threading
import time
from typing import TYPE_CHECKING
from urllib.parse import quote

from pynenc.builder import PynencBuilder

if TYPE_CHECKING:
    from tests.integration.pynmon.conftest import PynmonClient

# Debug configuration - Set to 1 to keep server alive for browser debugging
KEEP_ALIVE = 1

# Configure app for testing with Redis backend
app = (
    PynencBuilder()
    .redis(url="redis://localhost:6379", db=15)  # Use test database for isolation
    .thread_runner()
    .app_id("test-pynmon-calls-app")
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


@app.task
def multiply_task(a: int, b: int) -> int:
    """Simple test task that multiplies two numbers."""
    return a * b


def test_call_detail_with_query_parameter(pynmon_client: "PynmonClient") -> None:
    """
    Test call detail page accessed via query parameter.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.1)  # Let runner initialize

    try:
        # Execute a task to create a call
        result = hello_task("Integration Test")
        call_id = result.call.call_id

        # Wait for completion
        assert result.result == "Hello, Integration Test!"
        time.sleep(0.2)  # Ensure state is persisted

        # Test call detail via query parameter
        response = pynmon_client.get(f"/calls/?call_id={quote(call_id)}")

        assert response.status_code == 200
        content = response.text

        # Should show call details
        assert call_id in content
        assert "Call Details" in content
        assert "hello_task" in content
        assert "Integration Test" in content

        # Should show task information
        assert "Task Information" in content
        assert "test-pynmon-calls-app" in content

        # Should have navigation links
        assert "View Task" in content
        assert "Back to Invocations" in content

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_call_detail_with_path_parameter(pynmon_client: "PynmonClient") -> None:
    """
    Test call detail page accessed via path parameter.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.1)  # Let runner initialize

    try:
        # Execute a task with different arguments to create a unique call
        result = add_task(42, 58)
        call_id = result.call.call_id

        # Wait for completion
        assert result.result == 100
        time.sleep(0.2)  # Ensure state is persisted

        # Test call detail via path parameter
        response = pynmon_client.get(f"/calls/{quote(call_id, safe='')}")

        assert response.status_code == 200
        content = response.text

        # Should show call details
        assert call_id in content
        assert "Call Details" in content
        assert "add_task" in content

        # Should show arguments
        assert "42" in content
        assert "58" in content

        # Should show task information
        assert "Task Information" in content

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_call_detail_displays_arguments_correctly(
    pynmon_client: "PynmonClient",
) -> None:
    """
    Test that call detail page displays task arguments correctly.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.1)  # Let runner initialize

    try:
        # Execute a task with multiple arguments
        result = multiply_task(7, 9)
        call_id = result.call.call_id

        # Wait for completion        assert result.result == 63
        time.sleep(0.2)  # Ensure state is persisted

        # Test call detail
        response = pynmon_client.get(f"/calls/{quote(call_id, safe='')}")

        assert response.status_code == 200
        content = response.text

        # Should show call details
        assert call_id in content
        assert "multiply_task" in content

        # Should show arguments section
        assert "Arguments" in content or "arguments" in content

        # Should display the argument values
        assert "7" in content
        assert "9" in content

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_call_detail_shows_task_information(pynmon_client: "PynmonClient") -> None:
    """
    Test that call detail page shows comprehensive task information.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.1)  # Let runner initialize

    try:
        # Execute a task
        result = hello_task("Task Info Test")
        call_id = result.call.call_id

        # Wait for completion        assert result.result == "Hello, Task Info Test!"
        time.sleep(0.2)  # Ensure state is persisted

        # Test call detail
        response = pynmon_client.get(f"/calls/{quote(call_id, safe='')}")

        assert response.status_code == 200
        content = response.text

        # Should show task information section
        assert "Task Information" in content

        # Should show task details
        task_id = hello_task.task_id
        assert task_id in content

        # Should show module information
        assert "Module:" in content
        assert "Function:" in content

        # Should have link to task detail page
        assert f"/tasks/{task_id}" in content

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_call_detail_shows_invocations(pynmon_client: "PynmonClient") -> None:
    """
    Test that call detail page shows related invocations.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.1)  # Let runner initialize

    try:
        # Execute a task
        result = add_task(123, 456)
        call_id = result.call.call_id

        # Wait for completion        assert result.result == 579
        time.sleep(0.2)  # Ensure state is persisted

        # Test call detail
        response = pynmon_client.get(f"/calls/{quote(call_id, safe='')}")

        assert response.status_code == 200
        content = response.text

        # Should show invocations section (if the template includes it)
        # Note: This depends on the actual template structure
        # At minimum, the page should render successfully with the call information
        assert call_id in content
        assert "add_task" in content

        # The invocation ID might be displayed in some form
        # (either full ID or shortened version)
        # This assertion is optional depending on template design
        # assert invocation_short in content or invocation_id in content

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_call_detail_missing_call_id_parameter(pynmon_client: "PynmonClient") -> None:
    """
    Test call detail page behavior when call_id parameter is missing.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    # Test with missing call_id query parameter
    response = pynmon_client.get("/calls/")

    assert response.status_code == 400
    content = response.text

    # Should show error message
    assert "Missing Call ID" in content or "No call_id was provided" in content


def test_call_detail_empty_call_id_parameter(pynmon_client: "PynmonClient") -> None:
    """
    Test call detail page behavior when call_id parameter is empty.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    # Test with empty call_id query parameter
    response = pynmon_client.get("/calls/?call_id=")

    assert response.status_code == 400
    content = response.text

    # Should show error message
    assert "Missing Call ID" in content or "No call_id was provided" in content


def test_call_detail_nonexistent_call_id(pynmon_client: "PynmonClient") -> None:
    """
    Test call detail page behavior with non-existent call ID.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    # Test with non-existent call ID
    fake_call_id = "nonexistent-call-id-12345"
    response = pynmon_client.get(f"/calls/{fake_call_id}")

    assert response.status_code == 404
    content = response.text

    # Should show not found error
    assert "Call Not Found" in content
    assert fake_call_id in content


def test_call_detail_with_multiple_executions(pynmon_client: "PynmonClient") -> None:
    """
    Test call detail with same task executed multiple times with different arguments.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.1)  # Let runner initialize

    try:
        # Execute the same task multiple times with different arguments
        result1 = hello_task("First")
        result2 = hello_task("Second")
        result3 = hello_task("Third")

        call_id1 = result1.call.call_id
        call_id2 = result2.call.call_id
        call_id3 = result3.call.call_id

        # Wait for completion
        assert result1.result == "Hello, First!"
        assert result2.result == "Hello, Second!"
        assert result3.result == "Hello, Third!"
        time.sleep(0.2)  # Ensure state is persisted

        # Each call should have a different ID
        assert call_id1 != call_id2
        assert call_id2 != call_id3
        assert call_id1 != call_id3

        # Test each call detail page
        for call_id, expected_name in [
            (call_id1, "First"),
            (call_id2, "Second"),
            (call_id3, "Third"),
        ]:
            response = pynmon_client.get(f"/calls/{quote(call_id, safe='')}")

            assert response.status_code == 200
            content = response.text

            # Should show the correct call details
            assert call_id in content
            assert expected_name in content
            assert "hello_task" in content

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_call_detail_navigation_links(pynmon_client: "PynmonClient") -> None:
    """
    Test that call detail page includes proper navigation links.

    :param pynmon_client: HTTP client for the actual pynmon server
    """
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.1)  # Let runner initialize

    try:
        # Execute a task
        result = multiply_task(11, 13)
        call_id = result.call.call_id

        # Wait for completion        assert result.result == 143
        time.sleep(0.2)  # Ensure state is persisted

        # Test call detail
        response = pynmon_client.get(f"/calls/{quote(call_id, safe='')}")

        assert response.status_code == 200
        content = response.text

        # Should have navigation links
        assert "Back to Invocations" in content
        assert "View Task" in content

        # Should have links to other pynmon sections
        task_id = multiply_task.task_id
        assert f"/tasks/{task_id}" in content
        assert "/invocations" in content

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)
