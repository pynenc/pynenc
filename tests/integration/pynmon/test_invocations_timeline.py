"""
Integration tests for pynmon invocations timeline.

Tests the complete timeline functionality with real Redis backend,
multiple tasks with different durations, and realistic timing scenarios.
"""

import threading
import time
from typing import TYPE_CHECKING

import pytest

from pynenc.builder import PynencBuilder

if TYPE_CHECKING:
    from tests.integration.pynmon.conftest import PynmonClient

# Debug configuration - Set to 1 to keep server alive for browser debugging
KEEP_ALIVE = 0

# Configure app for testing with Redis backend
app = (
    PynencBuilder()
    .redis(url="redis://localhost:6379", db=15)  # Use test database for isolation
    .thread_runner()
    .app_id("test-pynmon-timeline-app")
    .build()
)


@app.task
def quick_task(message: str) -> str:
    """Fast task that completes immediately."""
    return f"Quick: {message}"


@app.task
def short_task(duration: int, message: str) -> str:
    """Task that takes a short amount of time."""
    time.sleep(duration)
    return f"Short({duration}s): {message}"


@app.task
def medium_task(duration: int, message: str) -> str:
    """Task that takes a medium amount of time."""
    time.sleep(duration)
    return f"Medium({duration}s): {message}"


@app.task
def long_task(duration: int, message: str) -> str:
    """Task that takes a longer amount of time."""
    time.sleep(duration)
    return f"Long({duration}s): {message}"


@app.task
def failing_task(message: str) -> str:
    """Task that always fails."""
    raise RuntimeError(f"Intentional failure: {message}")


def test_timeline_with_no_invocations(pynmon_client: "PynmonClient") -> None:
    """Test timeline page renders correctly with no invocations."""
    # Purge any existing data
    app.purge()

    response = pynmon_client.get("/invocations/timeline")

    assert response.status_code == 200
    content = response.text

    # Should show empty state message
    assert "No invocations found" in content
    assert "Invocations Timeline" in content


def test_timeline_with_quick_tasks(pynmon_client: "PynmonClient") -> None:
    """Test timeline with quick tasks that complete immediately."""
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.1)  # Let runner initialize

    try:
        # Execute some quick tasks
        results = [quick_task(f"Task {i}") for i in range(3)]

        # Wait for all tasks to complete
        for result in results:
            assert result.result.startswith("Quick:")

        # Small delay to ensure state is persisted
        time.sleep(0.2)

        # Test timeline page
        response = pynmon_client.get("/invocations/timeline?time_range=15m")

        assert response.status_code == 200
        content = response.text

        # Should show invocations
        assert "No invocations found" not in content
        assert "Invocations Timeline" in content
        assert "quick_task" in content

        # Should contain plotly script for visualization
        assert "plotly" in content.lower()

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_timeline_with_mixed_duration_tasks(pynmon_client: "PynmonClient") -> None:
    """Test timeline with tasks of varying durations to create realistic timeline."""
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.1)  # Let runner initialize

    try:
        # Create a sequence of tasks with different durations
        # This will create overlapping executions for a good timeline visualization

        # Start some longer tasks first
        long_results = [long_task(3, f"Long task {i}") for i in range(2)]

        # Small delay then start medium tasks
        time.sleep(0.5)
        medium_results = [medium_task(2, f"Medium task {i}") for i in range(3)]

        # Small delay then start short tasks
        time.sleep(0.5)
        short_results = [short_task(1, f"Short task {i}") for i in range(4)]

        # Add some quick tasks and a failing task
        quick_results = [quick_task(f"Quick task {i}") for i in range(2)]

        # Add a failing task
        failing_result = failing_task("This will fail")

        # Wait for all tasks to complete (the long ones will take ~3 seconds)
        for result in long_results + medium_results + short_results + quick_results:
            assert result.result is not None  # Wait for completion

        # The failing task should fail
        with pytest.raises(RuntimeError):
            _ = failing_result.result

        # Small delay to ensure all state is persisted
        time.sleep(0.5)

        # Test timeline page with different time ranges
        response = pynmon_client.get("/invocations/timeline?time_range=15m")

        assert response.status_code == 200
        content = response.text

        # Should show all task types
        assert "long_task" in content
        assert "medium_task" in content
        assert "short_task" in content
        assert "quick_task" in content
        assert "failing_task" in content

        # Should show various statuses
        assert "SUCCESS" in content
        assert "FAILED" in content

        # Should contain plotly visualization
        assert "plotly" in content.lower()

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_timeline_with_task_filter(pynmon_client: "PynmonClient") -> None:
    """Test timeline filtering by specific task ID."""
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.1)  # Let runner initialize

    try:
        # Execute different types of tasks
        quick_result = quick_task("Filter test")
        short_result = short_task(1, "Filter test")

        # Wait for completion
        assert quick_result.result.startswith("Quick:")
        assert short_result.result.startswith("Short")

        time.sleep(0.2)

        # Test filtering by quick_task only
        response = pynmon_client.get(
            "/invocations/timeline?time_range=15m&task_id=test_invocations_timeline.quick_task"
        )

        assert response.status_code == 200
        content = response.text

        # Should show only quick_task
        assert "quick_task" in content
        # Should not show short_task (assuming it's filtered out)
        # Note: We can't be 100% sure about this without parsing the JSON data

        # Test filtering by short_task only
        response = pynmon_client.get(
            "/invocations/timeline?time_range=15m&task_id=test_invocations_timeline.short_task"
        )

        assert response.status_code == 200
        content = response.text

        # Should show short_task
        assert "short_task" in content

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_timeline_different_time_ranges(pynmon_client: "PynmonClient") -> None:
    """Test timeline with different time range filters."""
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.1)  # Let runner initialize

    try:
        # Execute a task
        quick_result = quick_task("Time range test")
        assert quick_result.result.startswith("Quick:")

        time.sleep(0.2)

        # Test different time ranges
        time_ranges = ["15m", "1h", "3h", "1d"]

        for time_range in time_ranges:
            response = pynmon_client.get(
                f"/invocations/timeline?time_range={time_range}"
            )

            assert response.status_code == 200
            content = response.text

            # All should show the timeline page structure
            assert "Invocations Timeline" in content
            assert "Time Range" in content

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_timeline_api_endpoints(pynmon_client: "PynmonClient") -> None:
    """Test the API endpoints used by the timeline visualization."""
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.1)  # Let runner initialize

    try:
        # Execute a task to create an invocation
        quick_result = quick_task("API test")
        invocation_id = quick_result.invocation_id

        # Wait for completion
        assert quick_result.result.startswith("Quick:")
        time.sleep(0.2)

        # Test the invocation API endpoint
        response = pynmon_client.get(f"/invocations/{invocation_id}/api")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("application/json")

        data = response.json()
        assert "invocation_id" in data
        assert data["invocation_id"] == invocation_id

        # Test the history API endpoint
        response = pynmon_client.get(f"/invocations/{invocation_id}/history")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("application/json")

        history = response.json()
        assert isinstance(history, list)
        assert len(history) > 0  # Should have at least one status entry

        # Check first history entry structure
        first_entry = history[0]
        assert "status" in first_entry
        assert "timestamp" in first_entry

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_timeline_with_staggered_task_execution(pynmon_client: "PynmonClient") -> None:
    """Test timeline with tasks starting at different times to simulate real-world usage."""
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(0.1)  # Let runner initialize

    try:
        # Create a more realistic scenario with staggered task execution

        # First batch: Start some medium tasks
        batch1_results = [medium_task(2, f"Batch 1 Task {i}") for i in range(2)]

        # Wait a bit then start second batch
        time.sleep(1)
        batch2_results = [short_task(1, f"Batch 2 Task {i}") for i in range(3)]

        # Wait a bit then start third batch
        time.sleep(1)
        batch3_results = [quick_task(f"Batch 3 Task {i}") for i in range(2)]

        # Wait for all to complete
        all_results = batch1_results + batch2_results + batch3_results
        for result in all_results:
            assert result.result is not None

        time.sleep(0.5)  # Ensure state is persisted

        # Test timeline
        response = pynmon_client.get("/invocations/timeline?time_range=15m")

        assert response.status_code == 200
        content = response.text

        # Should show all task types
        assert "medium_task" in content
        assert "short_task" in content
        assert "quick_task" in content

        # Should show successful completions
        assert "SUCCESS" in content

        # Should have timeline visualization
        assert "plotly" in content.lower()

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_timeline_error_handling(pynmon_client: "PynmonClient") -> None:
    """Test timeline handles invalid parameters gracefully."""
    # Test with invalid time range
    response = pynmon_client.get("/invocations/timeline?time_range=invalid")
    assert response.status_code == 200  # Should default to 1h

    # Test with invalid task ID - should return 404 with error message
    response = pynmon_client.get(
        "/invocations/timeline?task_id=test_invocations_timeline.nonexistent_task"
    )
    assert response.status_code == 404  # Should return 404 for non-existent task
    assert "Task Not Found" in response.text
    assert "nonexistent_task" in response.text

    # Test with invalid module in task ID
    response = pynmon_client.get(
        "/invocations/timeline?task_id=nonexistent_module.some_task"
    )
    assert response.status_code == 404  # Should return 404 for non-existent module
    assert "Task Not Found" in response.text
    assert "nonexistent_module" in response.text

    # Test with invalid limit
    response = pynmon_client.get("/invocations/timeline?limit=-1")
    assert response.status_code == 200  # Should handle gracefully


def test_timeline_detailed_error_messages(pynmon_client: "PynmonClient") -> None:
    """Test that timeline provides detailed error messages for different task lookup failures."""

    # Test 1: Invalid task ID format
    response = pynmon_client.get("/invocations/timeline?task_id=invalid_format")
    assert response.status_code == 404
    content = response.text
    assert "Task Not Found" in content
    assert "Invalid task ID format" in content

    # Test 2: Valid format but non-existent module
    response = pynmon_client.get(
        "/invocations/timeline?task_id=nonexistent_module.some_task"
    )
    assert response.status_code == 404
    content = response.text
    assert "Task Not Found" in content
    assert "could not be imported" in content or "not found" in content

    # Test 3: Valid module but non-existent function
    response = pynmon_client.get(
        "/invocations/timeline?task_id=test_invocations_timeline.nonexistent_function"
    )
    assert response.status_code == 404
    content = response.text
    assert "Task Not Found" in content
    assert "not found" in content
