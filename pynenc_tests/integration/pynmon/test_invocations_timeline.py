"""
Integration tests for pynmon invocations timeline.

Tests the complete timeline functionality with memory backend,
multiple tasks with different durations, and realistic timing scenarios.
"""

import threading
import time
from typing import TYPE_CHECKING

import pytest

from pynenc.builder import PynencBuilder
from pynenc_tests.conftest import check_all_status_transitions

if TYPE_CHECKING:
    from pynenc_tests.integration.pynmon.conftest import PynmonClient

# Debug configuration - Set to 1 to keep server alive for browser debugging
KEEP_ALIVE = 0

# Configure app for testing with Memory backend
app = (
    PynencBuilder().memory().thread_runner().app_id("test-pynmon-timeline-app").build()
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

    # Should show timeline page structure (SVG-based now)
    assert "Invocations Timeline" in content
    # SVG content should be present (even if empty, shows legend)
    assert "<svg" in content


def test_timeline_with_quick_tasks(pynmon_client: "PynmonClient") -> None:
    """Test timeline with quick tasks that complete immediately."""
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()

    try:
        # Execute some quick tasks
        results = [quick_task(f"Task {i}") for i in range(3)]

        # Wait for all tasks to complete
        for result in results:
            assert result.result.startswith("Quick:")

        # Test timeline page
        response = pynmon_client.get("/invocations/timeline?time_range=15m")

        assert response.status_code == 200
        content = response.text

        # Should show timeline with SVG content
        assert "Invocations Timeline" in content
        assert "<svg" in content

        # The SVG should contain invocation bars (check for runner lane content)
        # Note: task names appear in tooltips within the SVG

    finally:
        app.runner.stop_runner_loop()
        check_all_status_transitions(app)


def test_timeline_with_mixed_duration_tasks(pynmon_client: "PynmonClient") -> None:
    """Test timeline with tasks of varying durations to create realistic timeline."""
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()

    try:
        # Create a sequence of tasks with different durations
        # This will create overlapping executions for a good timeline visualization

        # Start some longer tasks first
        long_results = [long_task(3, f"Long task {i}") for i in range(2)]

        # Small delay then start medium tasks
        medium_results = [medium_task(2, f"Medium task {i}") for i in range(3)]

        # Small delay then start short tasks
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

        # Test timeline page with different time ranges
        response = pynmon_client.get("/invocations/timeline?time_range=15m")

        assert response.status_code == 200
        content = response.text

        # Should show SVG timeline with invocations
        assert "Invocations Timeline" in content
        assert "<svg" in content

        # Should show various statuses in the SVG legend
        assert "SUCCESS" in content
        assert "FAILED" in content

    finally:
        app.runner.stop_runner_loop()
        check_all_status_transitions(app)


def test_timeline_different_time_ranges(pynmon_client: "PynmonClient") -> None:
    """Test timeline with different time range filters."""
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()

    try:
        # Execute a task
        quick_result = quick_task("Time range test")
        assert quick_result.result.startswith("Quick:")

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
        check_all_status_transitions(app)


def test_timeline_api_endpoints(pynmon_client: "PynmonClient") -> None:
    """Test the API endpoints used by the timeline visualization."""
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()

    try:
        # Execute a task to create an invocation
        quick_result = quick_task("API test")
        invocation_id = quick_result.invocation_id

        # Wait for completion
        assert quick_result.result.startswith("Quick:")

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
        check_all_status_transitions(app)


def test_timeline_error_handling(pynmon_client: "PynmonClient") -> None:
    """Test timeline handles invalid parameters gracefully."""
    # Test with invalid time range - should default to 1h
    response = pynmon_client.get("/invocations/timeline?time_range=invalid")
    assert response.status_code == 200
    assert "<svg" in response.text

    # Test with invalid limit - should handle gracefully
    response = pynmon_client.get("/invocations/timeline?limit=-1")
    assert response.status_code == 200
    assert "<svg" in response.text

    # Test with very large limit
    response = pynmon_client.get("/invocations/timeline?limit=1000")
    assert response.status_code == 200
    assert "<svg" in response.text
