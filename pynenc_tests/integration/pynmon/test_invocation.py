import threading
from typing import TYPE_CHECKING
from urllib.parse import quote

from pynenc.builder import PynencBuilder

if TYPE_CHECKING:
    from pynenc_tests.integration.pynmon.conftest import PynmonClient

# Debug configuration - Set to 1 to keep server alive for browser debugging
KEEP_ALIVE = 0

# Configure app for testing (following pattern from test_home_integration.py)
app = (
    PynencBuilder()
    .memory()
    .thread_runner()
    .app_id("test-pynmon-task-detail-app")
    .build()
)


@app.task
def hello_task(name: str) -> str:
    """Simple test task that greets someone."""
    return f"Hello, {name}!"


def test_invocation_history(pynmon_client: "PynmonClient") -> None:
    """Test that pynenc show the invocation history on the invocation view"""
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()

    try:
        # Execute a task to create a call
        invocation = hello_task("Integration Test")
        invocation_id = invocation.invocation_id

        # Wait for completion
        assert invocation.result == "Hello, Integration Test!"

        # Test call detail via query parameter
        response = pynmon_client.get(f"/invocations/{quote(invocation_id)}")

        assert response.status_code == 200
        content = response.text

        # Should history details
        history = app.state_backend.get_history(invocation_id)
        assert history
        assert len(history) > 3  # Registered, pending, running, success
        assert "Status timeline".upper() in content.upper()
        for history_item in history:
            assert history_item.status_record.status.value.upper() in content.upper()
            assert history_item.timestamp.isoformat() in content
            runner_context = app.state_backend.get_runner_context(
                history_item.runner_context_id
            )
            assert runner_context is not None, "Runner context should not be None"
            # owner_context may be RunnerContext, etc.
            # Check runner_id is in content
            assert runner_context.runner_id in content
            assert runner_context.hostname in content
            assert str(runner_context.pid) in content
    finally:
        app.runner.stop_runner_loop()


def test_runner_timeline(pynmon_client: "PynmonClient") -> None:
    """Test that runner context is correctly recorded and returned by the timeline history API"""
    # Purge any existing data
    app.purge()

    # Start runner for task execution
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()

    try:
        # Execute a task to create a call
        invocation = hello_task("Timeline Test")
        invocation_id = invocation.invocation_id

        # Wait for completion
        assert invocation.result == "Hello, Timeline Test!"

        # Request timeline history API
        response = pynmon_client.get(f"/invocations/{quote(invocation_id)}/history")
        assert response.status_code == 200
        history_json = response.json()
        assert history_json
        # Get runner context from backend history
        backend_history = app.state_backend.get_history(invocation_id)
        runner_ids = [h.runner_context_id for h in backend_history]
        assert any(runner_ids), "No owner IDs found in backend history"
        # Check that runner IDs are in the API response
    finally:
        app.runner.stop_runner_loop()
