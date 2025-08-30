"""
Integration tests for pynmon tasks page.

Tests the complete tasks page functionality with real Redis backend
and real Pynenc app integration.
"""

from fastapi.testclient import TestClient

from pynenc.builder import PynencBuilder

# Debug configuration - Set to 1 to keep server alive for browser debugging
KEEP_ALIVE = 0


app = (
    PynencBuilder()
    .redis(db=15)  # Use test database for isolation, host configured via env vars
    .thread_runner()
    .app_id("test-pynmon-tasks-app")
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


def test_tasks_list(pynmon_client: TestClient) -> None:
    """
    Test that the tasks list page displays registered tasks.

    :param pynmon_client: TestClient fixture with configured pynmon
    """
    response = pynmon_client.get("/tasks/")

    assert response.status_code == 200
    content = response.text

    # Check that our registered tasks are displayed
    assert "hello_task" in content
    assert "add_task" in content

    # Check that app ID is displayed
    assert "test-pynmon-tasks-app" in content

    # Check that the tasks page title is present
    assert "Tasks Monitor" in content or "Tasks" in content

    assert hello_task.task_id in content
    assert add_task.task_id in content
