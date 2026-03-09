"""
Integration tests for pynmon task detail page.

Tests the complete task detail page functionality with Memory backend
and real Pynenc app integration.
"""

from fastapi.testclient import TestClient

from pynenc.builder import PynencBuilder
from pynenc.identifiers.task_id import TaskId

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


@app.task
def add_task(x: int, y: int) -> int:
    """Simple test task that adds two numbers."""
    return x + y


def test_task_detail_page_renders_successfully(pynmon_client: TestClient) -> None:
    """
    Test that the task detail page renders successfully for a valid task.

    :param pynmon_client: TestClient fixture with configured pynmon
    """
    # Use the task_id from our registered task
    task_id = hello_task.task_id
    response = pynmon_client.get(f"/tasks/{task_id.key}")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_task_detail_displays_task_information(pynmon_client: TestClient) -> None:
    """
    Test that the task detail page displays basic task information.

    :param pynmon_client: TestClient fixture with configured pynmon
    """
    task_id = add_task.task_id
    response = pynmon_client.get(f"/tasks/{task_id.key}")

    assert response.status_code == 200
    content = response.text

    # Check that task information is displayed
    assert task_id.module in content
    assert task_id.func_name in content
    assert "test-pynmon-task-detail-app" in content

    # Check for page structure
    assert "Task Details" in content
    assert "Basic Information" in content
    assert "Configuration" in content

    # Check that module and function information is present
    assert "Module:" in content
    assert "Function:" in content


def test_task_detail_shows_configuration_details(pynmon_client: TestClient) -> None:
    """
    Test that the task detail page shows task configuration details.

    :param pynmon_client: TestClient fixture with configured pynmon
    """
    task_id = hello_task.task_id
    response = pynmon_client.get(f"/tasks/{task_id.key}")

    assert response.status_code == 200
    content = response.text

    # Check for configuration fields
    assert "Registration Concurrency:" in content
    assert "Running Concurrency:" in content
    assert "Max Retries:" in content
    assert "Key Arguments:" in content
    assert "Retry For Exceptions:" in content


def test_task_detail_returns_404_for_not_found_tasks(
    pynmon_client: TestClient, task_id: "TaskId"
) -> None:
    """
    Test that the task detail page returns 404 for a properly formatted but non-existent task ID.

    :param pynmon_client: TestClient fixture with configured pynmon
    """
    # Use a properly formatted but non-existent task ID
    response = pynmon_client.get(f"/tasks/{task_id.key}")

    assert response.status_code == 404
    content = response.text

    # Check error message content
    assert "Task Not Found" in content
    assert task_id.module in content
    assert task_id.func_name in content


def test_task_detail_invalid_task_id_format(pynmon_client: TestClient) -> None:
    """
    Test that the task detail page returns 400 for an improperly formatted task ID.

    :param pynmon_client: TestClient fixture with configured pynmon
    """
    # Test with badly formatted task ID (missing colon separator)
    invalid_task_id = "invalid-task-id-format"
    response = pynmon_client.get(f"/tasks/{invalid_task_id}")

    assert response.status_code == 400
    content = response.text

    # Check error message content
    assert "Invalid Task ID Format" in content
    assert "not properly formatted" in content
