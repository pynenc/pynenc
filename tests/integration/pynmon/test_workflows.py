"""
Integration tests for pynmon workflow views.

Tests the workflow-related functionality with Memory backend,
verifying workflow discovery and display capabilities.
"""
import threading
import time
from typing import TYPE_CHECKING

from pynenc.builder import PynencBuilder

if TYPE_CHECKING:
    from tests.integration.pynmon.conftest import PynmonClient

# Debug configuration - Set to 1 to keep server alive for browser debugging
KEEP_ALIVE = 0

# Configure app for testing with Memory backend
app = (
    PynencBuilder()
    .memory()
    .thread_runner()
    .app_id("test-pynmon-workflows")
    .serializer("json")
    .build()
)


@app.task
def simple_workflow_task() -> dict[str, str]:
    """Simple workflow for testing pynmon workflow views."""
    workflow_id = simple_workflow_task.wf.identity.workflow_id

    # Store some workflow data for testing
    simple_workflow_task.wf.set_data("test_key", "test_value")
    simple_workflow_task.wf.set_data("step", "completed")

    # Add a small delay to make the workflow visible in timeline
    time.sleep(0.1)

    return {"workflow_id": workflow_id, "status": "completed"}


@app.task
def data_processing_workflow(batch_size: int) -> dict[str, str | int]:
    """Data processing workflow with parameters."""
    workflow_id = data_processing_workflow.wf.identity.workflow_id

    # Store processing parameters
    data_processing_workflow.wf.set_data("batch_size", batch_size)
    data_processing_workflow.wf.set_data(
        "start_time", data_processing_workflow.wf.utc_now().isoformat()
    )

    # Simulate processing steps
    for step in range(3):
        step_id = data_processing_workflow.wf.uuid()
        data_processing_workflow.wf.set_data(f"step_{step}_id", step_id)
        time.sleep(0.05)  # Small delay between steps

    data_processing_workflow.wf.set_data(
        "end_time", data_processing_workflow.wf.utc_now().isoformat()
    )

    return {"workflow_id": workflow_id, "batch_size": batch_size, "status": "processed"}


def test_workflow_discovery_basic(pynmon_client: "PynmonClient") -> None:
    """Test basic workflow discovery functionality."""
    # Purge any existing data
    app.purge()

    # Execute a simple workflow
    invocation = simple_workflow_task()

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for workflow completion
        result = invocation.result
        assert result is not None
        assert "workflow_id" in result

        # Give some time for the workflow to be stored
        time.sleep(0.2)

        # Test that workflows are discoverable via pynmon workflow views
        response = pynmon_client.get("/workflows/")
        assert response.status_code == 200
        response_text = response.text
        assert simple_workflow_task.task_id in response_text

        # Test workflow detail view
        response = pynmon_client.get(f"/workflows/{simple_workflow_task.task_id}")
        assert response.status_code == 200
        response_text = response.text
        assert result["workflow_id"] in response_text

        # Test workflow runs list view
        response = pynmon_client.get("/workflows/runs")
        assert response.status_code == 200
        response_text = response.text
        assert result["workflow_id"] in response_text

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_multiple_workflow_types(pynmon_client: "PynmonClient") -> None:
    """Test discovery of multiple workflow types."""
    # Purge any existing data
    app.purge()

    # Execute different workflow types
    invocation1 = simple_workflow_task()
    invocation2 = data_processing_workflow(100)

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for both workflows to complete
        result1 = invocation1.result
        result2 = invocation2.result

        assert result1 is not None
        assert result2 is not None

        time.sleep(0.2)

        # Test that both workflow types are discoverable via pynmon workflow views
        response = pynmon_client.get("/workflows/")
        assert response.status_code == 200
        response_text = response.text
        assert simple_workflow_task.task_id in response_text
        assert data_processing_workflow.task_id in response_text

        # Test individual workflow detail views
        response1 = pynmon_client.get(f"/workflows/{simple_workflow_task.task_id}")
        assert response1.status_code == 200
        assert result1["workflow_id"] in response1.text

        response2 = pynmon_client.get(f"/workflows/{data_processing_workflow.task_id}")
        assert response2.status_code == 200
        assert result2["workflow_id"] in response2.text
        assert str(result2["batch_size"]) in response2.text

        # Test workflow runs list view contains both workflow types
        response = pynmon_client.get("/workflows/runs")
        assert response.status_code == 200
        response_text = response.text
        assert result1["workflow_id"] in response_text
        assert result2["workflow_id"] in response_text

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_workflow_execution_multiple_instances(pynmon_client: "PynmonClient") -> None:
    """Test multiple instances of the same workflow type."""
    # Purge any existing data
    app.purge()

    # Execute the same workflow multiple times with different parameters
    invocation1 = data_processing_workflow(50)
    invocation2 = data_processing_workflow(100)
    invocation3 = data_processing_workflow(200)

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for all workflows to complete
        results = [invocation1.result, invocation2.result, invocation3.result]

        for result in results:
            assert result is not None
            assert "workflow_id" in result
            assert "batch_size" in result

        time.sleep(0.3)

        # Test that all instances are tracked via pynmon workflow views
        response = pynmon_client.get(f"/workflows/{data_processing_workflow.task_id}")
        assert response.status_code == 200
        response_text = response.text

        # Verify all three workflow instances appear in the detail view
        for result in results:
            assert result["workflow_id"] in response_text
            assert str(result["batch_size"]) in response_text

        # Test workflow runs list view contains all instances
        response = pynmon_client.get("/workflows/runs")
        assert response.status_code == 200
        response_text = response.text

        # Verify all workflow IDs appear in the runs list
        for result in results:
            assert result["workflow_id"] in response_text

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_workflow_detail_view_error_reproduction(pynmon_client: "PynmonClient") -> None:
    """
    Test to reproduce the 500 error when accessing workflow detail views.

    This test emulates the "View Details" button click scenario to capture
    the actual error within the test context where logs are visible.
    """
    # Purge any existing data
    app.purge()

    # Execute a workflow to have data to work with
    invocation = simple_workflow_task()

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for workflow completion
        result = invocation.result
        assert result is not None
        assert "workflow_id" in result
        workflow_id = result["workflow_id"]

        # Give some time for the workflow to be stored
        time.sleep(0.2)

        # Test the workflow list page first (this should work)
        print("Testing workflow list page...")
        response = pynmon_client.get("/workflows/")
        print(f"Workflow list response status: {response.status_code}")
        assert response.status_code == 200

        # Test the workflow runs page (this should work)
        print("Testing workflow runs page...")
        response = pynmon_client.get("/workflows/runs")
        print(f"Workflow runs response status: {response.status_code}")
        assert response.status_code == 200

        # Now test the problematic workflow detail view
        print(
            f"Testing workflow detail view for task_id: {simple_workflow_task.task_id}"
        )
        print(f"Workflow ID from result: {workflow_id}")

        # This is where the 500 error occurs - let's capture it
        response = pynmon_client.get(f"/workflows/{simple_workflow_task.task_id}")
        print(f"Workflow detail response status: {response.status_code}")

        if response.status_code != 200:
            print("ERROR: Workflow detail view failed!")
            print(f"Response status: {response.status_code}")
            print(f"Response headers: {response.headers}")
            print(f"Response content: {response.text[:500]}...")  # First 500 chars

            # Try to get more debugging info
            debug_response = pynmon_client.get("/workflows/debug/info")
            print(f"Debug info response status: {debug_response.status_code}")
            if debug_response.status_code == 200:
                print(f"Debug info: {debug_response.text}")

            # Also try to access the invocation directly if we can extract it
            runs_response = pynmon_client.get("/workflows/runs")
            if runs_response.status_code == 200:
                print("Current workflow runs data available for debugging")

        # The assertion will fail if there's a 500 error, showing us the issue
        assert (
            response.status_code == 200
        ), f"Workflow detail view returned {response.status_code}"

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)
