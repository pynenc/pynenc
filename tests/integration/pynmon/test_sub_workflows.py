"""
Integration tests for pynmon sub-workflow functionality.

Tests the new sub-workflow monitoring features that allow viewing and tracking
invocations that run within specific workflows. This includes both regular task
invocations and child workflows that are spawned from parent workflows.

Key components tested:
- Parent-child workflow relationships
- Sub-invocation tracking via state backend
- Sub-invocations monitoring view
- Child workflow navigation and hierarchy display
"""
import threading
import time
from typing import TYPE_CHECKING, Any

from pynenc.builder import PynencBuilder

if TYPE_CHECKING:
    from tests.integration.pynmon.conftest import PynmonClient

# Debug configuration - Set to 1 to keep server alive for browser debugging
KEEP_ALIVE = 0

# Configure app for testing with Redis backend
app = (
    PynencBuilder()
    .redis()
    .thread_runner()
    .app_id("test-pynmon-sub-workflows")
    .serializer("json")
    .build()
)


# Helper task for validation within workflows
@app.task
def validate_input_data(data: dict[str, Any]) -> dict[str, Any]:
    """
    Validate input data structure and content.

    :param data: Input data to validate
    :return: Validation result with status and normalized data
    """
    # Simulate validation logic
    is_valid = "user_id" in data and "email" in data

    if is_valid:
        normalized_data = {
            "user_id": str(data["user_id"]).strip(),
            "email": data["email"].lower().strip(),
            "metadata": data.get("metadata", {}),
        }
        return {
            "valid": True,
            "normalized_data": normalized_data,
            "validation_timestamp": validate_input_data.wf.utc_now().isoformat(),
        }
    else:
        return {
            "valid": False,
            "errors": ["Missing required fields: user_id and/or email"],
            "validation_timestamp": validate_input_data.wf.utc_now().isoformat(),
        }


# Sub-workflow tasks for testing child workflow functionality
@app.task(force_new_workflow=True)
def child_workflow_task(task_data: dict[str, Any]) -> dict[str, Any]:
    """
    A task that will be executed as a child workflow.

    This task demonstrates child workflow execution where a new workflow context
    is created and runs independently from the parent workflow while being tracked
    as a sub-invocation.

    :param task_data: Data to process in the child workflow
    :return: Processing result
    """
    workflow_id = child_workflow_task.wf.identity.workflow_id
    start_time = child_workflow_task.wf.utc_now()

    # Store child workflow metadata
    child_workflow_task.wf.set_data("workflow_type", "child_processor")
    child_workflow_task.wf.set_data("started_at", start_time.isoformat())
    child_workflow_task.wf.set_data("input_data", task_data)

    # Simulate some processing
    time.sleep(0.1)

    # Execute a sub-task within this child workflow
    validation_result = child_workflow_task.wf.execute_task(
        validate_input_data, task_data
    )

    end_time = child_workflow_task.wf.utc_now()
    child_workflow_task.wf.set_data("completed_at", end_time.isoformat())

    return {
        "child_workflow_id": workflow_id,
        "processed_data": task_data,
        "validation": validation_result.result,
        "processing_time": (end_time - start_time).total_seconds(),
        "completed_at": end_time.isoformat(),
    }


@app.task(force_new_workflow=True)
def parent_workflow_with_children(parent_config: dict[str, Any]) -> dict[str, Any]:
    """
    Parent workflow that creates multiple child workflows.

    This workflow demonstrates hierarchical workflow execution where a parent
    workflow spawns multiple child workflows, each running in their own
    workflow context while being tracked as sub-invocations of the parent.

    :param parent_config: Configuration for the parent workflow
    :return: Aggregated results from all child workflows
    """
    workflow_id = parent_workflow_with_children.wf.identity.workflow_id
    start_time = parent_workflow_with_children.wf.utc_now()

    # Store parent workflow metadata
    parent_workflow_with_children.wf.set_data("workflow_type", "parent_with_children")
    parent_workflow_with_children.wf.set_data("started_at", start_time.isoformat())
    parent_workflow_with_children.wf.set_data("parent_config", parent_config)

    child_results = []
    num_children = parent_config.get("num_children", 3)

    try:
        # Create multiple child workflows
        for i in range(num_children):
            child_data = {
                "user_id": f"child_user_{i}",
                "email": f"child{i}@example.com",
                "child_index": i,
                "parent_workflow": workflow_id,
            }

            # Execute child task to create child workflow
            child_result = parent_workflow_with_children.wf.execute_task(
                child_workflow_task, child_data
            )
            child_results.append(child_result.result)

        # Also execute some regular tasks within the parent workflow
        for i in range(2):
            regular_task_data = {
                "user_id": f"regular_user_{i}",
                "email": f"regular{i}@example.com",
            }
            parent_workflow_with_children.wf.execute_task(
                validate_input_data, regular_task_data
            )

        end_time = parent_workflow_with_children.wf.utc_now()
        parent_workflow_with_children.wf.set_data("status", "completed")
        parent_workflow_with_children.wf.set_data("completed_at", end_time.isoformat())

        return {
            "parent_workflow_id": workflow_id,
            "status": "completed",
            "num_children_created": len(child_results),
            "child_results": child_results,
            "processing_time_seconds": (end_time - start_time).total_seconds(),
            "completed_at": end_time.isoformat(),
        }

    except Exception as e:
        error_time = parent_workflow_with_children.wf.utc_now()
        parent_workflow_with_children.wf.set_data("status", "error")
        parent_workflow_with_children.wf.set_data("error", str(e))
        parent_workflow_with_children.wf.set_data("error_at", error_time.isoformat())

        return {
            "parent_workflow_id": workflow_id,
            "status": "error",
            "error": str(e),
            "error_at": error_time.isoformat(),
        }


def test_parent_workflow_with_child_workflows_and_sub_invocations_view(
    pynmon_client: "PynmonClient",
) -> None:
    """
    Test parent workflow that creates child workflows and verify the sub-invocations view.

    This comprehensive test validates:
    1. Parent workflow execution with child workflows
    2. Child workflows are tracked correctly as sub-invocations
    3. Sub-invocations view shows both task invocations and child workflows
    4. Proper distinction between task invocations and child workflows in the UI
    5. Navigation between parent and child workflow views
    6. State backend methods for sub-invocation tracking work correctly
    """
    # Purge any existing data
    app.purge()

    # Execute parent workflow that creates child workflows
    parent_config = {"num_children": 3}
    parent_invocation = parent_workflow_with_children(parent_config)

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for parent workflow to complete
        result = parent_invocation.result
        assert result is not None

        # Debug: Print the result to see what the error is
        if result["status"] == "error":
            print(f"Workflow failed with error: {result.get('error', 'Unknown error')}")

        assert result["status"] == "completed"
        assert result["num_children_created"] == 3

        parent_workflow_id = result["parent_workflow_id"]

        # Give time for all workflow data to be stored in state backend
        time.sleep(0.5)

        # Test parent workflow detail view shows the workflow
        response = pynmon_client.get(
            f"/workflows/{parent_workflow_with_children.task_id}"
        )
        assert response.status_code == 200
        assert parent_workflow_id in response.text
        # Test the new sub-invocations view
        sub_invocations_response = pynmon_client.get(
            f"/workflows/{parent_workflow_id}/invocations"
        )
        print(
            f"DEBUG: Sub-invocations response status: {sub_invocations_response.status_code}"
        )
        assert sub_invocations_response.status_code == 200

        # Debug: Check what sub-invocations are tracked in the state backend
        sub_invocation_ids = list(
            app.state_backend.get_workflow_sub_invocations(parent_workflow_id)
        )
        print(f"DEBUG: Sub-invocation IDs tracked: {sub_invocation_ids}")

        # Debug: Check actual invocations
        for inv_id in sub_invocation_ids:
            inv = app.orchestrator.get_invocation(inv_id)
            if inv:
                print(
                    f"DEBUG: Invocation {inv_id}: task_id={getattr(inv, 'task_id', 'unknown')}, workflow_id={getattr(inv, 'workflow_id', 'unknown')}"
                )
            else:
                print(f"DEBUG: Could not retrieve invocation {inv_id}")

        # Verify page content shows workflow information
        content = sub_invocations_response.text

        # Debug: Print part of the response to see what's actually being returned
        print(f"DEBUG: Response content snippet (1000-3000): {content[1000:3000]}")
        print(f"DEBUG: 'Child Workflows' in content: {'Child Workflows' in content}")
        print(
            f"DEBUG: 'Workflow Sub-Invocations' in content: {'Workflow Sub-Invocations' in content}"
        )

        assert "Workflow Sub-Invocations" in content
        assert parent_workflow_id in content

        # Should show the parent workflow task ID
        assert parent_workflow_with_children.task_id in content

        # Verify child workflows section exists and shows child workflows
        assert "Child Workflows" in content

        # Should show all 3 child workflows
        child_workflow_ids = [
            child["child_workflow_id"] for child in result["child_results"]
        ]
        for child_id in child_workflow_ids:
            assert child_id in content

        # Verify task invocations section exists and shows regular task invocations
        assert "Task Invocations" in content

        # Should show validate_input_data task invocations (from regular tasks in parent)
        assert validate_input_data.task_id in content

        # Verify action buttons exist
        assert "View Child Invocations" in content
        assert "View Details" in content
        assert "View Call" in content

        # Test that clicking on child workflow leads to its sub-invocations
        if child_workflow_ids:
            first_child_id = child_workflow_ids[0]
            child_sub_invocations_response = pynmon_client.get(
                f"/workflows/{first_child_id}/invocations"
            )
            assert child_sub_invocations_response.status_code == 200

            child_content = child_sub_invocations_response.text
            assert "Workflow Sub-Invocations" in child_content
            assert first_child_id in child_content

            # Child workflow should show validate_input_data task invocation
            assert validate_input_data.task_id in child_content

            # Should show parent workflow link
            assert "Parent Workflow" in child_content
            assert parent_workflow_id in child_content

        # Test workflow runs view includes all workflows
        runs_response = pynmon_client.get("/workflows/runs")
        assert runs_response.status_code == 200
        runs_content = runs_response.text

        # Should show parent workflow
        assert parent_workflow_id in runs_content

        # Should show all child workflows
        for child_id in child_workflow_ids:
            assert child_id in runs_content

        # Verify the state backend methods are working correctly
        # Get sub-invocations directly from state backend to verify data integrity
        sub_invocation_ids = list(
            app.state_backend.get_workflow_sub_invocations(parent_workflow_id)
        )

        # Should have invocations for:
        # - 3 child workflows (each creates one invocation for child_workflow_task)
        # - 2 regular task invocations (validate_input_data called twice)
        # Total: 5 sub-invocations
        assert len(sub_invocation_ids) == 5

        # Get actual invocation objects to analyze
        sub_invocations = []
        for inv_id in sub_invocation_ids:
            if invocation := app.orchestrator.get_invocation(inv_id):
                sub_invocations.append(invocation)

        # Count child workflows vs regular tasks
        child_workflow_invocations = []
        task_invocations = []

        for inv in sub_invocations:
            if inv.is_main_workflow_task():
                child_workflow_invocations.append(inv)
            else:
                task_invocations.append(inv)

        assert (
            len(child_workflow_invocations) == 3
        ), "Should have 3 child workflow invocations"
        assert len(task_invocations) == 2, "Should have 2 regular task invocations"

        # Verify child workflow invocations are for child_workflow_task
        for inv in child_workflow_invocations:
            assert inv.task.task_id == child_workflow_task.task_id

        # Verify regular task invocations are for validate_input_data
        for inv in task_invocations:
            assert inv.task.task_id == validate_input_data.task_id

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=2)


def test_sub_workflow_state_backend_tracking() -> None:
    """
    Test that the state backend correctly tracks sub-invocations.

    This test focuses specifically on the state backend functionality
    for storing and retrieving workflow sub-invocations.
    """
    # Purge any existing data
    app.purge()

    # Execute a simple parent workflow
    parent_config = {"num_children": 2}
    parent_invocation = parent_workflow_with_children(parent_config)

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for workflow completion
        result = parent_invocation.result
        assert result is not None
        assert result["status"] == "completed"

        parent_workflow_id = result["parent_workflow_id"]

        # Give time for sub-invocation tracking to complete
        time.sleep(0.3)

        # Test state backend methods directly
        sub_invocation_ids = list(
            app.state_backend.get_workflow_sub_invocations(parent_workflow_id)
        )

        # Should have 4 sub-invocations total:
        # - 2 child workflows (each creates one invocation for child_workflow_task)
        # - 2 regular task invocations (validate_input_data called twice)
        assert len(sub_invocation_ids) == 4

        # Verify all sub-invocation IDs are valid
        for inv_id in sub_invocation_ids:
            invocation = app.orchestrator.get_invocation(inv_id)
            assert invocation is not None
            assert hasattr(invocation, "task")
            assert invocation.task.task_id in [
                child_workflow_task.task_id,
                validate_input_data.task_id,
            ]

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=2)
