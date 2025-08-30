"""
Integration tests for workflow discovery functionality.

This module tests workflow registration and discovery across different state backend
implementations, ensuring that workflow data is properly stored and retrieved for
monitoring and management purposes.
"""
import threading
from typing import TYPE_CHECKING

from pynenc.builder import PynencBuilder

if TYPE_CHECKING:
    pass

# Configure app for testing with Redis backend
app = (
    PynencBuilder()
    .redis(db=15)
    .thread_runner()
    .app_id("test-workflow-discovery")
    .build()
)


@app.task
def simple_workflow() -> dict[str, str]:
    """Simple workflow for testing workflow discovery."""
    workflow_id = simple_workflow.wf.identity.workflow_id

    # Store some workflow data for testing
    simple_workflow.wf.set_data("test_key", "test_value")

    return {"workflow_id": workflow_id}


@app.task
def another_workflow(value: int) -> dict[str, str | int]:
    """Another workflow for testing with parameters."""
    workflow_id = another_workflow.wf.identity.workflow_id

    # Store parameter value in workflow data
    another_workflow.wf.set_data("input_value", value)
    return {"workflow_id": workflow_id, "value": value}


def test_get_all_workflows_single_workflow() -> None:
    """Test workflow discovery with a single workflow execution."""
    # Purge any existing data
    app.purge()
    # Execute a workflow to register it
    invocation = simple_workflow()

    def run_in_thread() -> None:
        app.runner.run()

    # Run the workflow in a separate thread
    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for workflow completion
        result = invocation.result

        # Verify workflow execution completed
        assert result is not None
        assert "workflow_id" in result

        # Test get_all_workflows returns the workflow type
        workflows = list(app.state_backend.get_all_workflows())
        assert len(workflows) >= 1
        assert simple_workflow.task_id in workflows
    finally:
        # Stop the runner
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_get_all_workflows_multiple_workflows() -> None:
    """Test workflow discovery with multiple different workflows."""
    # Purge any existing data
    app.purge()
    # Execute both workflows
    invocation1 = simple_workflow()
    invocation2 = another_workflow(42)

    def run_in_thread() -> None:
        app.runner.run()

    # Run both workflows in a separate thread
    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for workflow completion
        result1 = invocation1.result
        result2 = invocation2.result

        # Verify both workflows executed
        assert result1 is not None
        assert result2 is not None
        assert "workflow_id" in result1
        assert "workflow_id" in result2

        # Test get_all_workflows returns both workflow types
        workflows = list(app.state_backend.get_all_workflows())
        assert simple_workflow.task_id in workflows
        assert another_workflow.task_id in workflows
    finally:
        # Stop the runner
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_get_all_workflows_runs() -> None:
    """Test getting all workflow runs for a specific workflow type."""
    # Purge any existing data
    app.purge()
    # Execute the same workflow multiple times
    invocation1 = simple_workflow()
    invocation2 = simple_workflow()

    def run_in_thread() -> None:
        app.runner.run()

    # Run both instances in a separate thread
    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for workflow completion
        result1 = invocation1.result
        result2 = invocation2.result

        # Verify both executions completed
        assert result1 is not None
        assert result2 is not None

        # Test get_workflow_runs returns both instances
        workflow_runs = list(
            app.state_backend.get_workflow_runs(simple_workflow.task_id)
        )
        assert len(workflow_runs) >= 2

        # Verify the workflow identities are different (different invocation IDs)
        workflow_ids = [run.workflow_id for run in workflow_runs]
        assert len(set(workflow_ids)) >= 2  # Should be unique

        # Verify workflow identities match what we expect
        for run in workflow_runs:
            assert run.workflow_task_id == simple_workflow.task_id
    finally:
        # Stop the runner
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)


def test_workflow_runs() -> None:
    """Test that different workflow types have isolated run lists."""
    # Purge any existing data
    app.purge()
    # Execute both workflow types
    invocation1 = simple_workflow()
    invocation2 = another_workflow(100)

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for completion
        result1 = invocation1.result
        result2 = invocation2.result

        assert result1 is not None
        assert result2 is not None

        # Get runs for each workflow type
        simple_runs = app.state_backend.get_workflow_runs(simple_workflow.task_id)
        another_runs = app.state_backend.get_workflow_runs(another_workflow.task_id)

        # Verify isolation - each workflow type has its own runs
        simple_workflow_ids = {run.workflow_task_id for run in simple_runs}
        another_workflow_ids = {run.workflow_task_id for run in another_runs}

        assert simple_workflow.task_id in simple_workflow_ids
        assert another_workflow.task_id in another_workflow_ids
        assert simple_workflow_ids.isdisjoint(another_workflow_ids)

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=1)
