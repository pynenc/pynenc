"""
Integration tests for pynmon workflow runs with hierarchical task execution.

Tests comprehensive workflow scenarios where main tasks orchestrate multiple
sub-tasks with various execution patterns. These tests validate both the
workflow execution and the pynmon monitoring views that display workflow
hierarchies and execution histories.
"""
import threading
import time
from typing import TYPE_CHECKING, Any

from pynenc.builder import PynencBuilder

if TYPE_CHECKING:
    from pynenc_tests.integration.pynmon.conftest import PynmonClient

# Debug configuration - Set to 1 to keep server alive for browser debugging
KEEP_ALIVE = 0

# Configure app for testing with Memory backend
app = (
    PynencBuilder()
    .memory()
    .thread_runner()
    .app_id("test-pynmon-workflow-runs")
    .serializer_json()
    .build()
)


# Sub-task definitions for hierarchical workflows
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
            "validation_id": validate_input_data.wf.uuid(),
            "validated_at": validate_input_data.wf.utc_now().isoformat(),
        }
    else:
        return {
            "valid": False,
            "errors": ["Missing required fields: user_id, email"],
            "validation_id": validate_input_data.wf.uuid(),
            "validated_at": validate_input_data.wf.utc_now().isoformat(),
        }


@app.task
def process_user_data(user_data: dict[str, Any]) -> dict[str, Any]:
    """
    Process user data and create user profile.

    :param user_data: Validated user data
    :return: Processing result with user profile
    """
    # Generate deterministic values
    profile_id = process_user_data.wf.uuid()
    processing_timestamp = process_user_data.wf.utc_now()

    # Simulate processing delay
    time.sleep(0.1)

    # Store processing data in workflow
    process_user_data.wf.set_data("profile_id", profile_id)
    process_user_data.wf.set_data("processed_at", processing_timestamp.isoformat())

    return {
        "profile_id": profile_id,
        "user_id": user_data["user_id"],
        "email": user_data["email"],
        "status": "processed",
        "created_at": processing_timestamp.isoformat(),
        "metadata": user_data.get("metadata", {}),
    }


@app.task
def generate_user_report(profile_data: dict[str, Any]) -> dict[str, Any]:
    """
    Generate user report based on profile data.

    :param profile_data: User profile data
    :return: Generated report information
    """
    report_id = generate_user_report.wf.uuid()
    report_timestamp = generate_user_report.wf.utc_now()

    # Generate report content
    report_content = {
        "user_summary": f"User {profile_data['user_id']} with email {profile_data['email']}",
        "profile_id": profile_data["profile_id"],
        "generation_time": report_timestamp.isoformat(),
        "report_type": "user_onboarding",
    }

    return {
        "report_id": report_id,
        "content": report_content,
        "generated_at": report_timestamp.isoformat(),
        "status": "completed",
    }


@app.task
def analyze_data_batch(batch_data: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Analyze a batch of data items.

    :param batch_data: List of data items to analyze
    :return: Analysis results
    """
    analysis_id = analyze_data_batch.wf.uuid()
    analysis_start = analyze_data_batch.wf.utc_now()

    # Store batch info in workflow
    analyze_data_batch.wf.set_data("batch_size", len(batch_data))
    analyze_data_batch.wf.set_data("analysis_id", analysis_id)

    # Simulate analysis processing
    total_items = len(batch_data)
    processed_items = 0

    for item in batch_data:
        if "value" in item and isinstance(item["value"], (int, float)):
            processed_items += 1
        time.sleep(0.02)  # Small delay per item

    analysis_end = analyze_data_batch.wf.utc_now()
    processing_time = (analysis_end - analysis_start).total_seconds()

    return {
        "analysis_id": analysis_id,
        "total_items": total_items,
        "processed_items": processed_items,
        "success_rate": processed_items / total_items if total_items > 0 else 0,
        "processing_time_seconds": processing_time,
        "completed_at": analysis_end.isoformat(),
    }


# Main workflow definitions
@app.task(force_new_workflow=True)
def user_onboarding_workflow(user_input: dict[str, Any]) -> dict[str, Any]:
    """
    Main user onboarding workflow that orchestrates multiple sub-tasks.

    This workflow demonstrates hierarchical task execution where a main
    workflow task coordinates multiple sub-tasks in sequence.

    :param user_input: User input data to process
    :return: Complete onboarding result
    """
    workflow_id = user_onboarding_workflow.wf.identity.workflow_id
    start_time = user_onboarding_workflow.wf.utc_now()

    # Store workflow metadata
    user_onboarding_workflow.wf.set_data("workflow_type", "user_onboarding")
    user_onboarding_workflow.wf.set_data("started_at", start_time.isoformat())
    user_onboarding_workflow.wf.set_data("input_data", user_input)

    try:
        # Step 1: Validate input data
        validation_result = user_onboarding_workflow.wf.execute_task(
            validate_input_data, user_input
        )

        if not validation_result.result["valid"]:
            user_onboarding_workflow.wf.set_data("status", "validation_failed")
            return {
                "workflow_id": workflow_id,
                "status": "failed",
                "step": "validation",
                "error": validation_result.result["errors"],
                "completed_at": user_onboarding_workflow.wf.utc_now().isoformat(),
            }

        # Step 2: Process user data
        processing_result = user_onboarding_workflow.wf.execute_task(
            process_user_data, validation_result.result["normalized_data"]
        )

        # Step 3: Generate user report
        report_result = user_onboarding_workflow.wf.execute_task(
            generate_user_report, processing_result.result
        )

        # Mark workflow as completed
        end_time = user_onboarding_workflow.wf.utc_now()
        user_onboarding_workflow.wf.set_data("status", "completed")
        user_onboarding_workflow.wf.set_data("completed_at", end_time.isoformat())

        return {
            "workflow_id": workflow_id,
            "status": "completed",
            "user_profile": processing_result.result,
            "report": report_result.result,
            "validation": validation_result.result,
            "processing_time_seconds": (end_time - start_time).total_seconds(),
            "completed_at": end_time.isoformat(),
        }

    except Exception as e:
        error_time = user_onboarding_workflow.wf.utc_now()
        user_onboarding_workflow.wf.set_data("status", "error")
        user_onboarding_workflow.wf.set_data("error", str(e))
        user_onboarding_workflow.wf.set_data("error_at", error_time.isoformat())

        return {
            "workflow_id": workflow_id,
            "status": "error",
            "error": str(e),
            "error_at": error_time.isoformat(),
        }


@app.task(force_new_workflow=True)
def data_analysis_workflow(batch_config: dict[str, Any]) -> dict[str, Any]:
    """
    Main data analysis workflow that processes multiple data batches.

    This workflow demonstrates parallel sub-task execution and aggregation
    of results from multiple sub-tasks.

    :param batch_config: Configuration for data batches to process
    :return: Aggregated analysis results
    """
    workflow_id = data_analysis_workflow.wf.identity.workflow_id
    start_time = data_analysis_workflow.wf.utc_now()

    # Store workflow metadata
    data_analysis_workflow.wf.set_data("workflow_type", "data_analysis")
    data_analysis_workflow.wf.set_data("started_at", start_time.isoformat())
    data_analysis_workflow.wf.set_data("batch_config", batch_config)

    try:
        # Generate test data batches
        num_batches = batch_config.get("num_batches", 3)
        batch_size = batch_config.get("batch_size", 5)

        analysis_results = []

        # Process each batch using sub-tasks
        for batch_num in range(num_batches):
            # Generate deterministic test data for this batch
            batch_data = []
            for item_num in range(batch_size):
                # Use workflow random for deterministic data generation
                value = data_analysis_workflow.wf.random() * 100
                batch_data.append(
                    {
                        "id": f"item_{batch_num}_{item_num}",
                        "value": value,
                        "batch": batch_num,
                    }
                )

            # Execute analysis task for this batch
            batch_result = data_analysis_workflow.wf.execute_task(
                analyze_data_batch, batch_data
            )
            analysis_results.append(batch_result.result)

            # Store intermediate results
            data_analysis_workflow.wf.set_data(
                f"batch_{batch_num}_result", batch_result.result
            )

        # Aggregate results
        total_items = sum(result["total_items"] for result in analysis_results)
        total_processed = sum(result["processed_items"] for result in analysis_results)
        avg_success_rate = sum(
            result["success_rate"] for result in analysis_results
        ) / len(analysis_results)
        total_processing_time = sum(
            result["processing_time_seconds"] for result in analysis_results
        )

        end_time = data_analysis_workflow.wf.utc_now()
        data_analysis_workflow.wf.set_data("status", "completed")
        data_analysis_workflow.wf.set_data("completed_at", end_time.isoformat())

        return {
            "workflow_id": workflow_id,
            "status": "completed",
            "summary": {
                "total_batches": num_batches,
                "total_items": total_items,
                "total_processed": total_processed,
                "avg_success_rate": avg_success_rate,
                "total_processing_time": total_processing_time,
            },
            "batch_results": analysis_results,
            "workflow_time_seconds": (end_time - start_time).total_seconds(),
            "completed_at": end_time.isoformat(),
        }

    except Exception as e:
        error_time = data_analysis_workflow.wf.utc_now()
        data_analysis_workflow.wf.set_data("status", "error")
        data_analysis_workflow.wf.set_data("error", str(e))
        data_analysis_workflow.wf.set_data("error_at", error_time.isoformat())

        return {
            "workflow_id": workflow_id,
            "status": "error",
            "error": str(e),
            "error_at": error_time.isoformat(),
        }


def test_hierarchical_workflow_execution(pynmon_client: "PynmonClient") -> None:
    """Test hierarchical workflow execution with main task calling sub-tasks."""
    # Purge any existing data
    app.purge()

    # Test data for user onboarding
    user_data = {
        "user_id": "test_user_123",
        "email": "test@example.com",
        "metadata": {"source": "api", "priority": "high"},
    }

    # Execute user onboarding workflow
    invocation = user_onboarding_workflow(user_data)

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for workflow completion
        result = invocation.result
        assert result is not None
        assert result["status"] == "completed"
        assert "workflow_id" in result
        assert "user_profile" in result
        assert "report" in result

        # Give time for workflow data to be stored
        time.sleep(0.3)

        # Test workflow discovery in pynmon
        response = pynmon_client.get("/workflows/")
        assert response.status_code == 200
        assert user_onboarding_workflow.task_id in response.text

        # Test workflow detail view
        response = pynmon_client.get(f"/workflows/{user_onboarding_workflow.task_id}")
        assert response.status_code == 200
        assert result["workflow_id"] in response.text

        # Test workflow runs list
        response = pynmon_client.get("/workflows/runs")
        assert response.status_code == 200
        assert result["workflow_id"] in response.text

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=2)


def test_multiple_workflow_types_with_hierarchies(
    pynmon_client: "PynmonClient",
) -> None:
    """Test multiple workflow types with different hierarchical patterns."""
    # Purge any existing data
    app.purge()

    # Execute user onboarding workflow
    user_data = {"user_id": "test_user_456", "email": "user456@example.com"}
    user_invocation = user_onboarding_workflow(user_data)

    # Execute data analysis workflow
    batch_config = {"num_batches": 2, "batch_size": 3}
    analysis_invocation = data_analysis_workflow(batch_config)

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for both workflows to complete
        user_result = user_invocation.result
        analysis_result = analysis_invocation.result

        assert user_result is not None
        assert analysis_result is not None
        assert user_result["status"] == "completed"
        assert analysis_result["status"] == "completed"

        time.sleep(0.3)

        # Test that both workflow types are discoverable
        response = pynmon_client.get("/workflows/")
        assert response.status_code == 200
        assert user_onboarding_workflow.task_id in response.text
        assert data_analysis_workflow.task_id in response.text

        # Test individual workflow detail views
        user_detail_response = pynmon_client.get(
            f"/workflows/{user_onboarding_workflow.task_id}"
        )
        assert user_detail_response.status_code == 200
        assert user_result["workflow_id"] in user_detail_response.text

        analysis_detail_response = pynmon_client.get(
            f"/workflows/{data_analysis_workflow.task_id}"
        )
        assert analysis_detail_response.status_code == 200
        assert analysis_result["workflow_id"] in analysis_detail_response.text

        # Test that workflow runs contains both workflows
        runs_response = pynmon_client.get("/workflows/runs")
        assert runs_response.status_code == 200
        assert user_result["workflow_id"] in runs_response.text
        assert analysis_result["workflow_id"] in runs_response.text

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=2)


def test_rapid_workflow_execution_stress(pynmon_client: "PynmonClient") -> None:
    """Test rapid execution of multiple workflow instances to stress test monitoring."""
    # Purge any existing data
    app.purge()

    # Execute multiple instances rapidly
    invocations = []

    # Create 5 user onboarding workflows with different data
    for i in range(5):
        user_data = {
            "user_id": f"rapid_user_{i}",
            "email": f"rapid{i}@example.com",
            "metadata": {"batch": "rapid_test", "index": i},
        }
        invocation = user_onboarding_workflow(user_data)
        invocations.append(invocation)

    # Create 3 data analysis workflows
    for _ in range(3):
        batch_config = {"num_batches": 2, "batch_size": 2}
        invocation = data_analysis_workflow(batch_config)
        invocations.append(invocation)

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for all workflows to complete
        results = []
        for invocation in invocations:
            result = invocation.result
            assert result is not None
            assert result["status"] == "completed"
            results.append(result)

        time.sleep(0.5)  # Extra time for rapid execution data to settle

        # Test that all workflows are tracked
        response = pynmon_client.get("/workflows/runs")
        assert response.status_code == 200

        # Verify all workflow IDs appear in the runs list
        for result in results:
            assert result["workflow_id"] in response.text

        # Test that workflow detail views handle multiple instances
        user_detail_response = pynmon_client.get(
            f"/workflows/{user_onboarding_workflow.task_id}"
        )
        assert user_detail_response.status_code == 200

        analysis_detail_response = pynmon_client.get(
            f"/workflows/{data_analysis_workflow.task_id}"
        )
        assert analysis_detail_response.status_code == 200

        # Verify multiple instances of each workflow type are shown
        user_workflow_ids = [
            r["workflow_id"] for r in results[:5]
        ]  # First 5 are user workflows
        analysis_workflow_ids = [
            r["workflow_id"] for r in results[5:]
        ]  # Last 3 are analysis workflows

        for workflow_id in user_workflow_ids:
            assert workflow_id in user_detail_response.text

        for workflow_id in analysis_workflow_ids:
            assert workflow_id in analysis_detail_response.text

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=3)


def test_workflow_with_validation_failure(pynmon_client: "PynmonClient") -> None:
    """Test workflow execution with validation failure to test error handling."""
    # Purge any existing data
    app.purge()

    # Test data missing required fields
    invalid_user_data = {
        "name": "Test User",  # Missing user_id and email
        "metadata": {"source": "test"},
    }

    # Execute user onboarding workflow with invalid data
    invocation = user_onboarding_workflow(invalid_user_data)

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for workflow completion
        result = invocation.result
        assert result is not None
        assert result["status"] == "failed"
        assert result["step"] == "validation"
        assert "error" in result
        assert "workflow_id" in result

        time.sleep(0.3)

        # Test that failed workflow is still discoverable in pynmon
        response = pynmon_client.get("/workflows/")
        assert response.status_code == 200
        assert user_onboarding_workflow.task_id in response.text

        # Test workflow detail view shows failed workflow
        response = pynmon_client.get(f"/workflows/{user_onboarding_workflow.task_id}")
        assert response.status_code == 200
        assert result["workflow_id"] in response.text

        # Test workflow runs list includes failed workflow
        response = pynmon_client.get("/workflows/runs")
        assert response.status_code == 200
        assert result["workflow_id"] in response.text

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=2)


def test_deterministic_workflow_behavior(pynmon_client: "PynmonClient") -> None:
    """Test that workflows exhibit deterministic behavior for monitoring consistency."""
    # Purge any existing data
    app.purge()

    # Execute the same workflow configuration multiple times
    batch_config = {"num_batches": 2, "batch_size": 2}

    invocations = []
    for _ in range(3):
        invocation = data_analysis_workflow(batch_config)
        invocations.append(invocation)

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    try:
        # Wait for all workflows to complete
        results = []
        for invocation in invocations:
            result = invocation.result
            assert result is not None
            assert result["status"] == "completed"
            results.append(result)

        time.sleep(0.3)

        # Verify each workflow has a unique workflow_id
        workflow_ids = [result["workflow_id"] for result in results]
        assert len(set(workflow_ids)) == 3, "Each workflow should have unique ID"

        # Verify similar structure but deterministic differences
        for result in results:
            assert "summary" in result
            assert result["summary"]["total_batches"] == 2
            assert result["summary"]["total_items"] == 4  # 2 batches * 2 items each

        # Test that all instances are tracked in pynmon
        response = pynmon_client.get(f"/workflows/{data_analysis_workflow.task_id}")
        assert response.status_code == 200

        for workflow_id in workflow_ids:
            assert workflow_id in response.text

        # Test workflow runs shows all instances
        runs_response = pynmon_client.get("/workflows/runs")
        assert runs_response.status_code == 200

        for workflow_id in workflow_ids:
            assert workflow_id in runs_response.text

    finally:
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=2)
