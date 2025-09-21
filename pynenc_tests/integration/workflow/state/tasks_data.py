"""
Task definitions for workflow data integration tests.

This module defines tasks used for testing workflow data persistence
and management across different state backend implementations.
"""
from typing import Any

from pynenc import Pynenc

# Create a mock app for task registration
mock_app = Pynenc()


@mock_app.task
def workflow_data_test_runner() -> dict[str, bool]:
    """
    Test workflow data operations within a single workflow context.

    This task runs all data operations within the same workflow to ensure
    proper data persistence and sharing between operations.

    :return: Dictionary of test results indicating which tests passed
    """
    results = {}

    # Test 1: Basic set/get operations
    try:
        # Set a value
        workflow_data_test_runner.wf.set_data("test_key", "test_value")

        # Get the value back
        retrieved_value = workflow_data_test_runner.wf.get_data("test_key")
        results["basic_set_get"] = retrieved_value == "test_value"

        # Update the value
        workflow_data_test_runner.wf.set_data("test_key", "new_value")

        # Get updated value
        updated_value = workflow_data_test_runner.wf.get_data("test_key")
        results["basic_update"] = updated_value == "new_value"

    except Exception:
        results["basic_set_get"] = False
        results["basic_update"] = False

    # Test 2: Default values
    try:
        # Get non-existent key with default
        default_value = workflow_data_test_runner.wf.get_data("missing_key", "default")
        results["default_value"] = default_value == "default"

        # Get non-existent key without default (should return None)
        none_value = workflow_data_test_runner.wf.get_data("missing_key")
        results["none_value"] = none_value is None

    except Exception:
        results["default_value"] = False
        results["none_value"] = False

    # Test 3: Counter persistence within workflow
    try:
        # Initialize counter
        counter = workflow_data_test_runner.wf.get_data("counter", 0)
        counter += 1
        workflow_data_test_runner.wf.set_data("counter", counter)

        # Increment again
        counter = workflow_data_test_runner.wf.get_data("counter", 0)
        counter += 1
        workflow_data_test_runner.wf.set_data("counter", counter)

        # Check final value
        final_counter = workflow_data_test_runner.wf.get_data("counter", 0)
        results["counter_persistence"] = final_counter == 2

    except Exception:
        results["counter_persistence"] = False

    # Test 4: Different data types
    try:
        test_data = {
            "string": "hello",
            "number": 42,
            "list": [1, 2, 3],
            "dict": {"nested": "value"},
        }

        # Store different types
        for key, value in test_data.items():
            workflow_data_test_runner.wf.set_data(key, value)

        # Retrieve and verify
        type_tests_passed = True
        for key, expected_value in test_data.items():
            retrieved = workflow_data_test_runner.wf.get_data(key)
            if retrieved != expected_value:
                type_tests_passed = False
                break

        results["different_types"] = type_tests_passed

    except Exception:
        results["different_types"] = False

    return results


@mock_app.task
def multi_counter_workflow() -> dict[str, int]:
    """
    Test multiple counter operations within a single workflow.

    :return: Dictionary with counter progression showing persistence
    """
    results = {}

    # First increment
    first_count = multi_counter_workflow.wf.get_data("counter", 0) + 1
    multi_counter_workflow.wf.set_data("counter", first_count)
    results["first"] = first_count

    # Second increment
    second_count = multi_counter_workflow.wf.get_data("counter", 0) + 1
    multi_counter_workflow.wf.set_data("counter", second_count)
    results["second"] = second_count

    # Third increment
    third_count = multi_counter_workflow.wf.get_data("counter", 0) + 1
    multi_counter_workflow.wf.set_data("counter", third_count)
    results["third"] = third_count

    return results


@mock_app.task
def isolated_workflow_test(value: int) -> dict[str, Any]:
    """
    Test workflow that stores data and returns workflow identity.

    Each invocation creates a new workflow with isolated data storage.

    :param value: Value to store in workflow data
    :return: Workflow identity and stored data for verification
    """
    # Store the value
    isolated_workflow_test.wf.set_data("stored_value", value)

    # Initialize and increment counter
    counter = isolated_workflow_test.wf.get_data("counter", 0) + 1
    isolated_workflow_test.wf.set_data("counter", counter)

    return {
        "workflow_id": isolated_workflow_test.wf.identity.workflow_id,
        "invocation_id": isolated_workflow_test.wf.identity.workflow_invocation_id,
        "stored_value": isolated_workflow_test.wf.get_data("stored_value"),
        "counter": isolated_workflow_test.wf.get_data("counter"),
    }


@mock_app.task
def counter_workflow() -> int:
    """
    A workflow that maintains a counter within a single workflow execution.

    :return: Current counter value after increment
    """
    # Get current counter value or initialize to 0
    counter = counter_workflow.wf.get_data("counter", 0)

    # Increment counter
    counter += 1

    # Save updated counter
    counter_workflow.wf.set_data("counter", counter)

    return counter
