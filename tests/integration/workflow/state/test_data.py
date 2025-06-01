"""
Integration tests for workflow data persistence and management.

This module verifies that workflow data is properly persisted across task
executions and that different workflow instances have isolated data across
different state backend implementations.
"""
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.task import Task


def test_workflow_data_basic(workflow_data_test_runner: "Task") -> None:
    """
    Test basic workflow data operations within a single workflow context.

    :param workflow_data_test_runner: Task fixture for testing workflow data operations
    """
    app = workflow_data_test_runner.app

    def run_in_thread() -> None:
        app.runner.run()

    invocation = workflow_data_test_runner()
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    # Get the result
    results = invocation.result

    # Verify all tests passed
    assert results["basic_set_get"], "Basic set/get operations should work"
    assert results["basic_update"], "Data updates should persist"
    assert results[
        "default_value"
    ], "Default values should be returned for missing keys"
    assert results["none_value"], "None should be returned when no default is provided"
    assert results[
        "counter_persistence"
    ], "Counter should persist across operations within workflow"
    assert results[
        "different_types"
    ], "Different data types should be stored and retrieved correctly"

    app.runner.stop_runner_loop()
    thread.join()


def test_workflow_data_persistence(multi_counter_workflow: "Task") -> None:
    """
    Test that data persists across multiple operations within a workflow.

    :param multi_counter_workflow: Task fixture for testing data persistence
    """
    app = multi_counter_workflow.app

    def run_in_thread() -> None:
        app.runner.run()

    invocation = multi_counter_workflow()
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    # Get the result
    results = invocation.result

    # Verify counter progression
    assert results["first"] == 1, "First counter increment should be 1"
    assert results["second"] == 2, "Second counter increment should be 2"
    assert results["third"] == 3, "Third counter increment should be 3"

    app.runner.stop_runner_loop()
    thread.join()


def test_workflow_data_isolation(isolated_workflow_test: "Task") -> None:
    """
    Test that each workflow invocation creates isolated data storage.

    :param isolated_workflow_test: Task fixture for testing workflow isolation
    """
    app = isolated_workflow_test.app

    def run_in_thread() -> None:
        app.runner.run()

    # Create multiple workflow invocations
    first_invocation = isolated_workflow_test(100)
    second_invocation = isolated_workflow_test(200)
    third_invocation = isolated_workflow_test(300)

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    # Get results
    first_result = first_invocation.result
    second_result = second_invocation.result
    third_result = third_invocation.result

    # Each workflow should have different identities
    assert (
        first_result["workflow_id"] != second_result["workflow_id"]
    ), "Workflow IDs should be different"
    assert (
        first_result["workflow_id"] != third_result["workflow_id"]
    ), "Workflow IDs should be different"
    assert (
        second_result["workflow_id"] != third_result["workflow_id"]
    ), "Workflow IDs should be different"

    # Each workflow should have different invocation IDs
    assert (
        first_result["invocation_id"] != second_result["invocation_id"]
    ), "Invocation IDs should be different"
    assert (
        first_result["invocation_id"] != third_result["invocation_id"]
    ), "Invocation IDs should be different"
    assert (
        second_result["invocation_id"] != third_result["invocation_id"]
    ), "Invocation IDs should be different"

    # Each workflow should have its own isolated data
    assert first_result["stored_value"] == 100, "First workflow should store its value"
    assert (
        second_result["stored_value"] == 200
    ), "Second workflow should store its value"
    assert third_result["stored_value"] == 300, "Third workflow should store its value"

    # Each workflow should have its own counter starting at 1
    assert first_result["counter"] == 1, "Each workflow starts with fresh counter"
    assert second_result["counter"] == 1, "Each workflow starts with fresh counter"
    assert third_result["counter"] == 1, "Each workflow starts with fresh counter"

    app.runner.stop_runner_loop()
    thread.join()


def test_single_counter_workflow(counter_workflow: "Task") -> None:
    """
    Test a simple counter workflow.

    :param counter_workflow: Task fixture for testing simple counter workflow
    """
    app = counter_workflow.app

    def run_in_thread() -> None:
        app.runner.run()

    # Each call creates a new workflow, so counter starts fresh
    first_invocation = counter_workflow()
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    assert first_invocation.result == 1
    app.runner.stop_runner_loop()
    thread.join()

    # Second call creates another new workflow
    def run_in_thread2() -> None:
        app.runner.run()

    second_invocation = counter_workflow()
    thread2 = threading.Thread(target=run_in_thread2, daemon=True)
    thread2.start()
    assert second_invocation.result == 1  # Starts fresh in new workflow
    app.runner.stop_runner_loop()
    thread2.join()
