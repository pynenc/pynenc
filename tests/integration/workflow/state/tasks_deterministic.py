"""
Task definitions for deterministic workflow integration tests.

This module defines tasks used for testing deterministic operations
across different state backend implementations.
"""
import datetime
from typing import Any

from pynenc import Pynenc

# Create a mock app for task registration
mock_app = Pynenc()


@mock_app.task
def simple_add_task(a: int, b: int) -> int:
    """
    Simple task for testing deterministic task execution.

    :param a: First number
    :param b: Second number
    :return: Sum of the two numbers
    """
    return a + b


@mock_app.task
def deterministic_random_workflow() -> dict[str, Any]:
    """
    Test deterministic random number generation.

    :return: Dictionary with random values and their properties
    """
    # Generate multiple random numbers
    random_values = []
    for _ in range(5):
        random_values.append(deterministic_random_workflow.wf.random())

    return {
        "random_values": random_values,
        "first_value": random_values[0],
        "values_count": len(random_values),
        "all_different": len(set(random_values)) == len(random_values),
        "all_in_range": all(0.0 <= val <= 1.0 for val in random_values),
    }


@mock_app.task
def deterministic_time_workflow() -> dict[str, Any]:
    """
    Test deterministic time operations.

    :return: Dictionary with time values and their properties
    """
    # Generate multiple timestamps
    timestamps = []
    for _ in range(3):
        timestamps.append(deterministic_time_workflow.wf.utc_now())

    # Get base time for comparison
    base_time = deterministic_time_workflow.wf.deterministic.get_base_time()

    return {
        "timestamps": [ts.isoformat() for ts in timestamps],
        "base_time": base_time.isoformat(),
        "first_timestamp": timestamps[0].isoformat(),
        "timestamps_count": len(timestamps),
        "timestamps_ascending": timestamps == sorted(timestamps),
        "all_utc": all(ts.tzinfo == datetime.timezone.utc for ts in timestamps),
    }


@mock_app.task
def deterministic_uuid_workflow() -> dict[str, Any]:
    """
    Test deterministic UUID generation.

    :return: Dictionary with UUID values and their properties
    """
    # Generate multiple UUIDs
    uuids = []
    for _ in range(4):
        uuids.append(deterministic_uuid_workflow.wf.uuid())

    return {
        "uuids": uuids,
        "first_uuid": uuids[0],
        "uuids_count": len(uuids),
        "all_different": len(set(uuids)) == len(uuids),
        "all_valid_format": all(
            len(uuid_str) == 36 and uuid_str.count("-") == 4 for uuid_str in uuids
        ),
    }


@mock_app.task
def deterministic_mixed_workflow() -> dict[str, Any]:
    """
    Test mixed deterministic operations within a single workflow.

    :return: Dictionary with all deterministic operation results
    """
    results: dict[str, Any] = {}

    # Random operations
    results["random1"] = deterministic_mixed_workflow.wf.random()
    results["random2"] = deterministic_mixed_workflow.wf.random()

    # Time operations
    results["time1"] = deterministic_mixed_workflow.wf.utc_now().isoformat()
    results["time2"] = deterministic_mixed_workflow.wf.utc_now().isoformat()

    # UUID operations
    results["uuid1"] = deterministic_mixed_workflow.wf.uuid()
    results["uuid2"] = deterministic_mixed_workflow.wf.uuid()

    # Another random to test sequence
    results["random3"] = deterministic_mixed_workflow.wf.random()

    # Base time for reference
    results[
        "base_time"
    ] = deterministic_mixed_workflow.wf.deterministic.get_base_time().isoformat()

    # Operation counts
    results[
        "random_count"
    ] = deterministic_mixed_workflow.wf.deterministic.get_operation_count("random")
    results[
        "time_count"
    ] = deterministic_mixed_workflow.wf.deterministic.get_operation_count("time")
    results[
        "uuid_count"
    ] = deterministic_mixed_workflow.wf.deterministic.get_operation_count("uuid")

    return results


@mock_app.task
def deterministic_task_execution_workflow() -> dict[str, Any]:
    """
    Test deterministic task execution within a workflow.

    :return: Dictionary with task execution results
    """
    results = {}

    # Execute the same task multiple times with different parameters
    results["task_result_1"] = deterministic_task_execution_workflow.wf.execute_task(
        simple_add_task, 10, 20
    )
    results["task_result_2"] = deterministic_task_execution_workflow.wf.execute_task(
        simple_add_task, 5, 15
    )
    results["task_result_3"] = deterministic_task_execution_workflow.wf.execute_task(
        simple_add_task, 100, 200
    )

    # Execute the same task with same parameters again (should be deterministic)
    results[
        "task_result_1_repeat"
    ] = deterministic_task_execution_workflow.wf.execute_task(simple_add_task, 10, 20)

    # Mix with other deterministic operations
    results["random_between_tasks"] = deterministic_task_execution_workflow.wf.random()
    results["uuid_between_tasks"] = deterministic_task_execution_workflow.wf.uuid()

    # Execute another task
    results["task_result_4"] = deterministic_task_execution_workflow.wf.execute_task(
        simple_add_task, 7, 8
    )

    return results
