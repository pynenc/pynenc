#!/usr/bin/env python3
"""
Simple test to verify that workflow sub-invocations are properly tracked.
"""

from pynenc import Pynenc

# Create app with memory backend for testing
app = Pynenc(config_values={"runner_cls": "ThreadRunner"})


@app.task
def child_task(x: int) -> int:
    """A simple child task that will be tracked as a sub-invocation."""
    return x * 2


@app.task
def parent_workflow(numbers: list[int]) -> dict:
    """A workflow that executes child tasks, creating sub-invocations."""
    results = []

    # Execute multiple child tasks within this workflow
    for num in numbers:
        result = parent_workflow.wf.execute_task(child_task, num)
        results.append(result.result)

    return {
        "original": numbers,
        "doubled": results,
        "workflow_id": parent_workflow.invocation.workflow.workflow_invocation_id,
    }


def test_sub_invocation_tracking(runner: None) -> None:
    """Test that sub-invocations are properly tracked."""
    print("Testing sub-invocation tracking...")

    # Execute the parent workflow
    invocation = parent_workflow([1, 2, 3, 4])
    result = invocation.result

    print(f"Workflow result: {result}")
    print(f"Workflow ID: {result['workflow_id']}")

    # Check that sub-invocations were tracked
    sub_invocations = list(
        app.state_backend.get_workflow_sub_invocations(result["workflow_id"])
    )

    print(f"Sub-invocations tracked: {len(sub_invocations)}")
    for sub_id in sub_invocations:
        print(f"  - {sub_id}")

    # Should have 4 sub-invocations (one for each child_task call)
    assert (
        len(sub_invocations) == 4
    ), f"Expected 4 sub-invocations, got {len(sub_invocations)}"

    print("✅ Sub-invocation tracking is working correctly!")
