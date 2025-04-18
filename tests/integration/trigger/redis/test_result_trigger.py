"""
Test for result-based task triggering in the Redis trigger system.

This test verifies that tasks can be triggered when another task
produces a specific result, with arguments derived from that result.
"""

import time
from typing import Any

from pynenc import Pynenc
from pynenc.trigger.conditions.result import ResultContext
from pynenc.trigger.trigger_builder import TriggerBuilder

# Configure app for testing
config = {
    "state_backend_cls": "RedisStateBackend",
    "serializer_cls": "JsonSerializer",
    "orchestrator_cls": "RedisOrchestrator",
    "broker_cls": "RedisBroker",
    "runner_cls": "ThreadRunner",
    "trigger_cls": "RedisTrigger",
}

app = Pynenc(app_id="test_trigger_redis_result", config_values=config)


@app.task
def multiply(x: int, y: int) -> int:
    """Multiply two numbers together."""
    return x * y


def build_args_from_result(r: ResultContext) -> dict[str, Any]:
    """
    Build arguments from the result context.

    :param r: The result context
    :return: A dictionary of arguments
    """
    input_args = r.arguments.kwargs
    return {
        "original_args": f"x={input_args.get('x')}, y={input_args.get('y')}",
        "result_value": r.result,
    }


@app.task(
    triggers=TriggerBuilder()
    .on_result(multiply, filter_result=12, call_arguments={"x": 3, "y": 4})
    .with_args_from_result(build_args_from_result)
)
def report_multiplication(original_args: str, result_value: int) -> str:
    """
    Task that runs after multiply task produces a specific result.

    :param original_args: Original arguments passed to the multiply task
    :param result_value: Result value from the multiply task
    :return: A formatted string with details about the multiplication
    """
    return f"Multiplication with {original_args} produced result: {result_value}"


def test_on_result_trigger(runner: None) -> None:
    """
    Test that a task is triggered when another task produces a specific result.

    Verifies that report_multiplication task is executed when the multiply task
    produces the expected result with specific arguments.
    """
    # Execute the first task with the arguments that should trigger report_multiplication
    multiply_args = {"x": 3, "y": 4}
    multiply_invocation = multiply(**multiply_args)

    # Wait for it to complete and trigger the next task
    time.sleep(0.5)

    # Verify the result of the first task
    assert multiply_invocation.result == 12

    # Check that report_multiplication was triggered
    invocations = list(app.orchestrator.get_existing_invocations(report_multiplication))

    # Should have exactly one invocation
    assert len(invocations) == 1

    # Verify the most recent invocation has the expected arguments
    expected_args = {
        "original_args": "x=3, y=4",
        "result_value": 12,
    }
    triggered_invocation = invocations[-1]
    assert triggered_invocation.call.arguments.kwargs == expected_args

    # Verify the result of the triggered task
    assert (
        triggered_invocation.result
        == "Multiplication with x=3, y=4 produced result: 12"
    )
