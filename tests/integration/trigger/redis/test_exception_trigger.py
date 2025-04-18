"""
Test for exception-based task triggering in the Redis trigger system.

This test verifies that tasks can be triggered when another task
raises a specific exception, with arguments derived from that exception.
"""

import time
from typing import Any

import pytest

from pynenc import Pynenc
from pynenc.trigger.conditions.exception import ExceptionContext
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

app = Pynenc(app_id="test_trigger_redis_exception", config_values=config)


@app.task
def divide(x: int, y: int) -> float:
    if x < 0 or y < 0:
        raise ValueError("Negative numbers not allowed")
    return x / y


def build_args_from_exception(ctx: ExceptionContext) -> dict[str, Any]:
    input_args = ctx.arguments.kwargs
    return {
        "original_args": f"x={input_args.get('x')}, y={input_args.get('y')}",
        "exception_type": ctx.exception_type,
        "exception_message": ctx.exception_message,
    }


@app.task(
    triggers=TriggerBuilder()
    .on_exception(divide, exception_types="ZeroDivisionError")
    .with_args_from_exception(build_args_from_exception)
)
def report_div_zero_error(
    original_args: str, exception_type: str, exception_message: str
) -> str:
    return f"Division with {original_args} failed with {exception_type}: {exception_message}"


@app.task(
    triggers=TriggerBuilder()
    .on_exception(divide, exception_types=["ValueError"])
    .with_args_from_exception(build_args_from_exception)
)
def report_value_error(
    original_args: str, exception_type: str, exception_message: str
) -> str:
    return f"Division with {original_args} failed with {exception_type}: {exception_message}"


@app.task(
    triggers=TriggerBuilder()
    .on_exception(divide)
    .with_args_from_exception(build_args_from_exception)
)
def report_any_exception(
    original_args: str, exception_type: str, exception_message: str
) -> str:
    return f"Division with {original_args} failed with {exception_type}: {exception_message}"


def test_exception_trigger(runner: None) -> None:
    """
    Test that a task is triggered when another task raises a specific exception.

    Verifies that report_division_error task is executed when the divide task
    raises a ZeroDivisionError with specific arguments.
    """
    # Execute the task with arguments that will raise ZeroDivisionError
    invocation_div_zero = divide(x=10, y=0)
    invocation_neg_val = divide(x=-10, y=5)

    with pytest.raises(ZeroDivisionError):
        _ = invocation_div_zero.result
    with pytest.raises(ValueError):
        _ = invocation_neg_val.result

    # Wait for exception handling and triggering to complete
    time.sleep(2)

    # Check that report_division_error was triggered
    invocations_report_zero = list(
        app.orchestrator.get_existing_invocations(report_div_zero_error)
    )
    invocations_report_val = list(
        app.orchestrator.get_existing_invocations(report_value_error)
    )
    invocations_report_any = list(
        app.orchestrator.get_existing_invocations(report_any_exception)
    )

    # Should have exactly one invocation
    assert len(invocations_report_zero) == 1
    assert len(invocations_report_val) == 1
    assert len(invocations_report_any) == 1

    # Verify the most recent invocation has the expected arguments
    expected_report_zero = (
        "Division with x=10, y=0 failed with ZeroDivisionError: division by zero"
    )
    expected_report_val = (
        "Division with x=-10, y=5 failed with ValueError: Negative numbers not allowed"
    )
    assert invocations_report_zero[-1].result == expected_report_zero
    assert invocations_report_val[-1].result == expected_report_val
    any_results = [i.result for i in invocations_report_any]
    assert any_results[0] in [expected_report_val, expected_report_zero]
