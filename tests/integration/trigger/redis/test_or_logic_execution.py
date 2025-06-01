"""Test OR logic trigger execution."""

import time
from typing import Any

from pynenc import Pynenc
from pynenc.trigger.trigger_builder import TriggerBuilder

config = {
    "state_backend_cls": "RedisStateBackend",
    "serializer_cls": "JsonSerializer",
    "orchestrator_cls": "RedisOrchestrator",
    "broker_cls": "RedisBroker",
    "runner_cls": "ThreadRunner",
    "trigger_cls": "RedisTrigger",
}
app = Pynenc(app_id="test_or_logic_execution", config_values=config)


@app.task
def task_a() -> str:
    return "task_a_result"


@app.task
def task_b() -> str:
    return "task_b_result"


# OR logic trigger: task_c executes when EITHER task_a OR task_b complete successfully
@app.task(
    triggers=TriggerBuilder()
    .on_status(task_a, statuses=["SUCCESS"])
    .on_status(task_b, statuses=["SUCCESS"])
    .with_logic("or")
)
def task_c() -> str:
    return "task_c_result"


def test_or_logic_multiple_executions(runner: Any) -> None:
    """Test that OR logic triggers execute multiple times when multiple conditions are met."""

    # Execute task_a (first OR condition)
    task_a()
    time.sleep(0.5)

    # Execute task_b (second OR condition)
    task_b()
    time.sleep(0.5)

    # task_c should execute twice due to OR logic
    task_c_invocations = list(app.orchestrator.get_existing_invocations(task_c))
    assert (
        len(task_c_invocations) == 2
    ), f"Expected 2 executions of task_c, got {len(task_c_invocations)}"

    task_a_invocations = list(app.orchestrator.get_existing_invocations(task_a))
    assert (
        len(task_a_invocations) == 1
    ), f"Expected 1 execution of task_a, got {len(task_a_invocations)}"

    task_b_invocations = list(app.orchestrator.get_existing_invocations(task_b))
    assert (
        len(task_b_invocations) == 1
    ), f"Expected 1 execution of task_b, got {len(task_b_invocations)}"
