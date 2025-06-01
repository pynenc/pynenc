"""Test AND logic trigger execution."""

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
app = Pynenc(app_id="test_and_logic_execution", config_values=config)


@app.task
def task_a() -> str:
    return "task_a_result"


@app.task
def task_b() -> str:
    return "task_b_result"


# AND logic trigger: task_c executes only when BOTH task_a AND task_b complete successfully
@app.task(
    triggers=TriggerBuilder()
    .on_status(task_a, statuses=["SUCCESS"])
    .on_status(task_b, statuses=["SUCCESS"])
    .with_logic("and")
)
def task_c() -> str:
    return "task_c_result"


def test_and_logic_single_execution(runner: Any) -> None:
    """Test that AND logic triggers execute only once when all conditions are met."""

    # Execute task_a (first AND condition)
    task_a()
    time.sleep(0.5)

    # task_c should not execute yet (only one condition met)
    task_c_invocations = list(app.orchestrator.get_existing_invocations(task_c))
    assert (
        len(task_c_invocations) == 0
    ), f"Expected 0 executions of task_c, got {len(task_c_invocations)}"

    # Execute task_b (second AND condition)
    task_b()
    time.sleep(0.5)

    # task_c should execute once (all conditions met)
    task_c_invocations = list(app.orchestrator.get_existing_invocations(task_c))
    assert (
        len(task_c_invocations) == 1
    ), f"Expected 1 execution of task_c, got {len(task_c_invocations)}"

    task_a_invocations = list(app.orchestrator.get_existing_invocations(task_a))
    assert (
        len(task_a_invocations) == 1
    ), f"Expected 1 execution of task_a, got {len(task_a_invocations)}"

    task_b_invocations = list(app.orchestrator.get_existing_invocations(task_b))
    assert (
        len(task_b_invocations) == 1
    ), f"Expected 1 execution of task_b, got {len(task_b_invocations)}"
