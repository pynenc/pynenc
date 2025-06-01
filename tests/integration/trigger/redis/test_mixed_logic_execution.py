"""Test mixed OR and AND logic triggers."""

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
app = Pynenc(app_id="test_mixed_logic_execution", config_values=config)


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


# AND logic trigger: task_d executes only when BOTH task_a AND task_b complete successfully
@app.task(
    triggers=TriggerBuilder()
    .on_status(task_a, statuses=["SUCCESS"])
    .on_status(task_b, statuses=["SUCCESS"])
    .with_logic("and")
)
def task_d() -> str:
    return "task_d_result"


def test_mixed_or_and_logic(runner: Any) -> None:
    """Test that OR and AND triggers with overlapping conditions work correctly."""

    # Execute task_a (triggers OR but not AND yet)
    task_a()
    time.sleep(0.5)

    # task_c should execute once (OR condition met)
    # task_d should not execute yet (AND not complete)
    task_c_invocations = list(app.orchestrator.get_existing_invocations(task_c))
    assert (
        len(task_c_invocations) == 1
    ), f"Expected 1 execution of task_c after task_a, got {len(task_c_invocations)}"

    task_d_invocations = list(app.orchestrator.get_existing_invocations(task_d))
    assert (
        len(task_d_invocations) == 0
    ), f"Expected 0 executions of task_d after task_a, got {len(task_d_invocations)}"

    # Execute task_b (triggers OR again and completes AND)
    task_b()
    time.sleep(0.5)

    # task_c should execute twice total (OR triggered twice)
    # task_d should execute once (AND condition met)
    task_c_invocations = list(app.orchestrator.get_existing_invocations(task_c))
    assert (
        len(task_c_invocations) == 2
    ), f"Expected 2 executions of task_c total, got {len(task_c_invocations)}"

    task_d_invocations = list(app.orchestrator.get_existing_invocations(task_d))
    assert (
        len(task_d_invocations) == 1
    ), f"Expected 1 execution of task_d total, got {len(task_d_invocations)}"

    task_a_invocations = list(app.orchestrator.get_existing_invocations(task_a))
    assert (
        len(task_a_invocations) == 1
    ), f"Expected 1 execution of task_a, got {len(task_a_invocations)}"

    task_b_invocations = list(app.orchestrator.get_existing_invocations(task_b))
    assert (
        len(task_b_invocations) == 1
    ), f"Expected 1 execution of task_b, got {len(task_b_invocations)}"
