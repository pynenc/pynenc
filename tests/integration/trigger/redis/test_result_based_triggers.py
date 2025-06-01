"""Test result-based trigger conditions."""

import time

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
app = Pynenc(app_id="test_result_based_triggers", config_values=config)


@app.task
def task_a() -> str:
    return "task_a_result"


@app.task
def task_b() -> str:
    return "task_b_result"


# Result-based OR trigger: task_c executes when task_a returns "task_a_result" OR task_b returns "task_b_result"
@app.task(
    triggers=TriggerBuilder()
    .on_result(task_a, filter_result="task_a_result")
    .on_result(task_b, filter_result="task_b_result")
    .with_logic("or")
)
def task_c() -> str:
    return "task_c_result"


def test_result_based_or_logic(runner: None) -> None:
    """Test that result-based OR triggers work correctly."""

    # Execute task_a (should trigger based on result)
    task_a()
    time.sleep(0.5)

    # task_c should execute once (OR condition met by result)
    task_c_invocations = list(app.orchestrator.get_existing_invocations(task_c))
    assert (
        len(task_c_invocations) == 1
    ), f"Expected 1 execution of task_c after task_a, got {len(task_c_invocations)}"

    # Execute task_b (should trigger again based on result)
    task_b()
    time.sleep(0.5)

    # task_c should execute twice total (OR triggered twice by results)
    task_c_invocations = list(app.orchestrator.get_existing_invocations(task_c))
    assert (
        len(task_c_invocations) == 2
    ), f"Expected 2 executions of task_c total, got {len(task_c_invocations)}"

    task_a_invocations = list(app.orchestrator.get_existing_invocations(task_a))
    assert (
        len(task_a_invocations) == 1
    ), f"Expected 1 execution of task_a, got {len(task_a_invocations)}"

    task_b_invocations = list(app.orchestrator.get_existing_invocations(task_b))
    assert (
        len(task_b_invocations) == 1
    ), f"Expected 1 execution of task_b, got {len(task_b_invocations)}"
