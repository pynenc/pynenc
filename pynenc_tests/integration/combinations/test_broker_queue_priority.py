from typing import TYPE_CHECKING

from pynenc.conf.config_broker import BrokerPriorityRule
from pynenc.invocation import DistributedInvocation
from pynenc.runner.runner_context import RunnerContext
from pynenc_tests.integration.combinations import tasks
from pynenc_tests.integration.combinations.conftest import replace_tasks_app

if TYPE_CHECKING:
    from pynenc import Pynenc


def test_broker_respects_runner_queues_and_task_priority(app: "Pynenc") -> None:
    """Only selected queues are consumed, with priority respected inside a queue."""
    app.broker.conf.queues = ("default", "payments")
    app.runner.conf.queues = ("payments",)
    replace_tasks_app(app)
    app.broker.purge()

    default_invocation = tasks.broker_default_queue_task("default")
    normal_payment_invocation = tasks.broker_payment_queue_task("normal")
    urgent_payment_invocation = tasks.broker_urgent_payment_queue_task("urgent")

    assert isinstance(default_invocation, DistributedInvocation)
    assert isinstance(normal_payment_invocation, DistributedInvocation)
    assert isinstance(urgent_payment_invocation, DistributedInvocation)

    assert app.broker.count_invocations(("default",)) == 1
    assert app.broker.count_invocations(("payments",)) == 2

    runner_ctx = RunnerContext.from_runner(app.runner)
    invocations_to_run = list(
        app.orchestrator.get_invocations_to_run(
            3,
            runner_ctx,
            queue_provider=app.runner.queue_names_for_retrieval,
            on_queue_retrieved=app.runner.note_queue_retrieved,
        )
    )

    assert [inv.invocation_id for inv in invocations_to_run] == [
        urgent_payment_invocation.invocation_id,
        normal_payment_invocation.invocation_id,
    ]
    assert app.broker.count_invocations(("payments",)) == 0
    assert app.broker.count_invocations(("default",)) == 1


def test_broker_rules_override_concrete_task_priority(app: "Pynenc") -> None:
    """Matching broker rules override the task's concrete priority value."""
    app.broker.conf.queues = ("default", "payments")
    app.broker.conf.priority_rules = (
        BrokerPriorityRule(tasks.broker_payment_queue_task.task_id.key, 50.0),
        BrokerPriorityRule(
            tasks.broker_zero_override_payment_queue_task.task_id.key, 100.0
        ),
    )
    app.runner.conf.queues = ("payments",)
    replace_tasks_app(app)
    app.broker.purge()

    broker_rule_invocation = tasks.broker_payment_queue_task("rule")
    zero_override_invocation = tasks.broker_zero_override_payment_queue_task("zero")

    assert isinstance(broker_rule_invocation, DistributedInvocation)
    assert isinstance(zero_override_invocation, DistributedInvocation)
    assert tasks.broker_payment_queue_task.broker_priority == 50.0
    assert tasks.broker_zero_override_payment_queue_task.broker_priority == 100.0

    runner_ctx = RunnerContext.from_runner(app.runner)
    invocations_to_run = list(
        app.orchestrator.get_invocations_to_run(
            2,
            runner_ctx,
            queue_provider=app.runner.queue_names_for_retrieval,
            on_queue_retrieved=app.runner.note_queue_retrieved,
        )
    )

    assert [inv.invocation_id for inv in invocations_to_run] == [
        zero_override_invocation.invocation_id,
        broker_rule_invocation.invocation_id,
    ]
