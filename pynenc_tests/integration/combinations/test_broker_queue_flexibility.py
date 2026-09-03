from typing import TYPE_CHECKING

from pynenc.conf.config_runner import QueueSelectionStrategy
from pynenc.invocation import DistributedInvocation
from pynenc.runner.runner_context import RunnerContext
from pynenc_tests.integration.combinations import queue_flexibility_tasks

if TYPE_CHECKING:
    from pynenc import Pynenc


def bind_queue_flexibility_tasks(app: "Pynenc") -> None:
    for task in (
        queue_flexibility_tasks.undeclared_combo_task,
        queue_flexibility_tasks.default_combo_task,
        queue_flexibility_tasks.payment_combo_task,
        queue_flexibility_tasks.urgent_payment_combo_task,
    ):
        task.app = app
        task.__dict__.pop("conf", None)
        app.tasks[task.task_id] = task


def test_runner_can_drain_explicit_unknown_queue(app: "Pynenc") -> None:
    app.broker.conf.queues = ("default", "payments")
    app.runner.conf.queues = ("undeclared",)
    bind_queue_flexibility_tasks(app)
    app.broker.purge()

    undeclared_invocation = queue_flexibility_tasks.undeclared_combo_task("value")
    payment_invocation = queue_flexibility_tasks.urgent_payment_combo_task("new")

    assert isinstance(undeclared_invocation, DistributedInvocation)
    assert isinstance(payment_invocation, DistributedInvocation)
    assert app.broker.count_invocations(("undeclared",)) == 1
    assert app.broker.count_invocations(("payments",)) == 1

    runner_ctx = RunnerContext.from_runner(app.runner)
    invocations_to_run = list(
        app.orchestrator.get_invocations_to_run(
            1,
            runner_ctx,
            queue_provider=app.runner.queue_names_for_retrieval,
            on_queue_retrieved=app.runner.note_queue_retrieved,
        )
    )

    assert [inv.invocation_id for inv in invocations_to_run] == [
        undeclared_invocation.invocation_id
    ]
    assert app.broker.count_invocations(("undeclared",)) == 0
    assert app.broker.count_invocations(("payments",)) == 1


def test_runner_default_consumes_configured_queue_priorities(app: "Pynenc") -> None:
    app.broker.conf.queues = ("default", "payments")
    app.runner.conf.queues = ()
    bind_queue_flexibility_tasks(app)
    app.broker.purge()

    normal_invocation = queue_flexibility_tasks.payment_combo_task("normal")
    urgent_invocation = queue_flexibility_tasks.urgent_payment_combo_task("urgent")

    assert isinstance(normal_invocation, DistributedInvocation)
    assert isinstance(urgent_invocation, DistributedInvocation)

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
        urgent_invocation.invocation_id,
        normal_invocation.invocation_id,
    ]


def test_runner_rotates_across_selected_queues_without_starvation(
    app: "Pynenc",
) -> None:
    app.broker.conf.queues = ("default", "payments")
    app.runner.conf.queues = ("payments", "default")
    bind_queue_flexibility_tasks(app)
    app.broker.purge()

    default_invocation_1 = queue_flexibility_tasks.default_combo_task("default-1")
    normal_invocation = queue_flexibility_tasks.payment_combo_task("normal")
    urgent_invocation = queue_flexibility_tasks.urgent_payment_combo_task("urgent")
    default_invocation_2 = queue_flexibility_tasks.default_combo_task("default-2")

    assert isinstance(default_invocation_1, DistributedInvocation)
    assert isinstance(normal_invocation, DistributedInvocation)
    assert isinstance(urgent_invocation, DistributedInvocation)
    assert isinstance(default_invocation_2, DistributedInvocation)

    runner_ctx = RunnerContext.from_runner(app.runner)
    invocations_to_run = list(
        app.orchestrator.get_invocations_to_run(
            4,
            runner_ctx,
            queue_provider=app.runner.queue_names_for_retrieval,
            on_queue_retrieved=app.runner.note_queue_retrieved,
        )
    )

    assert [inv.invocation_id for inv in invocations_to_run] == [
        urgent_invocation.invocation_id,
        default_invocation_1.invocation_id,
        normal_invocation.invocation_id,
        default_invocation_2.invocation_id,
    ]


def test_ordered_runner_consumes_first_queue_until_empty(app: "Pynenc") -> None:
    app.broker.conf.queues = ("default", "payments")
    app.runner.conf.queues = ("payments", "default")
    app.runner.conf.queue_selection_strategy = QueueSelectionStrategy.ORDERED
    bind_queue_flexibility_tasks(app)
    app.broker.purge()

    default_invocation = queue_flexibility_tasks.default_combo_task("default")
    normal_invocation = queue_flexibility_tasks.payment_combo_task("normal")
    urgent_invocation = queue_flexibility_tasks.urgent_payment_combo_task("urgent")

    assert isinstance(default_invocation, DistributedInvocation)
    assert isinstance(normal_invocation, DistributedInvocation)
    assert isinstance(urgent_invocation, DistributedInvocation)

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
        urgent_invocation.invocation_id,
        normal_invocation.invocation_id,
        default_invocation.invocation_id,
    ]
