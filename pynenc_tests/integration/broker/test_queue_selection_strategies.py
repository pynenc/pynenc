from typing import TYPE_CHECKING
from unittest.mock import patch

from pynenc.conf.config_runner import QueueSelectionStrategy
from pynenc.invocation import DistributedInvocation
from pynenc.runner.runner_context import RunnerContext
from pynenc_tests.integration.broker import queue_rotation_tasks

if TYPE_CHECKING:
    from pynenc import Pynenc


def bind_rotation_tasks(app: "Pynenc") -> None:
    for task in (
        queue_rotation_tasks.default_rotation_task,
        queue_rotation_tasks.payment_rotation_task,
        queue_rotation_tasks.report_rotation_task,
    ):
        task.app = app
        task.__dict__.pop("conf", None)
        app.tasks[task.task_id] = task


def invocations_to_run(app: "Pynenc", count: int) -> list[DistributedInvocation]:
    runner_ctx = RunnerContext.from_runner(app.runner)
    return list(
        app.orchestrator.get_invocations_to_run(
            count,
            runner_ctx,
            queue_provider=app.runner.queue_names_for_retrieval,
            on_queue_retrieved=app.runner.note_queue_retrieved,
        )
    )


def configure_queue_rotation_app(
    app: "Pynenc",
    strategy: QueueSelectionStrategy,
    queue_order: tuple[str, ...] = ("default", "payments", "reports"),
) -> None:
    app.broker.conf.queues = ("default", "payments", "reports")
    app.runner.conf.queues = queue_order
    app.runner.conf.queue_selection_strategy = strategy
    bind_rotation_tasks(app)
    app.broker.purge()


def test_round_robin_strategy_rotates_successful_dequeues_per_broker(
    app_instance: "Pynenc",
) -> None:
    app = app_instance
    configure_queue_rotation_app(app, QueueSelectionStrategy.ROUND_ROBIN)

    default_invocation = queue_rotation_tasks.default_rotation_task("default-1")
    payment_invocation_1 = queue_rotation_tasks.payment_rotation_task("payment-1")
    report_invocation = queue_rotation_tasks.report_rotation_task("report-1")
    payment_invocation_2 = queue_rotation_tasks.payment_rotation_task("payment-2")
    assert isinstance(default_invocation, DistributedInvocation)
    assert isinstance(payment_invocation_1, DistributedInvocation)
    assert isinstance(report_invocation, DistributedInvocation)
    assert isinstance(payment_invocation_2, DistributedInvocation)

    assert [invocation.invocation_id for invocation in invocations_to_run(app, 4)] == [
        default_invocation.invocation_id,
        payment_invocation_1.invocation_id,
        report_invocation.invocation_id,
        payment_invocation_2.invocation_id,
    ]


def test_ordered_strategy_drains_earlier_queues_first_per_broker(
    app_instance: "Pynenc",
) -> None:
    app = app_instance
    configure_queue_rotation_app(
        app,
        QueueSelectionStrategy.ORDERED,
        queue_order=("payments", "default", "reports"),
    )

    default_invocation = queue_rotation_tasks.default_rotation_task("default-1")
    payment_invocation_1 = queue_rotation_tasks.payment_rotation_task("payment-1")
    report_invocation = queue_rotation_tasks.report_rotation_task("report-1")
    payment_invocation_2 = queue_rotation_tasks.payment_rotation_task("payment-2")
    assert isinstance(default_invocation, DistributedInvocation)
    assert isinstance(payment_invocation_1, DistributedInvocation)
    assert isinstance(report_invocation, DistributedInvocation)
    assert isinstance(payment_invocation_2, DistributedInvocation)

    assert [invocation.invocation_id for invocation in invocations_to_run(app, 4)] == [
        payment_invocation_1.invocation_id,
        payment_invocation_2.invocation_id,
        default_invocation.invocation_id,
        report_invocation.invocation_id,
    ]


def test_random_strategy_uses_shuffled_queue_attempt_order_per_broker(
    app_instance: "Pynenc",
) -> None:
    app = app_instance
    configure_queue_rotation_app(app, QueueSelectionStrategy.RANDOM)

    default_invocation = queue_rotation_tasks.default_rotation_task("default-1")
    payment_invocation = queue_rotation_tasks.payment_rotation_task("payment-1")
    report_invocation = queue_rotation_tasks.report_rotation_task("report-1")
    assert isinstance(default_invocation, DistributedInvocation)
    assert isinstance(payment_invocation, DistributedInvocation)
    assert isinstance(report_invocation, DistributedInvocation)

    with patch("pynenc.runner.base_runner.random.shuffle") as mock_shuffle:
        mock_shuffle.side_effect = lambda queue_names: queue_names.reverse()
        assert [
            invocation.invocation_id for invocation in invocations_to_run(app, 3)
        ] == [
            report_invocation.invocation_id,
            payment_invocation.invocation_id,
            default_invocation.invocation_id,
        ]


def test_broker_retrieval_still_accepts_only_one_queue_per_call(
    app_instance: "Pynenc",
) -> None:
    app = app_instance
    configure_queue_rotation_app(app, QueueSelectionStrategy.ROUND_ROBIN)
    invocation = queue_rotation_tasks.payment_rotation_task("payment")
    assert isinstance(invocation, DistributedInvocation)

    assert app.broker.retrieve_invocation("default") is None
    assert app.broker.retrieve_invocation("payments") == invocation.invocation_id
