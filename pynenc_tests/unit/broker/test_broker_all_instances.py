from typing import TYPE_CHECKING

import pytest

from pynenc import PynencBuilder
from pynenc.conf.config_broker import MAX_PRIORITY, MIN_PRIORITY
from pynenc.exceptions import ConfigError
from pynenc.task import Task
from pynenc_tests.conftest import MockPynenc
from pynenc_tests.util import capture_logs

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.identifiers.invocation_id import InvocationId
    from pynenc.invocation import DistributedInvocation

mock_app = MockPynenc()


@mock_app.task
def dummy_task() -> None:
    pass


@mock_app.task
def default_task() -> str:
    return "default"


@mock_app.task(queue="payments")
def payment_task() -> str:
    return "payment"


@mock_app.task(queue="payments", priority=100.0)
def urgent_payment_task() -> str:
    return "urgent-payment"


@mock_app.task(queue="reports")
def report_task() -> str:
    return "report"


@mock_app.task(queue="missing")
def missing_queue_task() -> str:
    return "missing"


@pytest.fixture
def task(app_instance: "Pynenc") -> "Task":
    """Helper to create a dummy invocation."""
    dummy_task.app = app_instance
    return dummy_task


def build_queue_app(backend: str, sqlite_db_path: str) -> "Pynenc":
    builder = (
        PynencBuilder()
        .memory()
        .custom_config(
            queues=("default", "payments", "reports"),
            priority_rules=(
                {"task_id": f"{__name__}.payment_task", "priority": 50.0},
                {"task_id": f"{__name__}.report_task", "priority": -10.0},
            ),
        )
    )
    if backend == "sqlite":
        builder = (
            PynencBuilder()
            .sqlite(sqlite_db_path=sqlite_db_path)
            .custom_config(
                queues=("default", "payments", "reports"),
                priority_rules=(
                    {"task_id": f"{__name__}.payment_task", "priority": 50.0},
                    {"task_id": f"{__name__}.report_task", "priority": -10.0},
                ),
            )
        )
    return builder.build()


def bind_task(app: "Pynenc", source_task: "Task") -> "Task":
    task = Task.clone_for_app(app, source_task)
    app.tasks[task.task_id] = task
    return task


def broker_args_for(
    invocation: "DistributedInvocation",
) -> tuple["InvocationId", str, float]:
    return (
        invocation.invocation_id,
        invocation.task.broker_queue,
        invocation.task.broker_priority,
    )


def test_route_and_retrieve_invocation(task: "Task") -> None:
    """Test that invocations can be routed and retrieved in FIFO order."""
    broker = task.app.broker

    inv_1: DistributedInvocation = task()  # type: ignore
    assert broker.retrieve_invocation() == inv_1.invocation_id
    inv_2: DistributedInvocation = task()  # type: ignore
    assert broker.retrieve_invocation() == inv_2.invocation_id

    broker.route_invocation(*broker_args_for(inv_1))
    broker.route_invocation(*broker_args_for(inv_1))
    broker.route_invocation(*broker_args_for(inv_2))
    broker.route_invocation(*broker_args_for(inv_1))
    assert broker.retrieve_invocation() == inv_1.invocation_id
    assert broker.retrieve_invocation() == inv_1.invocation_id
    assert broker.retrieve_invocation() == inv_2.invocation_id
    assert broker.retrieve_invocation() == inv_1.invocation_id
    assert broker.retrieve_invocation() is None


def test_route_and_retrieve_multiple_invocations(task: "Task") -> None:
    """Test that multiple invocations can be routed and retrieved in FIFO order."""
    broker = task.app.broker

    assert broker.retrieve_invocation() is None

    # It will route automatically when calling the task
    inv_1: DistributedInvocation = task()  # type: ignore
    inv_2: DistributedInvocation = task()  # type: ignore

    assert broker.retrieve_invocation() == inv_1.invocation_id
    assert broker.retrieve_invocation() == inv_2.invocation_id
    assert broker.retrieve_invocation() is None

    # Route again with route_invocations
    broker.route_invocations(
        [inv_1.invocation_id, inv_2.invocation_id, inv_1.invocation_id],
        inv_1.task.broker_queue,
        inv_1.task.broker_priority,
    )
    assert broker.retrieve_invocation() == inv_1.invocation_id
    assert broker.retrieve_invocation() == inv_2.invocation_id
    assert broker.retrieve_invocation() == inv_1.invocation_id
    assert broker.retrieve_invocation() is None


def test_route_invocations_validates_batch_before_queueing(
    task: "Task",
) -> None:
    app = task.app
    invocation: DistributedInvocation = task()  # type: ignore
    app.broker.purge()

    with pytest.raises(ConfigError, match="between -100.0 and 100.0"):
        app.broker.route_invocations(
            [invocation.invocation_id],
            "default",
            float("nan"),
        )

    assert app.broker.count_invocations() == 0

    with pytest.raises(ConfigError, match="between -100.0 and 100.0"):
        app.broker.route_invocation(
            invocation.invocation_id,
            "default",
            MAX_PRIORITY + 1.0,
        )


def test_priority_boundaries_and_equal_priority_fifo(task: "Task") -> None:
    """Every broker implements the full portable priority contract."""
    broker = task.app.broker
    low: DistributedInvocation = task()  # type: ignore
    high_first: DistributedInvocation = task()  # type: ignore
    high_second: DistributedInvocation = task()  # type: ignore
    broker.purge()

    broker.route_invocation(low.invocation_id, "default", MIN_PRIORITY)
    broker.route_invocation(high_first.invocation_id, "default", MAX_PRIORITY)
    broker.route_invocation(high_second.invocation_id, "default", MAX_PRIORITY)

    assert broker.retrieve_invocation("default") == high_first.invocation_id
    assert broker.retrieve_invocation("default") == high_second.invocation_id
    assert broker.retrieve_invocation("default") == low.invocation_id


def test_count_invocations(task: "Task") -> None:
    """Test that counting invocations works correctly."""
    broker = task.app.broker

    assert broker.count_invocations() == 0

    inv_1: DistributedInvocation = task()  # type: ignore
    inv_2: DistributedInvocation = task()  # type: ignore

    assert broker.count_invocations() == 2
    assert broker.retrieve_invocation() == inv_1.invocation_id
    assert broker.count_invocations() == 1
    assert broker.retrieve_invocation() == inv_2.invocation_id
    assert broker.count_invocations() == 0
    assert broker.retrieve_invocation() is None
    assert broker.count_invocations() == 0

    broker.route_invocations(
        [inv_1.invocation_id, inv_2.invocation_id, inv_1.invocation_id],
        inv_1.task.broker_queue,
        inv_1.task.broker_priority,
    )
    assert broker.count_invocations() == 3
    assert broker.retrieve_invocation() == inv_1.invocation_id
    assert broker.count_invocations() == 2


def test_purge_invocations(task: "Task") -> None:
    """Test that purging invocations works correctly."""
    broker = task.app.broker

    assert broker.count_invocations() == 0

    _ = task()
    _ = task()

    assert broker.count_invocations() == 2

    broker.purge()
    assert broker.count_invocations() == 0
    assert broker.retrieve_invocation() is None

    # Purge again to test idempotency
    broker.purge()
    assert broker.count_invocations() == 0
    assert broker.retrieve_invocation() is None


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_named_queue_filtering_and_priority(
    backend: str, temp_sqlite_db_path: str
) -> None:
    app = build_queue_app(backend, temp_sqlite_db_path)
    default = bind_task(app, default_task)
    payment = bind_task(app, payment_task)
    urgent = bind_task(app, urgent_payment_task)
    report = bind_task(app, report_task)

    default_inv: DistributedInvocation = default()  # type: ignore
    payment_inv: DistributedInvocation = payment()  # type: ignore
    urgent_inv: DistributedInvocation = urgent()  # type: ignore
    report_inv: DistributedInvocation = report()  # type: ignore

    assert app.broker.count_invocations(("default",)) == 1
    assert app.broker.count_invocations(("payments",)) == 2
    assert app.broker.count_invocations(("reports",)) == 1
    assert app.broker.count_invocations(("payments", "reports")) == 3

    assert app.broker.retrieve_invocation("payments") == urgent_inv.invocation_id
    assert app.broker.retrieve_invocation("payments") == payment_inv.invocation_id
    assert app.broker.retrieve_invocation("default") == default_inv.invocation_id
    assert app.broker.retrieve_invocation("reports") == report_inv.invocation_id
    assert app.broker.retrieve_invocation("payments") is None


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_broker_retrieves_from_one_queue_at_a_time(
    backend: str, temp_sqlite_db_path: str
) -> None:
    app = build_queue_app(backend, temp_sqlite_db_path)
    default = bind_task(app, default_task)
    payment = bind_task(app, payment_task)
    urgent = bind_task(app, urgent_payment_task)

    default_inv_1: DistributedInvocation = default()  # type: ignore
    payment_inv: DistributedInvocation = payment()  # type: ignore
    urgent_inv: DistributedInvocation = urgent()  # type: ignore
    default_inv_2: DistributedInvocation = default()  # type: ignore

    with pytest.raises(ConfigError, match="Invalid queue name"):
        app.broker.retrieve_invocation("*")

    assert app.broker.retrieve_invocation("payments") == urgent_inv.invocation_id
    assert app.broker.retrieve_invocation("default") == default_inv_1.invocation_id
    assert app.broker.retrieve_invocation("payments") == payment_inv.invocation_id
    assert app.broker.retrieve_invocation("default") == default_inv_2.invocation_id


def test_strict_broker_rejects_unknown_task_queue(
    temp_sqlite_db_path: str,
) -> None:
    app = (
        PynencBuilder()
        .sqlite(sqlite_db_path=temp_sqlite_db_path)
        .custom_config(queues=("default",), raise_on_queue_mismatch=True)
        .build()
    )
    missing = bind_task(app, missing_queue_task)

    with pytest.raises(ConfigError, match="not configured in broker.queues"):
        missing()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_broker_warns_but_routes_unknown_queue_by_default(
    backend: str,
    temp_sqlite_db_path: str,
) -> None:
    app = build_queue_app(backend, temp_sqlite_db_path)
    missing = bind_task(app, missing_queue_task)

    with capture_logs(app.logger) as log_buffer:
        invocation: DistributedInvocation = missing()  # type: ignore

    assert "not configured in broker.queues: missing" in log_buffer.getvalue()
    assert app.broker.retrieve_invocation("missing") == invocation.invocation_id
