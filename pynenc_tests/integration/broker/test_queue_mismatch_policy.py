from typing import TYPE_CHECKING

import pytest

from pynenc.exceptions import ConfigError
from pynenc.invocation import DistributedInvocation
from pynenc.task import Task
from pynenc_tests.integration.broker.queue_mismatch_tasks import (
    default_queue_task,
    undeclared_queue_task,
)
from pynenc_tests.util import capture_logs

if TYPE_CHECKING:
    from pynenc import Pynenc


def bind_task(app: "Pynenc", source_task: "Task") -> "Task":
    task = Task.clone_for_app(app, source_task)
    app.tasks[task.task_id] = task
    return task


def test_broker_warns_and_routes_unknown_task_queue(app_instance: "Pynenc") -> None:
    app = app_instance
    app.broker.conf.queues = ("default",)
    undeclared_task = bind_task(app, undeclared_queue_task)

    with capture_logs(app.logger) as log_buffer:
        invocation = undeclared_task("value")

    assert isinstance(invocation, DistributedInvocation)
    assert "not configured in broker.queues: undeclared" in log_buffer.getvalue()
    assert app.broker.count_invocations(("undeclared",)) == 1
    assert app.broker.retrieve_invocation("undeclared") == invocation.invocation_id


def test_broker_can_raise_for_unknown_task_queue(app_instance: "Pynenc") -> None:
    app = app_instance
    app.broker.conf.queues = ("default",)
    app.broker.conf.raise_on_queue_mismatch = True
    undeclared_task = bind_task(app, undeclared_queue_task)

    with pytest.raises(ConfigError, match="not configured in broker.queues"):
        undeclared_task("value")


def test_broker_rejects_invalid_dequeue_queue_name(app_instance: "Pynenc") -> None:
    app = app_instance
    app.broker.conf.queues = ("default",)
    default_task = bind_task(app, default_queue_task)
    invocation = default_task("new")

    assert isinstance(invocation, DistributedInvocation)
    with pytest.raises(ConfigError, match="Invalid queue name"):
        app.broker.retrieve_invocation("*")
    assert app.broker.retrieve_invocation("default") == invocation.invocation_id
