from typing import TYPE_CHECKING

import pytest

from pynenc.call import Call
from pynenc.invocation import DistributedInvocation
from tests.integration.orchestrator.orchestrator_tasks import (
    dummy_concat,
    dummy_key_arg,
    dummy_mirror,
    dummy_sum,
    dummy_task,
)

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.task import Task


@pytest.fixture
def task_dummy(app_instance: "Pynenc") -> "Task":
    dummy_task.app = app_instance
    return dummy_task


@pytest.fixture
def task_sum(app_instance: "Pynenc") -> "Task":
    dummy_sum.app = app_instance
    return dummy_sum


@pytest.fixture
def task_concat(app_instance: "Pynenc") -> "Task":
    dummy_concat.app = app_instance
    return dummy_concat


@pytest.fixture
def task_mirror(app_instance: "Pynenc") -> "Task":
    dummy_mirror.app = app_instance
    return dummy_mirror


@pytest.fixture
def task_key_arg(app_instance: "Pynenc") -> "Task":
    dummy_key_arg.app = app_instance
    return dummy_key_arg


@pytest.fixture
def dummy_invocation(task_dummy: "Task") -> "DistributedInvocation":
    return DistributedInvocation(Call(task_dummy), None)
