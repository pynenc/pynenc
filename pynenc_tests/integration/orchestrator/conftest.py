from typing import TYPE_CHECKING

import pytest

from pynenc.call import Call
from pynenc.invocation import DistributedInvocation
from pynenc_tests.integration.orchestrator.orchestrator_tasks import (
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
def task_dummy_io(app_instance: "Pynenc") -> "Task":
    dummy_task.app = app_instance
    return dummy_task


@pytest.fixture
def task_sum_io(app_instance: "Pynenc") -> "Task":
    dummy_sum.app = app_instance
    return dummy_sum


@pytest.fixture
def task_concat_io(app_instance: "Pynenc") -> "Task":
    dummy_concat.app = app_instance
    return dummy_concat


@pytest.fixture
def task_mirror_io(app_instance: "Pynenc") -> "Task":
    dummy_mirror.app = app_instance
    return dummy_mirror


@pytest.fixture
def task_key_arg_io(app_instance: "Pynenc") -> "Task":
    dummy_key_arg.app = app_instance
    return dummy_key_arg


@pytest.fixture
def dummy_invocation_io(task_dummy_io: "Task") -> "DistributedInvocation":
    return DistributedInvocation(Call(task_dummy_io), None)
