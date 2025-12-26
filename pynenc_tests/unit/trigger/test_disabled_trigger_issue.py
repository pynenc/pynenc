import pytest
from unittest.mock import Mock
from pynenc.trigger.disabled_trigger import DisabledTrigger
from pynenc.trigger.conditions import CronCondition
from pynenc.trigger.trigger_builder import TriggerBuilder
from pynenc_tests.conftest import MockPynenc


def test_disabled_trigger_register_condition_error() -> None:
    """Reproduce the AttributeError when registering a condition on a DisabledTrigger."""
    app = MockPynenc()
    trigger = DisabledTrigger(app)
    condition = CronCondition("0 * * * *")

    # This should not raise AttributeError
    try:
        trigger.register_condition(condition)
    except AttributeError as e:
        pytest.fail(f"DisabledTrigger raised AttributeError: {e}")


def test_disabled_trigger_register_task_triggers_error() -> None:
    """Reproduce the AttributeError when registering task triggers on a DisabledTrigger."""
    app = MockPynenc()
    trigger = DisabledTrigger(app)
    task = Mock()
    task.task_id = "test_task"
    builder = TriggerBuilder().on_cron("0 * * * *")

    # This should not raise AttributeError
    try:
        trigger.register_task_triggers(task, builder)
    except AttributeError as e:
        pytest.fail(f"DisabledTrigger raised AttributeError: {e}")
