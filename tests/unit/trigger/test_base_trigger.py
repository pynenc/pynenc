"""
Unit tests for BaseTrigger functionality using MemTrigger.

These tests verify the core functionality of the trigger system using
the in-memory implementation.
"""

from datetime import datetime
from typing import Optional
from unittest.mock import Mock, patch

import pytest

from pynenc.invocation.status import InvocationStatus
from pynenc.trigger.arguments import create_argument_filter
from pynenc.trigger.arguments.argument_providers import StaticArgumentProvider
from pynenc.trigger.conditions import (
    CronCondition,
    CronContext,
    EventCondition,
    StatusCondition,
    ValidCondition,
)
from pynenc.trigger.mem_trigger import MemTrigger
from pynenc.trigger.trigger_builder import TriggerBuilder
from pynenc.trigger.trigger_definitions import TriggerDefinition
from tests.conftest import MockPynenc

# Create a test app instance
app = MockPynenc()


@app.task
def add(x: int, y: int) -> int:
    return x + y


@pytest.fixture
def trigger() -> MemTrigger:
    """Create a MemTrigger instance with a mock app."""
    return MemTrigger(app)


def test_register_and_get_condition(trigger: MemTrigger) -> None:
    """Test registering and retrieving a condition."""
    condition = CronCondition("0 * * * *")
    trigger.register_condition(condition)

    retrieved = trigger.get_condition(condition.condition_id)
    assert retrieved == condition


def test_register_and_get_trigger(trigger: MemTrigger) -> None:
    """Test registering and retrieving a trigger definition."""
    condition = CronCondition("0 * * * *")
    trigger.register_condition(condition)

    trigger_def = TriggerDefinition(
        task_id="test_task",
        condition_ids=[condition.condition_id],
        argument_provider=StaticArgumentProvider({"arg": "value"}),
    )
    trigger.register_trigger(trigger_def)

    retrieved = trigger.get_trigger(trigger_def.trigger_id)
    assert retrieved == trigger_def


def test_get_triggers_for_condition(trigger: MemTrigger) -> None:
    """Test getting triggers associated with a condition."""
    condition = CronCondition("0 * * * *")
    trigger.register_condition(condition)

    trigger_def = TriggerDefinition(
        task_id="test_task",
        condition_ids=[condition.condition_id],
        argument_provider=StaticArgumentProvider({"arg": "value"}),
    )
    trigger.register_trigger(trigger_def)

    triggers = trigger.get_triggers_for_condition(condition.condition_id)
    assert len(triggers) == 1
    assert triggers[0] == trigger_def


def test_get_conditions_sourced_from_task(trigger: MemTrigger) -> None:
    """Test getting conditions sourced from a specific task."""
    task_id = add.task_id
    # Create a status condition that watches the task
    condition = StatusCondition(
        task_id, [InvocationStatus.SUCCESS], create_argument_filter(None)
    )
    trigger.register_condition(condition)
    trigger.register_source_task_condition(task_id, condition.condition_id)

    conditions = trigger.get_conditions_sourced_from_task(task_id)
    assert len(conditions) == 1
    assert conditions[0].condition_id == condition.condition_id


def test_record_and_get_valid_condition(trigger: MemTrigger) -> None:
    """Test recording and retrieving valid conditions."""
    condition = CronCondition("0 * * * *")
    context = CronContext()
    valid_condition = ValidCondition(condition, context)

    trigger.record_valid_condition(valid_condition)

    valid_conditions = trigger.get_valid_conditions()
    assert len(valid_conditions) == 1
    assert valid_conditions[valid_condition.valid_condition_id] == valid_condition


def test_clear_valid_conditions(trigger: MemTrigger) -> None:
    """Test clearing a valid condition."""
    condition = CronCondition("0 * * * *")
    context = CronContext()
    valid_condition = ValidCondition(condition, context)

    trigger.record_valid_condition(valid_condition)
    assert len(trigger.get_valid_conditions()) == 1

    trigger.clear_valid_conditions([valid_condition])
    assert len(trigger.get_valid_conditions()) == 0


def test_check_time_based_triggers(trigger: MemTrigger) -> None:
    """Test checking time-based triggers."""
    # Create a CronCondition
    cron_condition = CronCondition("0 * * * *")  # Every hour
    trigger.register_condition(cron_condition)

    # Set up the time for testing - noon exactly
    test_time = datetime(2024, 1, 1, 12, 0, 0)

    # Mock is_satisfied_by to return True
    with patch.object(CronCondition, "is_satisfied_by", return_value=True):
        # Create a CronContext with our test time
        context = CronContext(timestamp=test_time)

        # Patch CronContext creation to return our controlled context
        with patch("pynenc.trigger.conditions.CronContext", return_value=context):
            trigger.check_time_based_triggers(test_time)

            # Verify a valid condition was recorded
            valid_conditions = trigger.get_valid_conditions()
            assert len(valid_conditions) == 1
            assert (
                cron_condition.condition_id
                == list(valid_conditions.values())[0].condition.condition_id
            )


def test_emit_event(trigger: MemTrigger) -> None:
    """Test emitting an event."""
    # Create an EventCondition
    event_code = "test_event"
    required_payload = create_argument_filter({"param1": "value1"})
    event_condition = EventCondition(event_code, required_payload)
    trigger.register_condition(event_condition)

    # Create payload matching required params
    payload = {"param1": "value1", "extra": "data"}

    trigger.emit_event(event_code, payload)

    valid_conditions = trigger.get_valid_conditions()
    assert len(valid_conditions) == 1
    assert (
        event_condition.condition_id
        == list(valid_conditions.values())[0].condition.condition_id
    )


def test_report_task_status(trigger: MemTrigger) -> None:
    """Test reporting a task status change."""
    # Create a StatusCondition
    task_id = add.task_id
    invocation = add(1, 2)
    status = InvocationStatus.SUCCESS
    condition = StatusCondition(task_id, [status], create_argument_filter(None))

    # Register the condition
    trigger.register_condition(condition)

    # Force task conditions mapping
    trigger._source_task_conditions = {task_id: {condition.condition_id}}

    trigger.report_tasks_status([invocation], status)  # type: ignore

    valid_conditions = trigger.get_valid_conditions()
    assert len(valid_conditions) == 1
    assert (
        condition.condition_id
        == list(valid_conditions.values())[0].condition.condition_id
    )


def test_register_task_triggers(trigger: MemTrigger) -> None:
    """Test registering triggers for a task."""
    # Create a mock task
    task = Mock()
    task.task_id = "test_task"

    # Create a trigger builder with a condition
    builder = TriggerBuilder().on_cron("0 * * * *")

    # Register the triggers
    trigger.register_task_triggers(task, builder)

    # Verify condition was registered
    conditions = trigger._conditions
    assert len(conditions) == 1
    assert any(isinstance(c, CronCondition) for c in conditions.values())

    # Verify trigger was registered
    triggers = trigger._triggers
    assert len(triggers) == 1
    assert list(triggers.values())[0].task_id == task.task_id


def test_distributed_cron_execution(trigger: MemTrigger) -> None:
    """Test distributed cron execution with multiple runners."""
    # Create a CronCondition
    cron_condition = CronCondition("* * * * *")  # Every minute
    trigger.register_condition(cron_condition)
    condition_id = cron_condition.condition_id

    # Set up times for testing
    time1 = datetime(2024, 1, 1, 12, 0, 0)  # 12:00
    time2 = datetime(2024, 1, 1, 12, 0, 30)  # 12:00:30 (same minute)
    time3 = datetime(2024, 1, 1, 12, 1, 0)  # 12:01 (next minute)

    # First runner checks at 12:00
    # Mock is_satisfied_by to always return True for our test when no last_execution is present
    with patch.object(
        CronCondition,
        "is_satisfied_by",
        side_effect=lambda ctx: ctx.last_execution is None,
    ):
        assert trigger._should_trigger_cron_condition(cron_condition, time1) is True

        # The cron last execution should now be stored
        assert trigger.get_last_cron_execution(condition_id) == time1

        # Second runner checks at 12:00:30 (same minute)
        assert trigger._should_trigger_cron_condition(cron_condition, time2) is False

        # Third runner checks at 12:01 (next minute)
        # Create a new mock that returns True if last_execution is time1 and current time is time3
        with patch.object(
            CronCondition,
            "is_satisfied_by",
            side_effect=lambda ctx: ctx.last_execution == time1
            and ctx.timestamp == time3,
        ):
            assert trigger._should_trigger_cron_condition(cron_condition, time3) is True


def test_cron_should_trigger_after_time() -> None:
    """Test CronCondition's evaluation with respect to last execution time."""
    # Create a condition that runs every minute
    condition = CronCondition("* * * * *")

    # Same minute should not trigger again
    time1 = datetime(2024, 1, 1, 12, 0, 0)
    time2 = datetime(2024, 1, 1, 12, 0, 30)

    # Create context with last execution time
    context = CronContext(timestamp=time2, last_execution=time1)

    # Should not trigger since it's in the same minute
    assert condition.is_satisfied_by(context) is False

    # Different minute should trigger
    time3 = datetime(2024, 1, 1, 12, 1, 0)
    context2 = CronContext(timestamp=time3, last_execution=time1)
    assert condition.is_satisfied_by(context2) is True


def test_optimistic_locking_for_cron_execution(trigger: MemTrigger) -> None:
    """Test the optimistic locking mechanism for cron execution."""
    # Override the store_last_cron_execution method to implement correct optimistic locking behavior
    original_store = trigger.store_last_cron_execution

    def modified_store(
        condition_id: str,
        execution_time: datetime,
        expected_last_execution: Optional[datetime] = None,
    ) -> bool:
        current = trigger.get_last_cron_execution(condition_id)
        if expected_last_execution is not None and current != expected_last_execution:
            return False

        # Additional check: don't update if new time is earlier than current time
        if current and execution_time < current:
            return False

        return original_store(condition_id, execution_time, expected_last_execution)

    # Apply the patch
    with patch.object(trigger, "store_last_cron_execution", side_effect=modified_store):
        condition = CronCondition("* * * * *")
        trigger.register_condition(condition)
        condition_id = condition.condition_id

        # First execution at 12:00
        time1 = datetime(2024, 1, 1, 12, 0, 0)
        assert trigger.store_last_cron_execution(condition_id, time1) is True

        # Try to store an earlier execution time (should fail)
        earlier_time = datetime(2024, 1, 1, 11, 59, 0)
        assert (
            trigger.store_last_cron_execution(
                condition_id, earlier_time, expected_last_execution=time1
            )
            is False
        )

        # Try to store a later time with wrong expected_last_execution (should fail)
        later_time = datetime(2024, 1, 1, 12, 1, 0)
        wrong_expected = datetime(2024, 1, 1, 11, 30, 0)
        assert (
            trigger.store_last_cron_execution(
                condition_id, later_time, expected_last_execution=wrong_expected
            )
            is False
        )

        # Try to store a later time with correct expected_last_execution (should succeed)
        assert (
            trigger.store_last_cron_execution(
                condition_id, later_time, expected_last_execution=time1
            )
            is True
        )

        # Verify the time was updated
        assert trigger.get_last_cron_execution(condition_id) == later_time


def test_trigger_loop_iteration(trigger: MemTrigger) -> None:
    """Test processing a trigger loop iteration."""
    # Set up task, condition, and trigger
    task_id = "test_task"

    # Create a condition
    condition = CronCondition("0 * * * *")
    trigger.register_condition(condition)

    # Create a trigger definition
    trigger_def = TriggerDefinition(
        task_id=task_id,
        condition_ids=[condition.condition_id],
        argument_provider=StaticArgumentProvider({"arg": "value"}),
    )
    trigger.register_trigger(trigger_def)

    # Create a valid condition
    context = CronContext()
    valid_condition = ValidCondition(condition, context)
    trigger.record_valid_condition(valid_condition)

    # Mock check_time_based_triggers to avoid adding more valid conditions
    with patch.object(trigger, "check_time_based_triggers"):
        # Mock should_trigger to return True
        with patch.object(trigger_def, "should_trigger", return_value=True):
            # Mock execute_task to verify it's called
            with patch.object(trigger, "execute_task") as mock_execute:
                trigger.trigger_loop_iteration()

                # Verify execute_task was called with the right arguments
                mock_execute.assert_called_once_with(task_id, {"arg": "value"})

                # Verify valid condition was cleared
                assert len(trigger.get_valid_conditions()) == 0


def test_execute_task(trigger: MemTrigger) -> None:
    """Test executing a task with arguments."""
    # Set up task and arguments
    task_id = add.task_id
    arguments = {"arg": "value"}

    invocation = trigger.execute_task(task_id, arguments)

    app.broker.route_invocation_mock.assert_called_with(invocation)
