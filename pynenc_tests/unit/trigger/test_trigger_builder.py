import pytest

from pynenc import Pynenc
from pynenc.invocation.status import InvocationStatus
from pynenc.trigger.arguments import create_argument_filter
from pynenc.trigger.arguments.argument_providers import StaticArgumentProvider
from pynenc.trigger.conditions import (
    CompositeLogic,
    CronCondition,
    EventCondition,
    StatusCondition,
)
from pynenc.trigger.trigger_builder import TriggerBuilder, on_cron, on_event, on_status
from pynenc.trigger.trigger_definitions import TriggerDefinition

app = Pynenc()


@app.task
def dummy_task(x: int, y: int) -> int:
    """Dummy task for testing."""
    return x + y


def test_trigger_builder_init() -> None:
    """Test TriggerBuilder initialization."""
    builder = TriggerBuilder()
    assert builder.conditions == []
    assert builder.logic == CompositeLogic.AND
    assert not builder.argument_providers


def test_on_cron() -> None:
    """Test creating a cron-based condition."""
    cron_expression = "0 0 * * *"
    builder = TriggerBuilder().on_cron(cron_expression)

    assert len(builder.conditions) == 1
    assert isinstance(builder.conditions[0], CronCondition)
    assert builder.conditions[0].cron_expression == cron_expression


def test_on_event() -> None:
    """Test creating an event-based condition."""
    event_code = "test_event"
    filter_args = {"key": "value"}
    payload_filter = create_argument_filter(filter_args)

    # Test with required parameters
    builder = TriggerBuilder().on_event(event_code, filter_args)
    assert len(builder.conditions) == 1
    assert isinstance(builder.conditions[0], EventCondition)
    assert builder.conditions[0].event_code == event_code
    assert builder.conditions[0].payload_filter == payload_filter

    # Test without parameters
    builder = TriggerBuilder().on_event(event_code)
    assert len(builder.conditions) == 1
    assert isinstance(builder.conditions[0], EventCondition)
    assert builder.conditions[0].event_code == event_code
    assert builder.conditions[0].payload_filter == create_argument_filter(None)


def test_on_status() -> None:
    """Test creating a task-status condition."""
    # Test with default status
    builder = TriggerBuilder().on_status(dummy_task)
    assert len(builder.conditions) == 1
    assert isinstance(builder.conditions[0], StatusCondition)
    assert builder.conditions[0].task_id == dummy_task.task_id
    assert builder.conditions[0].statuses == [InvocationStatus.SUCCESS]

    # Test with single status string
    builder = TriggerBuilder().on_status(dummy_task, InvocationStatus.FAILED)
    assert len(builder.conditions) == 1
    assert isinstance(builder.conditions[0], StatusCondition)
    assert builder.conditions[0].statuses == [InvocationStatus.FAILED]

    # Test with list of statuses
    statuses = [InvocationStatus.SUCCESS, InvocationStatus.FAILED]
    builder = TriggerBuilder().on_status(dummy_task, statuses)
    assert len(builder.conditions) == 1
    assert isinstance(builder.conditions[0], StatusCondition)
    assert builder.conditions[0].statuses == statuses

    # Test with call arguments
    args = {"arg1": "value1"}
    builder = TriggerBuilder().on_status(dummy_task, InvocationStatus.SUCCESS, args)
    assert len(builder.conditions) == 1
    assert isinstance(builder.conditions[0], StatusCondition)
    assert builder.conditions[0].arguments_filter == create_argument_filter(args)


def test_with_logic() -> None:
    """Test setting logic for composite conditions."""
    builder = TriggerBuilder().with_logic(CompositeLogic.OR)
    assert builder.logic == CompositeLogic.OR


def test_with_arguments() -> None:
    """Test setting arguments with a dictionary."""
    args = {"arg1": 1, "arg2": "test"}
    builder = TriggerBuilder().with_arguments(args)
    assert builder.argument_providers == [StaticArgumentProvider(args)]


def test_add_condition() -> None:
    """Test adding a condition directly."""
    condition = CronCondition("0 0 * * *")
    builder = TriggerBuilder().add_condition(condition)
    assert len(builder.conditions) == 1
    assert builder.conditions[0] == condition


def test_build() -> None:
    """Test building a trigger definition."""
    task_id = "test_task"
    builder = TriggerBuilder().on_cron("0 0 * * *").with_arguments({"arg1": "value1"})

    trigger_def = builder.build(task_id)

    assert isinstance(trigger_def, TriggerDefinition)
    assert trigger_def.task_id == task_id
    assert len(trigger_def.condition_ids) == 1
    assert trigger_def.get_arguments(None) == {"arg1": "value1"}  # type: ignore


def test_build_empty_conditions() -> None:
    """Test building with no conditions raises ValueError."""
    builder = TriggerBuilder()
    with pytest.raises(
        ValueError, match="Cannot create a trigger definition with no conditions"
    ):
        builder.build("test_task")


def test_helper_functions() -> None:
    """Test the helper functions for creating trigger builders."""
    # Test on_cron helper
    cron_builder = on_cron("0 0 * * *")
    assert isinstance(cron_builder, TriggerBuilder)
    assert len(cron_builder.conditions) == 1
    assert isinstance(cron_builder.conditions[0], CronCondition)

    # Test on_event helper
    event_builder = on_event("test_event", {"key": "value"})
    assert isinstance(event_builder, TriggerBuilder)
    assert len(event_builder.conditions) == 1
    assert isinstance(event_builder.conditions[0], EventCondition)

    # Test on_status helper
    task_builder = on_status(dummy_task, InvocationStatus.SUCCESS)
    assert isinstance(task_builder, TriggerBuilder)
    assert len(task_builder.conditions) == 1
    assert isinstance(task_builder.conditions[0], StatusCondition)


def test_method_chaining() -> None:
    """Test method chaining works properly."""
    builder = (
        TriggerBuilder()
        .on_cron("0 0 * * *")
        .on_status(dummy_task, [InvocationStatus.SUCCESS])
        .with_logic(CompositeLogic.OR)
        .with_arguments({"arg1": "value1"})
    )

    assert len(builder.conditions) == 2
    assert builder.logic == CompositeLogic.OR
    assert builder.argument_providers == [StaticArgumentProvider({"arg1": "value1"})]

    trigger_def = builder.build("task2")
    assert trigger_def.task_id == "task2"
    assert len(trigger_def.condition_ids) == 2
    assert trigger_def.logic == CompositeLogic.OR
    assert trigger_def.get_arguments(None) == {"arg1": "value1"}  # type: ignore
