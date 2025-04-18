"""
Unit tests for composite trigger conditions.

Tests the functionality of CompositeCondition which combines multiple conditions
with AND/OR logic.
"""

from unittest.mock import Mock

import pytest

from pynenc.trigger.conditions.base import TriggerCondition
from pynenc.trigger.conditions.composite import CompositeCondition, CompositeLogic
from pynenc.trigger.conditions.cron import CronContext


@pytest.fixture
def composite_conditions_setup() -> (
    tuple[TriggerCondition, TriggerCondition, CronContext]
):
    """Set up test fixtures for composite conditions."""
    # Create mock conditions
    condition1 = Mock(spec=TriggerCondition)
    condition1.condition_id = "condition1"
    condition1.is_satisfied_by.return_value = True
    condition1.affects_task.return_value = False

    condition2 = Mock(spec=TriggerCondition)
    condition2.condition_id = "condition2"
    condition2.is_satisfied_by.return_value = True
    condition2.affects_task.return_value = False

    context = CronContext()

    return condition1, condition2, context


def test_init_requires_conditions() -> None:
    """Test that CompositeCondition requires at least one condition."""
    with pytest.raises(ValueError):
        CompositeCondition([], CompositeLogic.AND)


def test_condition_id_is_deterministic(
    composite_conditions_setup: tuple[Mock, Mock, CronContext]
) -> None:
    """Test that condition_id is deterministic and based on child conditions."""
    condition1, condition2, _ = composite_conditions_setup

    comp1 = CompositeCondition([condition1, condition2], CompositeLogic.AND)
    comp2 = CompositeCondition([condition2, condition1], CompositeLogic.AND)

    # The order of conditions shouldn't matter for the ID
    assert comp1.condition_id == comp2.condition_id

    # Different logic should produce different IDs
    comp3 = CompositeCondition([condition1, condition2], CompositeLogic.OR)
    assert comp1.condition_id != comp3.condition_id


def test_and_logic(composite_conditions_setup: tuple[Mock, Mock, CronContext]) -> None:
    """Test AND logic when combining conditions."""
    condition1, condition2, context = composite_conditions_setup

    composite = CompositeCondition([condition1, condition2], CompositeLogic.AND)

    # When both conditions are satisfied
    condition1.is_satisfied_by.return_value = True
    condition2.is_satisfied_by.return_value = True
    assert composite.is_satisfied_by(context)

    # When one condition is not satisfied
    condition1.is_satisfied_by.return_value = False
    condition2.is_satisfied_by.return_value = True
    assert not composite.is_satisfied_by(context)

    # When both conditions are not satisfied
    condition1.is_satisfied_by.return_value = False
    condition2.is_satisfied_by.return_value = False
    assert not composite.is_satisfied_by(context)


def test_or_logic(composite_conditions_setup: tuple[Mock, Mock, CronContext]) -> None:
    """Test OR logic when combining conditions."""
    condition1, condition2, context = composite_conditions_setup

    composite = CompositeCondition([condition1, condition2], CompositeLogic.OR)

    # When both conditions are satisfied
    condition1.is_satisfied_by.return_value = True
    condition2.is_satisfied_by.return_value = True
    assert composite.is_satisfied_by(context)

    # When one condition is satisfied
    condition1.is_satisfied_by.return_value = False
    condition2.is_satisfied_by.return_value = True
    assert composite.is_satisfied_by(context)

    # When no conditions are satisfied
    condition1.is_satisfied_by.return_value = False
    condition2.is_satisfied_by.return_value = False
    assert not composite.is_satisfied_by(context)


def test_get_condition_data(
    composite_conditions_setup: tuple[Mock, Mock, CronContext]
) -> None:
    """Test getting condition data."""
    condition1, condition2, _ = composite_conditions_setup
    condition1._to_json.return_value = {}
    condition2._to_json.return_value = {}

    composite = CompositeCondition([condition1, condition2], CompositeLogic.AND)

    data = composite._to_json(None)  # type: ignore
    assert data["logic"] == "AND"
    assert len(data["conditions"]) == 2


def test_affects_task(
    composite_conditions_setup: tuple[Mock, Mock, CronContext]
) -> None:
    """Test affects_task method checks all child conditions."""
    condition1, condition2, _ = composite_conditions_setup

    composite = CompositeCondition([condition1, condition2], CompositeLogic.OR)

    # When no child condition is affected
    condition1.affects_task.return_value = False
    condition2.affects_task.return_value = False
    assert not composite.affects_task("task1")

    # When one child condition is affected
    condition1.affects_task.return_value = True
    condition2.affects_task.return_value = False
    assert composite.affects_task("task1")
