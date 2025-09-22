"""
Unit tests for event trigger conditions.

Tests the functionality of EventCondition which triggers tasks based on
custom events in the system.
"""

from pynenc.trigger.arguments import StaticArgumentFilter, create_argument_filter
from pynenc.trigger.conditions.event import EventCondition, EventContext

no_arg_filter = create_argument_filter(None)
filter_param_1 = create_argument_filter({"param1": "value1"})
filter_param_2 = create_argument_filter({"param2": "value2"})
filter_param_mix = create_argument_filter({"param1": "value2"})


def test_event_condition_init() -> None:
    """Test initialization of EventCondition."""
    # Test with no parameters
    condition1 = EventCondition("test_event", no_arg_filter)
    assert condition1.event_code == "test_event"
    assert condition1.payload_filter == no_arg_filter

    # Test with parameters
    condition2 = EventCondition("test_event", filter_param_1)
    assert condition2.event_code == "test_event"
    assert condition2.payload_filter == filter_param_1


def test_condition_id_consistency() -> None:
    """Test that condition_id is consistently generated for equivalent conditions."""
    # Same event and params should have same ID
    condition1 = EventCondition("test_event", filter_param_1)
    condition2 = EventCondition("test_event", filter_param_1)
    assert condition1.condition_id == condition2.condition_id

    # Parameter order shouldn't affect ID
    condition3 = EventCondition("test_event", create_argument_filter({"b": 2, "a": 1}))
    condition4 = EventCondition("test_event", create_argument_filter({"a": 1, "b": 2}))
    assert condition3.condition_id == condition4.condition_id


def test_condition_id_uniqueness() -> None:
    """Test that different conditions produce different IDs."""
    base = EventCondition("test_event", filter_param_1)

    # Different event code should have different ID
    different_event = EventCondition("other_event", filter_param_1)
    assert base.condition_id != different_event.condition_id

    # Different params should have different ID
    different_params = EventCondition("test_event", filter_param_2)
    assert base.condition_id != different_params.condition_id

    # Different param values should have different ID
    different_values = EventCondition("test_event", filter_param_mix)
    assert base.condition_id != different_values.condition_id


def test_is_satisfied_by_matching_event() -> None:
    """Test is_satisfied_by with matching event code and parameters."""
    # Setup
    condition = EventCondition("test_event", filter_param_1)
    context = EventContext(
        event_id="event1",
        event_code="test_event",
        payload={"param1": "value1", "extra": "data"},
    )

    # Verify - context with matching event_code and params should satisfy
    assert condition.is_satisfied_by(context)


def test_is_satisfied_by_different_event_code() -> None:
    """Test is_satisfied_by with non-matching event code."""
    # Setup
    condition = EventCondition("test_event", filter_param_1)
    assert isinstance(filter_param_1, StaticArgumentFilter)
    context = EventContext(
        event_id="event2",
        event_code="different_event",  # Different event code
        payload=filter_param_1.arguments.kwargs,
    )

    # Verify - context with different event_code should not satisfy
    assert not condition.is_satisfied_by(context)
