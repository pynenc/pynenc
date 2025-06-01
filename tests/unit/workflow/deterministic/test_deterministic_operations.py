"""
Tests for the core _deterministic_operation method behavior.

This module tests value storage, retrieval, replay logic, and sequence isolation
for the fundamental deterministic operation mechanism.
"""
from pynenc.workflow.deterministic import DeterministicExecutor


def test_stores_and_retrieves_values(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test _deterministic_operation stores generated values and retrieves them."""
    call_count = 0

    def mock_generator() -> str:
        nonlocal call_count
        call_count += 1
        return f"generated_value_{call_count}"

    # First call should generate and store value
    result1 = deterministic_executor._deterministic_operation("test", mock_generator)
    assert result1 == "generated_value_1"
    assert call_count == 1

    # Verify value was stored with correct key
    stored_value = (
        deterministic_executor.app.state_backend.get_workflow_deterministic_value(
            deterministic_executor.workflow_identity, "test:1"
        )
    )
    assert stored_value == "generated_value_1"


def test_creates_unique_sequences(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test _deterministic_operation creates separate sequences for multiple calls."""
    call_count = 0

    def mock_generator() -> str:
        nonlocal call_count
        call_count += 1
        return f"value_{call_count}"

    # Multiple calls should create separate sequence entries
    result1 = deterministic_executor._deterministic_operation("test", mock_generator)
    result2 = deterministic_executor._deterministic_operation("test", mock_generator)
    result3 = deterministic_executor._deterministic_operation("test", mock_generator)

    assert result1 == "value_1"
    assert result2 == "value_2"
    assert result3 == "value_3"
    assert call_count == 3

    # Verify each stored with unique key
    assert (
        deterministic_executor.app.state_backend.get_workflow_deterministic_value(
            deterministic_executor.workflow_identity, "test:1"
        )
        == "value_1"
    )
    assert (
        deterministic_executor.app.state_backend.get_workflow_deterministic_value(
            deterministic_executor.workflow_identity, "test:2"
        )
        == "value_2"
    )
    assert (
        deterministic_executor.app.state_backend.get_workflow_deterministic_value(
            deterministic_executor.workflow_identity, "test:3"
        )
        == "value_3"
    )


def test_replays_stored_values_without_calling_generator(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test _deterministic_operation replays stored values without calling generator."""
    call_count = 0

    def mock_generator() -> str:
        nonlocal call_count
        call_count += 1
        return f"fresh_value_{call_count}"

    # Store values in first executor
    result1 = deterministic_executor._deterministic_operation("test", mock_generator)
    result2 = deterministic_executor._deterministic_operation("test", mock_generator)
    assert result1 == "fresh_value_1"
    assert result2 == "fresh_value_2"
    assert call_count == 2

    # Create new executor - should replay without calling generator
    new_executor = DeterministicExecutor(
        deterministic_executor.workflow_identity, deterministic_executor.app
    )

    # Reset call count to verify generator isn't called
    call_count = 0

    replay_result1 = new_executor._deterministic_operation("test", mock_generator)
    replay_result2 = new_executor._deterministic_operation("test", mock_generator)

    # Should get same values without calling generator
    assert replay_result1 == "fresh_value_1"
    assert replay_result2 == "fresh_value_2"
    assert call_count == 0  # Generator should not be called during replay


def test_handles_mixed_replay_and_generation(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test _deterministic_operation handles scenarios with partial stored values."""
    call_count = 0

    def mock_generator() -> str:
        nonlocal call_count
        call_count += 1
        return f"value_{call_count}"

    # Generate two values in first executor
    result1 = deterministic_executor._deterministic_operation("test", mock_generator)
    result2 = deterministic_executor._deterministic_operation("test", mock_generator)
    assert call_count == 2

    # Create new executor and replay plus generate new
    new_executor = DeterministicExecutor(
        deterministic_executor.workflow_identity, deterministic_executor.app
    )

    # First two should replay
    replay1 = new_executor._deterministic_operation("test", mock_generator)
    replay2 = new_executor._deterministic_operation("test", mock_generator)

    # Third should generate new value
    new_result = new_executor._deterministic_operation("test", mock_generator)

    assert replay1 == result1
    assert replay2 == result2
    assert new_result == "value_3"
    assert call_count == 3  # Only one new call for the third value


def test_isolated_by_operation_type(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test _deterministic_operation maintains separate sequences per operation type."""

    def generator_a() -> str:
        return "type_a_value"

    def generator_b() -> str:
        return "type_b_value"

    # Generate values for different operation types
    _result_a1 = deterministic_executor._deterministic_operation("type_a", generator_a)
    _result_b1 = deterministic_executor._deterministic_operation("type_b", generator_b)
    _result_a2 = deterministic_executor._deterministic_operation("type_a", generator_a)

    # Verify independent sequences
    assert (
        deterministic_executor.app.state_backend.get_workflow_deterministic_value(
            deterministic_executor.workflow_identity, "type_a:1"
        )
        == "type_a_value"
    )
    assert (
        deterministic_executor.app.state_backend.get_workflow_deterministic_value(
            deterministic_executor.workflow_identity, "type_b:1"
        )
        == "type_b_value"
    )
    assert (
        deterministic_executor.app.state_backend.get_workflow_deterministic_value(
            deterministic_executor.workflow_identity, "type_a:2"
        )
        == "type_a_value"
    )
