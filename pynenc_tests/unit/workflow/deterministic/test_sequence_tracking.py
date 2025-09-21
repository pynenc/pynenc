"""
Tests for operation sequence tracking and counter management.

This module tests the core sequence tracking logic that ensures deterministic
operation ordering and proper counter persistence across executor instances.
"""
from pynenc.workflow.deterministic import DeterministicExecutor


def test_operation_sequence_increments_correctly(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test that operation sequences increment properly."""
    # First operation should start at 1
    sequence1 = deterministic_executor._get_next_sequence("test_op")
    assert sequence1 == 1

    # Second operation should increment
    sequence2 = deterministic_executor._get_next_sequence("test_op")
    assert sequence2 == 2

    # Different operation should start at 1
    sequence3 = deterministic_executor._get_next_sequence("other_op")
    assert sequence3 == 1

    # Original operation should continue sequence
    sequence4 = deterministic_executor._get_next_sequence("test_op")
    assert sequence4 == 3


def test_operation_count_retrieval(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test that operation counts can be retrieved correctly."""
    # Initially should be 0
    assert deterministic_executor.get_operation_count("test_op") == 0

    # After operations should reflect count
    deterministic_executor._get_next_sequence("test_op")
    deterministic_executor._get_next_sequence("test_op")

    assert deterministic_executor.get_operation_count("test_op") == 2


def test_operation_count_is_per_executor_instance(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test that operation counts are per executor instance, not global."""
    # Increment counter in first executor
    deterministic_executor._get_next_sequence("test")
    deterministic_executor._get_next_sequence("test")
    assert deterministic_executor.get_operation_count("test") == 2

    # Create new executor with same workflow identity
    new_executor = DeterministicExecutor(
        deterministic_executor.workflow_identity, deterministic_executor.app
    )

    # New executor should start with fresh counters
    assert new_executor.get_operation_count("test") == 0

    # After performing operations, it should track its own count
    new_executor._get_next_sequence("test")
    assert new_executor.get_operation_count("test") == 1

    # Original executor should maintain its own count
    assert deterministic_executor.get_operation_count("test") == 2


def test_replay_behavior_with_stored_values(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test that new executors can replay stored deterministic values."""

    # Store some deterministic values using the first executor
    def mock_generator() -> str:
        return "test_value"

    result1 = deterministic_executor._deterministic_operation("test", mock_generator)
    result2 = deterministic_executor._deterministic_operation("test", mock_generator)

    # Create new executor and verify it replays the same values
    new_executor = DeterministicExecutor(
        deterministic_executor.workflow_identity, deterministic_executor.app
    )

    replay_result1 = new_executor._deterministic_operation("test", mock_generator)
    replay_result2 = new_executor._deterministic_operation("test", mock_generator)

    # Should get same values from storage
    assert replay_result1 == result1
    assert replay_result2 == result2

    # Both executors should have their own operation counts
    assert deterministic_executor.get_operation_count("test") == 2
    assert new_executor.get_operation_count("test") == 2


def test_operation_count_isolated_by_type(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test get_operation_count maintains separate counters per operation type."""
    # Increment different operation types
    deterministic_executor._get_next_sequence("random")
    deterministic_executor._get_next_sequence("random")
    deterministic_executor._get_next_sequence("time")

    # Each should have independent counts
    assert deterministic_executor.get_operation_count("random") == 2
    assert deterministic_executor.get_operation_count("time") == 1
    assert deterministic_executor.get_operation_count("uuid") == 0
