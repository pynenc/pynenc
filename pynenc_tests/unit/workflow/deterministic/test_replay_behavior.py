"""
Tests for replay behavior and edge cases.

This module tests complete workflow replay scenarios, partial replay with new generation,
and counter consistency across replay operations.
"""
from pynenc.workflow.deterministic import DeterministicExecutor


def test_complete_workflow_replay(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test complete workflow replay scenario."""
    # Execute a sequence of mixed operations
    original_random = deterministic_executor.random()
    original_time = deterministic_executor.utc_now()
    original_uuid = deterministic_executor.uuid()

    # Custom deterministic operation
    call_count = 0

    def custom_generator() -> str:
        nonlocal call_count
        call_count += 1
        return f"custom_{call_count}"

    original_custom = deterministic_executor._deterministic_operation(
        "custom", custom_generator
    )
    assert call_count == 1

    # Create new executor and replay entire sequence
    replay_executor = DeterministicExecutor(
        deterministic_executor.workflow_identity, deterministic_executor.app
    )

    # Reset call count to verify no new generation
    call_count = 0

    # Replay all operations in same order
    replay_random = replay_executor.random()
    replay_time = replay_executor.utc_now()
    replay_uuid = replay_executor.uuid()
    replay_custom = replay_executor._deterministic_operation("custom", custom_generator)

    # Should get identical values
    assert replay_random == original_random
    assert replay_time == original_time
    assert replay_uuid == original_uuid
    assert replay_custom == original_custom
    assert call_count == 0  # No new generation during replay


def test_partial_replay_then_new_generation(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test scenario where we replay some operations then generate new ones."""
    # Generate initial operations
    values = []
    for i in range(3):

        def generator(value: int = i + 1) -> str:
            return f"initial_{value}"

        result = deterministic_executor._deterministic_operation("test", generator)
        values.append(result)

    # New executor replays existing then generates new
    new_executor = DeterministicExecutor(
        deterministic_executor.workflow_identity, deterministic_executor.app
    )

    call_count = 0

    def new_generator() -> str:
        nonlocal call_count
        call_count += 1
        return f"new_{call_count}"

    # First 3 should replay
    replayed = []
    for _ in range(3):
        result = new_executor._deterministic_operation("test", new_generator)
        replayed.append(result)

    assert replayed == values
    assert call_count == 0  # No generation during replay

    # 4th should generate new
    new_result = new_executor._deterministic_operation("test", new_generator)
    assert new_result == "new_1"
    assert call_count == 1


def test_counter_consistency_across_replay(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test that counters remain consistent across replay operations."""

    def mock_generator() -> str:
        return "test_value"

    # Generate some operations
    deterministic_executor._deterministic_operation("test", mock_generator)
    deterministic_executor._deterministic_operation("test", mock_generator)
    deterministic_executor._deterministic_operation("test", mock_generator)

    # Counter should match number of operations
    assert deterministic_executor.get_operation_count("test") == 3

    # Create new executor and replay all operations
    new_executor = DeterministicExecutor(
        deterministic_executor.workflow_identity, deterministic_executor.app
    )

    # Replay all 3 operations
    for _ in range(3):
        new_executor._deterministic_operation("test", mock_generator)

    # Counter should now match in new executor
    assert new_executor.get_operation_count("test") == 3

    # Next operation should continue from correct sequence
    new_executor._deterministic_operation("test", mock_generator)
    assert new_executor.get_operation_count("test") == 4
