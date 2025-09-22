"""
Tests for built-in deterministic functions (random, time, uuid).

This module tests the deterministic random number generation, time progression,
and UUID generation capabilities that ensure reproducible workflow execution.
"""
import datetime

from pynenc.workflow.deterministic import DeterministicExecutor


def test_base_time_establishment(deterministic_executor: DeterministicExecutor) -> None:
    """Test that base time is established and consistent."""
    # First call establishes base time
    base_time1 = deterministic_executor.get_base_time()
    assert isinstance(base_time1, datetime.datetime)
    assert base_time1.tzinfo == datetime.timezone.utc

    # Second call should return the same base time
    base_time2 = deterministic_executor.get_base_time()
    assert base_time1 == base_time2


def test_deterministic_random_generation(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test deterministic random number generation."""
    # Generate sequence of random numbers
    randoms = [deterministic_executor.random() for _ in range(5)]

    # All should be valid random numbers
    assert all(0.0 <= r <= 1.0 for r in randoms)

    # All should be different
    assert len(set(randoms)) == len(randoms)

    # Create new executor with same workflow - should get same sequence
    new_executor = DeterministicExecutor(
        deterministic_executor.workflow_identity, deterministic_executor.app
    )
    new_randoms = [new_executor.random() for _ in range(5)]

    assert randoms == new_randoms


def test_deterministic_time_progression(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test deterministic time progression."""
    # Generate sequence of timestamps
    times = [deterministic_executor.utc_now() for _ in range(3)]

    # All should be UTC
    assert all(t.tzinfo == datetime.timezone.utc for t in times)

    # Should be in ascending order (each call advances time)
    assert times == sorted(times)

    # Should be based on the established base time
    base_time = deterministic_executor.get_base_time()
    assert times[0] >= base_time

    # Create new executor - should get same sequence
    new_executor = DeterministicExecutor(
        deterministic_executor.workflow_identity, deterministic_executor.app
    )
    new_times = [new_executor.utc_now() for _ in range(3)]

    assert times == new_times


def test_deterministic_uuid_generation(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test deterministic UUID generation."""
    # Generate sequence of UUIDs
    uuids = [deterministic_executor.uuid() for _ in range(3)]

    # All should be valid UUID format
    assert all(len(u) == 36 and u.count("-") == 4 for u in uuids)

    # All should be different
    assert len(set(uuids)) == len(uuids)

    # Create new executor - should get same sequence
    new_executor = DeterministicExecutor(
        deterministic_executor.workflow_identity, deterministic_executor.app
    )
    new_uuids = [new_executor.uuid() for _ in range(3)]

    assert uuids == new_uuids


def test_mixed_deterministic_operations_sequence(
    deterministic_executor: DeterministicExecutor,
) -> None:
    """Test that mixed deterministic operations maintain proper sequences."""
    # Mix different operation types
    random1 = deterministic_executor.random()
    time1 = deterministic_executor.utc_now()
    uuid1 = deterministic_executor.uuid()
    random2 = deterministic_executor.random()
    time2 = deterministic_executor.utc_now()

    # Verify they're different values
    assert random1 != random2
    assert time1 != time2
    assert isinstance(uuid1, str)

    # Create new executor and verify same sequence
    new_executor = DeterministicExecutor(
        deterministic_executor.workflow_identity, deterministic_executor.app
    )

    new_random1 = new_executor.random()
    new_time1 = new_executor.utc_now()
    new_uuid1 = new_executor.uuid()
    new_random2 = new_executor.random()
    new_time2 = new_executor.utc_now()

    assert random1 == new_random1
    assert time1 == new_time1
    assert uuid1 == new_uuid1
    assert random2 == new_random2
    assert time2 == new_time2
