"""Tests for invocation status recovery transitions."""

from datetime import UTC, datetime

import pytest

from pynenc.invocation.status import (
    InvocationStatus,
    InvocationStatusRecord,
    status_record_transition,
)
from pynenc.exceptions import (
    InvocationStatusTransitionError,
    InvocationStatusOwnershipError,
)


def test_pending_to_pending_recovery_should_bypass_ownership() -> None:
    """Test that PENDING → PENDING_RECOVERY transition bypasses ownership validation."""
    # Arrange: Create a PENDING status record owned by runner-1
    current_record = InvocationStatusRecord(
        status=InvocationStatus.PENDING,
        runner_id="runner-1",
        timestamp=datetime.now(UTC),
    )

    # Act: Attempt recovery by a different runner (runner-2)
    new_record = status_record_transition(
        current_record=current_record,
        new_status=InvocationStatus.PENDING_RECOVERY,
        runner_id="runner-2",  # Different runner attempting recovery
    )

    # Assert: Transition should succeed
    assert new_record.status == InvocationStatus.PENDING_RECOVERY
    assert new_record.runner_id is None  # Ownership released


def test_pending_to_pending_recovery_should_release_ownership() -> None:
    """Test that PENDING_RECOVERY status releases ownership."""
    # Arrange
    current_record = InvocationStatusRecord(
        status=InvocationStatus.PENDING,
        runner_id="runner-1",
        timestamp=datetime.now(UTC),
    )

    # Act
    new_record = status_record_transition(
        current_record=current_record,
        new_status=InvocationStatus.PENDING_RECOVERY,
        runner_id="recovery-runner",
    )

    # Assert: Ownership should be released
    assert new_record.runner_id is None


def test_pending_recovery_to_rerouted_should_succeed() -> None:
    """Test that PENDING_RECOVERY → REROUTED transition works."""
    # Arrange
    current_record = InvocationStatusRecord(
        status=InvocationStatus.PENDING_RECOVERY,
        runner_id=None,
        timestamp=datetime.now(UTC),
    )

    # Act
    new_record = status_record_transition(
        current_record=current_record,
        new_status=InvocationStatus.REROUTED,
        runner_id="runner-1",
    )

    # Assert
    assert new_record.status == InvocationStatus.REROUTED
    assert new_record.runner_id is None  # REROUTED also releases ownership


def test_running_to_pending_recovery_should_fail() -> None:
    """Test that RUNNING → PENDING_RECOVERY is not allowed."""
    # Arrange
    current_record = InvocationStatusRecord(
        status=InvocationStatus.RUNNING,
        runner_id="runner-1",
        timestamp=datetime.now(UTC),
    )

    # Act & Assert: Should fail because RUNNING doesn't allow transition to PENDING_RECOVERY
    with pytest.raises(InvocationStatusTransitionError):
        status_record_transition(
            current_record=current_record,
            new_status=InvocationStatus.PENDING_RECOVERY,
            runner_id="runner-1",
        )


def test_pending_to_running_should_still_require_ownership() -> None:
    """Test that normal PENDING → RUNNING transition still requires ownership."""
    # Arrange
    current_record = InvocationStatusRecord(
        status=InvocationStatus.PENDING,
        runner_id="runner-1",
        timestamp=datetime.now(UTC),
    )

    # Act & Assert: Should fail because runner-2 doesn't own the invocation
    with pytest.raises(InvocationStatusOwnershipError):
        status_record_transition(
            current_record=current_record,
            new_status=InvocationStatus.RUNNING,
            runner_id="runner-2",  # Different runner
        )


def test_pending_to_running_should_succeed_with_correct_owner() -> None:
    """Test that PENDING → RUNNING succeeds with correct ownership."""
    # Arrange
    current_record = InvocationStatusRecord(
        status=InvocationStatus.PENDING,
        runner_id="runner-1",
        timestamp=datetime.now(UTC),
    )

    # Act
    new_record = status_record_transition(
        current_record=current_record,
        new_status=InvocationStatus.RUNNING,
        runner_id="runner-1",  # Same runner as owner
    )

    # Assert
    assert new_record.status == InvocationStatus.RUNNING
    assert new_record.runner_id == "runner-1"  # Ownership maintained
