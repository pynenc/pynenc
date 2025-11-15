# Instruct mypy to ignore errors for this test module (tests may intentionally mutate internals)
# mypy: ignore-errors
# Also add a generic type-ignore for tools that look for the PEP 484 pragma
# type: ignore

import pytest
from datetime import datetime, UTC

from pynenc.invocation.status import (
    InvocationStatus,
    InvocationStatusRecord,
    StatusDefinition,
    StatusConfiguration,
    validate_transition,
    validate_ownership,
    compute_new_owner,
    status_record_transition,
)
from pynenc.exceptions import (
    InvocationStatusTransitionError,
    InvocationStatusOwnershipError,
)


# Test StatusDefinition validation
def test_status_definition_should_reject_both_acquire_and_release() -> None:
    """Status cannot both acquire and release ownership."""
    with pytest.raises(ValueError, match="cannot both acquire and release"):
        StatusDefinition(acquires_ownership=True, releases_ownership=True)


def test_status_definition_should_reject_final_with_transitions() -> None:
    """Final statuses cannot have allowed transitions."""
    with pytest.raises(
        ValueError, match="Final statuses cannot have allowed transitions"
    ):
        StatusDefinition(
            is_final=True,
            allowed_transitions=frozenset({InvocationStatus.REGISTERED}),
        )


# Test StatusConfiguration validation
def test_status_config_should_require_all_status_definitions() -> None:
    """Configuration must include all InvocationStatus values."""
    with pytest.raises(ValueError, match="Missing definitions for"):
        StatusConfiguration(
            definitions={
                None: StatusDefinition(
                    allowed_transitions=frozenset({InvocationStatus.REGISTERED})
                ),
                InvocationStatus.REGISTERED: StatusDefinition(),
            }
        )


# Test StatusConfiguration cached properties
def test_status_config_should_identify_final_statuses() -> None:
    """Configuration correctly identifies final statuses."""
    config = StatusConfiguration(
        definitions={
            None: StatusDefinition(),
            InvocationStatus.REGISTERED: StatusDefinition(),
            InvocationStatus.SUCCESS: StatusDefinition(is_final=True),
            InvocationStatus.FAILED: StatusDefinition(is_final=True),
            **{
                s: StatusDefinition()
                for s in InvocationStatus
                if s
                not in {
                    InvocationStatus.SUCCESS,
                    InvocationStatus.FAILED,
                    InvocationStatus.REGISTERED,
                }
            },
        }
    )
    assert config.final_statuses == {InvocationStatus.SUCCESS, InvocationStatus.FAILED}


def test_status_config_should_identify_available_for_run() -> None:
    """Configuration correctly identifies statuses available for run."""
    config = StatusConfiguration(
        definitions={
            None: StatusDefinition(),
            InvocationStatus.REGISTERED: StatusDefinition(available_for_run=True),
            InvocationStatus.RETRY: StatusDefinition(available_for_run=True),
            **{
                s: StatusDefinition()
                for s in InvocationStatus
                if s not in {InvocationStatus.REGISTERED, InvocationStatus.RETRY}
            },
        }
    )
    assert config.available_for_run_statuses == {
        InvocationStatus.REGISTERED,
        InvocationStatus.RETRY,
    }


def test_status_config_should_identify_ownership_rules() -> None:
    """Configuration correctly identifies ownership-related statuses."""
    config = StatusConfiguration(
        definitions={
            None: StatusDefinition(),
            InvocationStatus.REGISTERED: StatusDefinition(releases_ownership=True),
            InvocationStatus.PENDING: StatusDefinition(
                requires_ownership=True, acquires_ownership=True
            ),
            InvocationStatus.RUNNING: StatusDefinition(requires_ownership=True),
            **{
                s: StatusDefinition()
                for s in InvocationStatus
                if s
                not in {
                    InvocationStatus.REGISTERED,
                    InvocationStatus.PENDING,
                    InvocationStatus.RUNNING,
                }
            },
        }
    )
    assert InvocationStatus.PENDING in config.ownership_required_statuses
    assert InvocationStatus.RUNNING in config.ownership_required_statuses
    assert InvocationStatus.PENDING in config.ownership_acquire_statuses
    assert InvocationStatus.REGISTERED in config.ownership_release_statuses


# Test validate_transition
def test_validate_transition_should_allow_valid_transitions() -> None:
    """Valid transitions should not raise exceptions."""
    test_config = StatusConfiguration(
        definitions={
            None: StatusDefinition(
                allowed_transitions=frozenset({InvocationStatus.REGISTERED})
            ),
            InvocationStatus.REGISTERED: StatusDefinition(
                allowed_transitions=frozenset({InvocationStatus.PENDING})
            ),
            **{
                s: StatusDefinition()
                for s in InvocationStatus
                if s not in {InvocationStatus.REGISTERED}
            },
        }
    )

    # Temporarily replace global config
    import pynenc.invocation.status as status_module

    original_config = status_module._CONFIG
    status_module._CONFIG = test_config

    try:
        validate_transition(None, InvocationStatus.REGISTERED)
        validate_transition(InvocationStatus.REGISTERED, InvocationStatus.PENDING)
    finally:
        status_module._CONFIG = original_config


def test_validate_transition_should_reject_invalid_transitions() -> None:
    """Invalid transitions should raise InvocationStatusTransitionError."""
    test_config = StatusConfiguration(
        definitions={
            None: StatusDefinition(
                allowed_transitions=frozenset({InvocationStatus.REGISTERED})
            ),
            InvocationStatus.REGISTERED: StatusDefinition(
                allowed_transitions=frozenset({InvocationStatus.PENDING})
            ),
            **{
                s: StatusDefinition()
                for s in InvocationStatus
                if s not in {InvocationStatus.REGISTERED}
            },
        }
    )

    import pynenc.invocation.status as status_module

    original_config = status_module._CONFIG
    status_module._CONFIG = test_config

    try:
        with pytest.raises(InvocationStatusTransitionError):
            validate_transition(None, InvocationStatus.RUNNING)

        with pytest.raises(InvocationStatusTransitionError):
            validate_transition(InvocationStatus.REGISTERED, InvocationStatus.SUCCESS)
    finally:
        status_module._CONFIG = original_config


# Test validate_ownership
def test_validate_ownership_should_allow_no_current_record() -> None:
    """Ownership validation should pass when no current record exists."""
    validate_ownership(None, InvocationStatus.REGISTERED, None)


def test_validate_ownership_should_enforce_ownership_requirement() -> None:
    """Should reject transitions when ownership is required but not held."""
    test_config = StatusConfiguration(
        definitions={
            None: StatusDefinition(),
            InvocationStatus.RUNNING: StatusDefinition(requires_ownership=True),
            **{
                s: StatusDefinition()
                for s in InvocationStatus
                if s != InvocationStatus.RUNNING
            },
        }
    )

    import pynenc.invocation.status as status_module

    original_config = status_module._CONFIG
    status_module._CONFIG = test_config

    try:
        current = InvocationStatusRecord(
            status=InvocationStatus.RUNNING, owner_id="runner-1"
        )

        with pytest.raises(InvocationStatusOwnershipError, match="requires ownership"):
            validate_ownership(current, InvocationStatus.SUCCESS, "runner-2")
    finally:
        status_module._CONFIG = original_config


def test_validate_ownership_should_allow_same_owner() -> None:
    """Should allow transitions when ownership is held by same runner."""
    test_config = StatusConfiguration(
        definitions={
            None: StatusDefinition(),
            InvocationStatus.RUNNING: StatusDefinition(requires_ownership=True),
            InvocationStatus.SUCCESS: StatusDefinition(),
            **{
                s: StatusDefinition()
                for s in InvocationStatus
                if s not in {InvocationStatus.RUNNING, InvocationStatus.SUCCESS}
            },
        }
    )

    import pynenc.invocation.status as status_module

    original_config = status_module._CONFIG
    status_module._CONFIG = test_config

    try:
        current = InvocationStatusRecord(
            status=InvocationStatus.RUNNING, owner_id="runner-1"
        )
        validate_ownership(current, InvocationStatus.SUCCESS, "runner-1")
    finally:
        status_module._CONFIG = original_config


def test_validate_ownership_should_require_runner_for_acquisition() -> None:
    """Should reject transitions that acquire ownership without a runner_id."""
    test_config = StatusConfiguration(
        definitions={
            None: StatusDefinition(),
            InvocationStatus.REGISTERED: StatusDefinition(),
            InvocationStatus.PENDING: StatusDefinition(acquires_ownership=True),
            **{
                s: StatusDefinition()
                for s in InvocationStatus
                if s not in {InvocationStatus.REGISTERED, InvocationStatus.PENDING}
            },
        }
    )

    import pynenc.invocation.status as status_module

    original_config = status_module._CONFIG
    status_module._CONFIG = test_config

    try:
        current = InvocationStatusRecord(
            status=InvocationStatus.REGISTERED, owner_id=None
        )

        with pytest.raises(
            InvocationStatusOwnershipError, match="requires a runner_id"
        ):
            validate_ownership(current, InvocationStatus.PENDING, None)
    finally:
        status_module._CONFIG = original_config


# Test compute_new_owner
def test_compute_new_owner_should_release_ownership() -> None:
    """Compute new owner should return None when releasing."""
    test_config = StatusConfiguration(
        definitions={
            None: StatusDefinition(),
            InvocationStatus.SUCCESS: StatusDefinition(releases_ownership=True),
            **{
                s: StatusDefinition()
                for s in InvocationStatus
                if s != InvocationStatus.SUCCESS
            },
        }
    )

    import pynenc.invocation.status as status_module

    original_config = status_module._CONFIG
    status_module._CONFIG = test_config

    try:
        current = InvocationStatusRecord(
            status=InvocationStatus.RUNNING, owner_id="runner-1"
        )
        new_owner = compute_new_owner(current, InvocationStatus.SUCCESS, "runner-1")
        assert new_owner is None
    finally:
        status_module._CONFIG = original_config


def test_compute_new_owner_should_acquire_ownership() -> None:
    """Compute new owner should return runner_id when acquiring."""
    test_config = StatusConfiguration(
        definitions={
            None: StatusDefinition(),
            InvocationStatus.PENDING: StatusDefinition(acquires_ownership=True),
            **{
                s: StatusDefinition()
                for s in InvocationStatus
                if s != InvocationStatus.PENDING
            },
        }
    )

    import pynenc.invocation.status as status_module

    original_config = status_module._CONFIG
    status_module._CONFIG = test_config

    try:
        current = InvocationStatusRecord(
            status=InvocationStatus.REGISTERED, owner_id=None
        )
        new_owner = compute_new_owner(current, InvocationStatus.PENDING, "runner-1")
        assert new_owner == "runner-1"
    finally:
        status_module._CONFIG = original_config


def test_compute_new_owner_should_maintain_ownership() -> None:
    """Compute new owner should maintain current owner when not releasing or acquiring."""
    test_config = StatusConfiguration(
        definitions={
            None: StatusDefinition(),
            InvocationStatus.RUNNING: StatusDefinition(),
            InvocationStatus.PAUSED: StatusDefinition(),
            **{
                s: StatusDefinition()
                for s in InvocationStatus
                if s not in {InvocationStatus.RUNNING, InvocationStatus.PAUSED}
            },
        }
    )

    import pynenc.invocation.status as status_module

    original_config = status_module._CONFIG
    status_module._CONFIG = test_config

    try:
        current = InvocationStatusRecord(
            status=InvocationStatus.RUNNING, owner_id="runner-1"
        )
        new_owner = compute_new_owner(current, InvocationStatus.PAUSED, "runner-1")
        assert new_owner == "runner-1"
    finally:
        status_module._CONFIG = original_config


# Test transition function (integration)
def test_transition_should_create_new_record() -> None:
    """Transition should create a new status record with updated values."""
    test_config = StatusConfiguration(
        definitions={
            None: StatusDefinition(
                allowed_transitions=frozenset({InvocationStatus.REGISTERED})
            ),
            InvocationStatus.REGISTERED: StatusDefinition(
                allowed_transitions=frozenset({InvocationStatus.PENDING}),
                releases_ownership=True,
            ),
            InvocationStatus.PENDING: StatusDefinition(acquires_ownership=True),
            **{
                s: StatusDefinition()
                for s in InvocationStatus
                if s not in {InvocationStatus.REGISTERED, InvocationStatus.PENDING}
            },
        }
    )

    import pynenc.invocation.status as status_module

    original_config = status_module._CONFIG
    status_module._CONFIG = test_config

    try:
        new_record = status_record_transition(None, InvocationStatus.REGISTERED, None)
        assert new_record.status == InvocationStatus.REGISTERED
        assert new_record.owner_id is None

        next_record = status_record_transition(
            new_record, InvocationStatus.PENDING, "runner-1"
        )
        assert next_record.status == InvocationStatus.PENDING
        assert next_record.owner_id == "runner-1"
    finally:
        status_module._CONFIG = original_config


def test_transition_should_reject_invalid_state_change() -> None:
    """Transition should reject invalid state transitions."""
    test_config = StatusConfiguration(
        definitions={
            None: StatusDefinition(
                allowed_transitions=frozenset({InvocationStatus.REGISTERED})
            ),
            InvocationStatus.REGISTERED: StatusDefinition(
                allowed_transitions=frozenset({InvocationStatus.PENDING})
            ),
            **{
                s: StatusDefinition()
                for s in InvocationStatus
                if s != InvocationStatus.REGISTERED
            },
        }
    )

    import pynenc.invocation.status as status_module

    original_config = status_module._CONFIG
    status_module._CONFIG = test_config

    try:
        with pytest.raises(InvocationStatusTransitionError):
            status_record_transition(None, InvocationStatus.RUNNING, None)
    finally:
        status_module._CONFIG = original_config


def test_transition_should_reject_ownership_violation() -> None:
    """Transition should reject transitions that violate ownership rules."""
    test_config = StatusConfiguration(
        definitions={
            None: StatusDefinition(
                allowed_transitions=frozenset({InvocationStatus.REGISTERED})
            ),
            InvocationStatus.REGISTERED: StatusDefinition(
                allowed_transitions=frozenset({InvocationStatus.PENDING})
            ),
            InvocationStatus.PENDING: StatusDefinition(
                allowed_transitions=frozenset({InvocationStatus.RUNNING}),
                requires_ownership=True,
                acquires_ownership=True,
            ),
            InvocationStatus.RUNNING: StatusDefinition(requires_ownership=True),
            **{
                s: StatusDefinition()
                for s in InvocationStatus
                if s
                not in {
                    InvocationStatus.REGISTERED,
                    InvocationStatus.PENDING,
                    InvocationStatus.RUNNING,
                }
            },
        }
    )

    import pynenc.invocation.status as status_module

    original_config = status_module._CONFIG
    status_module._CONFIG = test_config

    try:
        current = InvocationStatusRecord(
            status=InvocationStatus.PENDING, owner_id="runner-1"
        )

        with pytest.raises(InvocationStatusOwnershipError):
            status_record_transition(current, InvocationStatus.RUNNING, "runner-2")
    finally:
        status_module._CONFIG = original_config


# Test InvocationStatus methods
def test_invocation_status_is_final() -> None:
    """InvocationStatus.is_final() should use configuration."""
    assert InvocationStatus.SUCCESS.is_final()
    assert InvocationStatus.FAILED.is_final()
    assert not InvocationStatus.REGISTERED.is_final()


def test_invocation_status_is_available_for_run() -> None:
    """InvocationStatus.is_available_for_run() should use configuration."""
    assert InvocationStatus.REGISTERED.is_available_for_run()
    assert not InvocationStatus.SUCCESS.is_available_for_run()


def test_invocation_status_get_final_statuses() -> None:
    """InvocationStatus.get_final_statuses() should return configured final statuses."""
    final = InvocationStatus.get_final_statuses()
    assert InvocationStatus.SUCCESS in final
    assert InvocationStatus.FAILED in final


def test_invocation_status_get_available_for_run_statuses() -> None:
    """InvocationStatus.get_available_for_run_statuses() should return configured statuses."""
    available = InvocationStatus.get_available_for_run_statuses()
    assert InvocationStatus.REGISTERED in available


# Test InvocationStatusRecord
def test_invocation_status_record_default_values() -> None:
    """InvocationStatusRecord should use sensible defaults."""
    record = InvocationStatusRecord(status=InvocationStatus.REGISTERED)
    assert record.status == InvocationStatus.REGISTERED
    assert record.owner_id is None
    assert isinstance(record.timestamp, datetime)
    assert record.timestamp.tzinfo == UTC


def test_invocation_status_record_immutability() -> None:
    """InvocationStatusRecord should be frozen."""
    record = InvocationStatusRecord(status=InvocationStatus.REGISTERED)
    with pytest.raises(AttributeError):  # dataclass(frozen=True) raises AttributeError
        record.status = InvocationStatus.RUNNING  # type: ignore
