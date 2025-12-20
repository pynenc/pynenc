"""
Invocation status management.

This module defines the status lifecycle of task invocations, including state transitions,
ownership rules, and the state machine that enforces them.

Key components:
- InvocationStatus: Enum of all possible invocation states
- InvocationStatusRecord: Immutable record combining status with ownership
- StatusDefinition: Declarative rules for status behavior
- State machine functions: Validation and transition logic
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum, auto
from functools import cached_property
from typing import Final

from pynenc.exceptions import (
    InvocationStatusTransitionError,
    InvocationStatusOwnershipError,
)


class InvocationStatus(StrEnum):
    """
    An enumeration representing the status of a task invocation.

    The PENDING status will expire after the time specified in
    :attr:`~pynenc.conf.config_pynenc.ConfigPynenc.max_pending_seconds`.

    ```{note}
    FAILED, RETRY, and SUCCESS are final statuses that terminate the invocation lifecycle.
    ```

    :cvar REGISTERED: The task call has been routed and is registered
    :cvar CONCURRENCY_CONTROLLED: The task call is not allowed to run due to concurrency control
    :cvar CONCURRENCY_CONTROLLED_FINAL: The task call was blocked by concurrency control and will not be retried
    :cvar REROUTED: The task call has been re-routed and is registered
    :cvar PENDING: The task call was picked by a runner but is not yet executed
    :cvar PENDING_RECOVERY: The task call exceeded PENDING timeout and is being recovered
    :cvar RUNNING: The task call is currently running
    :cvar RUNNING_RECOVERY: The task call is being recovered because the owner runner is inactive
    :cvar PAUSED: The task call execution is paused
    :cvar RESUMED: The task call execution has been resumed
    :cvar KILLED: The task call execution has been killed
    :cvar SUCCESS: The task call finished without errors
    :cvar FAILED: The task call finished with exceptions
    :cvar RETRY: The task call finished with a retriable exception
    """

    REGISTERED = auto()
    CONCURRENCY_CONTROLLED = auto()
    CONCURRENCY_CONTROLLED_FINAL = auto()
    REROUTED = auto()
    PENDING = auto()
    PENDING_RECOVERY = auto()
    RUNNING = auto()
    RUNNING_RECOVERY = auto()
    PAUSED = auto()
    RESUMED = auto()
    KILLED = auto()
    SUCCESS = auto()
    FAILED = auto()
    RETRY = auto()

    def is_final(self) -> bool:
        """Check if the status terminates the invocation lifecycle."""
        return self in _CONFIG.final_statuses

    def is_available_for_run(self) -> bool:
        """Check if the task can be picked up and run by any broker."""
        return self in _CONFIG.available_for_run_statuses

    @classmethod
    def get_final_statuses(cls) -> frozenset["InvocationStatus"]:
        """Return all statuses that terminate the invocation lifecycle."""
        return _CONFIG.final_statuses

    @classmethod
    def get_available_for_run_statuses(cls) -> frozenset["InvocationStatus"]:
        """Return all statuses where invocations can be picked up by runners."""
        return _CONFIG.available_for_run_statuses


@dataclass(frozen=True)
class InvocationStatusRecord:
    """
    Immutable record combining status with ownership information.

    :ivar InvocationStatus status: The current invocation status
    :ivar str | None owner_id: Runner ID that owns this invocation (None if no owner)
    :ivar datetime timestamp: When this status was set
    """

    status: InvocationStatus
    owner_id: str | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_json(self) -> dict:
        """Serialize the status record to a JSON-compatible dictionary."""
        return {
            "status": self.status.value,
            "owner_id": self.owner_id,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_json(cls, json_dict: dict) -> "InvocationStatusRecord":
        """Deserialize a JSON-compatible dictionary into a status record."""
        timestamp = datetime.fromisoformat(json_dict["timestamp"])
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        return cls(
            status=InvocationStatus(json_dict["status"]),
            owner_id=json_dict.get("owner_id"),
            timestamp=timestamp,
        )


@dataclass(frozen=True)
class StatusDefinition:
    """
    Declarative definition of status behavior and ownership rules.

    :ivar frozenset[InvocationStatus] allowed_transitions: Valid next statuses
    :ivar bool is_final: Terminates invocation lifecycle
    :ivar bool available_for_run: Can be picked up by runners
    :ivar bool requires_ownership: Only owner can modify
    :ivar bool acquires_ownership: Claims ownership on entry
    :ivar bool releases_ownership: Releases ownership on entry
    :ivar bool overrides_ownership: Bypasses ownership validation (for recovery scenarios)
    """

    allowed_transitions: frozenset[InvocationStatus] = field(default_factory=frozenset)
    is_final: bool = False
    available_for_run: bool = False
    requires_ownership: bool = False
    acquires_ownership: bool = False
    releases_ownership: bool = False
    overrides_ownership: bool = False

    def __post_init__(self) -> None:
        if self.acquires_ownership and self.releases_ownership:
            raise ValueError("Status cannot both acquire and release ownership")
        if self.is_final and self.allowed_transitions:
            raise ValueError("Final statuses cannot have allowed transitions")
        if self.overrides_ownership and self.requires_ownership:
            raise ValueError("Status cannot both override and require ownership")


@dataclass(frozen=True)
class StatusConfiguration:
    """
    Complete configuration for invocation status behavior.

    :ivar dict[InvocationStatus | None, StatusDefinition] definitions: Behavior rules per status
    """

    definitions: dict[InvocationStatus | None, StatusDefinition]

    def __post_init__(self) -> None:
        defined = {k for k in self.definitions if k is not None}
        if missing := set(InvocationStatus) - defined:
            raise ValueError(f"Missing definitions for: {missing}")

        for status, definition in self.definitions.items():
            for target in definition.allowed_transitions:
                if target not in InvocationStatus:
                    raise ValueError(
                        f"{status} references undefined transition: {target}"
                    )

    @cached_property
    def final_statuses(self) -> frozenset[InvocationStatus]:
        return frozenset(s for s, d in self.definitions.items() if s and d.is_final)

    @cached_property
    def available_for_run_statuses(self) -> frozenset[InvocationStatus]:
        return frozenset(
            s for s, d in self.definitions.items() if s and d.available_for_run
        )

    @cached_property
    def ownership_required_statuses(self) -> frozenset[InvocationStatus]:
        return frozenset(
            s for s, d in self.definitions.items() if s and d.requires_ownership
        )

    @cached_property
    def ownership_acquire_statuses(self) -> frozenset[InvocationStatus]:
        return frozenset(
            s for s, d in self.definitions.items() if s and d.acquires_ownership
        )

    @cached_property
    def ownership_release_statuses(self) -> frozenset[InvocationStatus]:
        return frozenset(
            s for s, d in self.definitions.items() if s and d.releases_ownership
        )

    def get_definition(self, status: InvocationStatus | None) -> StatusDefinition:
        return self.definitions[status]


# ============================================================================
# Status Configuration
# ============================================================================

_CONFIG: Final[StatusConfiguration] = StatusConfiguration(
    definitions={
        None: StatusDefinition(
            allowed_transitions=frozenset({InvocationStatus.REGISTERED}),
        ),
        InvocationStatus.REGISTERED: StatusDefinition(
            allowed_transitions=frozenset(
                {
                    InvocationStatus.PENDING,
                    InvocationStatus.CONCURRENCY_CONTROLLED,
                    InvocationStatus.CONCURRENCY_CONTROLLED_FINAL,
                }
            ),
            available_for_run=True,
            releases_ownership=True,
        ),
        # The invocation is not allowed to run due to concurrency control
        InvocationStatus.CONCURRENCY_CONTROLLED: StatusDefinition(
            allowed_transitions=frozenset({InvocationStatus.REROUTED}),
            available_for_run=False,
            releases_ownership=True,
        ),
        InvocationStatus.REROUTED: StatusDefinition(
            allowed_transitions=frozenset(
                {InvocationStatus.PENDING, InvocationStatus.CONCURRENCY_CONTROLLED}
            ),
            available_for_run=True,
            releases_ownership=True,
        ),
        InvocationStatus.PENDING: StatusDefinition(
            # An invocation can FAILED without running by the CYCLE-CONTROL mechanism
            # to avoid deadlocks.
            # PENDING_RECOVERY is for timeout recovery without ownership validation
            allowed_transitions=frozenset(
                {
                    InvocationStatus.RUNNING,
                    InvocationStatus.KILLED,
                    InvocationStatus.REROUTED,
                    InvocationStatus.FAILED,
                    InvocationStatus.PENDING_RECOVERY,
                }
            ),
            requires_ownership=True,
            acquires_ownership=True,
        ),
        # Recovery status for PENDING invocations that exceeded timeout
        # Overrides ownership validation since original owner is unresponsive
        InvocationStatus.PENDING_RECOVERY: StatusDefinition(
            allowed_transitions=frozenset({InvocationStatus.REROUTED}),
            releases_ownership=True,
            overrides_ownership=True,
        ),
        InvocationStatus.RUNNING: StatusDefinition(
            allowed_transitions=frozenset(
                {
                    InvocationStatus.PAUSED,
                    InvocationStatus.KILLED,
                    InvocationStatus.RETRY,
                    InvocationStatus.SUCCESS,
                    InvocationStatus.FAILED,
                    InvocationStatus.RUNNING_RECOVERY,
                }
            ),
            requires_ownership=True,
        ),
        # Recovery status for RUNNING invocations owned by inactive runners
        # Overrides ownership validation since original owner is unresponsive
        InvocationStatus.RUNNING_RECOVERY: StatusDefinition(
            allowed_transitions=frozenset({InvocationStatus.REROUTED}),
            releases_ownership=True,
            overrides_ownership=True,
        ),
        InvocationStatus.PAUSED: StatusDefinition(
            allowed_transitions=frozenset(
                {InvocationStatus.RESUMED, InvocationStatus.KILLED}
            ),
            requires_ownership=True,
        ),
        InvocationStatus.RESUMED: StatusDefinition(
            allowed_transitions=frozenset(
                {
                    InvocationStatus.PAUSED,
                    InvocationStatus.KILLED,
                    InvocationStatus.RETRY,
                    InvocationStatus.SUCCESS,
                    InvocationStatus.FAILED,
                }
            ),
            requires_ownership=True,
        ),
        InvocationStatus.KILLED: StatusDefinition(
            allowed_transitions=frozenset({InvocationStatus.REROUTED}),
            releases_ownership=True,
        ),
        InvocationStatus.RETRY: StatusDefinition(
            allowed_transitions=frozenset({InvocationStatus.PENDING}),
            available_for_run=True,
            releases_ownership=True,
        ),
        InvocationStatus.SUCCESS: StatusDefinition(
            is_final=True,
            releases_ownership=True,
        ),
        InvocationStatus.FAILED: StatusDefinition(
            is_final=True,
            releases_ownership=True,
        ),
        # Final status for invocations blocked by concurrency control that should not retry
        InvocationStatus.CONCURRENCY_CONTROLLED_FINAL: StatusDefinition(
            is_final=True,
            releases_ownership=True,
        ),
    }
)


def get_status_definition(status: InvocationStatus) -> StatusDefinition:
    """
    Get the status definition for a given invocation status.

    :param status: The invocation status to look up
    :return: The status definition with rules and behavior
    """
    return _CONFIG.get_definition(status)


# ============================================================================
# State Machine Functions
# ============================================================================


def validate_transition(
    from_status: InvocationStatus | None,
    to_status: InvocationStatus,
) -> None:
    """
    Validate state transition or raise exception.

    :param InvocationStatus | None from_status: Current status (None for new invocations)
    :param InvocationStatus to_status: Target status
    :raises InvocationStatusTransitionError: If transition is invalid
    """
    definition = _CONFIG.get_definition(from_status)
    if to_status not in definition.allowed_transitions:
        raise InvocationStatusTransitionError(
            from_status=from_status,
            to_status=to_status,
            allowed_statuses=definition.allowed_transitions,
        )


def validate_ownership(
    current_record: InvocationStatusRecord | None,
    new_status: InvocationStatus,
    runner_id: str | None,
) -> None:
    """
    Validate ownership requirements for a transition.

    :param InvocationStatusRecord | None current_record: Current status record
    :param InvocationStatus new_status: Target status
    :param str | None runner_id: Runner attempting the transition
    :raises InvocationStatusOwnershipError: If ownership rules are violated
    """
    if not current_record:
        return

    new_def = _CONFIG.get_definition(new_status)

    # Allow transitions to statuses that override ownership validation
    if new_def.overrides_ownership:
        return

    current_def = _CONFIG.get_definition(current_record.status)

    msg: str | None = None
    attempted_owner: str | None = runner_id

    if current_def.requires_ownership and runner_id != current_record.owner_id:
        msg = f"Status requires ownership by '{current_record.owner_id}'"
    elif new_def.acquires_ownership and not runner_id:
        msg = f"Status {new_status} requires a runner_id to acquire ownership"
        attempted_owner = None

    if msg:
        raise InvocationStatusOwnershipError(
            from_status=current_record.status,
            to_status=new_status,
            current_owner=current_record.owner_id,
            attempted_owner=attempted_owner,
            reason=msg,
        )


def compute_new_owner(
    current_record: InvocationStatusRecord | None,
    new_status: InvocationStatus,
    runner_id: str | None,
) -> str | None:
    """Compute new owner based on status transition."""
    new_def = _CONFIG.get_definition(new_status)
    if new_def.releases_ownership:
        return None
    if new_def.acquires_ownership:
        return runner_id
    return current_record.owner_id if current_record else None


def status_record_transition(
    current_record: InvocationStatusRecord | None,
    new_status: InvocationStatus,
    runner_id: str | None,
) -> InvocationStatusRecord:
    """
    Execute a status change with safety checks.

    :param InvocationStatusRecord | None current_record: Current state (None for new invocations)
    :param InvocationStatus new_status: Desired new status
    :param str | None runner_id: ID of runner making the change
    :return: New validated status record
    :raises InvocationStatusTransitionError: If transition is not allowed
    :raises InvocationStatusOwnershipError: If ownership rules are violated
    """
    from_status = current_record.status if current_record else None
    validate_transition(from_status, new_status)
    validate_ownership(current_record, new_status, runner_id)
    new_owner = compute_new_owner(current_record, new_status, runner_id)

    return InvocationStatusRecord(status=new_status, owner_id=new_owner)
