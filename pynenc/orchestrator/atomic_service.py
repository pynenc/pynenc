"""Atomic service coordination for distributed Pynenc runners.

Pure scheduling logic: no I/O, no orchestrator dependencies. The single entry
point is :func:`decide_atomic_service_claim` which returns one
:class:`AtomicServiceClaim` describing what the caller should do next.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum, auto
from typing import NamedTuple


# ── Identity / data types ────────────────────────────────────────────────────


@dataclass(frozen=True)
class AtomicServiceId:
    """Identifies one atomic-service run."""

    runner_id: str
    atomic_service_run_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __str__(self) -> str:
        return (
            f"runner:{self.runner_id} atomic-service-run:{self.atomic_service_run_id}"
        )


@dataclass
class AtomicServiceRun:
    """Identifies one atomic-service cycle claimed by a runner.

    Accepts either a composed ``atomic_service_id`` or the flat ``runner_id`` /
    ``atomic_service_run_id`` pair so downstream call sites can construct runs
    without first building an ``AtomicServiceId``.
    """

    atomic_service_id: AtomicServiceId = None  # type: ignore[assignment]
    cycle_start: datetime | None = None
    slot_start: datetime | None = None
    slot_end: datetime | None = None
    started_at: datetime | None = None
    # Flat-init aliases; folded into ``atomic_service_id`` in ``__post_init__``.
    runner_id: str | None = None  # type: ignore[assignment]
    atomic_service_run_id: str | None = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.atomic_service_id is None:
            if self.runner_id is None:
                raise TypeError(
                    "AtomicServiceRun requires either atomic_service_id or runner_id"
                )
            self.atomic_service_id = AtomicServiceId(
                runner_id=self.runner_id,
                atomic_service_run_id=(
                    self.atomic_service_run_id
                    if self.atomic_service_run_id is not None
                    else str(uuid.uuid4())
                ),
            )
        # Always project flat aliases from the composite id so they stay in sync.
        object.__setattr__(self, "runner_id", self.atomic_service_id.runner_id)
        object.__setattr__(
            self,
            "atomic_service_run_id",
            self.atomic_service_id.atomic_service_run_id,
        )

    def __str__(self) -> str:
        msg = str(self.atomic_service_id)
        if self.cycle_start is not None:
            msg += f" cycle_start={self.cycle_start.isoformat()}"
        if self.slot_start is not None and self.slot_end is not None:
            msg += (
                f" slot=({self.slot_start.isoformat()} - {self.slot_end.isoformat()})"
            )
        if self.started_at is not None:
            msg += f" started_at={self.started_at.isoformat()}"
        return msg


class AtomicServiceDecisionReason(StrEnum):
    """Outcome labels produced by the scheduler claim decision."""

    ASSIGNED = auto()
    NOT_ASSIGNED_SLOT = auto()
    NO_STABLE_RUNNERS = auto()
    SCHEDULED_RUNNER_IN_GRACE = auto()
    SLOT_WINDOW_INVALID = auto()
    LATE_START = auto()

    def is_persisted(self) -> bool:
        """Whether this reason should be persisted as a BLOCKED execution."""
        return self in _PERSISTED_REASONS


_PERSISTED_REASONS = frozenset(
    {
        AtomicServiceDecisionReason.SLOT_WINDOW_INVALID,
        AtomicServiceDecisionReason.SCHEDULED_RUNNER_IN_GRACE,
        AtomicServiceDecisionReason.LATE_START,
    }
)


class ActiveRunnerInfo(NamedTuple):
    """Information about an active runner including heartbeat tracking."""

    runner_id: str
    creation_time: datetime
    last_heartbeat: datetime
    allow_to_run_atomic_service: bool = False


class AtomicServiceExecutionStatus(StrEnum):
    """Lifecycle status of a recorded atomic-service execution attempt."""

    RUNNING = "running"
    COMPLETED = "completed"
    ABANDONED = "abandoned"
    BLOCKED = "blocked"


class AtomicServiceExecution(NamedTuple):
    """A recorded atomic-service execution attempt by one runner."""

    atomic_service_id: AtomicServiceId
    start_time: datetime
    end_time: datetime | None
    status: AtomicServiceExecutionStatus = AtomicServiceExecutionStatus.RUNNING
    reason: str = ""

    @classmethod
    def from_raw(
        cls,
        runner_id: str,
        atomic_service_run_id: str,
        start_time: datetime,
        end_time: datetime | None,
        status: AtomicServiceExecutionStatus = AtomicServiceExecutionStatus.RUNNING,
        reason: str = "",
    ) -> AtomicServiceExecution:
        return cls(
            AtomicServiceId(runner_id, atomic_service_run_id),
            start_time,
            end_time,
            status,
            reason,
        )

    @property
    def runner_id(self) -> str:
        return self.atomic_service_id.runner_id

    @property
    def atomic_service_run_id(self) -> str:
        return self.atomic_service_id.atomic_service_run_id

    @property
    def atomic_service_run(self) -> AtomicServiceRun:
        return AtomicServiceRun(
            atomic_service_id=self.atomic_service_id,
            started_at=self.start_time,
        )

    @property
    def is_active(self) -> bool:
        return self.status == AtomicServiceExecutionStatus.RUNNING

    @property
    def duration_seconds(self) -> float:
        if self.end_time is None:
            return 0.0
        return (self.end_time - self.start_time).total_seconds()


# ── Single result type ───────────────────────────────────────────────────────


@dataclass(frozen=True)
class AtomicServiceClaim:
    """Final scheduling result. The single object callers consume.

    Carries everything an orchestrator or UI needs: the reason, the slot
    layout, the assigned runner, the optional ``AtomicServiceRun`` when the
    runner is cleared to start, and the optional ``skip_message`` /
    ``late_start_fraction`` when the slot is held reserved.
    """

    runner_id: str
    reason: AtomicServiceDecisionReason
    cycle_start: datetime
    assigned_runner_id: str | None = None
    runner_position: int | None = None
    stable_runner_count: int = 0
    slot_start: datetime | None = None
    slot_end: datetime | None = None
    is_slot_owner_now: bool = False
    atomic_service_run: AtomicServiceRun | None = None
    skip_message: str = ""
    late_start_fraction: float | None = None

    @property
    def should_try_start(self) -> bool:
        return self.atomic_service_run is not None

    @property
    def skip_reason(self) -> AtomicServiceDecisionReason | None:
        return self.reason if self.reason.is_persisted() else None

    @property
    def decision(self) -> AtomicServiceClaim:
        # Legacy alias: previously the claim wrapped a separate decision
        # object exposing the same scheduling fields. Returning ``self``
        # keeps ``claim.decision.<field>`` call sites working.
        return self


# ── Builder ──────────────────────────────────────────────────────────────────


@dataclass
class _ClaimBuilder:
    """Mutable scratch-pad. The scheduler mutates this object and then calls
    one of the named terminal methods, each of which calls ``_build()`` exactly
    once with the right reason.

    Defaults represent the "skip, nothing to report" outcome — every branch
    only sets the fields that differ.
    """

    runner_id: str
    started_at: datetime
    current_time: float
    interval_seconds: float
    spread_minutes: float
    stabilization_seconds: float

    cycle_start: datetime = field(init=False)
    stable_runner_count: int = 0
    assigned_runner_id: str | None = None
    runner_position: int | None = None
    slot_start: datetime | None = None
    slot_end: datetime | None = None
    is_slot_owner_now: bool = False

    def __post_init__(self) -> None:
        cycle_ts = (self.current_time // self.interval_seconds) * self.interval_seconds
        self.cycle_start = datetime.fromtimestamp(cycle_ts, tz=UTC)

    # ── Mutators ──────────────────────────────────────────────────────────
    def use_runners(self, total: int, assigned_runner_id: str) -> None:
        self.stable_runner_count = total
        self.assigned_runner_id = assigned_runner_id

    def set_position(self, position: int) -> None:
        self.runner_position = position

    def set_slot(self, window: tuple[datetime, datetime], *, is_owner: bool) -> None:
        self.slot_start, self.slot_end = window
        self.is_slot_owner_now = (
            is_owner and self.current_time >= self.slot_start.timestamp()
        )

    # ── Terminal methods ──────────────────────────────────────────────────
    def no_runners(self) -> AtomicServiceClaim:
        return self._build(AtomicServiceDecisionReason.NO_STABLE_RUNNERS)

    def not_in_stable_set(self) -> AtomicServiceClaim:
        return self._build(AtomicServiceDecisionReason.NOT_ASSIGNED_SLOT)

    def not_in_slot(self) -> AtomicServiceClaim:
        return self._build(AtomicServiceDecisionReason.NOT_ASSIGNED_SLOT)

    def invalid_window(self) -> AtomicServiceClaim:
        return self._build(
            AtomicServiceDecisionReason.SLOT_WINDOW_INVALID,
            skip_message=(
                f"spread_margin={self.spread_minutes}m consumes the entire "
                f"per-runner window of interval/{self.stable_runner_count}"
            ),
        )

    def in_grace(self) -> AtomicServiceClaim:
        return self._build(
            AtomicServiceDecisionReason.SCHEDULED_RUNNER_IN_GRACE,
            skip_message=(
                f"assigned runner {self.assigned_runner_id} is still in "
                f"its {self.stabilization_seconds / 60.0}m membership grace window; "
                f"slot held reserved"
            ),
        )

    def late_start(self, fraction: float, max_fraction: float) -> AtomicServiceClaim:
        return self._build(
            AtomicServiceDecisionReason.LATE_START,
            skip_message=(
                f"started_at consumed {fraction:.2%} of slot (max={max_fraction:.2%})"
            ),
            late_start_fraction=fraction,
        )

    def cleared_to_start(self) -> AtomicServiceClaim:
        run = AtomicServiceRun(
            atomic_service_id=AtomicServiceId(self.runner_id),
            cycle_start=self.cycle_start,
            slot_start=self.slot_start,
            slot_end=self.slot_end,
            started_at=self.started_at,
        )
        return self._build(AtomicServiceDecisionReason.ASSIGNED, atomic_service_run=run)

    # ── Single construction point ─────────────────────────────────────────
    def _build(
        self,
        reason: AtomicServiceDecisionReason,
        *,
        atomic_service_run: AtomicServiceRun | None = None,
        skip_message: str = "",
        late_start_fraction: float | None = None,
    ) -> AtomicServiceClaim:
        return AtomicServiceClaim(
            runner_id=self.runner_id,
            reason=reason,
            cycle_start=self.cycle_start,
            assigned_runner_id=self.assigned_runner_id,
            runner_position=self.runner_position,
            stable_runner_count=self.stable_runner_count,
            slot_start=self.slot_start,
            slot_end=self.slot_end,
            is_slot_owner_now=self.is_slot_owner_now,
            atomic_service_run=atomic_service_run,
            skip_message=skip_message,
            late_start_fraction=late_start_fraction,
        )


# ── Small pure helpers ───────────────────────────────────────────────────────


def _slot_window(
    cycle_start_ts: float,
    position: int,
    slot_size: float,
    spread_seconds: float,
    *,
    single_runner: bool,
) -> tuple[datetime, datetime] | None:
    """(start, end) for this position, or None when the margin is too wide."""
    if single_runner:
        return (
            datetime.fromtimestamp(cycle_start_ts, tz=UTC),
            datetime.fromtimestamp(cycle_start_ts + slot_size, tz=UTC),
        )
    end_off = slot_size - spread_seconds
    if end_off <= 0:
        return None
    base = cycle_start_ts + position * slot_size
    return (
        datetime.fromtimestamp(base, tz=UTC),
        datetime.fromtimestamp(base + end_off, tz=UTC),
    )


def _is_in_grace(
    runner: ActiveRunnerInfo, now: float, stabilization_seconds: float
) -> bool:
    return (
        stabilization_seconds > 0.0
        and (now - runner.creation_time.timestamp()) < stabilization_seconds
    )


def _late_start_fraction(
    started_at: datetime,
    slot_start: datetime | None,
    slot_end: datetime | None,
    max_fraction: float,
) -> float | None:
    if slot_start is None or slot_end is None or not 0.0 < max_fraction < 1.0:
        return None
    slot_seconds = (slot_end - slot_start).total_seconds()
    consumed = (started_at - slot_start).total_seconds()
    if slot_seconds <= 0.0 or consumed <= 0.0:
        return None
    fraction = consumed / slot_seconds
    return fraction if fraction > max_fraction else None


# ── The one scheduler ────────────────────────────────────────────────────────


def decide_atomic_service_claim(
    *,
    runner_id: str,
    active_runners: list[ActiveRunnerInfo],
    current_time: float,
    service_interval_minutes: float,
    spread_margin_minutes: float,
    membership_stabilization_seconds: float,
    max_start_slot_fraction: float,
) -> AtomicServiceClaim:
    """Decide whether ``runner_id`` should start the atomic service now.

    Returns a single :class:`AtomicServiceClaim` describing the outcome.
    Callers inspect ``claim.atomic_service_run`` (non-None means start),
    ``claim.skip_reason`` (non-None means persist as BLOCKED), and
    ``claim.reason`` for diagnostics.
    """
    interval = service_interval_minutes * 60
    spread = spread_margin_minutes * 60
    builder = _ClaimBuilder(
        runner_id=runner_id,
        started_at=datetime.fromtimestamp(current_time, tz=UTC),
        current_time=current_time,
        interval_seconds=interval,
        spread_minutes=spread_margin_minutes,
        stabilization_seconds=membership_stabilization_seconds,
    )

    if not active_runners:
        return builder.no_runners()

    total = len(active_runners)
    if total == 0:
        return builder.no_runners()
    single = total == 1
    slot_size = interval / total
    cycle_start_ts = builder.cycle_start.timestamp()
    time_in_cycle = current_time - cycle_start_ts
    assigned_position = 0 if single else min(int(time_in_cycle // slot_size), total - 1)
    assigned_runner = active_runners[assigned_position]
    builder.use_runners(total, assigned_runner.runner_id)

    position = next(
        (i for i, r in enumerate(active_runners) if r.runner_id == runner_id), None
    )
    if position is None:
        return builder.not_in_stable_set()
    builder.set_position(position)

    window = _slot_window(
        cycle_start_ts, position, slot_size, spread, single_runner=single
    )
    if window is None:
        return builder.invalid_window()
    builder.set_slot(window, is_owner=assigned_runner.runner_id == runner_id)

    if _is_in_grace(assigned_runner, current_time, membership_stabilization_seconds):
        return builder.in_grace()

    assert builder.slot_start is not None and builder.slot_end is not None
    in_slot = single or (
        builder.slot_start.timestamp() <= current_time < builder.slot_end.timestamp()
    )
    if not in_slot:
        return builder.not_in_slot()

    late = _late_start_fraction(
        builder.started_at,
        builder.slot_start,
        builder.slot_end,
        max_start_slot_fraction,
    )
    if late is not None:
        return builder.late_start(late, max_start_slot_fraction)

    return builder.cleared_to_start()
