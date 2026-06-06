"""Concurrency tests for ``try_claim_atomic_service_run``.

The atomic-service algorithm relies on two cooperating mechanisms:

* Deterministic ordering of active runners — every runner independently
  computes the same slot owner from the shared
  ``(creation_time, runner_id)`` ordering, so only the slot owner ever
  tries to claim. This is the primary, scheduler-level consensus.
* Storage-level ``RUNNING`` execution records — even after a sudden
  membership change, a runner about to start atomically records the start or
  yields with ``BLOCKED`` / ``prior_running``. This protects against missed
  work being run twice across slot boundaries.

These tests exercise both invariants under thread contention against both
the in-memory and SQLite backends. Backend plugins (Redis, MongoDB) own
equivalent suites in their own repositories.

The storage-level start write is the final mutual-exclusion gate; scheduler
ownership reduces contention, but the backend still owns the one-active-run
invariant.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from pynenc.orchestrator.atomic_service import AtomicServiceExecutionStatus
from pynenc.orchestrator.atomic_service import AtomicServiceRun
from pynenc.runner.runner_context import RunnerContext

if TYPE_CHECKING:
    from pynenc import Pynenc


def _make_runner_ctx(runner_id: str) -> RunnerContext:
    return RunnerContext(
        runner_cls="TestRunner",
        runner_id=runner_id,
        pid=12345,
        hostname="test-host",
    )


def _finalize(app_instance: Pynenc, run: AtomicServiceRun) -> None:
    app_instance.orchestrator.finalize_atomic_service_execution(
        run,
        datetime.now(UTC),
        AtomicServiceExecutionStatus.COMPLETED,
    )


def test_concurrent_claims_in_same_slot_admit_only_the_assigned_runner(
    app_instance: Pynenc,
) -> None:
    """Under real scheduling, a single slot admits only its assigned runner.

    Many runners race against the real scheduler at the same simulated
    moment in time. Only the deterministically-assigned runner may claim;
    every other runner must observe its non-ownership and return ``None``.
    """
    runner_ctxs = [_make_runner_ctx(f"runner-{i:02d}") for i in range(8)]
    for ctx in runner_ctxs:
        app_instance.orchestrator.register_runner_heartbeats(
            [ctx.runner_id], can_run_atomic_service=True
        )

    # Long interval so a single slot dominates the test window.
    original_interval = app_instance.conf.atomic_service_interval_minutes
    original_margin = app_instance.conf.atomic_service_spread_margin_minutes
    original_fraction = app_instance.conf.atomic_service_max_start_slot_fraction
    original_retention = app_instance.conf.atomic_service_execution_retention_minutes
    app_instance.conf.atomic_service_interval_minutes = 60.0
    app_instance.conf.atomic_service_spread_margin_minutes = 0.0
    app_instance.conf.atomic_service_max_start_slot_fraction = 1.0
    app_instance.conf.atomic_service_execution_retention_minutes = 0
    try:
        with patch("pynenc.orchestrator.base_orchestrator.time", return_value=0.0):
            with ThreadPoolExecutor(max_workers=len(runner_ctxs)) as pool:
                futures = [
                    pool.submit(
                        app_instance.orchestrator.try_claim_atomic_service_run,
                        ctx,
                    )
                    for ctx in runner_ctxs
                ]
                results = [f.result() for f in as_completed(futures)]

        winners = [r for r in results if r is not None]
        assert len(winners) == 1, (
            f"expected exactly one assigned winner, got {len(winners)}: "
            f"{[w.runner_id for w in winners]}"
        )

        active = app_instance.orchestrator.get_active_atomic_service_executions()
        assert len(active) == 1
        assert active[0].atomic_service_run_id == winners[0].atomic_service_run_id

        for winner in winners:
            _finalize(app_instance, winner)
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval
        app_instance.conf.atomic_service_spread_margin_minutes = original_margin
        app_instance.conf.atomic_service_max_start_slot_fraction = original_fraction
        app_instance.conf.atomic_service_execution_retention_minutes = (
            original_retention
        )


def test_membership_change_cannot_overlap_existing_atomic_service(
    app_instance: Pynenc,
) -> None:
    """A newly-visible runner must not start while a prior run is still active.

    This reproduces the real startup race: runner A claims while it is the only
    atomic-service member, then runner B joins and its newly-computed slot says
    it may run. The backend claim must reject B until A finalizes.
    """
    runner_a = _make_runner_ctx("membership-runner-a")
    runner_b = _make_runner_ctx("membership-runner-b")

    original_interval = app_instance.conf.atomic_service_interval_minutes
    original_margin = app_instance.conf.atomic_service_spread_margin_minutes
    original_fraction = app_instance.conf.atomic_service_max_start_slot_fraction
    original_retention = app_instance.conf.atomic_service_execution_retention_minutes
    app_instance.conf.atomic_service_interval_minutes = 1.0
    app_instance.conf.atomic_service_spread_margin_minutes = 0.0
    app_instance.conf.atomic_service_max_start_slot_fraction = 1.0
    app_instance.conf.atomic_service_execution_retention_minutes = 0
    run_a: AtomicServiceRun | None = None
    try:
        app_instance.orchestrator.register_runner_heartbeats(
            [runner_a.runner_id], can_run_atomic_service=True
        )
        with patch("pynenc.orchestrator.base_orchestrator.time", return_value=0.0):
            run_a = app_instance.orchestrator.try_claim_atomic_service_run(runner_a)
        assert run_a is not None

        app_instance.orchestrator.register_runner_heartbeats(
            [runner_b.runner_id], can_run_atomic_service=True
        )
        with patch("pynenc.orchestrator.base_orchestrator.time", return_value=30.0):
            run_b = app_instance.orchestrator.try_claim_atomic_service_run(runner_b)

        assert run_b is None
        active = app_instance.orchestrator.get_active_atomic_service_executions()
        assert [execution.atomic_service_run_id for execution in active] == [
            run_a.atomic_service_run_id
        ]
        executions = (
            app_instance.orchestrator.get_atomic_service_executions_in_timerange(
                datetime.fromtimestamp(0, tz=UTC),
                datetime.now(UTC),
                limit=10,
            )
        )
        blocked = [
            execution
            for execution in executions
            if execution.status == AtomicServiceExecutionStatus.BLOCKED
        ]
        assert len(blocked) == 1
        assert blocked[0].runner_id == runner_b.runner_id
        assert "prior_running" in blocked[0].reason
    finally:
        if run_a is not None:
            _finalize(app_instance, run_a)
        app_instance.conf.atomic_service_interval_minutes = original_interval
        app_instance.conf.atomic_service_spread_margin_minutes = original_margin
        app_instance.conf.atomic_service_max_start_slot_fraction = original_fraction
        app_instance.conf.atomic_service_execution_retention_minutes = (
            original_retention
        )


def test_repeated_claims_across_cycles_never_overlap(
    app_instance: Pynenc,
) -> None:
    """Across many cycles, no two ``RUNNING`` records exist at the same time.

    Each cycle finalizes its winner before the next cycle starts; the
    algorithm must therefore admit exactly one winner per cycle and never
    leave a stale ``RUNNING`` record behind.
    """
    runner_ctxs = [_make_runner_ctx(f"runner-{i:02d}") for i in range(4)]
    for ctx in runner_ctxs:
        app_instance.orchestrator.register_runner_heartbeats(
            [ctx.runner_id], can_run_atomic_service=True
        )

    cycles = 10
    original_interval = app_instance.conf.atomic_service_interval_minutes
    original_margin = app_instance.conf.atomic_service_spread_margin_minutes
    original_fraction = app_instance.conf.atomic_service_max_start_slot_fraction
    original_retention = app_instance.conf.atomic_service_execution_retention_minutes
    app_instance.conf.atomic_service_interval_minutes = 60.0
    app_instance.conf.atomic_service_spread_margin_minutes = 0.0
    app_instance.conf.atomic_service_max_start_slot_fraction = 1.0
    app_instance.conf.atomic_service_execution_retention_minutes = 0
    try:
        winners_total = 0
        # Each cycle advances time by one slot (interval / runners = 900s).
        slot_seconds = 60.0 * 60.0 / len(runner_ctxs)
        for cycle in range(cycles):
            t = cycle * slot_seconds
            with patch("pynenc.orchestrator.base_orchestrator.time", return_value=t):
                with ThreadPoolExecutor(max_workers=len(runner_ctxs)) as pool:
                    futures = [
                        pool.submit(
                            app_instance.orchestrator.try_claim_atomic_service_run,
                            ctx,
                        )
                        for ctx in runner_ctxs
                    ]
                    results = [f.result() for f in as_completed(futures)]

            winners = [r for r in results if r is not None]
            assert len(winners) == 1, (
                f"cycle {cycle}: expected one winner, got {len(winners)}"
            )

            active = app_instance.orchestrator.get_active_atomic_service_executions()
            assert len(active) == 1
            assert active[0].atomic_service_run_id == winners[0].atomic_service_run_id

            _finalize(app_instance, winners[0])
            winners_total += 1

        assert winners_total == cycles
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval
        app_instance.conf.atomic_service_spread_margin_minutes = original_margin
        app_instance.conf.atomic_service_max_start_slot_fraction = original_fraction
        app_instance.conf.atomic_service_execution_retention_minutes = (
            original_retention
        )


@pytest.mark.parametrize("burst_threads", [4, 16, 32])
def test_same_runner_repeated_claims_after_finalization_admit_next_slot(
    app_instance: Pynenc, burst_threads: int
) -> None:
    """A finalized RUNNING record releases the consensus block.

    Guards against a deadlock where a stuck RUNNING record permanently
    blocks the assigned runner of the next slot from claiming.
    """
    runner_ctxs = [_make_runner_ctx(f"runner-{i:02d}") for i in range(burst_threads)]
    for ctx in runner_ctxs:
        app_instance.orchestrator.register_runner_heartbeats(
            [ctx.runner_id], can_run_atomic_service=True
        )

    original_interval = app_instance.conf.atomic_service_interval_minutes
    original_margin = app_instance.conf.atomic_service_spread_margin_minutes
    original_fraction = app_instance.conf.atomic_service_max_start_slot_fraction
    original_retention = app_instance.conf.atomic_service_execution_retention_minutes
    # Tight interval; each runner gets one slot per cycle in rotation.
    app_instance.conf.atomic_service_interval_minutes = (
        burst_threads * 1.0  # 1 minute per runner
    )
    app_instance.conf.atomic_service_spread_margin_minutes = 0.0
    app_instance.conf.atomic_service_max_start_slot_fraction = 1.0
    app_instance.conf.atomic_service_execution_retention_minutes = 0
    try:
        admitted = 0
        slot_seconds = 60.0
        for idx, _ctx in enumerate(runner_ctxs):
            t = idx * slot_seconds
            with patch("pynenc.orchestrator.base_orchestrator.time", return_value=t):
                # All runners attempt; only the one whose slot it is wins.
                with ThreadPoolExecutor(max_workers=burst_threads) as pool:
                    futures = [
                        pool.submit(
                            app_instance.orchestrator.try_claim_atomic_service_run,
                            other_ctx,
                        )
                        for other_ctx in runner_ctxs
                    ]
                    results = [f.result() for f in as_completed(futures)]

            winners = [r for r in results if r is not None]
            assert len(winners) == 1, (
                f"slot {idx}: expected one winner, got {len(winners)}: "
                f"{[w.runner_id for w in winners]}"
            )
            _finalize(app_instance, winners[0])
            admitted += 1

        assert admitted == burst_threads
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval
        app_instance.conf.atomic_service_spread_margin_minutes = original_margin
        app_instance.conf.atomic_service_max_start_slot_fraction = original_fraction
        app_instance.conf.atomic_service_execution_retention_minutes = (
            original_retention
        )
