"""Atomic-service start-recording atomicity tests."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from typing import TYPE_CHECKING

from pynenc.orchestrator.atomic_service import AtomicServiceExecutionStatus
from pynenc.orchestrator.atomic_service import AtomicServiceRun

if TYPE_CHECKING:
    from pynenc import Pynenc


def test_record_atomic_service_execution_start_allows_only_one_running_start(
    app_instance: Pynenc,
) -> None:
    """Concurrent start records must leave at most one active RUNNING execution."""
    run_count = 16
    started_at = datetime.now(UTC)
    runs = [
        AtomicServiceRun(
            runner_id=f"atomic-race-runner-{idx:02d}",
            atomic_service_run_id=f"atomic-race-run-{idx:02d}",
            started_at=started_at,
        )
        for idx in range(run_count)
    ]
    for run in runs:
        app_instance.orchestrator.register_runner_heartbeats(
            [run.atomic_service_id.runner_id], can_run_atomic_service=True
        )

    ready = Barrier(run_count)

    def record_start(run: AtomicServiceRun) -> None:
        ready.wait()
        app_instance.orchestrator.record_atomic_service_execution_start(
            run,
            started_at,
        )

    with ThreadPoolExecutor(max_workers=run_count) as pool:
        list(pool.map(record_start, runs))

    active = app_instance.orchestrator.get_active_atomic_service_executions()
    active_run_ids = sorted(execution.atomic_service_run_id for execution in active)

    assert len(active) == 1, (
        "record_atomic_service_execution_start must be atomic: "
        f"expected one active RUNNING execution, found {len(active)} "
        f"({active_run_ids})"
    )

    executions = app_instance.orchestrator.get_atomic_service_executions_in_timerange(
        started_at - timedelta(seconds=1),
        started_at + timedelta(seconds=1),
        limit=run_count,
    )
    blocked = [
        execution
        for execution in executions
        if execution.status == AtomicServiceExecutionStatus.BLOCKED
    ]
    assert len(blocked) == run_count - 1
    assert all("prior_running" in execution.reason for execution in blocked)
