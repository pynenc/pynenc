"""Integration tests: ``get_active_runners`` ordering contract.

Every backend must return runners ordered by ``(creation_time ASC,
runner_id ASC)``.  The atomic-service slot algorithm relies on all
runners seeing the same order from the same snapshot; any deviation
causes a split-brain assignment.
"""

from __future__ import annotations

from time import sleep
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc import Pynenc


def test_get_active_runners_ordered_by_creation_time(
    app_instance: Pynenc,
) -> None:
    """Runners registered earlier appear first."""
    app_instance.orchestrator.register_runner_heartbeats(["runner-1"])
    sleep(0.02)
    app_instance.orchestrator.register_runner_heartbeats(["runner-2"])
    sleep(0.02)
    app_instance.orchestrator.register_runner_heartbeats(["runner-3"])

    runners = app_instance.orchestrator.get_active_runners()

    assert [r.runner_id for r in runners] == ["runner-1", "runner-2", "runner-3"]


def test_get_active_runners_orders_equal_creation_time_by_runner_id(
    app_instance: Pynenc,
) -> None:
    """When creation times are equal, runners are ordered by runner_id."""
    app_instance.orchestrator.register_runner_heartbeats(
        ["runner-z", "runner-a", "runner-m"]
    )

    runners = app_instance.orchestrator.get_active_runners()

    assert [r.runner_id for r in runners] == ["runner-a", "runner-m", "runner-z"]


def test_heartbeat_refresh_does_not_change_runner_order(
    app_instance: Pynenc,
) -> None:
    """Refreshing a heartbeat must not promote the runner to a later slot."""
    app_instance.orchestrator.register_runner_heartbeats(["runner-1"])
    sleep(0.02)
    app_instance.orchestrator.register_runner_heartbeats(["runner-2"])
    sleep(0.02)
    # runner-1 refreshes its heartbeat — order must be unchanged
    app_instance.orchestrator.register_runner_heartbeats(["runner-1"])

    runners = app_instance.orchestrator.get_active_runners()

    assert len(runners) == 2
    assert runners[0].runner_id == "runner-1"
    assert runners[1].runner_id == "runner-2"


def test_get_active_runners_can_run_atomic_service_filter(
    app_instance: Pynenc,
) -> None:
    """``can_run_atomic_service=True`` returns only runners flagged as eligible."""
    app_instance.orchestrator.register_runner_heartbeats(
        ["runner-eligible"], can_run_atomic_service=True
    )
    app_instance.orchestrator.register_runner_heartbeats(
        ["runner-ineligible"], can_run_atomic_service=False
    )

    all_runners = app_instance.orchestrator.get_active_runners()
    eligible = app_instance.orchestrator.get_active_runners(can_run_atomic_service=True)
    ineligible = app_instance.orchestrator.get_active_runners(
        can_run_atomic_service=False
    )

    all_ids = {r.runner_id for r in all_runners}
    assert "runner-eligible" in all_ids
    assert "runner-ineligible" in all_ids

    assert [r.runner_id for r in eligible] == ["runner-eligible"]
    assert [r.runner_id for r in ineligible] == ["runner-ineligible"]
