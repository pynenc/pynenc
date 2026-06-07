"""Integration test for family-tree load-more on event-triggered trees.

Regression: when the focus invocation produces many children via
``@app.trigger.on_event`` consumers, the initial ``family-tree`` SVG is
truncated and shows ``ft-load-more`` badges. Clicking a badge sends a
follow-up request with ``?expand=<id>`` which must render MORE nodes
than the initial response. This test reproduces the production scenario
that motivated the fix.
"""

from __future__ import annotations

import os
import re
import tempfile
import threading
import time
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

import pytest

from pynenc.builder import PynencBuilder
from pynenc.runner.thread_runner import ThreadRunner
from pynenc.trigger.conditions import EventContext
from pynenc.trigger.trigger_builder import on_event
from pynenc_tests.conftest import (
    check_all_status_transitions,
    check_no_atomic_service_overlap,
)

if TYPE_CHECKING:
    from pynenc_tests.integration.pynmon.conftest import PynmonClient

APP_ID = "test-pynmon-family-tree-load-more-events"

# Tree shape — produces 1 root + FANOUT direct event-children +
# FANOUT * SECOND_FANOUT grandchildren. With max_nodes=60 the initial
# SVG MUST be truncated and contain load-more badges.
# Larger than max_nodes (60) by a wide margin so multiple load-more clicks
# are required to render the full tree.
FANOUT = 80
SECOND_FANOUT = 2
EXPECTED_GRANDLEAVES = FANOUT * SECOND_FANOUT
EXPECTED_TOTAL_NODES = 1 + FANOUT + EXPECTED_GRANDLEAVES

_temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_temp_db_path = _temp_db.name
_temp_db.close()

app = (
    PynencBuilder()
    .sqlite(_temp_db_path)
    .thread_runner()
    .runner_tuning(
        runner_loop_sleep_time_sec=0.01,
        invocation_wait_results_sleep_time_sec=0.01,
    )
    .app_id(APP_ID)
    .build()
)
app.conf.atomic_service_interval_minutes = 0.01
app.conf.atomic_service_check_interval_minutes = 0.005
app.conf.atomic_service_spread_margin_minutes = 0.0
app.conf.atomic_service_max_start_slot_fraction = 1.0

_emit_lock = threading.RLock()


def _emit(event_code: str, payload: dict[str, Any]) -> str:
    with _emit_lock:
        return app.trigger.emit_event(event_code, payload)


@app.task
def fanout_root(count: int) -> str:
    """Emit ``count`` leaf events from inside this invocation."""
    for index in range(count):
        _emit("loadmore.leaf", {"index": index})
    return f"emitted-{count}"


def _leaf_args(context: EventContext) -> dict[str, Any]:
    return {"index": int(context.payload["index"])}


@app.task(triggers=on_event("loadmore.leaf").with_args_from_event(_leaf_args))
def leaf_handler(index: int) -> str:
    """Event-triggered child; emits a single grand-leaf event."""
    for sub in range(SECOND_FANOUT):
        _emit("loadmore.grandleaf", {"index": index, "sub": sub})
    return f"leaf-{index}"


def _grand_args(context: EventContext) -> dict[str, Any]:
    return {
        "index": int(context.payload["index"]),
        "sub": int(context.payload["sub"]),
    }


@app.task(triggers=on_event("loadmore.grandleaf").with_args_from_event(_grand_args))
def grandleaf_handler(index: int, sub: int) -> str:
    return f"grand-{index}-{sub}"


@pytest.fixture(scope="module", autouse=True)
def cleanup_temp_db() -> Generator[None, None, None]:
    yield
    if _temp_db_path and os.path.exists(_temp_db_path):
        try:
            os.unlink(_temp_db_path)
        except OSError:
            pass


def _start_runners(count: int) -> list[ThreadRunner]:
    runners: list[ThreadRunner] = []
    for _ in range(count):
        runner = ThreadRunner(app)
        threading.Thread(target=runner.run, daemon=True).start()
        runners.append(runner)
    return runners


def _wait_for_event_count(
    pynmon_client: PynmonClient,
    event_code: str,
    expected: int,
    timeout: float = 60.0,
) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        count = _event_count_from_pynmon(pynmon_client, event_code)
        if count >= expected:
            return
        time.sleep(0.1)
    count = _event_count_from_pynmon(pynmon_client, event_code)
    raise AssertionError(
        f"Timed out waiting for {expected} {event_code} events (got {count})"
    )


def _event_count_from_pynmon(pynmon_client: PynmonClient, event_code: str) -> int:
    start = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
    end = (datetime.now(UTC) + timedelta(seconds=5)).isoformat()
    query = urlencode(
        {
            "start": start,
            "end": end,
            "event_code": event_code,
            "limit": 1,
        }
    )
    response = pynmon_client.get(f"/events/api/markers?{query}")
    assert response.status_code == 200, response.text[:500]
    return int(response.json()["total"])


def _count_ft_nodes(html: str) -> int:
    return len(re.findall(r'class="ft-node[" ]', html))


def _load_more_ids(html: str) -> list[str]:
    return re.findall(r'class="ft-load-more"[^>]*data-expand-id="([^"]+)"', html)


@pytest.mark.slow
def test_family_tree_load_more_expands_event_triggered_tree(
    pynmon_client: PynmonClient,
) -> None:
    """Clicking load-more on an event-triggered subtree must render more nodes."""
    app.purge()
    app.register_deferred_triggers()
    runners = _start_runners(count=3)
    try:
        root_inv = fanout_root(FANOUT)
        assert root_inv.result == f"emitted-{FANOUT}"
        _wait_for_event_count(pynmon_client, "loadmore.leaf", FANOUT)
        _wait_for_event_count(
            pynmon_client,
            "loadmore.grandleaf",
            EXPECTED_GRANDLEAVES,
        )

        root_id = str(root_inv.invocation_id)

        # --- 1. Initial render: tree must be truncated. ---
        resp = pynmon_client.get(f"/invocations/{root_id}/family-tree?bare=1")
        assert resp.status_code == 200, resp.text[:500]
        initial_html = resp.text
        initial_count = _count_ft_nodes(initial_html)
        load_more = _load_more_ids(initial_html)
        assert load_more, (
            "Expected initial tree to be truncated with a load-more badge "
            f"(initial node count: {initial_count}, html head: {initial_html[:400]!r})"
        )

        # Prefer a non-root expand target (mirrors the user-reported scenario
        # where the click happens on a mid-tree node, not the focus).
        non_root = [eid for eid in load_more if eid != root_id]
        target_id = non_root[0] if non_root else load_more[0]

        # --- 2. Expand once: must render strictly more nodes. ---
        expanded = pynmon_client.get(
            f"/invocations/{root_id}/family-tree?bare=1&expand={target_id}"
        )
        assert expanded.status_code == 200, expanded.text[:500]
        expanded_count = _count_ft_nodes(expanded.text)
        print(
            f"[load-more] expected_total={EXPECTED_TOTAL_NODES} "
            f"initial={initial_count} badges={len(load_more)} "
            f"target={target_id} expanded={expanded_count}"
        )
        assert expanded_count > initial_count, (
            f"Load-more should grow the tree, but got {expanded_count} nodes "
            f"vs initial {initial_count}. target_id={target_id}, "
            f"root_id={root_id}, badges={len(load_more)}"
        )

        # --- 3. Expand a second time on a *different* badge. ---
        next_ids = _load_more_ids(expanded.text)
        next_non_root = [eid for eid in next_ids if eid != root_id and eid != target_id]
        assert next_non_root, (
            f"Expected additional load-more badges after first expansion "
            f"(tree has {EXPECTED_TOTAL_NODES} nodes total, only "
            f"{expanded_count} rendered). next_ids={next_ids}"
        )
        second_target = next_non_root[0]
        expand_param = f"{target_id},{second_target}"
        expanded2 = pynmon_client.get(
            f"/invocations/{root_id}/family-tree?bare=1&expand={expand_param}"
        )
        assert expanded2.status_code == 200
        expanded2_count = _count_ft_nodes(expanded2.text)
        print(f"[load-more] second_target={second_target} expanded2={expanded2_count}")
        assert expanded2_count > expanded_count, (
            f"Second expansion did not grow the tree: "
            f"{expanded2_count} <= {expanded_count}. "
            f"first_target={target_id}, second_target={second_target}"
        )
    finally:
        for runner in runners:
            runner.stop_runner_loop()
        check_all_status_transitions(app)
        check_no_atomic_service_overlap(app)
