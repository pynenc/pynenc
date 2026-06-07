"""Resolution helpers for the Log Explorer.

Async helpers that resolve parsed log components (runners, invocations,
tasks) against the state backend and build timeline query strings.

Key components:
- resolve_single_line: Resolves all components for one parsed log line
- resolve_runners: Matches partial runner IDs to full contexts
- build_shared_timeline_qs: Creates time-scoped timeline URLs
"""

import asyncio
import logging
from collections.abc import Callable
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, cast
from urllib.parse import urlencode

from pynenc.exceptions import InvocationNotFoundError
from pynenc.identifiers.invocation_id import InvocationId
from pynmon.util.formatting import RunnerContextInfo
from pynmon.util.log_parser import (
    EntityRef,
    ParsedLogLine,
    ParsedRunner,
    timestamp_to_utc,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.task import Task

    from pynmon.views.log_explorer import LineAnalysis, RunnerMatch

logger = logging.getLogger("pynmon.views.log_explorer_resolve")


# ── single-line resolution ─────────────────────────────────────────────────────


async def resolve_single_line(
    app: "Pynenc",
    parsed: ParsedLogLine,
) -> "LineAnalysis":
    """Resolve components for one parsed log line.

    :param Pynenc app: The Pynenc application instance
    :param ParsedLogLine parsed: Already-parsed log line
    :return: LineAnalysis with resolved runners, invocation, and task
    """
    from pynmon.views.log_explorer import LineAnalysis

    if not parsed.is_valid:
        return LineAnalysis(parsed=parsed)

    runner_matches, (invocation, inv_error) = await asyncio.gather(
        resolve_runners(app, parsed.runners),
        _resolve_invocation(app, parsed.invocation_id),
    )
    task = _resolve_task(app, parsed.task_key)
    rid_map = _build_runner_id_map(runner_matches)

    return LineAnalysis(
        parsed=parsed,
        runner_matches=runner_matches,
        invocation=invocation,
        invocation_error=inv_error,
        task=task,
        timeline_qs=build_timeline_qs(parsed),
        invocation_timeline_qs=build_timeline_qs(parsed, selected=parsed.invocation_id),
        extra_entity_refs=filter_extra_refs(parsed),
        runner_id_map=rid_map,
    )


# ── runner resolution ──────────────────────────────────────────────────────────


async def resolve_runners(
    app: "Pynenc",
    runners: list[ParsedRunner],
) -> list["RunnerMatch"]:
    """Resolve each runner entry against the state backend.

    :param Pynenc app: The Pynenc application instance
    :param list[ParsedRunner] runners: Parsed runner entries from the log
    :return: RunnerMatch list with matched context dicts populated
    """
    from pynmon.views.log_explorer import RunnerMatch

    matches = []
    for runner in runners:
        raw = await asyncio.to_thread(
            cast(
                Callable[[], list],
                lambda pid=runner.partial_id: list(
                    app.state_backend.get_matching_runner_contexts(pid)
                ),
            )
        )
        infos = [RunnerContextInfo.from_context(c).to_dict() for c in raw]
        matches.append(RunnerMatch(parsed=runner, contexts=infos))
    return matches


def _build_runner_id_map(matches: list["RunnerMatch"]) -> dict[str, str]:
    """Map partial runner IDs to full IDs from resolved contexts.

    :param list matches: Resolved runner matches
    :return: Dict of partial_id → full_runner_id
    """
    return {rm.parsed.partial_id: full_runner_id(rm) for rm in matches}


def full_runner_id(rm: "RunnerMatch") -> str:
    """Return the full runner_id from resolved contexts, or partial as fallback."""
    if rm.contexts:
        return rm.contexts[0].get("runner_id") or rm.parsed.partial_id
    return rm.parsed.partial_id


# ── invocation / task resolution ───────────────────────────────────────────────


async def _resolve_invocation(
    app: "Pynenc",
    inv_id: str | None,
) -> tuple["DistributedInvocation | None", str | None]:
    """Fetch an invocation from the state backend by ID.

    :param Pynenc app: The Pynenc application instance
    :param str | None inv_id: UUID string of the invocation to fetch
    :return: (invocation, error_message) tuple — one will always be None
    """
    if not inv_id:
        return None, None
    try:
        inv = await asyncio.to_thread(
            app.state_backend.get_invocation, InvocationId(inv_id)
        )
        return inv, None
    except InvocationNotFoundError:
        return None, f"Invocation {inv_id} not found in state backend."
    except Exception as exc:
        logger.warning("Error fetching invocation %s: %s", inv_id, exc)
        return None, str(exc)


def _resolve_task(app: "Pynenc", task_key: str | None) -> "Task | None":
    """Look up a registered task by its module.func key.

    :param Pynenc app: The Pynenc application instance
    :param str | None task_key: The dot-separated task key to look up
    :return: Matching Task or None when not registered
    """
    if not task_key:
        return None
    for task_id, task in app.tasks.items():
        if str(task_id) == task_key:
            return task
    return None


# ── timeline query string builders ─────────────────────────────────────────────


def build_timeline_qs(
    parsed: ParsedLogLine,
    selected: str | None = None,
) -> str:
    """Build a timeline query string scoped to the log timestamp.

    Uses a tight window (±250ms) so millisecond-scale logs remain readable.

    :param ParsedLogLine parsed: Parsed log line with optional timestamp
    :param str | None selected: Invocation ID to highlight in the timeline
    :return: URL query string (without leading ``?``)
    """
    ts_utc = timestamp_to_utc(parsed.timestamp)
    if ts_utc:
        return build_timeline_qs_for_range(
            ts_utc - timedelta(milliseconds=250),
            ts_utc + timedelta(milliseconds=250),
            selected=selected,
        )
    else:
        params = {"time_range": "1h"}
    if selected:
        params["selected"] = selected
    return urlencode(params)


def build_shared_timeline_qs(parsed_lines: list[ParsedLogLine]) -> str:
    """Build a timeline query string spanning all log timestamps.

    Uses 10% duration padding (min 250ms) so invocations stay close to pasted logs.

    :param list parsed_lines: All parsed log lines
    :return: URL query string (without leading ``?``)
    """
    utc_timestamps = [ts for p in parsed_lines if (ts := timestamp_to_utc(p.timestamp))]
    if not utc_timestamps:
        return "time_range=1h"
    padding = _compute_padding(utc_timestamps)
    return build_timeline_qs_for_range(
        min(utc_timestamps) - padding,
        max(utc_timestamps) + padding,
    )


def build_timeline_qs_for_range(
    start: datetime,
    end: datetime,
    selected: str | None = None,
) -> str:
    """Build a custom timeline query string for an exact UTC range."""
    params = {
        "time_range": "custom",
        "start_date": _format_timeline_datetime(start),
        "end_date": _format_timeline_datetime(end),
        "resolution": "100ms",
    }
    if selected:
        params["selected"] = selected
    return urlencode(params)


def _format_timeline_datetime(value: datetime) -> str:
    """Format datetimes for timeline query params accepted by datetime-local."""
    return value.replace(tzinfo=None).isoformat(timespec="microseconds")


def _compute_padding(timestamps: list) -> timedelta:
    """Compute 10% duration padding (min 1s) for time range display.

    :param list timestamps: UTC timestamps
    :return: Padding timedelta
    """
    duration = (max(timestamps) - min(timestamps)).total_seconds()
    return timedelta(seconds=max(duration * 0.1, 0.25))


# ── extra invocation enrichment ───────────────────────────────────────────────

_INV_KINDS = frozenset(
    {
        "invocation",
        "parent-invocation",
        "child-invocation",
        "new-invocation",
        # Trigger-related invocation roles emitted by BaseTrigger logs.
        # Treat them as plain invocations so the existing batch enrichment
        # fetches their status/task in one round-trip.
        "source-invocation",
        "triggered-invocation",
    }
)


async def enrich_extra_invocations(
    app: "Pynenc",
    all_refs: list[EntityRef],
    details: dict[str, dict[str, str]],
) -> None:
    """Fetch and add details for invocations in refs not yet in details.

    Extra invocations (from message body entity refs) are not resolved
    during per-line analysis, so we fetch them here in a single batch.

    :param Pynenc app: Application instance
    :param list all_refs: Full deduplicated ref list
    :param dict details: In-place enriched details dict
    """
    missing = [
        ref.value
        for ref in all_refs
        if ref.kind in _INV_KINDS and f"invocation:{ref.value}" not in details
    ]
    if not missing:
        return
    results = await asyncio.gather(
        *[_try_fetch_invocation(app, inv_id) for inv_id in missing]
    )
    for inv_id, inv in zip(missing, results, strict=False):
        if inv:
            details[f"invocation:{inv_id}"] = {
                "status": inv.status.name,
                "task": str(inv.task.task_id) if inv.task else "",
            }


async def _try_fetch_invocation(
    app: "Pynenc",
    inv_id: str,
) -> "DistributedInvocation | None":
    """Safely fetch one invocation, returning None on any error."""
    try:
        return await asyncio.to_thread(
            app.state_backend.get_invocation, InvocationId(inv_id)
        )
    except Exception:
        return None


# ── entity ref collection ──────────────────────────────────────────────────────


def filter_extra_refs(parsed: ParsedLogLine) -> list[EntityRef]:
    """Return entity refs from the message body not already in the bracket.

    :param ParsedLogLine parsed: Parsed log line
    :return: Deduplicated list of extra entity references
    """
    bracket_ids: set[str] = set()
    if parsed.invocation_id:
        bracket_ids.add(parsed.invocation_id.lower())
    if parsed.task_key:
        bracket_ids.add(parsed.task_key.lower())
    for r in parsed.runners:
        bracket_ids.add(r.partial_id.lower())
    return [ref for ref in parsed.entity_refs if ref.value.lower() not in bracket_ids]


def collect_all_entity_refs(lines: list["LineAnalysis"]) -> list[EntityRef]:
    """Deduplicate entity refs across all log lines.

    Uses full runner IDs from resolved contexts instead of truncated partials.

    :param list lines: All per-line analyses
    :return: Deduplicated ordered list of entity refs
    """
    seen: OrderedDict[tuple[str, str], EntityRef] = OrderedDict()
    for la in lines:
        _collect_bracket_refs(la, seen)
        for ref in la.parsed.entity_refs:
            key = (ref.kind, ref.value)
            if key not in seen:
                seen[key] = ref
    return list(seen.values())


def _collect_bracket_refs(
    la: "LineAnalysis",
    seen: OrderedDict[tuple[str, str], EntityRef],
) -> None:
    """Add bracket-extracted entities to the ref dict with full IDs."""
    if la.parsed.invocation_id:
        key = ("invocation", la.parsed.invocation_id)
        if key not in seen:
            seen[key] = EntityRef(kind="invocation", value=la.parsed.invocation_id)
    if la.parsed.task_key:
        key = ("task", la.parsed.task_key)
        if key not in seen:
            seen[key] = EntityRef(kind="task", value=la.parsed.task_key)
    if la.parsed.atomic_service_run_id:
        key = ("atomic-service-run", la.parsed.atomic_service_run_id)
        if key not in seen:
            seen[key] = EntityRef(
                kind="atomic-service-run", value=la.parsed.atomic_service_run_id
            )
    for rm in la.runner_matches:
        fid = full_runner_id(rm)
        key = ("runner", fid)
        if key not in seen:
            seen[key] = EntityRef(kind="runner", value=fid)


# ── trigger entity enrichment ─────────────────────────────────────────────────


async def enrich_trigger_entities(
    app: "Pynenc",
    all_refs: list[EntityRef],
    details: dict[str, dict[str, str]],
) -> None:
    """Fetch summaries for event, trigger-run, condition, and trigger refs.

    Populates ``details`` in place with kind-prefixed keys so templates can
    look up ``details["event:<id>"]`` etc. without per-template lookups.
    Missing records are silently skipped — purged or unknown IDs render as
    plain chips without metadata.
    """
    await asyncio.gather(
        _enrich_events(app, all_refs, details),
        _enrich_trigger_runs(app, all_refs, details),
        _enrich_conditions(app, all_refs, details),
        _enrich_triggers(app, all_refs, details),
    )


async def _enrich_events(
    app: "Pynenc",
    all_refs: list[EntityRef],
    details: dict[str, dict[str, str]],
) -> None:
    """Fetch :class:`EventRecord` summaries for ``event:<id>`` refs."""
    missing = [
        ref.value
        for ref in all_refs
        if ref.kind == "event" and f"event:{ref.value}" not in details
    ]
    if not missing:
        return
    records = await asyncio.to_thread(app.trigger.get_events_batch, missing)
    for eid, rec in records.items():
        if rec is None:
            continue
        details[f"event:{eid}"] = {
            "code": rec.event_code,
            "matched": "1" if rec.matched else "",
            "triggered": "1" if rec.triggered else "",
            "emitted_by_invocation_id": rec.emitted_by_invocation_id or "",
            "emitted_by_task_id": rec.emitted_by_task_id or "",
            "timestamp": rec.timestamp.isoformat() if rec.timestamp else "",
        }


async def _enrich_trigger_runs(
    app: "Pynenc",
    all_refs: list[EntityRef],
    details: dict[str, dict[str, str]],
) -> None:
    """Fetch :class:`TriggerRunRecord` summaries for ``trigger-run:<id>`` refs."""
    missing = [
        ref.value
        for ref in all_refs
        if ref.kind == "trigger-run" and f"trigger-run:{ref.value}" not in details
    ]
    if not missing:
        return
    records = await asyncio.to_thread(app.trigger.get_trigger_runs_batch, missing)
    from pynmon.util.trigger_timeline import timeline_url_for_trigger_run

    for rid, rec in records.items():
        if rec is None:
            continue
        details[f"trigger-run:{rid}"] = {
            "trigger_id": rec.trigger_id,
            "task": rec.task_id_key,
            "logic": rec.logic_value,
            "triggered_invocation_id": rec.triggered_invocation_id or "",
            "executed_at": rec.executed_at.isoformat() if rec.executed_at else "",
            "timeline_url": timeline_url_for_trigger_run(rec),
        }


async def _enrich_conditions(
    app: "Pynenc",
    all_refs: list[EntityRef],
    details: dict[str, dict[str, str]],
) -> None:
    """Fetch :class:`TriggerCondition` summaries for ``condition:<id>`` refs.

    ``valid-condition`` refs are intentionally not enriched here because
    they are per-evaluation records, not durable conditions. The Log
    Explorer renders them as styled chips without a detail page.
    """
    missing = [
        ref.value
        for ref in all_refs
        if ref.kind == "condition" and f"condition:{ref.value}" not in details
    ]
    if not missing:
        return
    records = await asyncio.to_thread(app.trigger.get_conditions_batch, missing)
    for cid, cond in records.items():
        if cond is None:
            continue
        details[f"condition:{cid}"] = {
            "type": type(cond).__name__,
        }


async def _enrich_triggers(
    app: "Pynenc",
    all_refs: list[EntityRef],
    details: dict[str, dict[str, str]],
) -> None:
    """Fetch :class:`TriggerDefinition` summaries for ``trigger:<id>`` refs."""
    missing = [
        ref.value
        for ref in all_refs
        if ref.kind == "trigger" and f"trigger:{ref.value}" not in details
    ]
    if not missing:
        return
    records = await asyncio.to_thread(app.trigger.get_triggers_batch, missing)
    for tid, trig in records.items():
        if trig is None:
            continue
        details[f"trigger:{tid}"] = {
            "task": str(trig.task_id),
            "logic": trig.logic.value,
            "conditions": str(len(trig.condition_ids)),
        }
