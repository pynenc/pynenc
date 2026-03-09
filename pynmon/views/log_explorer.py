"""Log Explorer view: paste one or more pynenc log lines to get a contextual summary.

Parses pynenc log lines to extract runner context chains, invocation IDs,
task keys, timestamps, and structured entity references from the message body.
Resolves each component from the state backend and renders a summary with
links to timeline (time-scoped), invocation detail, runner detail, and task pages.

Key components:
- log_explorer: Route handler with multi-line textarea
- LineAnalysis / MultiLogAnalysis: Typed analysis results
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from pynmon.app import get_pynenc_instance, templates
from pynmon.util.log_parser import (
    EntityRef,
    ParsedLogLine,
    ParsedRunner,
    parse_log_lines,
    timestamp_to_utc,
)
from pynmon.views.log_explorer_resolve import (
    build_shared_timeline_qs,
    collect_all_entity_refs,
    enrich_extra_invocations,
    full_runner_id,
    resolve_single_line,
)
from pynmon.views.log_explorer_svg import LogSvgParams, build_log_svg

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.task import Task

router = APIRouter(prefix="/log-explorer", tags=["log-explorer"])
logger = logging.getLogger("pynmon.views.log_explorer")


@dataclass
class RunnerMatch:
    """One runner entry from the log with its resolved contexts.

    :param ParsedRunner parsed: The parsed runner abbreviation and partial ID
    :param list contexts: Matched runner context info dicts from the state backend
    """

    parsed: ParsedRunner
    contexts: list[dict[str, str | None]] = field(default_factory=list)


@dataclass
class LineAnalysis:
    """Resolved information for a single parsed log line.

    :param ParsedLogLine parsed: The raw parse result (includes .raw text)
    :param list runner_matches: Resolved runner contexts per log entry
    :param DistributedInvocation | None invocation: Fetched invocation or None
    :param str | None invocation_error: Error message when invocation lookup fails
    :param Task | None task: Registered task or None
    :param str timeline_qs: Pre-built query string for time-scoped timeline links
    :param str invocation_timeline_qs: Timeline link with selected param for zoom
    :param list extra_entity_refs: Entity refs from message body (not in bracket)
    :param dict runner_id_map: Mapping of partial runner ID → full runner ID
    """

    parsed: ParsedLogLine
    runner_matches: list[RunnerMatch] = field(default_factory=list)
    invocation: "DistributedInvocation | None" = None
    invocation_error: str | None = None
    task: "Task | None" = None
    timeline_qs: str = "time_range=1h"
    invocation_timeline_qs: str = "time_range=1h"
    extra_entity_refs: list[EntityRef] = field(default_factory=list)
    runner_id_map: dict[str, str] = field(default_factory=dict)


@dataclass
class MultiLogAnalysis:
    """Full analysis of a multi-line log block.

    :param list lines: Per-line analysis results
    :param str timeline_qs: Shared timeline query string spanning all timestamps
    :param list all_entity_refs: Deduplicated entity refs collected across all lines
    :param bool has_valid: True if at least one line parsed successfully
    :param str svg_content: Rendered SVG mini-timeline for referenced invocations
    :param dict ref_details: Extra resolved info per entity ref for the panel
    """

    lines: list[LineAnalysis] = field(default_factory=list)
    timeline_qs: str = "time_range=1h"
    all_entity_refs: list[EntityRef] = field(default_factory=list)
    has_valid: bool = False
    svg_content: str = ""
    ref_details: dict[str, dict[str, str]] = field(default_factory=dict)


@router.get("/", response_class=HTMLResponse)
async def log_explorer(
    request: Request,
    log: str = "",
) -> HTMLResponse:
    """Display the log explorer with an optional parsed analysis.

    :param Request request: The incoming HTTP request
    :param str log: Raw pynenc log text (single or multi-line) to analyse
    :return: Rendered log explorer page
    """
    app = get_pynenc_instance()
    analysis = await _analyse_logs(app, log) if log.strip() else None
    return templates.TemplateResponse(
        "log_explorer/index.html",
        {
            "request": request,
            "title": "Log Explorer",
            "app_id": app.app_id,
            "log": log,
            "analysis": analysis,
        },
    )


async def _analyse_logs(app: "Pynenc", log: str) -> MultiLogAnalysis:
    """Parse the log text block and resolve all components.

    :param Pynenc app: The Pynenc application instance
    :param str log: Raw log text (possibly multi-line)
    :return: MultiLogAnalysis containing per-line results and aggregated entities
    """
    parsed_lines = parse_log_lines(log)
    if not parsed_lines:
        return MultiLogAnalysis()

    lines = list(
        await asyncio.gather(*[resolve_single_line(app, p) for p in parsed_lines])
    )
    timeline_qs = build_shared_timeline_qs(parsed_lines)
    all_refs = collect_all_entity_refs(lines)
    has_valid = any(la.parsed.is_valid for la in lines)

    utc_ts = [ts for p in parsed_lines if (ts := timestamp_to_utc(p.timestamp))]
    svg_content = await build_log_svg(LogSvgParams(app, all_refs, utc_ts))
    ref_details = _build_ref_details(lines)
    await enrich_extra_invocations(app, all_refs, ref_details)

    return MultiLogAnalysis(
        lines=lines,
        timeline_qs=timeline_qs,
        all_entity_refs=all_refs,
        has_valid=has_valid,
        svg_content=svg_content,
        ref_details=ref_details,
    )


def _build_ref_details(lines: list[LineAnalysis]) -> dict[str, dict[str, str]]:
    """Collect extra display info for the references panel.

    Builds a dict keyed by ``kind:value`` with status, task, runner class, etc.

    :param list lines: Resolved line analyses
    :return: Mapping of ref key → detail dict
    """
    details: dict[str, dict[str, str]] = {}
    for la in lines:
        _collect_invocation_detail(la, details)
        _collect_runner_details(la, details)
    return details


def _collect_invocation_detail(
    la: LineAnalysis,
    details: dict[str, dict[str, str]],
) -> None:
    """Add invocation status and task info to details dict."""
    if not la.invocation:
        return
    inv_id = la.parsed.invocation_id or ""
    key = f"invocation:{inv_id}"
    if key in details:
        return
    details[key] = {
        "status": la.invocation.status.name,
        "task": str(la.invocation.task.task_id) if la.invocation.task else "",
    }


def _collect_runner_details(
    la: LineAnalysis,
    details: dict[str, dict[str, str]],
) -> None:
    """Add runner class and hostname info to details dict."""
    for rm in la.runner_matches:
        if not rm.contexts:
            continue
        ctx = rm.contexts[0]
        fid = full_runner_id(rm)
        key = f"runner:{fid}"
        if key in details:
            continue
        details[key] = {
            "cls": ctx.get("runner_cls") or "",
            "hostname": ctx.get("hostname") or "",
            "summary": ctx.get("summary") or "",
        }


# Import rendering module to trigger Jinja2 global registration
import pynmon.views.log_explorer_render as _render  # noqa: E402, F401
