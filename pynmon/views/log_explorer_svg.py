"""SVG mini-timeline generation for the Log Explorer.

Builds a compact SVG timeline showing only the runners and invocations
mentioned in the parsed log lines. Uses the existing SVG infrastructure
(TimelineDataBuilder, TimelineSVGRenderer) with a filtered data set.

Key components:
- build_log_svg: async entry point producing an SVG string
- LogSvgParams: typed bundle for build parameters
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from pynmon.util.log_parser import EntityRef
from pynmon.util.svg.builder import TimelineDataBuilder
from pynmon.util.svg.models import TimelineConfig
from pynmon.util.svg.renderer import TimelineSVGRenderer

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.runner.runner_context import RunnerContext
    from pynenc.state_backend.base_state_backend import InvocationHistory

logger = logging.getLogger("pynmon.views.log_explorer_svg")

# Use the same config as the main Timeline tab so lane proportions,
# font sizes, and bar heights are identical. The SVG is rendered at its
# native pixel width (not scaled to 100%) so labels never overflow.
_MINI_CONFIG = TimelineConfig()

_INV_KINDS = frozenset(
    {
        "invocation",
        "parent-invocation",
        "child-invocation",
        "new-invocation",
    }
)


@dataclass(frozen=True)
class LogSvgParams:
    """Parameters for building the log-explorer mini-timeline SVG.

    :param Pynenc app: Application instance for state backend access
    :param list[EntityRef] all_refs: Deduplicated entity refs from all log lines
    :param list[datetime] utc_timestamps: UTC-converted timestamps from log lines
    """

    app: "Pynenc"
    all_refs: list[EntityRef]
    utc_timestamps: list[datetime]


async def build_log_svg(params: LogSvgParams) -> str:
    """Build a compact SVG timeline from log-referenced invocations.

    :param LogSvgParams params: Typed parameters bundle
    :return: SVG markup string, or empty string if no data
    """
    inv_ids = _extract_invocation_ids(params.all_refs)
    if not inv_ids:
        return ""
    start, end = _compute_time_range(params.utc_timestamps)
    return await asyncio.to_thread(_build_sync, params.app, inv_ids, start, end)


def _extract_invocation_ids(refs: list[EntityRef]) -> set[str]:
    """Collect unique invocation IDs from entity refs."""
    return {r.value for r in refs if r.kind in _INV_KINDS}


def _compute_time_range(
    utc_timestamps: list[datetime],
) -> tuple[datetime, datetime]:
    """Determine start/end from timestamps with 10% duration padding.

    :param list[datetime] utc_timestamps: UTC timestamps from logs
    :return: (start, end) datetime pair
    """
    if not utc_timestamps:
        now = datetime.now(UTC)
        return now - timedelta(seconds=5), now
    duration_s = (max(utc_timestamps) - min(utc_timestamps)).total_seconds()
    pad = timedelta(seconds=max(duration_s * 0.1, 1.0))
    return min(utc_timestamps) - pad, max(utc_timestamps) + pad


def _build_sync(
    app: "Pynenc",
    inv_ids: set[str],
    start: datetime,
    end: datetime,
) -> str:
    """Synchronous SVG build — runs in a thread.

    Iterates history, filters to only relevant invocations, and renders SVG.
    """
    builder = TimelineDataBuilder(config=_MINI_CONFIG, collapse_external=True)
    contexts: dict[str, RunnerContext] = {}
    for batch in app.state_backend.iter_history_in_timerange(start, end):
        filtered = [h for h in batch if h.invocation_id in inv_ids]
        if not filtered:
            continue
        _fetch_new_contexts(filtered, app, contexts)
        builder.add_history_batch(filtered, contexts)
    data = builder.build(start_time=start, end_time=end)
    if not data.lanes:
        return ""
    svg = TimelineSVGRenderer().render(data)
    # Render at native pixel width (like the main timeline) so it scrolls
    # horizontally instead of scaling down and making labels unreadable.
    return svg.replace('width="100%"', f'width="{_MINI_CONFIG.width}"', 1)


def _fetch_new_contexts(
    batch: list["InvocationHistory"],
    app: "Pynenc",
    cache: dict[str, "RunnerContext"],
) -> None:
    """Fetch and cache runner contexts not yet in the cache."""
    new_ids = [h.runner_context_id for h in batch if h.runner_context_id not in cache]
    if not new_ids:
        return
    for ctx in app.state_backend.get_runner_contexts(new_ids):
        cache[ctx.runner_id] = ctx
