"""
Backward-compatible re-export of timeline models.

All types have been split into focused single-responsibility modules.
Import directly from the sub-modules for new code; this file re-exports
everything so existing imports continue to work without changes.
"""

from pynmon.util.svg.bounds import TimelineBounds
from pynmon.util.svg.config import TimelineConfig
from pynmon.util.svg.lane_models import LaneGroup, RunnerLane
from pynmon.util.svg.status_elements import (
    InvocationBar,
    StatusLine,
    StatusPoint,
    StatusSegment,
)
from pynmon.util.svg.timeline_data import TimelineData

__all__ = [
    "TimelineConfig",
    "TimelineBounds",
    "InvocationBar",
    "StatusPoint",
    "StatusSegment",
    "StatusLine",
    "LaneGroup",
    "RunnerLane",
    "TimelineData",
]
