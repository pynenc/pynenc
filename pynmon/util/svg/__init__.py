"""
SVG Timeline Generation Module.

This module provides utilities for generating SVG timeline visualizations
of task invocations in the Pynenc monitoring system.

Key components:
- TimelineData: Data container for timeline visualization
- TimelineDataBuilder: Transforms InvocationHistory into TimelineData
- TimelineSVGRenderer: Renders TimelineData as SVG markup
- TimelineConfig: Configuration options for timeline rendering

Usage:
    from pynmon.util.svg import TimelineDataBuilder, TimelineSVGRenderer

    builder = TimelineDataBuilder()
    for batch in state_backend.iter_history_in_timerange(start, end):
        builder.add_history_batch(batch)
    timeline_data = builder.build()

    renderer = TimelineSVGRenderer()
    svg_string = renderer.render(timeline_data)
"""

from pynmon.util.svg.models import (
    InvocationBar,
    RunnerLane,
    TimelineBounds,
    TimelineConfig,
    TimelineData,
)
from pynmon.util.svg.builder import TimelineDataBuilder
from pynmon.util.svg.renderer import SVGStyle, TimelineSVGRenderer, generate_svg

__all__ = [
    "InvocationBar",
    "RunnerLane",
    "SVGStyle",
    "TimelineBounds",
    "TimelineConfig",
    "TimelineData",
    "TimelineDataBuilder",
    "TimelineSVGRenderer",
    "generate_svg",
]
