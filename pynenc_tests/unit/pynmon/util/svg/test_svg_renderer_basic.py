"""
Tests for TimelineSVGRenderer.
"""

from datetime import timedelta

from pynmon.util.svg.atomic_service import AtomicServiceWindow
from pynmon.util.svg import (
    SVGStyle,
    TimelineData,
    TimelineSVGRenderer,
)
from pynmon.util.svg.elements import create_status_line


def test_renderer_init_default_style() -> None:
    """Test renderer initializes with default style."""
    renderer = TimelineSVGRenderer()

    assert renderer.style is not None
    assert isinstance(renderer.style, SVGStyle)


def test_renderer_init_custom_style() -> None:
    """Test renderer accepts custom style."""
    style = SVGStyle(font_size=16)
    renderer = TimelineSVGRenderer(style=style)

    assert renderer.style.font_size == 16


def test_renderer_produces_svg(sample_timeline_data: TimelineData) -> None:
    """Test render produces valid SVG string."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert isinstance(svg, str)
    assert svg.startswith('<svg xmlns="http://www.w3.org/2000/svg"')
    assert svg.endswith("</svg>")


def test_renderer_includes_dimensions(sample_timeline_data: TimelineData) -> None:
    """Test SVG includes correct dimensions via viewBox (no explicit height attribute).

    The explicit height attribute was removed so that browsers derive the SVG
    height from the viewBox aspect ratio when width="100%", preventing blank
    space at the bottom that grew with the number of invocations.
    """
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    # Width is responsive (100%) and height is absent; dimensions live in viewBox
    assert 'width="100%"' in svg
    assert 'height="' not in svg.split(">")[0]  # no height attr in opening tag
    expected_viewbox = f'viewBox="0 0 {sample_timeline_data.config.width} {sample_timeline_data.total_height}"'
    assert expected_viewbox in svg


def test_renderer_exposes_timeline_zoom_metadata(
    sample_timeline_data: TimelineData,
) -> None:
    """The browser zoom tool needs time bounds and x-axis geometry."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert 'data-start-time="2024-01-01T10:00:00+00:00"' in svg
    assert 'data-end-time="2024-01-01T10:01:00+00:00"' in svg
    assert 'data-left-margin="200"' in svg
    assert 'data-content-width="800"' in svg
    assert 'data-timeline-width="1000"' in svg


def test_renderer_includes_defs(sample_timeline_data: TimelineData) -> None:
    """Test SVG includes defs section with filters."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert "<defs>" in svg
    assert 'id="shadow"' in svg
    assert "</defs>" in svg


def test_renderer_includes_background(sample_timeline_data: TimelineData) -> None:
    """Test SVG includes background rectangle."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert 'x="0" y="0"' in svg
    assert 'fill="#ffffff"' in svg


def test_renderer_includes_lane_labels(sample_timeline_data: TimelineData) -> None:
    """Test SVG includes lane labels."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert "runner-1" in svg
    assert "host1" in svg


def test_renderer_includes_bars(sample_timeline_data: TimelineData) -> None:
    """Test SVG includes invocation bars."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert 'class="invocation-bar"' in svg
    assert 'data-invocation-id="inv-001"' in svg
    assert 'fill="#3498db"' in svg


def test_renderer_exposes_status_transition_metadata(
    sample_timeline_data: TimelineData,
) -> None:
    """Status connectors identify the transition for browser highlighting."""
    start = sample_timeline_data.bounds.start_time + timedelta(seconds=1)
    end = start + timedelta(seconds=1)
    sample_timeline_data.add_global_line(
        create_status_line(
            invocation_id="inv-transition",
            start_time=start,
            end_time=end,
            from_status="REGISTERED",
            to_status="PENDING",
            from_runner_id="runner-1@host1",
            to_runner_id="runner-1@host1",
        )
    )

    svg = TimelineSVGRenderer().render(sample_timeline_data)

    assert 'class="status-transition-line"' in svg
    assert 'data-invocation-id="inv-transition"' in svg
    assert 'data-from-status="REGISTERED"' in svg
    assert 'data-to-status="PENDING"' in svg


def test_renderer_includes_tooltip(sample_timeline_data: TimelineData) -> None:
    """Test SVG bars include tooltips."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert "<title>" in svg
    assert "Test invocation" in svg


def test_renderer_includes_atomic_service_run_metadata(
    sample_timeline_data: TimelineData,
) -> None:
    """Atomic-service windows expose service-run ids for links/highlighting."""
    start = sample_timeline_data.bounds.start_time + timedelta(seconds=5)
    end = start + timedelta(milliseconds=5)
    sample_timeline_data.atomic_service_windows = [
        AtomicServiceWindow(
            runner_id="runner-1@host1",
            start_time=start,
            end_time=end,
            duration_seconds=0.005,
            atomic_service_run_id="as-run-1",
        )
    ]

    svg = TimelineSVGRenderer().render(sample_timeline_data)

    assert 'data-atomic-service-run-id="as-run-1"' in svg
    assert "/runners/atomic-service/runs/as-run-1" in svg
    assert 'width="6.0"' in svg


def test_renderer_includes_time_axis(sample_timeline_data: TimelineData) -> None:
    """Test SVG includes time axis."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert "<line" in svg
    assert "<text" in svg


def test_renderer_empty_timeline(empty_timeline_data: TimelineData) -> None:
    """Test rendering empty timeline doesn't crash."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(empty_timeline_data)

    assert isinstance(svg, str)
    assert '<svg xmlns="http://www.w3.org/2000/svg"' in svg
    assert "</svg>" in svg
