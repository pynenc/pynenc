"""Unit tests for color management module."""

import pytest

from pynmon.util.colors import (
    Color,
    ColorScheme,
    HostColorAssigner,
    WorkerShadeGenerator,
)


# Test Color


def test_color_creation() -> None:
    """Test creating a color."""
    color = Color(r=52, g=152, b=219)
    assert color.r == 52
    assert color.g == 152
    assert color.b == 219


def test_to_hex() -> None:
    """Test hex conversion."""
    color = Color(r=52, g=152, b=219)
    assert color.to_hex() == "#3498db"


def test_to_hex_with_single_digit_values() -> None:
    """Test hex conversion with values < 16."""
    color = Color(r=5, g=10, b=15)
    assert color.to_hex() == "#050a0f"


def test_darken() -> None:
    """Test darkening a color."""
    color = Color(r=100, g=150, b=200)
    dark = color.darken(0.5)
    assert dark.r == 50
    assert dark.g == 75
    assert dark.b == 100


def test_lighten() -> None:
    """Test lightening a color."""
    color = Color(r=100, g=100, b=100)
    light = color.lighten(1.5)
    assert light.r == 150
    assert light.g == 150
    assert light.b == 150


def test_lighten_caps_at_255() -> None:
    """Test that lightening doesn't exceed 255."""
    color = Color(r=200, g=200, b=200)
    light = color.lighten(2.0)
    assert light.r == 255
    assert light.g == 255
    assert light.b == 255


# Test HostColorAssigner


@pytest.fixture
def host_assigner() -> HostColorAssigner:
    """Create a HostColorAssigner instance."""
    return HostColorAssigner()


def test_single_host_gets_first_color(host_assigner: HostColorAssigner) -> None:
    """Test that single host gets assigned first hue."""
    color = host_assigner.get_color_for_host("host1", total_hosts=1)
    assert isinstance(color, Color)
    # Should be at hue 0 (red-ish)
    assert color.r > color.g
    assert color.r > color.b


def test_two_hosts_get_contrasting_colors(host_assigner: HostColorAssigner) -> None:
    """Test that two hosts get contrasting colors (180 degrees apart)."""
    color1 = host_assigner.get_color_for_host("host1", total_hosts=2)
    color2 = host_assigner.get_color_for_host("host2", total_hosts=2)

    # Should be different colors
    assert color1 != color2


def test_consistent_color_assignment(host_assigner: HostColorAssigner) -> None:
    """Test that same host always gets same color."""
    color1 = host_assigner.get_color_for_host("host1", total_hosts=5)
    color2 = host_assigner.get_color_for_host("host1", total_hosts=5)
    assert color1 == color2


def test_multiple_hosts_get_distributed_hues(host_assigner: HostColorAssigner) -> None:
    """Test that multiple hosts get evenly distributed hues."""
    hosts = ["host1", "host2", "host3", "host4"]
    colors = [host_assigner.get_color_for_host(h, len(hosts)) for h in hosts]

    # All colors should be unique
    assert len(set(colors)) == len(colors)


def test_hsl_to_rgb_conversion() -> None:
    """Test HSL to RGB conversion."""
    # Red (hue=0)
    red = HostColorAssigner._hsl_to_rgb(0, 1.0, 0.5)
    assert red.r > 200
    assert red.g < 50
    assert red.b < 50

    # Green (hue=120)
    green = HostColorAssigner._hsl_to_rgb(120, 1.0, 0.5)
    assert green.r < 50
    assert green.g > 200
    assert green.b < 50

    # Blue (hue=240)
    blue = HostColorAssigner._hsl_to_rgb(240, 1.0, 0.5)
    assert blue.r < 50
    assert blue.g < 50
    assert blue.b > 200


# Test WorkerShadeGenerator


@pytest.fixture
def shade_generator() -> WorkerShadeGenerator:
    """Create a WorkerShadeGenerator instance."""
    return WorkerShadeGenerator()


def test_single_worker_gets_base_color(shade_generator: WorkerShadeGenerator) -> None:
    """Test that single worker gets unmodified base color."""
    base = Color(r=100, g=100, b=100)
    shade = shade_generator.get_shade_for_worker(base, worker_index=0, worker_count=1)
    assert shade == base


def test_even_worker_gets_lighter_shade(shade_generator: WorkerShadeGenerator) -> None:
    """Test that even-indexed workers get lighter shade."""
    base = Color(r=100, g=100, b=100)
    shade = shade_generator.get_shade_for_worker(base, worker_index=0, worker_count=2)
    assert shade.r > base.r
    assert shade.g > base.g
    assert shade.b > base.b


def test_odd_worker_gets_darker_shade(shade_generator: WorkerShadeGenerator) -> None:
    """Test that odd-indexed workers get darker shade."""
    base = Color(r=100, g=100, b=100)
    shade = shade_generator.get_shade_for_worker(base, worker_index=1, worker_count=2)
    assert shade.r < base.r
    assert shade.g < base.g
    assert shade.b < base.b


def test_alternating_pattern(shade_generator: WorkerShadeGenerator) -> None:
    """Test that workers alternate between light and dark."""
    base = Color(r=100, g=100, b=100)

    shade0 = shade_generator.get_shade_for_worker(base, worker_index=0, worker_count=4)
    shade1 = shade_generator.get_shade_for_worker(base, worker_index=1, worker_count=4)
    shade2 = shade_generator.get_shade_for_worker(base, worker_index=2, worker_count=4)
    shade3 = shade_generator.get_shade_for_worker(base, worker_index=3, worker_count=4)

    # Even indices should be lighter
    assert shade0.r > base.r
    assert shade2.r > base.r

    # Odd indices should be darker
    assert shade1.r < base.r
    assert shade3.r < base.r


# Test ColorScheme


@pytest.fixture
def color_scheme() -> ColorScheme:
    """Create a ColorScheme instance."""
    return ColorScheme()


def test_register_and_get_color(color_scheme: ColorScheme) -> None:
    """Test registering runners and getting colors."""
    color_scheme.register_runner("host1", "runner1")

    color = color_scheme.get_runner_color("host1", "runner1")
    assert color.startswith("#")
    assert len(color) == 7


def test_same_runner_gets_cached_color(color_scheme: ColorScheme) -> None:
    """Test that requesting same runner returns cached color."""
    color1 = color_scheme.get_runner_color("host1", "runner1")
    color2 = color_scheme.get_runner_color("host1", "runner1")
    assert color1 == color2


def test_multiple_runners_same_host_get_different_shades(
    color_scheme: ColorScheme,
) -> None:
    """Test that multiple runners on same host get different shades."""
    color1 = color_scheme.get_runner_color("host1", "runner1")
    color2 = color_scheme.get_runner_color("host1", "runner2")

    # Should be different colors
    assert color1 != color2


def test_runners_different_hosts_get_different_hues(color_scheme: ColorScheme) -> None:
    """Test that runners on different hosts get different hues."""
    color1 = color_scheme.get_runner_color("host1", "runner1")
    color2 = color_scheme.get_runner_color("host2", "runner1")

    # Should be different colors
    assert color1 != color2


def test_get_all_registered_hosts(color_scheme: ColorScheme) -> None:
    """Test getting all registered hosts."""
    color_scheme.register_runner("host1", "runner1")
    color_scheme.register_runner("host2", "runner2")
    color_scheme.register_runner("host1", "runner3")

    hosts = color_scheme.get_all_registered_hosts()
    assert set(hosts) == {"host1", "host2"}


def test_get_workers_for_host(color_scheme: ColorScheme) -> None:
    """Test getting workers for a specific host."""
    color_scheme.register_runner("host1", "runner1")
    color_scheme.register_runner("host1", "runner2")
    color_scheme.register_runner("host2", "runner3")

    workers = color_scheme.get_workers_for_host("host1")
    assert set(workers) == {"runner1", "runner2"}


def test_automatic_registration_on_get_color(color_scheme: ColorScheme) -> None:
    """Test that get_runner_color automatically registers runner."""
    # Just calling get_runner_color should register the runner
    color_scheme.get_runner_color("host1", "runner1")

    hosts = color_scheme.get_all_registered_hosts()
    assert "host1" in hosts

    workers = color_scheme.get_workers_for_host("host1")
    assert "runner1" in workers


def test_scalability_with_many_hosts(color_scheme: ColorScheme) -> None:
    """Test that scheme works with many hosts."""
    num_hosts = 50

    colors = []
    for i in range(num_hosts):
        color = color_scheme.get_runner_color(f"host{i}", f"runner{i}")
        colors.append(color)

    # All should be valid hex colors
    for color in colors:
        assert color.startswith("#")
        assert len(color) == 7

    # Should have good distribution (most should be unique)
    unique_colors = set(colors)
    assert len(unique_colors) >= num_hosts * 0.8  # At least 80% unique


def test_scalability_with_many_workers_per_host(color_scheme: ColorScheme) -> None:
    """Test that scheme works with many workers per host."""
    num_workers = 20

    colors = []
    for i in range(num_workers):
        color = color_scheme.get_runner_color("host1", f"worker{i}")
        colors.append(color)

    # All should be valid hex colors
    for color in colors:
        assert color.startswith("#")
        assert len(color) == 7

    # Should have alternating light/dark pattern
    # With alternating pattern, we get good visual distinction
    # At least half should be unique (alternating creates ~10 distinct shades for 20 workers)
    unique_colors = set(colors)
    assert len(unique_colors) >= num_workers // 2
