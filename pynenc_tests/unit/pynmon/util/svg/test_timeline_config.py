"""
Unit tests for TimelineConfig dataclass.

Tests configuration defaults, custom values, and computed properties.
"""

import pytest

from pynmon.util.svg.models import TimelineConfig


def test_default_values() -> None:
    """Test default configuration values."""
    config = TimelineConfig()
    # Reasonable ranges (these are invariants for visualization usability)
    assert 800 <= config.width <= 4000, (
        "width should be within a reasonable display range"
    )
    assert 16 <= config.lane_height <= 200, "lane_height should allow readable lanes"
    assert 0 <= config.lane_padding <= 40, "lane_padding should be small"
    assert 64 <= config.left_margin <= 1000, (
        "left_margin should reserve space for labels"
    )
    assert 0 <= config.top_margin <= 200, (
        "top_margin should be non-negative and reasonable"
    )
    assert 8 <= config.bar_height <= config.lane_height, (
        "bar_height must fit inside a lane"
    )
    assert 1 <= config.min_bar_width <= 64, (
        "min_bar_width must be positive and not excessive"
    )


def test_custom_values() -> None:
    """Test custom configuration values."""
    config = TimelineConfig(width=800, lane_height=30, left_margin=150)
    assert config.width == 800
    assert config.lane_height == 30
    assert config.left_margin == 150


def test_content_width() -> None:
    """Test content_width property calculation."""
    config = TimelineConfig(width=1000, left_margin=200)
    assert config.content_width == 800


def test_bar_y_offset() -> None:
    """Test bar_y_offset centers the bar in lane."""
    config = TimelineConfig(lane_height=40, bar_height=24)
    assert config.bar_y_offset == 8  # (40 - 24) // 2


def test_frozen_immutability() -> None:
    """Test that config is immutable (frozen dataclass)."""
    config = TimelineConfig()
    with pytest.raises(AttributeError):
        config.width = 999  # type: ignore
