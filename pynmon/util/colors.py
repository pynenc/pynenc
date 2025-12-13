"""
Algorithmic color distribution for timeline visualization.

This module provides intelligent color assignment for hosts and workers in the
timeline SVG visualization. Colors are distributed to maximize contrast between
neighboring runners while maintaining consistency within a session.

Key features:
- Automatic host color distribution with maximum contrast
- Worker shade variation within host color families
- Deterministic color assignment for consistency
- Scales from 1 to 1000+ hosts/workers

Key components:
- ColorScheme: Manages the overall color distribution strategy
- HostColorAssigner: Assigns colors to hosts with maximum spacing
- WorkerShadeGenerator: Generates contrasting shades for workers within a host
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Color:
    """
    RGB color representation with hex conversion.

    :param int r: Red component (0-255)
    :param int g: Green component (0-255)
    :param int b: Blue component (0-255)
    """

    r: int
    g: int
    b: int

    def to_hex(self) -> str:
        """Convert to hex color string for SVG."""
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"

    def darken(self, factor: float = 0.6) -> "Color":
        """Create a darker shade of this color."""
        return Color(
            r=int(self.r * factor), g=int(self.g * factor), b=int(self.b * factor)
        )

    def lighten(self, factor: float = 1.4) -> "Color":
        """Create a lighter shade of this color."""
        return Color(
            r=min(255, int(self.r * factor)),
            g=min(255, int(self.g * factor)),
            b=min(255, int(self.b * factor)),
        )


class HostColorAssigner:
    """
    Assigns colors to hosts using HSL color space for maximum contrast.

    Uses hue rotation to distribute colors evenly around the color wheel,
    ensuring maximum visual distinction between hosts.
    """

    def __init__(self, saturation: float = 0.7, lightness: float = 0.5):
        """
        Initialize host color assigner.

        :param float saturation: HSL saturation (0.0-1.0)
        :param float lightness: HSL lightness (0.0-1.0)
        """
        self.saturation = saturation
        self.lightness = lightness
        self._host_index_map: dict[str, int] = {}
        self._next_index = 0

    def get_host_index(self, hostname: str) -> int:
        """Get or assign an index for a hostname."""
        if hostname not in self._host_index_map:
            self._host_index_map[hostname] = self._next_index
            self._next_index += 1
        return self._host_index_map[hostname]

    def get_color_for_host(self, hostname: str, total_hosts: int) -> Color:
        """
        Get color for a specific host.

        Uses golden ratio spacing to maximize contrast between adjacent hosts,
        avoiding similar colors being placed next to each other.

        :param str hostname: The hostname to color
        :param int total_hosts: Total number of unique hosts
        :return: RGB color for this host
        """
        host_index = self.get_host_index(hostname)
        # Use golden ratio (φ ≈ 0.618) spacing for maximum visual contrast
        # This distributes colors evenly around the hue wheel while ensuring
        # adjacent indices get very different hues
        golden_ratio = 0.618033988749895
        hue = ((host_index * golden_ratio) % 1.0) * 360
        return self._hsl_to_rgb(hue, self.saturation, self.lightness)

    @staticmethod
    def _hsl_to_rgb(h: float, s: float, lightness: float) -> Color:
        """Convert HSL to RGB color."""
        c = (1 - abs(2 * lightness - 1)) * s
        x = c * (1 - abs((h / 60) % 2 - 1))
        m = lightness - c / 2

        r_prime: float
        g_prime: float
        b_prime: float

        if h < 60:
            r_prime, g_prime, b_prime = c, x, 0.0
        elif h < 120:
            r_prime, g_prime, b_prime = x, c, 0.0
        elif h < 180:
            r_prime, g_prime, b_prime = 0.0, c, x
        elif h < 240:
            r_prime, g_prime, b_prime = 0.0, x, c
        elif h < 300:
            r_prime, g_prime, b_prime = x, 0.0, c
        else:
            r_prime, g_prime, b_prime = c, 0.0, x

        return Color(
            r=int((r_prime + m) * 255),
            g=int((g_prime + m) * 255),
            b=int((b_prime + m) * 255),
        )


class WorkerShadeGenerator:
    """
    Generates alternating light/dark shades for workers within a host.

    Uses alternating pattern with increasing variation to create visual
    distinction between workers from the same host.
    """

    def get_shade_for_worker(
        self, base_color: Color, worker_index: int, worker_count: int
    ) -> Color:
        """
        Get shade for a specific worker.

        :param Color base_color: Base color from host
        :param int worker_index: Index of this worker within the host
        :param int worker_count: Total workers for this host
        :return: RGB color with appropriate shading
        """
        if worker_count == 1:
            return base_color

        # Alternate between lighter (even index) and darker (odd index)
        # with increasing intensity based on position
        # This creates unique colors while maintaining alternating pattern

        # Calculate intensity based on position (0.0 to 1.0)
        # Spread workers across the intensity range
        intensity_step = 1.0 / max(worker_count - 1, 1)
        intensity = worker_index * intensity_step

        # We'll compute RGB directly and add small index-based offsets
        # to increase uniqueness after integer quantization.
        br, bg, bb = base_color.r, base_color.g, base_color.b

        if worker_index % 2 == 0:
            # Even indices: lighter shades
            factor = 1.15 + (intensity * 0.85)
            # Add per-index offsets that vary across channels to increase uniqueness
            r = min(255, int(br * factor + intensity * 50 + (worker_index % 3) * 7))
            g = min(255, int(bg * factor + intensity * 30 + (worker_index % 5) * 5))
            b = min(255, int(bb * factor + intensity * 20 + (worker_index % 7) * 3))
            return Color(r=r, g=g, b=b)
        else:
            # Odd indices: darker shades
            factor = 0.85 - (intensity * 0.55)
            r = max(0, int(br * factor - intensity * 50 - (worker_index % 3) * 7))
            g = max(0, int(bg * factor - intensity * 30 - (worker_index % 5) * 5))
            b = max(0, int(bb * factor - intensity * 20 - (worker_index % 7) * 3))
            return Color(r=r, g=g, b=b)


class ColorScheme:
    """
    Main color scheme manager for timeline visualization.

    Assigns colors based on runner class at the parent/standalone level,
    with shade variations for child runners within a group.
    """

    def __init__(self) -> None:
        self.host_assigner = HostColorAssigner()
        self.shade_generator = WorkerShadeGenerator()
        self._runner_color_cache: dict[str, str] = {}
        self._runner_cls_groups: dict[
            str, list[str]
        ] = {}  # runner_cls -> list of group_ids

    def register_runner(self, runner_cls: str, group_id: str) -> None:
        """
        Register a runner class and group for color assignment.

        :param str runner_cls: The runner class name (for parent-level color)
        :param str group_id: Group identifier (parent's runner_id)
        """
        if runner_cls not in self._runner_cls_groups:
            self._runner_cls_groups[runner_cls] = []
        if group_id not in self._runner_cls_groups[runner_cls]:
            self._runner_cls_groups[runner_cls].append(group_id)

    def get_parent_color(self, runner_cls: str, group_id: str) -> str:
        """
        Get hex color for a parent/standalone runner.

        Color is based on runner_cls for maximum distinction between
        different runner types (ExternalRunner, ThreadRunner, etc.).

        :param str runner_cls: The runner class name
        :param str group_id: Group identifier
        :return: Hex color string
        """
        # Register and check cache
        self.register_runner(runner_cls, group_id)

        cache_key = f"parent:{runner_cls}:{group_id}"
        if cache_key in self._runner_color_cache:
            return self._runner_color_cache[cache_key]

        # Get base color for runner class
        total_classes = len(self._runner_cls_groups)
        base_color = self.host_assigner.get_color_for_host(
            runner_cls, max(total_classes, 3)
        )

        # Get shade based on group index within this runner class
        groups_in_class = self._runner_cls_groups[runner_cls]
        group_index = groups_in_class.index(group_id)
        group_count = len(groups_in_class)

        final_color = self.shade_generator.get_shade_for_worker(
            base_color, group_index, group_count
        )

        hex_color = final_color.to_hex()
        self._runner_color_cache[cache_key] = hex_color
        return hex_color

    def get_child_color(
        self, parent_color: str, child_index: int, total_children: int
    ) -> str:
        """
        Get hex color for a child runner within a group.

        Creates alternating light/dark shades based on the parent color.

        :param str parent_color: Hex color of the parent
        :param int child_index: Index of this child (0-based)
        :param int total_children: Total number of children in the group
        :return: Hex color string
        """
        cache_key = f"child:{parent_color}:{child_index}:{total_children}"
        if cache_key in self._runner_color_cache:
            return self._runner_color_cache[cache_key]

        # Parse parent color
        r = int(parent_color[1:3], 16)
        g = int(parent_color[3:5], 16)
        b = int(parent_color[5:7], 16)
        base_color = Color(r=r, g=g, b=b)

        # Alternate between lighter and darker shades
        if child_index % 2 == 0:
            # Even indices: slightly lighter
            factor = 1.1 + (child_index * 0.05)
            final_color = Color(
                r=min(255, int(base_color.r * factor)),
                g=min(255, int(base_color.g * factor)),
                b=min(255, int(base_color.b * factor)),
            )
        else:
            # Odd indices: slightly darker
            factor = 0.9 - (child_index * 0.05)
            final_color = Color(
                r=max(0, int(base_color.r * factor)),
                g=max(0, int(base_color.g * factor)),
                b=max(0, int(base_color.b * factor)),
            )

        hex_color = final_color.to_hex()
        self._runner_color_cache[cache_key] = hex_color
        return hex_color

    def get_runner_color(self, hostname: str, runner_id: str) -> str:
        """
        Legacy method for backward compatibility.

        :param str hostname: The hostname (used as runner_cls fallback)
        :param str runner_id: Unique identifier for the runner
        :return: Hex color string
        """
        return self.get_parent_color(hostname, runner_id)

    # New helper APIs for tests / callers to inspect registration state
    def get_all_registered_hosts(self) -> list[str]:
        """
        Get all registered runner classes (hosts).

        Returns a list of registered runner class identifiers that have been
        seen via register_runner or indirectly via get_runner_color.

        :returns: list[str] List of registered host identifiers
        """
        return list(self._runner_cls_groups.keys())

    def get_workers_for_host(self, hostname: str) -> list[str]:
        """
        Get all registered worker/group ids for a specific host.

        Returns the list of group ids registered under the provided hostname.
        An empty list is returned if the host is unknown.

        :param str hostname: Hostname / runner class to query
        :returns: list[str] Registered group ids for the host
        """
        return list(self._runner_cls_groups.get(hostname, []))
