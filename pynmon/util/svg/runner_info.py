"""
Runner information extraction and formatting.

Provides a clean, typed interface for extracting runner context information
for timeline visualization. Uses proper type checking instead of getattr.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.runner.runner_context import RunnerContext

# Truncation length for IDs (7 chars like Git short SHA)
TRUNCATE_LENGTH = 7


def truncate_id(runner_id: str) -> str:
    """Truncate an ID to display length."""
    return (
        runner_id[:TRUNCATE_LENGTH] if len(runner_id) > TRUNCATE_LENGTH else runner_id
    )


@dataclass(frozen=True)
class RunnerInfo:
    """
    Immutable information about a runner extracted from RunnerContext.

    Contains all data needed to display runner in timeline visualization.
    """

    runner_cls: str
    runner_id: str
    hostname: str
    pid: int
    thread_id: int = 0
    parent_runner_cls: str | None = None
    parent_runner_id: str | None = None

    @classmethod
    def from_context(cls, ctx: "RunnerContext | None") -> "RunnerInfo":
        """
        Create RunnerInfo from a RunnerContext with proper type access.

        :param ctx: RunnerContext to extract info from
        :return: RunnerInfo with context details
        """
        if ctx is None:
            return cls.unknown()

        parent_cls, parent_id = cls._extract_parent_info(ctx.parent_ctx)

        return cls(
            runner_cls=ctx.runner_cls,
            runner_id=ctx.runner_id,
            hostname=ctx.hostname,
            pid=ctx.pid,
            thread_id=ctx.thread_id,
            parent_runner_cls=parent_cls,
            parent_runner_id=parent_id,
        )

    @classmethod
    def unknown(cls) -> "RunnerInfo":
        """Create an unknown/default RunnerInfo."""
        return cls(
            runner_cls="Unknown",
            runner_id="unknown",
            hostname="unknown",
            pid=0,
            thread_id=0,
        )

    @staticmethod
    def _extract_parent_info(
        parent_ctx: "RunnerContext | None",
    ) -> tuple[str | None, str | None]:
        """Extract parent runner class and ID from parent context."""
        if parent_ctx is None:
            return None, None
        return parent_ctx.runner_cls, parent_ctx.runner_id

    @property
    def lane_id(self) -> str:
        """Lane identifier (each runner gets its own lane)."""
        return self.runner_id

    @property
    def group_id(self) -> str:
        """Group identifier (parent's runner_id or self if no parent)."""
        return self.parent_runner_id or self.runner_id

    @property
    def display_runner_id(self) -> str:
        """Truncated runner_id for display."""
        return truncate_id(self.runner_id)

    @property
    def has_parent(self) -> bool:
        """True if this runner has a parent context."""
        return self.parent_runner_id is not None

    @property
    def label(self) -> str:
        """Display label: RunnerClass(truncated_id)."""
        return f"{self.runner_cls}({self.display_runner_id})"

    @property
    def details(self) -> str:
        """Detailed runner info: hostname (pid:X)."""
        return f"{self.hostname} (pid:{self.pid})"

    @property
    def full_details(self) -> str:
        """Full details including thread: hostname (pid:X thread:Y)."""
        return f"{self.hostname} (pid:{self.pid} thread:{self.thread_id})"
