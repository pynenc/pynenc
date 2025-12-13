"""
Common formatting utilities for pynmon views.

Provides reusable formatting functions for tasks, runner contexts,
and other display data. Eliminates duplication across view modules.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.runner.runner_context import RunnerContext
    from pynenc.task import Task

# Truncation length for IDs (7 chars like Git short SHA)
TRUNCATE_LENGTH = 7


def truncate_id(value: str, length: int = TRUNCATE_LENGTH) -> str:
    """Truncate a string ID for display."""
    return value[:length] if len(value) > length else value


def format_task_extra_info(task: "Task") -> dict[str, str | list[str]]:
    """
    Create additional task information for template display.

    :param task: The task to extract information from
    :return: Dictionary with module, func_qualname, and retry_for info
    """
    return {
        "module": task.func.__module__,
        "func_qualname": task.func.__qualname__,
        "retry_for": [e.__name__ for e in task.conf.retry_for],
    }


class RunnerContextInfo:
    """
    Formatted runner context information for display.

    Provides a clean interface for extracting and displaying runner
    context data in templates, avoiding getattr throughout the codebase.
    """

    __slots__ = (
        "runner_cls",
        "runner_id",
        "hostname",
        "pid",
        "thread_id",
        "parent_runner_cls",
        "parent_runner_id",
        "parent_hostname",
        "parent_pid",
        "parent_thread_id",
    )

    def __init__(
        self,
        runner_cls: str | None = None,
        runner_id: str | None = None,
        hostname: str | None = None,
        pid: int | None = None,
        thread_id: int | None = None,
        parent_runner_cls: str | None = None,
        parent_runner_id: str | None = None,
        parent_hostname: str | None = None,
        parent_pid: int | None = None,
        parent_thread_id: int | None = None,
    ) -> None:
        self.runner_cls = runner_cls
        self.runner_id = runner_id
        self.hostname = hostname
        self.pid = pid
        self.thread_id = thread_id
        self.parent_runner_cls = parent_runner_cls
        self.parent_runner_id = parent_runner_id
        self.parent_hostname = parent_hostname
        self.parent_pid = parent_pid
        self.parent_thread_id = parent_thread_id

    @classmethod
    def from_context(cls, ctx: "RunnerContext | None") -> "RunnerContextInfo":
        """Create from a RunnerContext using direct attribute access."""
        if ctx is None:
            return cls()

        parent = ctx.parent_ctx
        return cls(
            runner_cls=ctx.runner_cls,
            runner_id=ctx.runner_id,
            hostname=ctx.hostname,
            pid=ctx.pid,
            thread_id=ctx.thread_id,
            parent_runner_cls=parent.runner_cls if parent else None,
            parent_runner_id=parent.runner_id if parent else None,
            parent_hostname=parent.hostname if parent else None,
            parent_pid=parent.pid if parent else None,
            parent_thread_id=parent.thread_id if parent else None,
        )

    @property
    def display_runner_id(self) -> str:
        """Truncated runner ID for display."""
        if not self.runner_id:
            return "unknown"
        return truncate_id(self.runner_id)

    @property
    def display_parent_id(self) -> str:
        """Truncated parent runner ID for display."""
        if not self.parent_runner_id:
            return ""
        return truncate_id(self.parent_runner_id)

    @property
    def summary(self) -> str:
        """Create a summary string for display."""
        if not self.runner_cls:
            return "N/A"

        if self.parent_runner_id:
            return (
                f"{self.parent_runner_cls}({self.display_parent_id})."
                f"{self.runner_cls}[{self.display_runner_id}]"
            )
        return f"{self.runner_cls}({self.display_runner_id})"

    def to_dict(self) -> dict[str, str | None]:
        """Convert to dictionary for template rendering."""
        return {
            "summary": self.summary,
            "runner_cls": self.runner_cls,
            "runner_id": self.runner_id,
            "hostname": self.hostname,
            "pid": str(self.pid) if self.pid else None,
            "thread_id": str(self.thread_id) if self.thread_id else None,
            "parent_runner_cls": self.parent_runner_cls,
            "parent_runner_id": self.parent_runner_id,
            "parent_hostname": self.parent_hostname,
            "parent_pid": str(self.parent_pid) if self.parent_pid else None,
            "parent_thread_id": str(self.parent_thread_id)
            if self.parent_thread_id
            else None,
        }
