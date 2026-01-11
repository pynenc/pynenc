"""
Runner context for tracking execution environment details.

This module provides a simple, composable context class that can be nested
to represent hierarchical execution environments (runner -> worker -> etc).

Key components:
- RunnerContext: Single context class with optional parent reference
- Automatic capture of pid, hostname, and thread_id
- JSON serialization for cross-process communication

The context is designed to be simple and flexible - callers specify
runner_cls and runner_id, and can create child contexts as needed.
"""

import json
import os
import socket
import uuid
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.runner.base_runner import BaseRunner


@dataclass
class RunnerContext:
    """
    Context information for a runner execution environment.

    A simple, composable context that captures execution environment details.
    Can be nested via parent_ctx to represent hierarchical relationships
    (e.g., main runner -> worker process -> thread).

    :param str runner_cls: The class name of the runner.
    :param str runner_id: Identifier for this runner/context level.
    :param RunnerContext | None parent_ctx: Optional parent context for hierarchy.
    :param int pid: Process ID (auto-captured).
    :param str hostname: Hostname (auto-captured).
    :param int thread_id: Thread ID (auto-captured).
    """

    runner_cls: str
    runner_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    parent_ctx: "RunnerContext | None" = None
    pid: int = field(default_factory=os.getpid)
    hostname: str = field(default_factory=socket.gethostname)
    thread_id: int = field(
        default_factory=lambda: threading.current_thread().ident or 0
    )

    @classmethod
    def from_runner(
        cls,
        runner: "BaseRunner",
        parent_ctx: "RunnerContext | None" = None,
    ) -> "RunnerContext":
        """
        Create RunnerContext from a BaseRunner instance.

        :param BaseRunner runner: The runner instance to extract context from.
        :param RunnerContext | None parent_ctx: Optional parent context.
        :return: A new RunnerContext instance populated with runner data.
        """
        return cls(
            runner_cls=runner.__class__.__name__,
            runner_id=runner.runner_id,
            parent_ctx=parent_ctx,
        )

    @property
    def root_runner_id(self) -> str:
        """
        Get the root runner_id by traversing up the parent chain.

        Returns the runner_id of the topmost context in the hierarchy.
        """
        if self.parent_ctx:
            return self.parent_ctx.root_runner_id
        return self.runner_id

    @property
    def root_runner_cls(self) -> str:
        """
        Get the root runner_cls by traversing up the parent chain.

        Returns the runner_cls of the topmost context in the hierarchy.
        """
        if self.parent_ctx:
            return self.parent_ctx.root_runner_cls
        return self.runner_cls

    def new_child_context(
        self,
        runner_cls: str,
        runner_id: str | None = None,
    ) -> "RunnerContext":
        """
        Create a child context with this context as parent.

        :param str runner_cls: Class name for the child context.
        :param str runner_id: Identifier for the child context.
        :return: A new RunnerContext with this context as parent.
        """
        return RunnerContext(
            runner_cls=runner_cls,
            runner_id=runner_id or str(uuid.uuid4()),
            parent_ctx=self,
        )

    def to_json(self) -> str:
        """
        Serialize the RunnerContext to a JSON string.

        Recursively serializes the parent context if present.

        :return: JSON representation of the context.
        """
        data: dict = {
            "runner_cls": self.runner_cls,
            "runner_id": self.runner_id,
            "pid": self.pid,
            "hostname": self.hostname,
            "thread_id": self.thread_id,
        }
        if self.parent_ctx:
            data["parent_ctx"] = json.loads(self.parent_ctx.to_json())
        return json.dumps(data, sort_keys=True)

    @classmethod
    def from_json(cls, json_str: str) -> "RunnerContext":
        """
        Deserialize a RunnerContext from a JSON string.

        :param str json_str: JSON string containing serialized context data.
        :return: A new RunnerContext instance.
        :raises ValueError: If the JSON data is invalid or missing required fields.
        """
        try:
            data = json.loads(json_str)
            parent_ctx = None
            if "parent_ctx" in data and data["parent_ctx"]:
                parent_ctx = cls.from_json(json.dumps(data["parent_ctx"]))
            return cls(
                runner_cls=data["runner_cls"],
                runner_id=data["runner_id"],
                parent_ctx=parent_ctx,
                pid=data.get("pid", os.getpid()),
                hostname=data.get("hostname", socket.gethostname()),
                thread_id=data.get("thread_id", threading.current_thread().ident or 0),
            )
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Invalid JSON data for RunnerContext: {e}") from e

    def __repr__(self) -> str:
        """Return a concise string representation."""
        parent_info = (
            f", parent={self.parent_ctx.runner_cls}" if self.parent_ctx else ""
        )
        return f"RunnerContext({self.runner_cls}, {self.runner_id[:8]}...{parent_info})"
