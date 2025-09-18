"""
Runner context for tracking execution environment details.

This module provides the RunnerContext class, which captures essential information
about the runner environment when executing invocations. It includes fixed attributes
like runner class, ID, process ID, and hostname, along with extensible extra data
for subrunners.

The class supports JSON serialization/deserialization for persistence and communication.
"""

import json
import os
import socket
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.runner.base_runner import BaseRunner


@dataclass
class RunnerContext:
    """
    Context information for a runner execution environment.

    Captures key details about the runner instance and execution context,
    with support for extensible custom data.

    :param str runner_cls: The class name of the runner.
    :param str runner_id: Unique identifier for the runner instance.
    :param int pid: Process ID of the running process.
    :param str hostname: Hostname of the machine running the process.
    :param dict[str, str | int | float | bool] extra_data: Extensible dictionary for additional context data.
    """

    runner_cls: str
    runner_id: str
    pid: int
    hostname: str
    extra_data: dict[str, str | int | float | bool]

    @classmethod
    def from_runner(
        cls,
        runner: "BaseRunner",
        extra_data: dict[str, str | int | float | bool] | None = None,
    ) -> "RunnerContext":
        """
        Create RunnerContext from a BaseRunner instance.

        Extracts relevant information from the runner and calculates
        system-level details like PID and hostname.

        :param BaseRunner runner: The runner instance to extract context from.
        :param dict[str, str | int | float | bool] | None extra_data: Optional extensible data dictionary.
        :return: A new RunnerContext instance populated with runner data.
        :rtype: RunnerContext
        """
        return cls(
            runner_cls=runner.__class__.__name__,
            runner_id=runner.runner_id,
            pid=os.getpid(),
            hostname=socket.gethostname(),
            extra_data=extra_data or {},
        )

    def to_json(self) -> str:
        """
        Serialize the RunnerContext to a JSON string.

        :return: JSON representation of the context.
        :rtype: str
        """
        data = {
            "runner_cls": self.runner_cls,
            "runner_id": self.runner_id,
            "pid": self.pid,
            "hostname": self.hostname,
            "extra_data": self.extra_data,
        }
        return json.dumps(data, sort_keys=True)

    @classmethod
    def from_json(cls, json_str: str) -> "RunnerContext":
        """
        Deserialize a RunnerContext from a JSON string.

        :param str json_str: JSON string containing serialized context data.
        :return: A new RunnerContext instance.
        :rtype: RunnerContext
        :raises ValueError: If the JSON data is invalid or missing required fields.
        """
        try:
            data = json.loads(json_str)
            return cls(
                runner_cls=data["runner_cls"],
                runner_id=data["runner_id"],
                pid=data["pid"],
                hostname=data["hostname"],
                extra_data=data.get("extra_data", {}),
            )
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Invalid JSON data for RunnerContext: {e}") from e
