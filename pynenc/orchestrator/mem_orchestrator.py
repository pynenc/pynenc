from collections import defaultdict
from typing import Any, Iterator, Optional, TYPE_CHECKING, Generic

from .base_orchestrator import BaseOrchestrator
from ..types import Params, Result

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..task import Task
    from ..invocation import InvocationStatus, DistributedInvocation


class ArgPair:
    """Helper to simulate a Memory cache for key:value pairs in Task Invocations"""

    def __init__(self, key: str, value: Any) -> None:
        self.key = key
        self.value = value

    def __hash__(self) -> int:
        return hash(self.key)


class TaskInvocationCache(Generic[Result]):
    def __init__(self) -> None:
        self.invocations: dict[str, "DistributedInvocation"] = {}
        self.args_index: dict[set[ArgPair], set[str]] = defaultdict(set)
        self.status_index: dict["InvocationStatus", set[str]] = defaultdict(set)
        self.invocation_status: dict[str, "InvocationStatus"] = {}

    @staticmethod
    def match_arguments(
        arg_pairs: set[ArgPair], invocation: "DistributedInvocation"
    ) -> bool:
        """Check if the invocation.arguments match the key_arguments"""
        for arg_pair in arg_pairs:
            if invocation.arguments[arg_pair.key] != arg_pair.value:
                return False
        return True

    def get_invocations(
        self,
        key_arguments: Optional[dict[str, Any]],
        status: Optional["InvocationStatus"],
    ) -> Iterator["DistributedInvocation"]:
        if key_arguments:
            arg_pairs = {ArgPair(key, value) for key, value in key_arguments.items()}
            for invocation_id in self.args_index[arg_pairs]:
                if status and self.invocation_status[invocation_id] != status:
                    continue
                yield self.invocations[invocation_id]
        elif status:
            for invocation_id in self.status_index[status]:
                yield self.invocations[invocation_id]
        else:
            iter(self.invocations.values())

    def set_status(
        self,
        invocation: "DistributedInvocation[Result]",
        status: "InvocationStatus",
    ) -> None:
        if (_id := invocation.invocation_id) not in self.invocations:
            self.invocations[_id] = invocation
            for key_args, invocations in self.args_index.items():
                if self.match_arguments(key_args, invocation):
                    invocations.add(_id)
            self.status_index[status].add(_id)
        else:
            self.status_index[self.invocation_status[_id]].discard(_id)
            self.status_index[status].add(_id)


class MemOrchestrator(BaseOrchestrator):
    def __init__(self, app: "Pynenc") -> None:
        self.cache: dict[str, TaskInvocationCache] = defaultdict(TaskInvocationCache)
        super().__init__(app)

    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_arguments: Optional[dict[str, Any]] = None,
        status: Optional["InvocationStatus"] = None,
    ) -> Iterator["DistributedInvocation"]:
        return self.cache[task.task_id].get_invocations(key_arguments, status)

    def set_invocation_status(
        self,
        invocation: "DistributedInvocation[Result]",
        status: "InvocationStatus",
    ) -> None:
        self.cache[invocation.task.task_id].set_status(invocation, status)
