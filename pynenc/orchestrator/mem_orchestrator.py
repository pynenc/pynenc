from collections import defaultdict
import pickle
import hashlib
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
        return hash((self.key, pickle.dumps(self.value)))

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ArgPair):
            return self.key == other.key and self.value == other.value
        return False

    def __str__(self) -> str:
        return f"{self.key}:{self.value}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__str__()})"


class TaskInvocationCache(Generic[Result]):
    def __init__(self) -> None:
        self.invocations: dict[str, "DistributedInvocation"] = {}
        self.args_index: dict[ArgPair, set[str]] = defaultdict(set)
        self.status_index: dict["InvocationStatus", set[str]] = defaultdict(set)
        self.invocation_status: dict[str, "InvocationStatus"] = {}

    def get_invocations(
        self,
        key_arguments: Optional[dict[str, Any]],
        status: Optional["InvocationStatus"],
    ) -> Iterator["DistributedInvocation"]:
        # Filtering by key_arguments
        if key_arguments:
            matches: list[set[str]] = []
            # Check all the task invocations related per each key_argument
            for key, value in key_arguments.items():
                invocation_ids = set()
                for _id in self.args_index[ArgPair(key, value)]:
                    # Check if the invocation also matches the (optionally) specified status
                    if not status or _id in self.status_index[status]:
                        invocation_ids.add(_id)
                # add the matching invocations for the key:val pair to the list
                if invocation_ids:
                    matches.append(invocation_ids)
            # yield the invocations that matches all the specified key:val pairs
            if matches:
                for invocation_id in set.intersection(*matches):
                    yield self.invocations[invocation_id]
        # Filtered only by statys
        elif status:
            for invocation_id in self.status_index[status]:
                yield self.invocations[invocation_id]
        # No filters, return all invocations
        else:
            for invocation in self.invocations.values():
                yield invocation

    def set_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: "InvocationStatus",
    ) -> None:
        if (_id := invocation.invocation_id) not in self.invocations:
            self.invocations[_id] = invocation
            for key, value in invocation.arguments.items():
                self.args_index[ArgPair(key, value)].add(_id)
            self.status_index[status].add(_id)
        else:
            # already exists, remove previous status
            self.status_index[self.invocation_status[_id]].discard(_id)
            self.status_index[status].add(_id)
        self.invocation_status[_id] = status


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
        invocation: "DistributedInvocation[Params, Result]",
        status: "InvocationStatus",
    ) -> None:
        self.cache[invocation.task.task_id].set_status(invocation, status)
