from collections import defaultdict, OrderedDict
import pickle
from typing import Any, Iterator, Optional, TYPE_CHECKING, Generic

from .base_orchestrator import BaseOrchestrator
from ..types import Params, Result
from ..exceptions import CycleDetectedError

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..call import Call
    from ..task import Task
    from ..invocation import InvocationStatus, DistributedInvocation


class CallGraph:
    """A directed acyclic graph representing the call dependencies"""

    def __init__(self) -> None:
        self.invocations: dict[str, "DistributedInvocation"] = {}
        self.calls: dict[str, "Call"] = {}
        self.call_to_invocation: dict[
            str, OrderedDict[str, "DistributedInvocation"]
        ] = defaultdict(OrderedDict)
        self.edges: dict[str, set[str]] = defaultdict(set)
        self.waiting_for: dict[str, set[str]] = defaultdict(set)
        self.waited_by: dict[str, set[str]] = defaultdict(set)

    def add_invocation_call(
        self, caller: "DistributedInvocation", callee: "DistributedInvocation"
    ) -> None:
        """
        Add a new invocation to the graph. This represents a dependency where the caller
        is dependent on the callee.

        Raises a CycleDetectedError if the invocation would cause a cycle.
        """
        if caller.call_id == callee.call_id:
            raise CycleDetectedError.from_cycle([caller.call])
        if cycle := self.find_cycle_caused_by_new_invocation(caller, callee):
            raise CycleDetectedError.from_cycle(cycle)
        self.invocations[caller.invocation_id] = caller
        self.invocations[callee.invocation_id] = callee
        self.calls[caller.call_id] = caller.call
        self.calls[callee.call_id] = callee.call
        self.call_to_invocation[caller.call_id][caller.invocation_id] = caller
        self.call_to_invocation[callee.call_id][callee.invocation_id] = callee
        self.edges[caller.call_id].add(callee.call_id)

    def add_waiting_for(
        self, waiter: "DistributedInvocation", waited: "DistributedInvocation"
    ) -> None:
        """
        Register that an invocation (waiter) is waiting for the results of another invocation (waited).
        """
        self.waiting_for[waiter.invocation_id].add(waited.invocation_id)
        self.waited_by[waited.invocation_id].add(waiter.invocation_id)

    def remove_invocation(self, invocation: "DistributedInvocation") -> None:
        """
        Remove an invocation from the graph. Also removes any edges to or from the invocation.
        """
        call_id = invocation.call_id
        if call_id in self.call_to_invocation:
            self.call_to_invocation[call_id].pop(invocation.invocation_id, None)
            if not self.call_to_invocation[call_id]:
                del self.call_to_invocation[call_id]
                if call_id in self.edges:
                    del self.edges[call_id]
                for edges in self.edges.values():
                    edges.discard(call_id)

    def get_blocking_invocations(
        self, max_num_invocations: int
    ) -> list["DistributedInvocation"]:
        """
        Returns the invocations that are blocking the maximum number of other invocations.

        Parameters
        ----------
        max_num_invocations : int
            The maximum number of invocations to return.

        Returns
        -------
        Optional[Set[DistributedInvocation]]
            A set of blocking invocations or None if no invocations are blocking.
        """
        if not any(self.waited_by.values()):
            return []
        blocking_invocations_ids = {i for i in self.waited_by if self.waited_by[i]}
        sorted_invocations_ids = sorted(
            blocking_invocations_ids, key=self._depth, reverse=True
        )
        sorted_invocations = [
            self.invocations[inv_id]
            for inv_id in sorted_invocations_ids[:max_num_invocations]
        ]
        return sorted_invocations

    def find_cycle_caused_by_new_invocation(
        self, caller: "DistributedInvocation", callee: "DistributedInvocation"
    ) -> list["Call"]:
        """
        Determines if adding an edge from the caller to the callee would create a cycle.

        Parameters
        ----------
        caller : DistributedInvocation
            The invocation making the call.
        callee : DistributedInvocation
            The invocation being called.

        Returns
        -------
        list
            List of invocations that would form the cycle after adding the new invocation, else an empty list.
        """
        # Temporarily add the edge to check if it would cause a cycle
        self.edges[caller.call_id].add(callee.call_id)

        # Set for tracking visited nodes
        visited: set[str] = set()

        # List for tracking the nodes on the path from caller to callee
        path: list[str] = []

        cycle = self._is_cyclic_util(caller.call_id, visited, path)

        # Remove the temporarily added edge
        self.edges[caller.call_id].remove(callee.call_id)

        return cycle

    def _is_cyclic_util(
        self,
        current_call_id: str,
        visited: set[str],
        path: list[str],
    ) -> list["Call"]:
        visited.add(current_call_id)
        path.append(current_call_id)

        for neighbour_call_id in self.edges.get(current_call_id, []):
            if neighbour_call_id not in visited:
                cycle = self._is_cyclic_util(neighbour_call_id, visited, path)
                if cycle:
                    return cycle
            elif neighbour_call_id in path:
                cycle_start_index = path.index(neighbour_call_id)
                return [self.calls[_id] for _id in path[cycle_start_index:]]

        path.pop()
        return []

    def _depth(self, node: str) -> int:
        """
        Calculates the depth of a node in terms of dependent invocations.

        Parameters
        ----------
        node : str
            The node to calculate the depth for.

        Returns
        -------
        int
            The depth of the node.
        """
        if not self.waited_by[node]:
            return 1
        else:
            return 1 + max(self._depth(child) for child in self.waited_by[node])


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
        key_arguments: Optional[dict[str, str]],
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
            for key, value in invocation.serialized_arguments.items():
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
        self.call_graph = CallGraph()
        super().__init__(app)

    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_serialized_arguments: Optional[dict[str, str]] = None,
        status: Optional["InvocationStatus"] = None,
    ) -> Iterator["DistributedInvocation"]:
        return self.cache[task.task_id].get_invocations(
            key_serialized_arguments, status
        )

    def set_invocation_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: "InvocationStatus",
    ) -> None:
        self.cache[invocation.task.task_id].set_status(invocation, status)

    def set_invocations_status(
        self,
        invocations: list["DistributedInvocation[Params, Result]"],
        status: "InvocationStatus",
    ) -> None:
        for invocation in invocations:
            self.cache[invocation.task.task_id].set_status(invocation, status)

    def check_for_call_cycle(
        self,
        caller_invocation: "DistributedInvocation[Params, Result]",
        callee_invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        self.call_graph.add_invocation_call(caller_invocation, callee_invocation)

    def waiting_for_result(
        self,
        caller_invocation: Optional["DistributedInvocation[Params, Result]"],
        result_invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        if caller_invocation:
            self.call_graph.add_waiting_for(caller_invocation, result_invocation)

    def get_invocation_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "InvocationStatus":
        return self.cache[invocation.task.task_id].invocation_status[
            invocation.invocation_id
        ]

    def purge(self) -> None:
        self.cache.clear()
        self.call_graph = CallGraph()
