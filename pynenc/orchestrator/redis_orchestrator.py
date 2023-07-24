from collections import defaultdict, OrderedDict
import pickle
from typing import Any, Iterator, Optional, TYPE_CHECKING

import redis

from .base_orchestrator import BaseOrchestrator
from ..types import Params, Result
from ..exceptions import CycleDetectedError
from ..conf.single_invocation_pending import (
    SingleInvocationPerArguments,
    SingleInvocationPerKeyArguments,
)
from ..invocation import DistributedInvocation
from ..util.redis_keys import Key

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..call import Call
    from ..task import Task
    from ..invocation import InvocationStatus


class CallGraph:
    """A directed acyclic graph representing the call dependencies"""

    def __init__(self, app: "Pynenc", client: redis.Redis) -> None:
        self.app = app
        self.key = Key("call_graph")
        self.client = client
        self.invocations: dict[str, "DistributedInvocation"] = {}
        self.calls: dict[str, "Call"] = {}
        self.call_to_invocation: dict[
            str, OrderedDict[str, "DistributedInvocation"]
        ] = defaultdict(OrderedDict)
        self.edges: dict[str, set[str]] = defaultdict(set)
        self.waiting_for: dict[str, set[str]] = defaultdict(set)
        self.waited_by: dict[str, set[str]] = defaultdict(set)

    def purge(self) -> None:
        self.key.purge(self.client)

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
        self.client.set(self.key.invocation(caller.invocation_id), caller.to_json())
        self.client.set(self.key.invocation(callee.invocation_id), callee.to_json())
        self.client.sadd(self.key.call(caller.call_id), caller.invocation_id)
        self.client.sadd(self.key.call(callee.call_id), callee.invocation_id)
        # self.calls[caller.call_id] = caller.call # todo check if it work with invocation_id
        # self.calls[callee.call_id] = callee.call
        # self.call_to_invocation[caller.call_id][caller.invocation_id] = caller
        # self.call_to_invocation[callee.call_id][callee.invocation_id] = callee
        # self.edges[caller.call_id].add(callee.call_id)
        self.client.sadd(self.key.edge(caller.call_id), callee.call_id)

    def remove_edges(self, call_id: str) -> None:
        """Remove all edges from a call"""
        callee_calls = self.client.smembers(self.key.edge(call_id))
        self.client.delete(self.key.edge(call_id))
        for callee_call_id in callee_calls:
            self.remove_edges(callee_call_id)

    def remove_invocation(self, invocation: "DistributedInvocation") -> None:
        """
        Remove an invocation from the graph. Also removes any edges to or from the invocation.
        """
        self.client.delete(self.key.invocation(invocation.invocation_id))
        self.client.srem(self.key.call(invocation.call_id), invocation.invocation_id)
        self.remove_edges(invocation.call_id)

    def add_waiting_for(
        self, waiter: "DistributedInvocation", waited: "DistributedInvocation"
    ) -> None:
        """
        Register that an invocation (waiter) is waiting for the results of another invocation (waited).
        """
        # self.waiting_for[waiter.invocation_id].add(waited.invocation_id)
        # self.waited_by[waited.invocation_id].add(waiter.invocation_id)
        self.client.sadd(
            self.key.waiting_for(waiter.invocation_id), waited.invocation_id
        )
        self.client.sadd(self.key.waited_by(waited.invocation_id), waiter.invocation_id)
        # Find all ancestors of the waited invocation and increment their count in the blocking set
        ancestors = self.get_all_ancestors(waited.invocation_id)
        for ancestor_id in ancestors:
            self.client.zincrby("total_blockers", 1, ancestor_id)

    def get_all_ancestors(self, invocation_id: str) -> set[str]:
        """Get all ancestors of an invocation in the waiting-for graph."""
        ancestors = set()
        waiters = self.client.smembers(self.key.waited_by(invocation_id))
        while waiters:
            waiter = waiters.pop()
            if waiter not in ancestors:
                ancestors.add(waiter)
                waiters.update(self.client.smembers(self.key.waited_by(waiter)))
        return ancestors

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
        list[DistributedInvocation]
            A list of blocking invocations or an empty list if no invocations are blocking.
        """
        # Retrieve the invocations with the highest scores
        ranked_invocations = self.client.zrevrange(
            "total_blockers", 0, -1, withscores=True
        )

        # Fetch the invocation objects from Redis
        invocations: list[DistributedInvocation] = []
        all_deps: set[str] = set()
        for invocation_id, _ in ranked_invocations:
            direct_deps = self.client.smembers(self.key.waiting_for(invocation_id))
            if not direct_deps.intersection(
                all_deps
            ):  # if no dependencies with the already selected invocations
                if inv := self.client.get(self.key.invocation(invocation_id.decode())):
                    invocations.append(
                        DistributedInvocation.from_json(self.app, inv.decode())
                    )
                all_deps.update(direct_deps)
                if len(invocations) == max_num_invocations:
                    break
        return invocations

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
        self.client.sadd(self.key.edge(caller.call_id), callee.call_id)

        # Set for tracking visited nodes
        visited: set[str] = set()

        # List for tracking the nodes on the path from caller to callee
        path: list[str] = []

        cycle = self._is_cyclic_util(caller.call_id, visited, path)

        # Remove the temporarily added edge
        self.client.srem(self.key.edge(caller.call_id), callee.call_id)

        return cycle

    def _is_cyclic_util(
        self,
        current_call_id: str,
        visited: set[str],
        path: list[str],
    ) -> list["Call"]:
        visited.add(current_call_id)
        path.append(current_call_id)

        for neighbour_call_id in self.client.smembers(self.key.edge(current_call_id)):
            if neighbour_call_id not in visited:
                cycle = self._is_cyclic_util(neighbour_call_id, visited, path)
                if cycle:
                    return cycle
            elif neighbour_call_id in path:
                cycle_start_index = path.index(neighbour_call_id)
                return [self.calls[_id] for _id in path[cycle_start_index:]]

        path.pop()
        return []


class TaskRedisCache:
    def __init__(self, app: "Pynenc", client: redis.Redis) -> None:
        self.app = app
        self.client = client
        self.key = Key("orchestrator")

    def purge(self) -> None:
        self.key.purge(self.client)

    def set_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: "InvocationStatus",
    ) -> None:
        task_id = invocation.task.task_id
        invocation_id = invocation.invocation_id
        for arg, val in invocation.serialized_arguments.items():
            self.client.sadd(self.key.args(task_id, arg, val), invocation_id)
        self.client.set(self.key.invocation(invocation_id), invocation.to_json())
        self.client.sadd(self.key.task(task_id), invocation_id)
        self.client.sadd(self.key.status(task_id, status), invocation_id)
        self.client.set(self.key.invocation_status(invocation_id), status.value)

    def get_invocations(
        self,
        task_id: str,
        key_arguments: Optional[dict[str, str]],
        status: Optional["InvocationStatus"],
    ) -> Iterator["DistributedInvocation"]:
        # Start with the set of obj_ids for the task_id
        invocation_ids = self.client.smembers(self.key.task(task_id))

        # If key_arguments were provided, intersect the current obj_ids with those matching each arg:val pair
        if key_arguments:
            for arg, val in key_arguments.items():
                arg_val_ids = self.client.smembers(self.key.args(task_id, arg, val))
                invocation_ids = invocation_ids.intersection(arg_val_ids)

        # If status was provided, intersect the current obj_ids with those matching the status
        if status:
            status_ids = self.client.smembers(self.key.status(task_id, status))
            invocation_ids = invocation_ids.intersection(status_ids)

        # Now obj_ids contains only the ids of obj_strings that match all the provided keys
        # Fetch the obj_strings for these ids
        for invocation_id in invocation_ids:
            if inv := self.client.get(self.key.invocation(invocation_id.decode())):
                yield DistributedInvocation.from_json(self.app, inv.decode())

    def get_invocation_status(
        self, invocation: "DistributedInvocation"
    ) -> "InvocationStatus":
        if status := self.client.get(
            self.key.invocation_status(invocation.invocation_id)
        ):
            return InvocationStatus(status.decode())
        raise ValueError(f"Invocation status {invocation} not found in Redis")


class RedisOrchestrator(BaseOrchestrator):
    def __init__(self, app: "Pynenc") -> None:
        client = redis.Redis(host="localhost", port=6379, db=0)
        self.redis_cache = TaskRedisCache(app, client)
        self.call_graph = CallGraph(app, client)
        super().__init__(app)

    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_serialized_arguments: Optional[dict[str, str]] = None,
        status: Optional["InvocationStatus"] = None,
    ) -> Iterator["DistributedInvocation"]:
        return self.redis_cache.get_invocations(
            task.task_id, key_serialized_arguments, status
        )

    def set_invocation_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: "InvocationStatus",
    ) -> None:
        self.redis_cache.set_status(invocation, status)

    def set_invocations_status(
        self,
        invocations: list["DistributedInvocation[Params, Result]"],
        status: "InvocationStatus",
    ) -> None:
        for invocation in invocations:
            self.redis_cache.set_status(invocation, status)

    def check_for_call_cycle(
        self,
        caller_invocation: DistributedInvocation[Params, Result],
        callee_invocation: DistributedInvocation[Params, Result],
    ) -> None:
        self.call_graph.add_invocation_call(caller_invocation, callee_invocation)

    def waiting_for_result(
        self,
        caller_invocation: Optional[DistributedInvocation[Params, Result]],
        result_invocation: DistributedInvocation[Params, Result],
    ) -> None:
        if caller_invocation:
            self.call_graph.add_waiting_for(caller_invocation, result_invocation)

    def get_invocation_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "InvocationStatus":
        return self.redis_cache.get_invocation_status(invocation)

    def purge(self) -> None:
        """Remove all invocations from the orchestrator"""
        self.redis_cache.purge()
        self.call_graph.purge()
