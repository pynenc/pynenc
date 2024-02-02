import pickle
import threading
from collections import OrderedDict, defaultdict, deque
from time import time
from typing import TYPE_CHECKING, Any, Generic, Iterator

from pynenc.exceptions import CycleDetectedError, PendingInvocationLockError
from pynenc.invocation.status import InvocationStatus
from pynenc.orchestrator.base_orchestrator import (
    BaseBlockingControl,
    BaseCycleControl,
    BaseOrchestrator,
)
from pynenc.types import Params, Result

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.call import Call
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.task import Task


class MemCycleControl(BaseCycleControl):
    """
    An implementation of cycle control using a directed acyclic graph (DAG) to represent call dependencies.

    This class manages dependencies between task invocations to prevent call cycles, which could lead to deadlocks or infinite loops.

    :param Pynenc app: The Pynenc application instance.
    """

    def __init__(self, app: "Pynenc") -> None:
        self.app = app
        self.invocations: dict[str, "DistributedInvocation"] = {}
        self.calls: dict[str, "Call"] = {}
        self.call_to_invocation: dict[
            str, OrderedDict[str, "DistributedInvocation"]
        ] = defaultdict(OrderedDict)
        self.edges: dict[str, set[str]] = defaultdict(set)

    def add_call_and_check_cycles(
        self, caller: "DistributedInvocation", callee: "DistributedInvocation"
    ) -> None:
        """
        Adds a new invocation to the graph, representing a dependency where the caller is dependent on the callee.

        Raises a CycleDetectedError if adding the invocation would cause a cycle in the call graph.

        :param DistributedInvocation caller: The invocation making the call.
        :param DistributedInvocation callee: The invocation being called.
        :raises CycleDetectedError: If adding the invocation causes a cycle.
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

    def clean_up_invocation_cycles(self, invocation: "DistributedInvocation") -> None:
        """
        Removes an invocation from the graph, along with any edges to or from the invocation.

        :param DistributedInvocation invocation: The invocation to be removed from the graph.
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

    def find_cycle_caused_by_new_invocation(
        self, caller: "DistributedInvocation", callee: "DistributedInvocation"
    ) -> list["Call"]:
        """
        Determines if adding a new edge from the caller to the callee would create a cycle in the graph.

        :param DistributedInvocation caller: The invocation making the call.
        :param DistributedInvocation callee: The invocation being called.
        :return: A list of Calls that would form a cycle after adding the new invocation, else an empty list.
        :rtype: list[Call]
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
        """
        Utility function for cycle detection in the graph.

        :param str current_call_id: The current call ID being checked for cycles.
        :param set[str] visited: Set of already visited call IDs.
        :param list[str] path: Current path of call IDs being traversed.
        :return: A list of Calls that form a cycle, if one is detected.
        :rtype: list[Call]
        """
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


class MemBlockingControl(BaseBlockingControl):
    """
    An implementation of blocking control using a directed acyclic graph (DAG) to represent invocation dependencies.

    This class manages dependencies between task invocations, ensuring that invocations waiting for others are properly handled.

    :param Pynenc app: The Pynenc application instance.
    """

    def __init__(self, app: "Pynenc") -> None:
        self.app = app
        self.invocations: dict[str, "DistributedInvocation"] = {}
        self.waiting_for: dict[str, set[str]] = defaultdict(set)
        self.waited_by: dict[str, set[str]] = OrderedDict()

    def waiting_for_results(
        self,
        caller_invocation: "DistributedInvocation[Params, Result]",
        result_invocations: list["DistributedInvocation[Params, Result]"],
    ) -> None:
        """
        Registers that an invocation (waiter) is waiting for the results of other invocations (waited).

        :param DistributedInvocation[Params, Result] caller_invocation: The invocation waiting for results.
        :param list[DistributedInvocation[Params, Result]] result_invocations: The invocations whose results are being waited for.
        """
        waiter = caller_invocation
        for waited in result_invocations:
            self.invocations[waited.invocation_id] = waited
            self.waiting_for[waiter.invocation_id].add(waited.invocation_id)
            if waited.invocation_id not in self.waited_by:
                self.waited_by[waited.invocation_id] = set()
            self.waited_by[waited.invocation_id].add(waiter.invocation_id)

    def release_waiters(self, invocation: "DistributedInvocation") -> None:
        """
        Removes an invocation from the graph, along with any dependencies related to it.

        :param DistributedInvocation invocation: The invocation that has finished and will no longer block other invocations.
        """
        for waiter_id in self.waited_by.get(invocation.invocation_id, []):
            self.waiting_for[waiter_id].discard(invocation.invocation_id)
            if not self.waiting_for[waiter_id]:
                del self.waiting_for[waiter_id]
        self.waited_by.pop(invocation.invocation_id, None)
        self.waiting_for.pop(invocation.invocation_id, None)

    def get_blocking_invocations(
        self, max_num_invocations: int
    ) -> Iterator["DistributedInvocation[Params, Result]"]:
        """
        Retrieves invocations that are blocking others but are not themselves waiting for any results.

        :param int max_num_invocations: The maximum number of blocking invocations to retrieve.
        :return: An iterator over invocations that are blocking others (older firsts).
        :rtype: Iterator[DistributedInvocation[Params, Result]]
        """
        for inv_id in self.waited_by:
            if inv_id not in self.waiting_for:
                if self.app.orchestrator.get_invocation_status(
                    self.invocations[inv_id]
                ).is_available_for_run():
                    max_num_invocations -= 1
                    yield self.invocations[inv_id]
                    if max_num_invocations == 0:
                        return


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
    """
    A cache for storing and managing task invocations and their statuses.

    This class provides functionalities to track task invocations, including their arguments, statuses, retries, and auto-purge mechanisms.

    :param Pynenc app: The Pynenc application instance.
    """

    def __init__(self, app: "Pynenc") -> None:
        self.app = app
        self.invocations: dict[str, "DistributedInvocation"] = {}
        self.args_index: dict[ArgPair, set[str]] = defaultdict(set)
        self.status_index: dict[InvocationStatus, set[str]] = defaultdict(set)
        self.pending_timer: dict[str, float] = {}
        self.pre_pending_status: dict[str, InvocationStatus] = {}
        self.invocation_status: dict[str, InvocationStatus] = {}
        self.invocation_retries: dict[str, int] = {}
        self.invocations_to_purge: deque[tuple[float, str]] = deque()
        self.locks: dict[str, threading.Lock] = {}

    def filter_by_key_arguments(self, key_arguments: dict[str, str]) -> set[str]:
        matches = []
        for key, value in key_arguments.items():
            invocation_ids = set()
            for _id in self.args_index[ArgPair(key, value)]:
                invocation_ids.add(_id)
            if invocation_ids:
                matches.append(invocation_ids)
        return set.intersection(*matches) if matches else set()

    def filter_by_statuses(self, statuses: list[InvocationStatus]) -> set[str]:
        matched_ids = set()
        for status in statuses:
            matched_ids.update(self.status_index[status])
        return matched_ids

    def get_invocations(
        self,
        key_arguments: dict[str, str] | None,
        statuses: list[InvocationStatus] | None,
    ) -> Iterator["DistributedInvocation"]:
        """
        Retrieves invocations based on provided key arguments and/or status.

        :param dict[str, str] | None key_arguments: The key arguments to filter the invocations.
        :param list[InvocationStatus] | None status: The statuses to filter the invocations.
        :return: An iterator over the filtered invocations.
        :rtype: Iterator[DistributedInvocation]
        """
        if key_arguments and statuses:
            key_matches = self.filter_by_key_arguments(key_arguments)
            status_matches = self.filter_by_statuses(statuses)
            invocation_ids = key_matches.intersection(status_matches)
        elif key_arguments:
            invocation_ids = self.filter_by_key_arguments(key_arguments)
        elif statuses:
            invocation_ids = self.filter_by_statuses(statuses)
        else:
            invocation_ids = set(self.invocations.keys())

        for invocation_id in invocation_ids:
            yield self.invocations[invocation_id]

    def set_up_invocation_auto_purge(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        """
        Sets up an invocation for automatic purging after a specified time.

        :param DistributedInvocation[Params, Result] invocation: The invocation to be set up for auto-purge.
        """
        self.invocations_to_purge.append((time(), invocation.invocation_id))

    def auto_purge(self) -> None:
        """
        Automatically purges invocations that have been in a final state for longer than a specified duration.
        """
        end_time = (
            time() - self.app.orchestrator.conf.auto_final_invocation_purge_hours * 3600
        )
        while self.invocations_to_purge and self.invocations_to_purge[0][0] <= end_time:
            _, elem = self.invocations_to_purge.popleft()
            self.clean_up_invocation(elem)

    def clean_up_invocation(self, invocation_id: str) -> None:
        """
        Cleans up an invocation from the cache.

        :param str invocation_id: The ID of the invocation to be cleaned up.
        """
        invocation = self.invocations.pop(invocation_id, None)
        if invocation:
            for key, value in invocation.serialized_arguments.items():
                self.args_index[ArgPair(key, value)].discard(invocation_id)
            self.status_index[self.invocation_status[invocation_id]].discard(
                invocation_id
            )
            self.invocation_status.pop(invocation_id, None)
            self.invocation_retries.pop(invocation_id, None)
            self.pending_timer.pop(invocation_id, None)
            self.pre_pending_status.pop(invocation_id, None)

    def set_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: InvocationStatus,
    ) -> None:
        """
        Sets the status of a specific invocation.

        :param DistributedInvocation[Params, Result] invocation: The invocation whose status is to be set.
        :param InvocationStatus status: The status to set for the invocation.
        """
        if status != InvocationStatus.PENDING:
            self.clean_pending_status(invocation)
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

    def clean_pending_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        """
        Cleans the pending status of an invocation if it has exceeded the maximum pending time.

        :param DistributedInvocation[Params, Result] invocation: The invocation whose pending status is to be cleaned.
        """
        self.pending_timer.pop(invocation.invocation_id, None)
        self.pre_pending_status.pop(invocation.invocation_id, None)

    def set_pending_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        """
        Sets the status of an invocation to pending, handling any potential locking issues.

        :param DistributedInvocation[Params, Result] invocation: The invocation to set to pending status.
        :raises PendingInvocationLockError: If the invocation is already in pending status or cannot acquire a lock.
        """
        invocation_id = invocation.invocation_id
        lock = self.locks.setdefault(invocation_id, threading.Lock())
        if not lock.acquire(False):
            raise PendingInvocationLockError(invocation_id)
        try:
            self.pending_timer[invocation_id] = time()
            previous_status = self.invocation_status[invocation.invocation_id]
            if previous_status == InvocationStatus.PENDING:
                raise PendingInvocationLockError(invocation_id)
            self.pre_pending_status[invocation_id] = previous_status
            self.set_status(invocation, InvocationStatus.PENDING)
        finally:
            lock.release()

    def get_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> InvocationStatus:
        """
        Retrieves the current status of an invocation, accounting for pending timeout.

        :param DistributedInvocation[Params, Result] invocation: The invocation to get the status for.
        :return: The current status of the invocation.
        :rtype: InvocationStatus
        """
        status = self.invocation_status[invocation.invocation_id]
        if status == InvocationStatus.PENDING:
            elapsed = time() - self.pending_timer[invocation.invocation_id]
            if elapsed > self.app.conf.max_pending_seconds:
                pre_pending_status = self.pre_pending_status[invocation.invocation_id]
                self.set_status(invocation, pre_pending_status)
                return pre_pending_status
        return status

    def increase_retries(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        """
        Increases the retry count for a given invocation.

        :param DistributedInvocation[Params, Result] invocation: The invocation for which the retry count is to be increased.
        """
        self.invocation_retries[invocation.invocation_id] = (
            self.invocation_retries.get(invocation.invocation_id, 0) + 1
        )

    def get_retries(self, invocation: "DistributedInvocation[Params, Result]") -> int:
        """
        Retrieves the current number of retries for a given invocation.

        :param DistributedInvocation[Params, Result] invocation: The invocation to get the retry count for.
        :return: The number of retries for the invocation.
        :rtype: int
        """
        return self.invocation_retries.get(invocation.invocation_id, 0)


class MemOrchestrator(BaseOrchestrator):
    """
    A memory-based implementation of the Orchestrator,
    managing task invocations and their lifecycle.

    This class provides an in-memory solution for orchestrating task invocations,
    including cycle and blocking controls, as well as caching of invocation statuses and retries.

    ```{warning}
        This orchestrator is not intended for production use.
        As it stores all invocations in the running process memory.
    ```

    :param Pynenc app: The Pynenc application instance.
    """

    def __init__(self, app: "Pynenc") -> None:
        self.cache: dict[str, TaskInvocationCache] = defaultdict(
            lambda: TaskInvocationCache(app)
        )
        self._cycle_control: MemCycleControl | None = None
        self._blocking_control: MemBlockingControl | None = None
        super().__init__(app)

    @property
    def cycle_control(self) -> "MemCycleControl":
        if not self._cycle_control:
            self._cycle_control = MemCycleControl(self.app)
        return self._cycle_control

    @property
    def blocking_control(self) -> "MemBlockingControl":
        if not self._blocking_control:
            self._blocking_control = MemBlockingControl(self.app)
        return self._blocking_control

    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_serialized_arguments: dict[str, str] | None = None,
        statuses: list[InvocationStatus] | None = None,
    ) -> Iterator["DistributedInvocation"]:
        return self.cache[task.task_id].get_invocations(
            key_serialized_arguments, statuses
        )

    def _set_invocation_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: InvocationStatus,
    ) -> None:
        self.cache[invocation.task.task_id].set_status(invocation, status)

    def _set_invocation_pending_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        self.cache[invocation.task.task_id].set_pending_status(invocation)

    def set_up_invocation_auto_purge(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        self.cache[invocation.task.task_id].set_up_invocation_auto_purge(invocation)

    def auto_purge(self) -> None:
        for cache in self.cache.values():
            cache.auto_purge()

    def get_invocation_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> InvocationStatus:
        return self.cache[invocation.task.task_id].get_status(invocation)

    def increment_invocation_retries(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        self.cache[invocation.task.task_id].increase_retries(invocation)

    def get_invocation_retries(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> int:
        return self.cache[invocation.task.task_id].get_retries(invocation)

    def purge(self) -> None:
        self.cache.clear()
        self._cycle_control = None
        self._blocking_control = None
