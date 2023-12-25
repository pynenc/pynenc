import pickle
import threading
from collections import OrderedDict, defaultdict, deque
from time import time
from typing import TYPE_CHECKING, Any, Generic, Iterator

from ..exceptions import CycleDetectedError, PendingInvocationLockError
from ..invocation import InvocationStatus
from ..types import Params, Result
from .base_orchestrator import BaseBlockingControl, BaseCycleControl, BaseOrchestrator

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..call import Call
    from ..invocation import DistributedInvocation
    from ..task import Task


class MemCycleControl(BaseCycleControl):
    """A directed acyclic graph representing the call dependencies"""

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

    def clean_up_invocation_cycles(self, invocation: "DistributedInvocation") -> None:
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


class MemBlockingControl(BaseBlockingControl):
    """A directed acyclic graph representing the call dependencies"""

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
        Register that an invocation (waiter) is waiting for the results of another invocation (waited).
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
        Remove an invocation from the graph. Also removes any edges to or from the invocation.
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
        Returns the invocations that are blocking others but not waiting for anything themselves.
        The oldest invocations are returned first.

        Parameters
        ----------
        max_num_invocations : int
            The maximum number of invocations to return.

        Returns
        -------
        Set[DistributedInvocation] | None
            A set of blocking invocations or None if no invocations are blocking.
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

    def get_invocations(
        self,
        key_arguments: dict[str, str] | None,
        status: InvocationStatus | None,
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
            yield from self.invocations.values()

    def set_up_invocation_auto_purge(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        self.invocations_to_purge.append((time(), invocation.invocation_id))

    def auto_purge(self) -> None:
        end_time = (
            time() - self.app.orchestrator.conf.auto_final_invocation_purge_hours * 3600
        )
        while self.invocations_to_purge and self.invocations_to_purge[0][0] <= end_time:
            _, elem = self.invocations_to_purge.popleft()
            self.clean_up_invocation(elem)

    def clean_up_invocation(self, invocation_id: str) -> None:
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
        self.pending_timer.pop(invocation.invocation_id, None)
        self.pre_pending_status.pop(invocation.invocation_id, None)

    def set_pending_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
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
        self.invocation_retries[invocation.invocation_id] = (
            self.invocation_retries.get(invocation.invocation_id, 0) + 1
        )

    def get_retries(self, invocation: "DistributedInvocation[Params, Result]") -> int:
        return self.invocation_retries.get(invocation.invocation_id, 0)


class MemOrchestrator(BaseOrchestrator):
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
        status: InvocationStatus | None = None,
    ) -> Iterator["DistributedInvocation"]:
        return self.cache[task.task_id].get_invocations(
            key_serialized_arguments, status
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
