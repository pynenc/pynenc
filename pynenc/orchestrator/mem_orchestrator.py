import pickle
import threading
from collections import OrderedDict, defaultdict, deque
from collections.abc import Iterator
from time import time
from typing import TYPE_CHECKING, Any, Optional

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
        self.calls: dict[str, "Call"] = {}
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
        self.calls[caller.call_id] = caller.call
        self.calls[callee.call_id] = callee.call
        self.edges[caller.call_id].add(callee.call_id)

    def get_callees(self, caller_call_id: str) -> Iterator[str]:
        """
        Returns an iterator of direct callee call_ids for the given caller_call_id.

        :param str caller_call_id: The call_id of the caller invocation.
        :return: Iterator of callee call_ids.
        :rtype: Iterator[str]
        """
        yield from self.edges.get(caller_call_id, set())

    def clean_up_invocation_cycles(self, invocation_id: str) -> None:
        """
        Removes an invocation from the graph, along with any edges to or from the invocation.

        :param str invocation_id: The ID of the invocation to be removed from the graph.
        """
        call_id = self.app.orchestrator.get_invocation_call_id(invocation_id)
        if not self.app.orchestrator.any_non_final_invocations(call_id):
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
        self.waiting_for: dict[str, set[str]] = defaultdict(set)
        self.waited_by: dict[str, set[str]] = OrderedDict()

    def waiting_for_results(
        self, caller_invocation_id: str, result_invocation_ids: list[str]
    ) -> None:
        """
        Notifies the system that an invocation is waiting for the results of other invocations.

        :param str caller_invocation_id: The ID of the invocation that is waiting.
        :param list[str] result_invocation_ids: The IDs of the invocations being waited on.
        """
        waiter_id = caller_invocation_id
        for waited_id in result_invocation_ids:
            self.waiting_for[waiter_id].add(waited_id)
            if waited_id not in self.waited_by:
                self.waited_by[waited_id] = set()
            self.waited_by[waited_id].add(waiter_id)

    def release_waiters(self, waited_invocation_id: str) -> None:
        """
        Removes an invocation from the graph, along with any dependencies related to it.

        :param str waited_invocation_id: The ID of the invocation that has finished and will no longer block other invocations.
        """
        for waiter_id in self.waited_by.get(waited_invocation_id, []):
            self.waiting_for[waiter_id].discard(waited_invocation_id)
            if not self.waiting_for[waiter_id]:
                del self.waiting_for[waiter_id]
        self.waited_by.pop(waited_invocation_id, None)
        self.waiting_for.pop(waited_invocation_id, None)

    def get_blocking_invocations(self, max_num_invocations: int) -> Iterator[str]:
        """
        Retrieves invocations that are blocking others but are not themselves waiting for any results.

        :param int max_num_invocations: The maximum number of blocking invocations to retrieve.
        :return: An iterator over invocations that are blocking others (older firsts).
        :rtype: Iterator[DistributedInvocation[Params, Result]]
        """
        # Create a snapshot of the keys to avoid mutation during iteration
        for inv_id in list(self.waited_by.keys()):
            if inv_id not in self.waiting_for:
                if self.app.orchestrator.get_invocation_status(
                    inv_id
                ).is_available_for_run():
                    max_num_invocations -= 1
                    yield inv_id
                    if max_num_invocations == 0:
                        return


class ArgPair:
    """Helper to simulate a Memory cache for key:value pairs in Task Invocations"""

    def __init__(self, key: str, value: Any) -> None:
        self.key = key
        self.value = value

    def __hash__(self) -> int:
        """Generate a hash that works with serialized values"""
        # For string values (like serialized JSON), use the string value directly
        if isinstance(self.value, str):
            return hash((self.key, self.value))
        # For other types, use pickle for consistent hashing
        return hash((self.key, pickle.dumps(self.value)))

    def __eq__(self, other: Any) -> bool:
        """Equality check that works with serialized values"""
        if not isinstance(other, ArgPair):
            return False

        # Basic key comparison
        if self.key != other.key:
            return False

        # For string values (likely serialized JSON), direct comparison
        if isinstance(self.value, str) and isinstance(other.value, str):
            return self.value == other.value

        # For other types, try direct comparison first
        if self.value == other.value:
            return True

        # Last resort: pickle comparison for complex objects
        try:
            return pickle.dumps(self.value) == pickle.dumps(other.value)
        except (pickle.PickleError, TypeError):
            # If pickling fails, they're not equal
            return False

    def __str__(self) -> str:
        return f"{self.key}:{self.value}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__str__()})"


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
        self.app = app
        self.task_id_to_inv_id: dict[str, set[str]] = defaultdict(set)
        self.call_id_to_inv_id: dict[str, set[str]] = defaultdict(set)
        self.inv_id_to_call_id: dict[str, str] = {}
        self.invocation_args: dict[str, set[ArgPair]] = defaultdict(set)
        self.args_index: dict[ArgPair, set[str]] = defaultdict(set)
        self.status_index: dict[InvocationStatus, set[str]] = defaultdict(set)
        self.pending_timer: dict[str, float] = {}
        self.pre_pending_status: dict[str, InvocationStatus] = {}
        self.invocation_status: dict[str, InvocationStatus] = {}
        self.invocation_retries: dict[str, int] = {}
        self.invocations_to_purge: deque[tuple[float, str]] = deque()
        self.locks: dict[str, threading.Lock] = {}

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

    def _register_new_invocations(
        self, invocations: list["DistributedInvocation[Params, Result]"]
    ) -> None:
        """Registers new invocations and sets them to REGISTERED status."""
        for invocation in invocations:
            if invocation.invocation_id in self.invocation_status:
                continue
            self.task_id_to_inv_id[invocation.task.task_id].add(
                invocation.invocation_id
            )
            self.call_id_to_inv_id[invocation.call_id].add(invocation.invocation_id)
            self.inv_id_to_call_id[invocation.invocation_id] = invocation.call_id
            self._internal_set_invocation_status(
                invocation.invocation_id, InvocationStatus.REGISTERED
            )
            self.invocation_retries[invocation.invocation_id] = 0

    def filter_by_key_arguments(self, key_arguments: dict[str, str]) -> set[str]:
        """
        Filters invocations by key arguments, requiring ALL keys to match.

        :param dict[str, str] key_arguments: Key-value pairs to filter by
        :return: Set of invocation IDs that match ALL provided key-value pairs
        """
        if not key_arguments:
            return set()

        # First, get candidates that match at least one key-value pair
        all_candidate_sets = []

        for key, value in key_arguments.items():
            # Get invocation IDs matching this specific key-value pair
            pair = ArgPair(key, value)
            matching_ids = self.args_index.get(pair, set())

            # If no matches for any single key, we can return early
            if not matching_ids:
                return set()

            all_candidate_sets.append(matching_ids)

        # Return only invocations that appear in ALL the candidate sets (intersection)
        if not all_candidate_sets:
            return set()

        # Start with the first set and intersect with each subsequent set
        result = all_candidate_sets[0].copy()
        for candidate_set in all_candidate_sets[1:]:
            result.intersection_update(candidate_set)
            # Early exit if intersection becomes empty
            if not result:
                break

        return result

    def filter_by_statuses(self, statuses: list[InvocationStatus]) -> set[str]:
        matched_ids = set()
        for status in statuses:
            matched_ids.update(self.status_index[status])
        return matched_ids

    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_serialized_arguments: dict[str, str] | None = None,
        statuses: Optional[list["InvocationStatus"]] = None,
    ) -> Iterator[str]:
        """
        Retrieves invocation ids based on provided key arguments and/or status.

        :param dict[str, str] | None key_arguments: The key arguments to filter the invocations.
        :param list[InvocationStatus] | None status: The statuses to filter the invocations.
        :return: An iterator over the filtered invocations.
        :rtype: Iterator[str]
        """
        task_matches = self.task_id_to_inv_id.get(task.task_id, set())
        if key_serialized_arguments and statuses:
            key_matches = self.filter_by_key_arguments(key_serialized_arguments)
            status_matches = self.filter_by_statuses(statuses)
            yield from task_matches.intersection(key_matches).intersection(
                status_matches
            )
        elif key_serialized_arguments:
            key_matches = self.filter_by_key_arguments(key_serialized_arguments)
            yield from task_matches.intersection(key_matches)
        elif statuses:
            status_matches = self.filter_by_statuses(statuses)
            yield from task_matches.intersection(status_matches)
        else:
            yield from task_matches

    def get_task_invocation_ids(self, task_id: str) -> Iterator[str]:
        """
        Retrieves all invocation ids for a given task id.

        :param str task_id: The task id to filter the invocations.
        :return: An iterator over the invocation ids for the specified task.
        :rtype: Iterator[str]
        """
        yield from self.task_id_to_inv_id.get(task_id, set())

    def get_call_invocation_ids(self, call_id: str) -> Iterator[str]:
        """Retrieves all invocation IDs associated with a specific call ID."""
        yield from self.call_id_to_inv_id.get(call_id, set())

    def get_invocation_call_id(self, invocation_id: str) -> str:
        """Retrieves the call ID associated with a specific invocation ID."""
        try:
            return self.inv_id_to_call_id[invocation_id]
        except Exception:
            # TODO remove this
            self.app.logger.error(
                f"Error get_invocation_call_id for invocation ID {invocation_id}"
            )
            for inv_id, call_id in self.inv_id_to_call_id.items():
                self.app.logger.error(f"inv_id: {inv_id} --> call_id: {call_id}")
            raise

    def any_non_final_invocations(self, call_id: str) -> bool:
        """Checks if there are any non-final invocations for a specific call ID."""
        for invocation_id in self.call_id_to_inv_id.get(call_id, set()):
            if not self.invocation_status[invocation_id].is_final():
                return True
        return False

    def set_up_invocation_auto_purge(self, invocation_id: str) -> None:
        """
        Sets up an invocation for automatic purging after a specified time.

        :param str invocation_id: The ID of the invocation to be set up for auto-purge.
        """
        self.invocations_to_purge.append((time(), invocation_id))

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
        self.clean_up_invocation_cycles(invocation_id)
        self.release_waiters(invocation_id)

        invocation = self.app.state_backend.get_invocation(invocation_id)
        for key, value in invocation.serialized_arguments.items():
            self.args_index[ArgPair(key, value)].discard(invocation_id)
        self.status_index[self.invocation_status[invocation_id]].discard(invocation_id)
        self.task_id_to_inv_id.get(invocation.task.task_id, set()).discard(
            invocation_id
        )
        self.call_id_to_inv_id.get(invocation.call_id, set()).discard(invocation_id)
        self.inv_id_to_call_id.pop(invocation_id, None)
        if args := self.invocation_args.pop(invocation_id, None):
            for arg in args:
                self.args_index[arg].discard(invocation_id)
        self.status_index[self.invocation_status[invocation_id]].discard(invocation_id)
        self.pending_timer.pop(invocation_id, None)
        self.pre_pending_status.pop(invocation_id, None)
        self.invocation_status.pop(invocation_id, None)
        self.invocation_retries.pop(invocation_id, None)

    def _set_invocation_status(
        self, invocation_id: str, status: InvocationStatus
    ) -> None:
        """Sets the status of a specific invocation."""
        if status == InvocationStatus.PENDING:
            raise ValueError("Cannot set multiple invocations to PENDING status")
        self._internal_set_invocation_status(invocation_id, status)

    def _internal_set_invocation_status(
        self, invocation_id: str, status: InvocationStatus
    ) -> None:
        """Sets the status of a specific invocation."""
        if status != InvocationStatus.PENDING:
            self.clean_pending_status(invocation_id)
        if invocation_id in self.invocation_status:
            # already exists, remove previous status
            self.status_index[self.invocation_status[invocation_id]].discard(
                invocation_id
            )
        self.status_index[status].add(invocation_id)
        self.invocation_status[invocation_id] = status

    def _set_invocations_status(
        self, invocation_ids: list[str], status: InvocationStatus
    ) -> None:
        for invocation_id in invocation_ids:
            self._internal_set_invocation_status(invocation_id, status)

    def _set_invocation_pending_status(self, invocation_id: str) -> None:
        """
        Sets the status of an invocation to pending, handling any potential locking issues.

        :param str invocation_id: The ID of the invocation to set to pending status.
        :raises PendingInvocationLockError: If the invocation is already in pending status or cannot acquire a lock.
        """
        lock = self.locks.setdefault(invocation_id, threading.Lock())
        if not lock.acquire(False):
            raise PendingInvocationLockError(invocation_id)
        try:
            self.pending_timer[invocation_id] = time()
            previous_status = self.invocation_status[invocation_id]
            if previous_status == InvocationStatus.PENDING:
                raise PendingInvocationLockError(invocation_id)
            self.pre_pending_status[invocation_id] = previous_status
            self._internal_set_invocation_status(
                invocation_id, InvocationStatus.PENDING
            )
        finally:
            lock.release()

    def index_arguments_for_concurrency_control(
        self,
        invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        for key, value in invocation.serialized_arguments.items():
            self.args_index[ArgPair(key, value)].add(invocation.invocation_id)

    def clean_pending_status(self, invocation_id: str) -> None:
        """
        Cleans the pending status of an invocation if it has exceeded the maximum pending time.

        :param str invocation_id: The ID of the invocation whose pending status is to be cleaned.
        """
        self.pending_timer.pop(invocation_id, None)
        self.pre_pending_status.pop(invocation_id, None)

    def get_invocation_pending_timer(self, invocation_id: str) -> Optional[float]:
        """Retrieves the pending timer for a specific invocation."""
        return self.pending_timer.get(invocation_id)

    def get_invocation_status(self, invocation_id: str) -> InvocationStatus:
        """Retrieves the current status of an invocation, accounting for pending timeout.:rtype: InvocationStatus"""
        status = self.invocation_status[invocation_id]
        if status == InvocationStatus.PENDING:
            elapsed = time() - self.pending_timer[invocation_id]
            if elapsed > self.app.conf.max_pending_seconds:
                pre_pending_status = self.pre_pending_status[invocation_id]
                self._internal_set_invocation_status(invocation_id, pre_pending_status)
                return pre_pending_status
        return status

    def increment_invocation_retries(self, invocation_id: str) -> None:
        """
        Increases the retry count for a given invocation.

        :param str invocation_id: The ID of the invocation for which the retry count is to be increased.
        """
        self.invocation_retries[invocation_id] = (
            self.invocation_retries.get(invocation_id, 0) + 1
        )

    def get_invocation_retries(self, invocation_id: str) -> int:
        """
        Retrieves the current number of retries for a given invocation.

        :param str invocation_id: The ID of the invocation to get the retry count for.
        :return: The number of retries for the invocation.
        :rtype: int
        """
        return self.invocation_retries.get(invocation_id, 0)

    def filter_by_status(
        self, invocation_ids: list[str], status_filter: set["InvocationStatus"]
    ) -> list[str]:
        if not invocation_ids or status_filter is None:
            return []
        return [
            inv_id
            for inv_id in invocation_ids
            if self.get_invocation_status(inv_id) in status_filter
        ]

    def purge(self) -> None:
        self._cycle_control = None
        self._blocking_control = None

        self.task_id_to_inv_id.clear()
        self.inv_id_to_call_id.clear()
        self.call_id_to_inv_id.clear()
        self.invocation_args.clear()
        self.args_index.clear()
        self.status_index.clear()
        self.pending_timer.clear()
        self.pre_pending_status.clear()
        self.invocation_status.clear()
        self.invocation_retries.clear()
        self.invocations_to_purge.clear()
        self.locks.clear()
