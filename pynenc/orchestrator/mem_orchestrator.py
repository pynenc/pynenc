import pickle
import threading
from collections import OrderedDict, defaultdict, deque
from collections.abc import Iterator
from datetime import UTC, datetime
from time import time
from typing import TYPE_CHECKING, Any

from pynenc.invocation.status import (
    InvocationStatus,
    InvocationStatusRecord,
    status_record_transition,
)
from pynenc.orchestrator.base_orchestrator import (
    BaseBlockingControl,
    BaseOrchestrator,
)
from pynenc.orchestrator.atomic_service import ActiveRunnerInfo
from pynenc.types import Params, Result

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.identifiers.call_id import CallId
    from pynenc.identifiers.invocation_id import InvocationId
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.task import Task, TaskId


class MemBlockingControl(BaseBlockingControl):
    """
    An implementation of blocking control using a directed acyclic graph (DAG) to represent invocation dependencies.

    This class manages dependencies between task invocations, ensuring that invocations waiting for others are properly handled.

    :param Pynenc app: The Pynenc application instance.
    """

    def __init__(self, app: "Pynenc") -> None:
        self.app = app
        self._lock = threading.RLock()
        self.waiting_for: dict[InvocationId, set[InvocationId]] = defaultdict(set)
        self.waited_by: dict[InvocationId, set[InvocationId]] = OrderedDict()
        # Maintained set of "ready" invocations: waited on by others but not waiting themselves
        self._ready: set[InvocationId] = set()

    def waiting_for_results(
        self,
        caller_invocation_id: "InvocationId",
        result_invocation_ids: list["InvocationId"],
    ) -> None:
        """
        Notifies the system that an invocation is waiting for the results of other invocations.

        :param InvocationId caller_invocation_id: The ID of the invocation that is waiting.
        :param list[InvocationId] result_invocation_ids: The IDs of the invocations being waited on.
        """
        waiter_id = caller_invocation_id
        with self._lock:
            for waited_id in result_invocation_ids:
                self.waiting_for[waiter_id].add(waited_id)
                if waited_id not in self.waited_by:
                    self.waited_by[waited_id] = set()
                self.waited_by[waited_id].add(waiter_id)
                # waited_id is ready if it's not itself waiting
                if waited_id not in self.waiting_for:
                    self._ready.add(waited_id)
            # waiter_id is now waiting, so it can't be ready
            self._ready.discard(waiter_id)

    def release_waiters(self, waited_invocation_id: "InvocationId") -> None:
        """
        Removes an invocation from the graph, along with any dependencies related to it.

        :param InvocationId waited_invocation_id: The ID of the invocation that has finished and will no longer block other invocations.
        """
        with self._lock:
            for waiter_id in self.waited_by.get(waited_invocation_id, []):
                self.waiting_for[waiter_id].discard(waited_invocation_id)
                if not self.waiting_for[waiter_id]:
                    del self.waiting_for[waiter_id]
                    # waiter_id no longer waiting; if it's waited on, it's now ready
                    if waiter_id in self.waited_by:
                        self._ready.add(waiter_id)
            self.waited_by.pop(waited_invocation_id, None)
            self.waiting_for.pop(waited_invocation_id, None)
            self._ready.discard(waited_invocation_id)

    def get_blocking_invocations(
        self, max_num_invocations: int
    ) -> Iterator["InvocationId"]:
        """
        Retrieves invocations that are blocking others but are not themselves waiting for any results.

        Uses a maintained ready set for O(1) lookup instead of scanning all keys.

        :param int max_num_invocations: The maximum number of blocking invocations to retrieve.
        :return: An iterator over invocations that are blocking others.
        :rtype: Iterator["InvocationId"]
        """
        with self._lock:
            candidates = list(self._ready)
        for inv_id in candidates:
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
        self._hash: int | None = None

    def __hash__(self) -> int:
        """Generate a hash that works with serialized values, cached after first call."""
        if self._hash is None:
            if isinstance(self.value, str):
                self._hash = hash((self.key, self.value))
            else:
                self._hash = hash((self.key, pickle.dumps(self.value)))
        return self._hash

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
    including blocking controls, as well as caching of invocation statuses and retries.

    ```{warning}
        This orchestrator is not intended for production use.
        As it stores all invocations in the running process memory.
    ```

    :param Pynenc app: The Pynenc application instance.
    """

    def __init__(self, app: "Pynenc") -> None:
        self.app = app
        self.task_id_to_inv_id: dict[TaskId, set[InvocationId]] = defaultdict(set)
        self.call_id_to_inv_id: dict[CallId, set[InvocationId]] = defaultdict(set)
        self.inv_id_to_call_id: dict[InvocationId, CallId] = {}
        self.invocation_args: dict[InvocationId, set[ArgPair]] = defaultdict(set)
        self.args_index: dict[ArgPair, set[InvocationId]] = defaultdict(set)
        self.status_index: dict[InvocationStatus, set[InvocationId]] = defaultdict(set)
        self.invocation_status_record: dict[InvocationId, InvocationStatusRecord] = {}
        self.invocation_retries: dict[InvocationId, int] = {}
        self.invocations_to_purge: deque[tuple[float, InvocationId]] = deque()
        self.locks: dict[InvocationId, threading.Lock] = {}

        # Runner heartbeat tracking
        self.runner_creation_time: dict[str, float] = {}
        self.runner_last_heartbeat: dict[str, float] = {}
        self.runner_last_service_start: dict[str, datetime] = {}
        self.runner_last_service_end: dict[str, datetime] = {}
        self.runner_atomic_service_eligible: dict[str, bool] = {}

        self._blocking_control: MemBlockingControl | None = None

        super().__init__(app)

    @property
    def blocking_control(self) -> "MemBlockingControl":
        if not self._blocking_control:
            self._blocking_control = MemBlockingControl(self.app)
        return self._blocking_control

    def _register_new_invocations(
        self,
        invocations: list["DistributedInvocation[Params, Result]"],
        runner_id: str | None = None,
    ) -> InvocationStatusRecord:
        """Registers new invocations and sets them to REGISTERED status."""
        status_record = InvocationStatusRecord(InvocationStatus.REGISTERED, runner_id)
        for invocation in invocations:
            self._interanl_atomic_status_transition(
                invocation.invocation_id, None, status_record
            )
            self.task_id_to_inv_id[invocation.call.task.task_id].add(
                invocation.invocation_id
            )
            self.call_id_to_inv_id[invocation.call.call_id].add(
                invocation.invocation_id
            )
            self.inv_id_to_call_id[invocation.invocation_id] = invocation.call.call_id
            self.invocation_retries[invocation.invocation_id] = 0
        return status_record

    def filter_by_key_arguments(
        self, key_arguments: dict[str, str]
    ) -> set["InvocationId"]:
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

    def filter_by_statuses(
        self, statuses: list[InvocationStatus]
    ) -> set["InvocationId"]:
        matched_ids = set()
        for status in statuses:
            matched_ids.update(self.status_index[status])
        return matched_ids

    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_serialized_arguments: dict[str, str] | None = None,
        statuses: "list[InvocationStatus] | None" = None,
    ) -> Iterator["InvocationId"]:
        """
        Retrieves invocation ids based on provided key arguments and/or status.

        :param dict[str, str] | None key_arguments: The key arguments to filter the invocations.
        :param list[InvocationStatus] | None status: The statuses to filter the invocations.
        :return: An iterator over the filtered invocations.
        :rtype: Iterator["InvocationId"]
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

    def get_task_invocation_ids(self, task_id: "TaskId") -> Iterator["InvocationId"]:
        """
        Retrieves all invocation ids for a given task id.

        :param TaskId task_id: The task id to filter the invocations.
        :return: An iterator over the invocation ids for the specified task.
        :rtype: Iterator["InvocationId"]
        """
        yield from self.task_id_to_inv_id.get(task_id, set())

    def get_invocation_ids_paginated(
        self,
        task_id: "TaskId | None" = None,
        statuses: list[InvocationStatus] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list["InvocationId"]:
        """
        Retrieves invocation IDs with pagination support.

        :param TaskId | None task_id: Optional task ID to filter by.
        :param list[InvocationStatus] | None statuses: Optional statuses to filter by.
        :param int limit: Maximum number of results to return.
        :param int offset: Number of results to skip.
        :return: List of matching invocation IDs.
        """
        # Build candidate set based on filters
        if task_id:
            candidates = self.task_id_to_inv_id.get(task_id, set()).copy()
        else:
            # Collect all invocation IDs across all tasks
            candidates = set()
            for inv_ids in self.task_id_to_inv_id.values():
                candidates.update(inv_ids)

        # Filter by statuses if provided
        if statuses:
            status_matches = self.filter_by_statuses(statuses)
            candidates = candidates.intersection(status_matches)

        # Sort by status timestamp (newest first) for consistent pagination
        sorted_ids = sorted(
            candidates,
            key=lambda inv_id: (
                self.invocation_status_record.get(
                    inv_id, InvocationStatusRecord(InvocationStatus.REGISTERED)
                ).timestamp
            ),
            reverse=True,
        )

        # Apply pagination
        return sorted_ids[offset : offset + limit]

    def count_invocations(
        self,
        task_id: "TaskId | None" = None,
        statuses: list[InvocationStatus] | None = None,
    ) -> int:
        """
        Counts invocations matching the given filters.

        :param str | None task_id: Optional task ID to filter by.
        :param list[InvocationStatus] | None statuses: Optional statuses to filter by.
        :return: The total count of matching invocations.
        """
        if task_id:
            candidates = self.task_id_to_inv_id.get(task_id, set())
        else:
            candidates = set()
            for inv_ids in self.task_id_to_inv_id.values():
                candidates.update(inv_ids)

        if statuses:
            status_matches = self.filter_by_statuses(statuses)
            candidates = candidates.intersection(status_matches)

        return len(candidates)

    def get_call_invocation_ids(self, call_id: "CallId") -> Iterator["InvocationId"]:
        """Retrieves all invocation IDs associated with a specific call ID."""
        yield from self.call_id_to_inv_id.get(call_id, set())

    def set_up_invocation_auto_purge(self, invocation_id: "InvocationId") -> None:
        """
        Sets up an invocation for automatic purging after a specified time.

        :param InvocationId invocation_id: The ID of the invocation to be set up for auto-purge.
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

    def clean_up_invocation(self, invocation_id: "InvocationId") -> None:
        """
        Cleans up an invocation from the cache.

        :param InvocationId invocation_id: The ID of the invocation to be cleaned up.
        """
        self.release_waiters(invocation_id)

        invocation = self.app.state_backend.get_invocation(invocation_id)
        for key, value in invocation.call.serialized_arguments.items():
            self.args_index[ArgPair(key, value)].discard(invocation_id)
        self.status_index[self.invocation_status_record[invocation_id].status].discard(
            invocation_id
        )
        self.task_id_to_inv_id.get(invocation.task.task_id, set()).discard(
            invocation_id
        )
        self.call_id_to_inv_id.get(invocation.call.call_id, set()).discard(
            invocation_id
        )
        self.inv_id_to_call_id.pop(invocation_id, None)
        if args := self.invocation_args.pop(invocation_id, None):
            for arg in args:
                self.args_index[arg].discard(invocation_id)
        self.status_index[self.invocation_status_record[invocation_id].status].discard(
            invocation_id
        )
        self.invocation_status_record.pop(invocation_id, None)
        self.invocation_retries.pop(invocation_id, None)

    def _get_invocation_lock(self, invocation_id: "InvocationId") -> threading.Lock:
        """Get or create a per-invocation lock for atomic transitions.

        :param invocation_id: The invocation to get the lock for.
        :return: A threading Lock for the given invocation.
        """
        if invocation_id not in self.locks:
            self.locks[invocation_id] = threading.Lock()
        return self.locks[invocation_id]

    def _atomic_status_transition(
        self,
        invocation_id: "InvocationId",
        status: InvocationStatus,
        runner_id: str | None = None,
    ) -> InvocationStatusRecord:
        """Sets the status record of a specific invocation.

        Uses per-invocation locking to prevent TOCTOU race conditions where
        multiple threads could read the same current status, both validate
        their transition, and both write — producing duplicate history entries.
        """
        lock = self._get_invocation_lock(invocation_id)
        with lock:
            prev_status_record = self.invocation_status_record.get(invocation_id)
            new_record = status_record_transition(prev_status_record, status, runner_id)
            return self._interanl_atomic_status_transition(
                invocation_id, prev_status_record, new_record
            )

    def _interanl_atomic_status_transition(
        self,
        invocation_id: "InvocationId",
        prev_status_record: InvocationStatusRecord | None,
        new_record: InvocationStatusRecord,
    ) -> InvocationStatusRecord:
        """Sets the status record of a specific invocation."""
        if prev_status_record:
            self.status_index[prev_status_record.status].discard(invocation_id)
        self.status_index[new_record.status].add(invocation_id)
        self.invocation_status_record[invocation_id] = new_record
        return new_record

    def index_arguments_for_concurrency_control(
        self,
        invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        for key, value in invocation.call.serialized_arguments.items():
            self.args_index[ArgPair(key, value)].add(invocation.invocation_id)

    def get_invocation_status_record(
        self, invocation_id: "InvocationId"
    ) -> InvocationStatusRecord:
        """Retrieves the current status of an invocation"""
        return self.invocation_status_record[invocation_id]

    def increment_invocation_retries(self, invocation_id: "InvocationId") -> None:
        """
        Increases the retry count for a given invocation.

        :param InvocationId invocation_id: The ID of the invocation for which the retry count is to be increased.
        """
        self.invocation_retries[invocation_id] = (
            self.invocation_retries.get(invocation_id, 0) + 1
        )

    def get_invocation_retries(self, invocation_id: "InvocationId") -> int:
        """
        Retrieves the current number of retries for a given invocation.

        :param InvocationId invocation_id: The ID of the invocation to get the retry count for.
        :return: The number of retries for the invocation.
        :rtype: int
        """
        return self.invocation_retries.get(invocation_id, 0)

    def filter_by_status(
        self,
        invocation_ids: list["InvocationId"],
        status_filter: frozenset["InvocationStatus"],
    ) -> list["InvocationId"]:
        if not invocation_ids or status_filter is None:
            return []
        return [
            inv_id
            for inv_id in invocation_ids
            if self.get_invocation_status(inv_id) in status_filter
        ]

    def register_runner_heartbeats(
        self, runner_ids: list[str], can_run_atomic_service: bool = False
    ) -> None:
        """Register or update heartbeat timestamps for one or more runners."""
        current_time = time()

        for runner_id in runner_ids:
            # Only update heartbeat for existing runners, or create new ones
            if runner_id not in self.runner_creation_time:
                self.runner_creation_time[runner_id] = current_time

            self.runner_last_heartbeat[runner_id] = current_time
            self.runner_atomic_service_eligible[runner_id] = can_run_atomic_service

    def _get_active_runners(
        self, timeout_seconds: float, can_run_atomic_service: bool | None = None
    ) -> list[ActiveRunnerInfo]:
        """Retrieve all active runners with heartbeat information."""
        current_time = time()
        cutoff_time = current_time - timeout_seconds

        active_runners = []
        for runner_id, last_heartbeat in self.runner_last_heartbeat.items():
            if last_heartbeat < cutoff_time:
                continue
            allow_to_run_atomic_service = self.runner_atomic_service_eligible[runner_id]
            if (
                can_run_atomic_service is not None
                and allow_to_run_atomic_service != can_run_atomic_service
            ):
                continue
            creation_ts = self.runner_creation_time[runner_id]
            service_start = self.runner_last_service_start.get(runner_id)
            service_end = self.runner_last_service_end.get(runner_id)
            active_runners.append(
                ActiveRunnerInfo(
                    runner_id=runner_id,
                    creation_time=datetime.fromtimestamp(creation_ts, tz=UTC),
                    last_heartbeat=datetime.fromtimestamp(last_heartbeat, tz=UTC),
                    allow_to_run_atomic_service=allow_to_run_atomic_service,
                    last_service_start=service_start,
                    last_service_end=service_end,
                )
            )

        # Sort by creation time (oldest first)
        active_runners.sort(key=lambda info: info.creation_time)

        return active_runners

    def record_atomic_service_execution(
        self, runner_id: str, start_time: datetime, end_time: datetime
    ) -> None:
        """Record the latest atomic service execution window for a runner."""
        self.runner_last_service_start[runner_id] = start_time
        self.runner_last_service_end[runner_id] = end_time

    def get_pending_invocations_for_recovery(self) -> Iterator["InvocationId"]:
        """Retrieve invocation IDs stuck in PENDING status beyond the allowed time."""
        max_pending_seconds = self.app.conf.max_pending_seconds
        current_time = time()
        cutoff_time = current_time - max_pending_seconds

        # Create a snapshot to avoid RuntimeError when status changes during iteration
        pending_invocations = list(
            self.status_index.get(InvocationStatus.PENDING, set())
        )

        for invocation_id in pending_invocations:
            status_record = self.invocation_status_record.get(invocation_id)
            if status_record and status_record.timestamp.timestamp() <= cutoff_time:
                yield invocation_id

    def _get_running_invocations_for_recovery(
        self, timeout_seconds: float
    ) -> Iterator["InvocationId"]:
        """Retrieve RUNNING invocation IDs owned by inactive runners."""
        current_time = time()
        cutoff_time = current_time - timeout_seconds

        # Get set of active runner IDs (those with recent heartbeats)
        active_runner_ids = {
            runner_id
            for runner_id, last_heartbeat in self.runner_last_heartbeat.items()
            if last_heartbeat >= cutoff_time
        }

        # Create a snapshot to avoid RuntimeError when status changes during iteration
        running_invocations = list(
            self.status_index.get(InvocationStatus.RUNNING, set())
        )

        for invocation_id in running_invocations:
            status_record = self.invocation_status_record.get(invocation_id)
            if (
                status_record
                and status_record.runner_id
                and status_record.runner_id not in active_runner_ids
            ):
                yield invocation_id

    def purge(self) -> None:
        self._blocking_control = None

        self.task_id_to_inv_id.clear()
        self.inv_id_to_call_id.clear()
        self.call_id_to_inv_id.clear()
        self.invocation_args.clear()
        self.args_index.clear()
        self.status_index.clear()
        self.invocation_status_record.clear()
        self.invocation_retries.clear()
        self.invocations_to_purge.clear()
        self.locks.clear()

        self.runner_creation_time.clear()
        self.runner_last_heartbeat.clear()
        self.runner_last_service_start.clear()
        self.runner_last_service_end.clear()
