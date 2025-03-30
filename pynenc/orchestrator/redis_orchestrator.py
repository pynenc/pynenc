import threading
from concurrent.futures import Future, ThreadPoolExecutor
from functools import cached_property
from time import time
from typing import TYPE_CHECKING, Iterator, Optional

import redis

from pynenc.call import Call
from pynenc.conf.config_orchestrator import ConfigOrchestratorRedis
from pynenc.exceptions import CycleDetectedError, PendingInvocationLockError
from pynenc.invocation.dist_invocation import DistributedInvocation
from pynenc.invocation.status import InvocationStatus
from pynenc.orchestrator.base_orchestrator import (
    BaseBlockingControl,
    BaseCycleControl,
    BaseOrchestrator,
)
from pynenc.types import Params, Result
from pynenc.util.redis_client import get_redis_client
from pynenc.util.redis_keys import Key

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.task import Task


# Thread registry to avoid duplicating status update threads for the same invocation
_pending_resolution_threads: dict[str, Future[None]] = {}
_registry_lock = threading.Lock()


def _clean_dead_threads() -> None:
    """Remove completed futures from the registry to prevent memory leaks."""
    with _registry_lock:
        completed_keys = [
            inv_id
            for inv_id, future in _pending_resolution_threads.items()
            if future.done()
        ]
        for inv_id in completed_keys:
            _pending_resolution_threads.pop(inv_id)


class StatusNotFound(Exception):
    """Raised when a status is not found in Redis"""


class RedisCycleControl(BaseCycleControl):
    """
    A Redis-based implementation of cycle control using a directed acyclic graph (DAG).

    This class manages the dependencies between task invocations in Redis
    to prevent cycles in task calling patterns, which could lead to deadlocks or infinite loops.

    :param Pynenc app: The Pynenc application instance.
    :param redis.Redis client: The Redis client instance.
    """

    def __init__(self, app: "Pynenc", client: redis.Redis) -> None:
        self.app = app
        self.key = Key(app.app_id, "cycle_control")
        self.client = client

    def purge(self) -> None:
        """
        Purges all data related to cycle control from Redis.
        This includes all stored invocations, calls, and their relationships.
        """
        self.key.purge(self.client)

    def add_call_and_check_cycles(
        self, caller: "DistributedInvocation", callee: "DistributedInvocation"
    ) -> None:
        """
        Adds a new call dependency between `caller` and `callee` invocations and checks for potential cycles.
        :param DistributedInvocation caller: The invocation that is making the call.
        :param DistributedInvocation callee: The invocation that is being called.
        :raises CycleDetectedError: If adding the call creates a cycle.
        """
        if caller.call_id == callee.call_id:
            raise CycleDetectedError.from_cycle([caller.call])
        if cycle := self.find_cycle_caused_by_new_invocation(caller, callee):
            raise CycleDetectedError.from_cycle(cycle)
        self.client.sadd(
            self.key.call_to_invocation(caller.call_id), caller.invocation_id
        )
        self.client.sadd(
            self.key.call_to_invocation(callee.call_id), callee.invocation_id
        )
        self.client.set(self.key.call(caller.call_id), caller.call.to_json())
        self.client.set(self.key.call(callee.call_id), callee.call.to_json())
        # self.calls[caller.call_id] = caller.call # todo check if it work with invocation_id
        # self.calls[callee.call_id] = callee.call
        # self.call_to_invocation[caller.call_id][caller.invocation_id] = caller
        # self.call_to_invocation[callee.call_id][callee.invocation_id] = callee
        # self.edges[caller.call_id].add(callee.call_id)
        self.client.sadd(self.key.edge(caller.call_id), callee.call_id)

    def remove_edges(self, call_id: str) -> None:
        """
        Recursively removes all edges from a given call in the graph.
        :param call_id: The ID of the call from which to remove edges.
        """
        callee_calls = self.client.smembers(self.key.edge(call_id))
        self.client.delete(self.key.edge(call_id))
        for callee_call_id in callee_calls:
            self.remove_edges(callee_call_id)

    def clean_up_invocation_cycles(self, invocation: "DistributedInvocation") -> None:
        """
        Cleans up the graph by removing a given invocation and its associated edges.
        :param DistributedInvocation invocation: The `DistributedInvocation` instance to be removed.
        """
        self.client.srem(
            self.key.call_to_invocation(invocation.call_id), invocation.invocation_id
        )
        remaining_call_invocations = self.client.smembers(
            self.key.call_to_invocation(invocation.call_id)
        )
        if not remaining_call_invocations:
            self.client.delete(self.key.call(invocation.call_id))
            self.client.delete(self.key.call_to_invocation(invocation.call_id))
            self.remove_edges(invocation.call_id)

    def find_cycle_caused_by_new_invocation(
        self, caller: "DistributedInvocation", callee: "DistributedInvocation"
    ) -> list["Call"]:
        """
        Checks if adding a new call from `caller` to `callee` would create a cycle.
        :param DistributedInvocation caller: The invocation making the call.
        :param DistributedInvocation callee: The invocation being called.
        :return: List of `Call` objects forming the cycle, if a cycle is detected; otherwise, an empty list.
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
        """
        A utility function for cycle detection.
        :param str current_call_id: The current call ID being examined.
        :param set[str] visited: A set of visited call IDs for cycle detection.
        :param list[str] path: A list representing the current path of call IDs.
        :return: List of `Call` objects forming a cycle, if a cycle is detected; otherwise, an empty list.
        """
        visited.add(current_call_id)
        path.append(current_call_id)

        call_cycle = []
        for _neighbour_call_id in self.client.smembers(self.key.edge(current_call_id)):
            neighbour_call_id = _neighbour_call_id.decode()
            if neighbour_call_id not in visited:
                cycle = self._is_cyclic_util(neighbour_call_id, visited, path)
                if cycle:
                    return cycle
            elif neighbour_call_id in path:
                cycle_start_index = path.index(neighbour_call_id)
                for _id in path[cycle_start_index:]:
                    if call_json := self.client.get(self.key.call(_id)):
                        call_cycle.append(Call.from_json(self.app, call_json.decode()))
        path.pop()
        return call_cycle


class RedisBlockingControl(BaseBlockingControl):
    """
    A Redis-based implementation of blocking control for task invocations.

    Manages invocation dependencies and blocking states in a Redis-backed environment,
    ensuring that invocations waiting for others are properly tracked and released.

    :param Pynenc app: The Pynenc application instance.
    :param redis.Redis client: The Redis client instance.
    """

    def __init__(self, app: "Pynenc", client: redis.Redis) -> None:
        self.app = app
        self.key = Key(app.app_id, "blocking_control")
        self.client = client

    def purge(self) -> None:
        """
        Purges all data related to blocking control from Redis.
        This includes all stored invocations and their waiting relationships.
        """
        self.key.purge(self.client)

    def waiting_for_results(
        self, waiter: "DistributedInvocation", waiteds: list["DistributedInvocation"]
    ) -> None:
        """
        Registers that an invocation (waiter) is waiting for the results of other invocations (waiteds).
        :param DistributedInvocation waiter: The invocation that is waiting.
        :param DistributedInvocation waiteds: A list of invocations that the waiter is waiting for.
        """
        waited_invocation_ids = []
        for waited in waiteds:
            waited_invocation_ids.append(waited.invocation_id)
            self.client.set(
                self.key.invocation(waited.invocation_id), waited.invocation_id
            )
            self.client.sadd(
                self.key.waited_by(waited.invocation_id), waiter.invocation_id
            )
            # Add the waited invocation to the 'all_waited' sorted set with the current time as the score
            self.client.zadd(self.key.all_waited(), {waited.invocation_id: time()})
            # If the waited invocation is not waiting for anything else, add it to the 'not_waiting' sorted set
            if not self.client.exists(self.key.waiting_for(waited.invocation_id)):
                self.client.zadd(self.key.not_waiting(), {waited.invocation_id: time()})
        # If the waiter is in the 'not_waiting' sorted set, remove it
        if self.client.zscore(self.key.not_waiting(), waiter.invocation_id) is not None:
            self.client.zrem(self.key.not_waiting(), waiter.invocation_id)
        self.client.sadd(
            self.key.waiting_for(waiter.invocation_id), *waited_invocation_ids
        )

    def release_waiters(self, invocation: "DistributedInvocation") -> None:
        """
        Removes an invocation from the tracking system. Also removes any dependencies related to the invocation.
        :param DistributedInvocation invocation: The `DistributedInvocation` instance to be removed.
        """
        # for each invocation thas is waiting for the invocation
        for waited_invocation_id in self.client.smembers(
            self.key.waited_by(invocation.invocation_id)
        ):
            # remove the invocation from the list of invocations waited by the waiter
            self.client.srem(
                self.key.waiting_for(waited_invocation_id.decode()),
                invocation.invocation_id,
            )
            # if the waiter is not waiting for anything else, add it to the 'not_waiting' sorted set
            if not self.client.exists(self.key.waiting_for(waited_invocation_id)):
                self.client.zadd(self.key.not_waiting(), {waited_invocation_id: time()})
        self.client.delete(self.key.invocation(invocation.invocation_id))
        self.client.delete(self.key.waiting_for(invocation.invocation_id))
        self.client.delete(self.key.waited_by(invocation.invocation_id))
        self.client.zrem(self.key.all_waited(), invocation.invocation_id)
        self.client.zrem(self.key.not_waiting(), invocation.invocation_id)

    def get_blocking_invocations(
        self, max_num_invocations: int
    ) -> Iterator["DistributedInvocation[Params, Result]"]:
        """
        Returns the invocations that are blocking others but are not waiting for anything themselves.
        :param int max_num_invocations: The maximum number of blocking invocations to retrieve.
        :return: An iterator of blocking `DistributedInvocation` instances.
        """
        index = 0
        page_size = max(10, max_num_invocations)  # adjust as needed
        while max_num_invocations > 0:
            if not (
                page := self.client.zrange(
                    self.key.not_waiting(), index, index + page_size - 1
                )
            ):
                break
            index += page_size
            for waited_invocation_id in page:
                invocation_id = waited_invocation_id.decode()
                # Ugly workaround
                # invocation existance holds some logic, use a set or something else
                # ! remove/fix this with issue 90 https://github.com/pynenc/pynenc/issues/90
                val_inv_id = self.client.get(self.key.invocation(invocation_id))
                if not val_inv_id:
                    continue
                if invocation := self.app.state_backend.get_invocation(
                    val_inv_id.decode()
                ):
                    try:
                        status = self.app.orchestrator.get_invocation_status(invocation)
                        if status.is_available_for_run():
                            max_num_invocations -= 1
                            yield invocation
                    except StatusNotFound:
                        self.app.logger.warning(
                            f"Skipping invocation {invocation_id} in get_blocking_invocations: "
                            "status not found in Redis"
                        )
                if max_num_invocations == 0:
                    break


class TaskRedisCache:
    """
    A Redis-based cache for managing task invocation statuses and retries.

    Provides methods to set and get the status and retry counts of task invocations, leveraging Redis for scalable and efficient storage.

    :param Pynenc app: The Pynenc application instance.
    :param redis.Redis client: The Redis client instance.
    """

    def __init__(self, app: "Pynenc", client: redis.Redis) -> None:
        self.app = app
        self.client = client
        self.key = Key(app.app_id, "orchestrator")
        self._executor = ThreadPoolExecutor(
            max_workers=self.conf.max_pending_resolution_threads
        )

    @property
    def conf(self) -> ConfigOrchestratorRedis:
        return self.app.orchestrator.conf

    def purge(self) -> None:
        """
        Purges all data related to task invocations from Redis.
        """
        self.key.purge(self.client)

    def _set_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: "InvocationStatus",
        previous_status: Optional["InvocationStatus"],
        pipeline: redis.client.Pipeline,
    ) -> None:
        """
        Inner method to set the status of a single invocation using a provided pipeline.

        :param DistributedInvocation invocation: The invocation to update.
        :param InvocationStatus status: The new status to set.
        :param Optional[InvocationStatus] previous_status: The previous status, or None if it's a new invocation.
        :param redis.client.Pipeline pipeline: The Redis pipeline to use for commands.
        """
        task_id = invocation.task.task_id
        invocation_id = invocation.invocation_id

        if previous_status is not None:
            # already exists in Redis, remove from previous status
            pipeline.srem(self.key.status(task_id, previous_status), invocation_id)
        else:
            # New invocation, initialize in Redis
            for arg, val in invocation.serialized_arguments.items():
                pipeline.sadd(self.key.args(task_id, arg, val), invocation_id)
            pipeline.sadd(self.key.task(task_id), invocation_id)

        # Set new status
        pipeline.sadd(self.key.status(task_id, status), invocation_id)
        pipeline.set(self.key.invocation_status(invocation_id), status.value)

        # Clean up pending status if applicable
        if status != InvocationStatus.PENDING:
            pipeline.delete(self.key.pending_timer(invocation_id))
            pipeline.delete(self.key.previous_status(invocation_id))

    def set_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: "InvocationStatus",
    ) -> None:
        """
        Set the status of a single invocation in Redis, optionally using a provided pipeline.

        :param DistributedInvocation invocation: The invocation to update.
        :param InvocationStatus status: The new status to set.
        :param Optional[redis.client.Pipeline] pipeline: Optional Redis pipeline; if None, creates and executes one.
        """
        pipeline = self.client.pipeline(transaction=True)
        try:
            previous_status = self._get_invocation_status(invocation.invocation_id)
        except StatusNotFound:
            previous_status = None
        self._set_status(invocation, status, previous_status, pipeline)
        pipeline.execute()
        invocation.update_status_cache(status)

    def set_batch_status(
        self,
        invocations: list["DistributedInvocation[Params, Result]"],
        status: "InvocationStatus",
    ) -> None:
        """
        Set the status of multiple invocations at once using a Redis pipeline.

        :param list[DistributedInvocation] invocations: The invocations to update.
        :param InvocationStatus status: The status to set.
        """
        invocation_ids = [inv.invocation_id for inv in invocations]
        status_keys = [self.key.invocation_status(inv_id) for inv_id in invocation_ids]
        statuses = self.client.mget(status_keys)  # Single round-trip

        # Map statuses to invocations (None if not found)
        previous_statuses = {
            inv.invocation_id: InvocationStatus(status.decode()) if status else None
            for inv, status in zip(invocations, statuses)
        }

        with self.client.pipeline(transaction=True) as pipe:
            for invocation in invocations:
                prev_status = previous_statuses.get(invocation.invocation_id)
                self._set_status(invocation, status, prev_status, pipe)
            pipe.execute()
            self.app.logger.debug(
                f"Batch set status of {len(invocations)} invocations to {status}"
            )

    def set_pending_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        """
        Set the status of an invocation to pending in Redis.

        ```{danger}
            We have to ensure that the invocation is not already pending.
            Otherwise, we could end up with a deadlock.
        ```

        :param DistributedInvocation invocation: The invocation to be updated.
        :raises PendingInvocationLockError: If was not possible to lock the Invocation.
        """
        invocation_id = invocation.invocation_id
        lock = self.client.lock(
            f"lock:pending_status:{invocation_id}",
            blocking_timeout=self.app.conf.max_pending_seconds,
        )
        if not lock.acquire(blocking=True):
            raise PendingInvocationLockError(invocation_id)
        try:
            self.client.set(self.key.pending_timer(invocation_id), time())
            previous_status = self.get_invocation_status(invocation)
            if previous_status == InvocationStatus.PENDING:
                raise PendingInvocationLockError(invocation_id)
            self.client.set(
                self.key.previous_status(invocation_id), previous_status.value
            )
            self.set_status(invocation, InvocationStatus.PENDING)
        finally:
            lock.release()

    def set_up_invocation_auto_purge(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        """
        Sets up an invocation for automatic purging after a specified time.

        :param DistributedInvocation invocation: The invocation to be updated.
        """
        self.client.zadd(
            self.key.invocation_auto_purge(),
            {invocation.invocation_id: time()},
        )

    def auto_purge(self) -> None:
        """
        Automatically purges invocations that have been in their final state beyond a specified duration.

        ```{note}
            The duration is specified in the configuration file using the `auto_final_invocation_purge_hours` parameter.
        ```
        """
        # TODO use expire, not auto_purge (at least for redis)
        end_time = (
            time() - self.app.orchestrator.conf.auto_final_invocation_purge_hours * 3600
        )
        for _invocation_id in self.client.zrangebyscore(
            self.key.invocation_auto_purge(), 0, end_time
        ):
            invocation_id = _invocation_id.decode()
            try:
                invocation = self.app.state_backend.get_invocation(invocation_id)
                task_id = invocation.task.task_id
                # clean up task keys
                self.client.srem(self.key.task(task_id), invocation_id)
                if not self.client.smembers(self.key.task(task_id)):
                    self.client.delete(self.key.task(task_id))
                # clean up task-status keys
                self.client.srem(
                    self.key.status(task_id, invocation.status), invocation_id
                )
                if not self.client.smembers(
                    self.key.status(task_id, invocation.status)
                ):
                    self.client.delete(self.key.status(task_id, invocation.status))
            except KeyError:
                self.app.logger.warning(f"{invocation_id=} not found during auto purge")
            self.client.delete(self.key.invocation_status(invocation_id))
            self.client.zrem(self.key.invocation_auto_purge(), invocation_id)
            self.client.delete(self.key.pending_timer(invocation_id))
            self.client.delete(self.key.previous_status(invocation_id))

    def clean_pending_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        """
        Cleans up the pending status of a task invocation.
        :param invocation: The task invocation instance.
        """
        self.client.delete(self.key.pending_timer(invocation.invocation_id))
        self.client.delete(self.key.previous_status(invocation.invocation_id))

    # @with_retry(
    #     StatusNotFound,
    #     max_retries=lambda self: self.conf.redis_retry_max_attempts,
    #     base_delay=lambda self: self.conf.redis_retry_base_delay_sec,
    #     max_delay=lambda self: self.conf.redis_retry_max_delay_sec,
    # )
    def _get_invocation_status(self, invocation_id: str) -> "InvocationStatus":
        """
        Gets the status of a specific task invocation.
        :param invocation_id: The ID of the task invocation.
        :return: The current status of the task invocation.
        """
        if encoded_status := self.client.get(self.key.invocation_status(invocation_id)):
            return InvocationStatus(encoded_status.decode())
        # redis_key = self.key.invocation_status(invocation_id)
        # self.app.logger.warning(f"STATUS NOT FOUND {invocation_id=} {redis_key=}")
        raise StatusNotFound(f"Invocation status {invocation_id} not found in Redis")

    def get_invocation_status(
        self, invocation: "DistributedInvocation"
    ) -> "InvocationStatus":
        """
        Retrieves the status of a task invocation.
        :param invocation: The task invocation instance.
        :return: The current status of the task invocation.
        """
        status = self._get_invocation_status(invocation.invocation_id)
        if status == InvocationStatus.PENDING:
            self._start_async_pending_resolution_for_invocation(invocation)
        return status

    def _async_resolve_pending_status(
        self, invocation: "DistributedInvocation"
    ) -> None:
        """
        Asynchronously resolve and update an expired PENDING status.

        This method is designed to run in a background thread.

        :param str invocation_id: The ID of the invocation to update
        """
        try:
            pending_timer_key = self.key.pending_timer(invocation.invocation_id)
            encoded_pending_timer = self.client.get(pending_timer_key)
            if not encoded_pending_timer:
                # Pending timer already removed, nothing to do
                return
            elapsed = time() - float(encoded_pending_timer.decode())
            if elapsed <= self.app.conf.max_pending_seconds:
                # Not expired yet, nothing to do
                return
            # Check for previous status
            prev_status_key = self.key.previous_status(invocation.invocation_id)
            encoded_previous_status = self.client.get(prev_status_key)
            if not encoded_previous_status:
                # No previous status, can't restore
                return
            # Get the previous status
            previous_status = InvocationStatus(encoded_previous_status.decode())
            self.set_status(invocation, previous_status)

            self.app.logger.debug(
                f"Async resolved PENDING status for {invocation.invocation_id} to {previous_status}"
            )
        except Exception as e:
            self.app.logger.exception(
                f"Error in async PENDING resolution for {invocation.invocation_id}: {e}"
            )

    def _start_async_pending_resolution_for_invocation(
        self, invocation: "DistributedInvocation"
    ) -> None:
        """
        Start asynchronous resolution of a PENDING status.

        Uses ThreadPoolExecutor to manage a pool of worker threads.

        :param invocation: The invocation to check PENDING status for
        """
        invocation_id = invocation.invocation_id
        with _registry_lock:
            if invocation_id in _pending_resolution_threads:
                if not _pending_resolution_threads[invocation_id].done():
                    return
                _pending_resolution_threads.pop(invocation_id)

            future = self._executor.submit(
                self._async_resolve_pending_status, invocation
            )
            _pending_resolution_threads[invocation_id] = future
            if len(_pending_resolution_threads) > 1000:
                _clean_dead_threads()

    def get_invocation_retries(self, invocation: "DistributedInvocation") -> int:
        """
        Gets the number of retries for a specific task invocation.
        :param invocation: The task invocation instance.
        :return: The number of retries of the task invocation.
        """
        if encoded_retries := self.client.get(
            self.key.invocation_retries(invocation.invocation_id)
        ):
            return int(encoded_retries.decode())
        return 0

    def increment_invocation_retries(self, invocation: "DistributedInvocation") -> None:
        """
        Increments the retry count for a specific task invocation.
        :param invocation: The task invocation instance.
        """
        self.client.incr(self.key.invocation_retries(invocation.invocation_id))

    def get_invocations(
        self,
        task_id: str,
        key_arguments: Optional[dict[str, str]],
        statuses: Optional[list["InvocationStatus"]],
    ) -> Iterator["DistributedInvocation"]:
        """
        Retrieves task invocations based on task ID, key arguments, and status.
        :param str task_id: The ID of the task.
        :param Optional[dict[str, str]] key_arguments: Optional key arguments for filtering.
        :param Optional[list["InvocationStatus"]] statuses: Optional status for filtering.
        :return: An iterator of task invocations that match the criteria.
        """
        # Start with the set of obj_ids for the task_id
        invocation_ids = self.client.smembers(self.key.task(task_id))

        # If key_arguments were provided, intersect the current obj_ids with those matching each arg:val pair
        if key_arguments:
            for arg, val in key_arguments.items():
                arg_val_ids = self.client.smembers(self.key.args(task_id, arg, val))
                invocation_ids = invocation_ids.intersection(arg_val_ids)

        # If status was provided, intersect the current obj_ids with those matching the status
        if statuses:
            all_status_ids = set()
            for status in statuses or []:
                status_ids = self.client.smembers(self.key.status(task_id, status))
                all_status_ids.update(status_ids)
            invocation_ids = invocation_ids.intersection(all_status_ids)
        # Fetch invocations
        for invocation_id in invocation_ids:
            yield self.app.state_backend.get_invocation(invocation_id.decode())

    def filter_by_status(
        self,
        invocations: list[DistributedInvocation],
        status_filter: set["InvocationStatus"],
    ) -> list[DistributedInvocation]:
        """
        Get statuses for multiple invocations in a single Redis operation.

        This is a low-level method that fetches the raw statuses from Redis,
        performing minimal processing. Unlike get_invocation_status, this
        method may return PENDING status even if it has expired, as it's
        optimized for high-throughput batch queries.

        :param invocations: List of DistributedInvocation to get statuses for
        :return: list of DistributedInvocation with matching statuses
        """
        if not invocations or not status_filter:
            return []
        with self.client.pipeline(transaction=False) as pipe:
            for inv in invocations:
                pipe.get(self.key.invocation_status(inv.invocation_id))
            status_results = pipe.execute()
        filtered = []
        for i, inv in enumerate(invocations):
            status_bytes = status_results[i]
            if not status_bytes:
                continue
            status = InvocationStatus(status_bytes.decode())
            inv.update_status_cache(status)
            if status == InvocationStatus.PENDING:
                self._start_async_pending_resolution_for_invocation(inv)
            if status in status_filter:
                filtered.append(inv)
        return filtered

    def __del__(self) -> None:
        """Ensure thread pool is shut down properly when the object is destroyed."""
        if hasattr(self, "_executor") and self._executor:
            self._executor.shutdown(wait=False)


class RedisOrchestrator(BaseOrchestrator):
    """
    An orchestrator implementation using Redis for managing distributed task invocations.

    This orchestrator leverages Redis to handle invocation status, retries, auto-purge, and cycle and blocking controls in a distributed task environment. It extends `BaseOrchestrator` and integrates closely with Redis to provide efficient and scalable task orchestration.

    :param Pynenc app: The Pynenc application instance.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self._client: redis.Redis | None = None
        self._redis_cache: TaskRedisCache | None = None
        self._cycle_control: Optional[RedisCycleControl] = None
        self._blocking_control: Optional[RedisBlockingControl] = None

    @cached_property
    def conf(self) -> ConfigOrchestratorRedis:
        return ConfigOrchestratorRedis(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @property
    def client(self) -> redis.Redis:
        """Lazy initialization of Redis client"""
        if self._client is None:
            self._client = get_redis_client(self.conf)
        return self._client

    @property
    def redis_cache(self) -> TaskRedisCache:
        if not self._redis_cache:
            self._redis_cache = TaskRedisCache(self.app, self.client)
        return self._redis_cache

    @property
    def cycle_control(self) -> "RedisCycleControl":
        if not self._cycle_control:
            self._cycle_control = RedisCycleControl(self.app, self.client)
        return self._cycle_control

    @property
    def blocking_control(self) -> "RedisBlockingControl":
        if not self._blocking_control:
            self._blocking_control = RedisBlockingControl(self.app, self.client)
        return self._blocking_control

    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_serialized_arguments: Optional[dict[str, str]] = None,
        statuses: Optional[list["InvocationStatus"]] = None,
    ) -> Iterator["DistributedInvocation"]:
        return self.redis_cache.get_invocations(
            task.task_id, key_serialized_arguments, statuses
        )

    def get_invocation(self, invocation_id: str) -> Optional["DistributedInvocation"]:
        self.app.logger.warning("Use state_backend.get_invocation instead")
        try:
            return self.app.state_backend.get_invocation(invocation_id)
        except KeyError:
            return None

    def _set_invocation_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: "InvocationStatus",
    ) -> None:
        self.redis_cache.set_status(invocation, status)
        self.app.logger.debug(f"Set status of invocation {invocation} to {status}")

    def _set_invocations_status(
        self, invocations: list[DistributedInvocation], status: InvocationStatus
    ) -> None:
        """
        Set the status of multiple invocations at once using Redis pipeline.

        :param list[DistributedInvocation] invocations: The invocations to update.
        :param InvocationStatus status: The status to set.
        """
        if not invocations:
            return
        self.redis_cache.set_batch_status(invocations, status)
        self.app.logger.debug(f"Set {len(invocations)} invocations to {status=}")

    def _set_invocation_pending_status(
        self, invocation: "DistributedInvocation"
    ) -> None:
        self.redis_cache.set_pending_status(invocation)
        self.app.logger.debug(f"Set status of invocation {invocation} to pending")

    def set_up_invocation_auto_purge(
        self, invocation: DistributedInvocation[Params, Result]
    ) -> None:
        self.redis_cache.set_up_invocation_auto_purge(invocation)

    def auto_purge(self) -> None:
        self.redis_cache.auto_purge()

    def get_invocation_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "InvocationStatus":
        return self.redis_cache.get_invocation_status(invocation)

    def get_invocation_retries(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> int:
        return self.redis_cache.get_invocation_retries(invocation)

    def increment_invocation_retries(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        self.redis_cache.increment_invocation_retries(invocation)

    def filter_by_status(
        self,
        invocations: list["DistributedInvocation"],
        status_filter: set["InvocationStatus"] | None = None,
    ) -> list["DistributedInvocation"]:
        """
        Filters a list of invocations by their status in an optimized way.

        Uses batch Redis operations for better performance.

        :param list[DistributedInvocation] invocations: The invocations to filter
        :param list[InvocationStatus] | None status_filter: The statuses to filter by
        :return: List of invocations matching the status filter
        """
        if not invocations or not status_filter:
            return []
        return self.redis_cache.filter_by_status(invocations, status_filter)

    def purge(self) -> None:
        """Remove all invocations from the orchestrator"""
        self.redis_cache.purge()
        self.cycle_control.purge()
        self.blocking_control.purge()
