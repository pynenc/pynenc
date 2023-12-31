from functools import cached_property
from time import time
from typing import TYPE_CHECKING, Iterator, Optional

import redis

from ..call import Call
from ..conf.config_orchestrator import ConfigOrchestratorRedis
from ..exceptions import CycleDetectedError, PendingInvocationLockError
from ..invocation import DistributedInvocation, InvocationStatus
from ..types import Params, Result
from ..util.redis_keys import Key
from .base_orchestrator import BaseBlockingControl, BaseCycleControl, BaseOrchestrator

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..task import Task


class RedisCycleControl(BaseCycleControl):
    """A directed acyclic graph representing the call dependencies"""

    def __init__(self, app: "Pynenc", client: redis.Redis) -> None:
        self.app = app
        self.key = Key(app.app_id, "cycle_control")
        self.client = client

    def purge(self) -> None:
        self.key.purge(self.client)

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
        self.client.set(self.key.invocation(caller.invocation_id), caller.to_json())
        self.client.set(self.key.invocation(callee.invocation_id), callee.to_json())
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
        """Remove all edges from a call"""
        callee_calls = self.client.smembers(self.key.edge(call_id))
        self.client.delete(self.key.edge(call_id))
        for callee_call_id in callee_calls:
            self.remove_edges(callee_call_id)

    def clean_up_invocation_cycles(self, invocation: "DistributedInvocation") -> None:
        """
        Remove an invocation from the graph. Also removes any edges to or from the invocation.
        """
        self.client.delete(self.key.invocation(invocation.invocation_id))
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
    """A directed acyclic graph representing the call dependencies"""

    def __init__(self, app: "Pynenc", client: redis.Redis) -> None:
        self.app = app
        self.key = Key(app.app_id, "blocking_control")
        self.client = client

    def purge(self) -> None:
        self.key.purge(self.client)

    def waiting_for_results(
        self, waiter: "DistributedInvocation", waiteds: list["DistributedInvocation"]
    ) -> None:
        """
        Register that an invocation (waiter) is waiting for the results of another invocation (waited).
        """
        waited_invocation_ids = []
        for waited in waiteds:
            waited_invocation_ids.append(waited.invocation_id)
            self.client.set(self.key.invocation(waited.invocation_id), waited.to_json())
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
        Remove an invocation from the graph. Also removes any edges to or from the invocation.
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
        Returns the invocations that are blocking others but not waiting for anything themselves.
        The oldest invocations are returned first.

        Parameters
        ----------
        max_num_invocations : int
            The maximum number of invocations to return.

        Returns
        -------
        list[DistributedInvocation]
            A list of blocking invocations or an empty list if no invocations are blocking.
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
                if inv := self.client.get(self.key.invocation(invocation_id)):
                    invocation = DistributedInvocation.from_json(self.app, inv.decode())
                    if self.app.orchestrator.get_invocation_status(
                        invocation
                    ).is_available_for_run():
                        max_num_invocations -= 1
                        yield invocation


class StatusNotFound(Exception):
    """Raised when a status is not found in Redis"""


class TaskRedisCache:
    def __init__(self, app: "Pynenc", client: redis.Redis) -> None:
        self.app = app
        self.client = client
        self.key = Key(app.app_id, "orchestrator")

    def purge(self) -> None:
        self.key.purge(self.client)

    def set_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: "InvocationStatus",
    ) -> None:
        task_id = invocation.task.task_id
        invocation_id = invocation.invocation_id
        try:
            previous_status = self._get_invocation_status(invocation.invocation_id)
            # already exists in Redis, remove from previous status
            self.client.srem(self.key.status(task_id, previous_status), invocation_id)
        except StatusNotFound:
            # new invocation, init invocation in Redis
            for arg, val in invocation.serialized_arguments.items():
                self.client.sadd(self.key.args(task_id, arg, val), invocation_id)
            self.client.set(self.key.invocation(invocation_id), invocation.to_json())
            self.client.sadd(self.key.task(task_id), invocation_id)
        self.client.sadd(self.key.status(task_id, status), invocation_id)
        self.client.set(self.key.invocation_status(invocation_id), status.value)
        if status != InvocationStatus.PENDING:
            self.clean_pending_status(invocation)

    def set_pending_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
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
        self.client.zadd(
            self.key.invocation_auto_purge(),
            {invocation.invocation_id: time()},
        )

    def auto_purge(self) -> None:
        end_time = (
            time() - self.app.orchestrator.conf.auto_final_invocation_purge_hours * 3600
        )
        for _invocation_id in self.client.zrangebyscore(
            self.key.invocation_auto_purge(), 0, end_time
        ):
            invocation_id = _invocation_id.decode()
            if inv := self.client.get(self.key.invocation(invocation_id)):
                invocation = DistributedInvocation.from_json(self.app, inv.decode())
                self.client.delete(self.key.invocation(invocation_id))
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
            self.client.delete(self.key.invocation_status(invocation_id))
            self.client.zrem(self.key.invocation_auto_purge(), invocation_id)
            self.client.delete(self.key.pending_timer(invocation_id))
            self.client.delete(self.key.previous_status(invocation_id))

    def clean_pending_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        self.client.delete(self.key.pending_timer(invocation.invocation_id))
        self.client.delete(self.key.previous_status(invocation.invocation_id))

    def _get_invocation_status(self, invocation_id: str) -> "InvocationStatus":
        if encoded_status := self.client.get(self.key.invocation_status(invocation_id)):
            return InvocationStatus(encoded_status.decode())
        raise StatusNotFound(f"Invocation status {invocation_id} not found in Redis")

    def get_invocation_status(
        self, invocation: "DistributedInvocation"
    ) -> "InvocationStatus":
        status = self._get_invocation_status(invocation.invocation_id)
        if status == InvocationStatus.PENDING:
            if encoded_pending_timer := self.client.get(
                self.key.pending_timer(invocation.invocation_id)
            ):
                elapsed = time() - float(encoded_pending_timer.decode())
                if elapsed > self.app.conf.max_pending_seconds:
                    if encoded_previous_status := self.client.get(
                        self.key.previous_status(invocation.invocation_id)
                    ):
                        previous_status = InvocationStatus(
                            encoded_previous_status.decode()
                        )
                        self.set_status(invocation, previous_status)
                        return previous_status
        return status

    def get_invocation_retries(self, invocation: "DistributedInvocation") -> int:
        if encoded_retries := self.client.get(
            self.key.invocation_retries(invocation.invocation_id)
        ):
            return int(encoded_retries.decode())
        return 0

    def increment_invocation_retries(self, invocation: "DistributedInvocation") -> None:
        self.client.incr(self.key.invocation_retries(invocation.invocation_id))

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


class RedisOrchestrator(BaseOrchestrator):
    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.client = redis.Redis(
            host=self.conf.redis_host, port=self.conf.redis_port, db=self.conf.redis_db
        )
        self.redis_cache = TaskRedisCache(app, self.client)
        self._cycle_control: Optional[RedisCycleControl] = None
        self._blocking_control: Optional[RedisBlockingControl] = None

    @cached_property
    def conf(self) -> ConfigOrchestratorRedis:
        return ConfigOrchestratorRedis(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

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
        status: Optional["InvocationStatus"] = None,
    ) -> Iterator["DistributedInvocation"]:
        return self.redis_cache.get_invocations(
            task.task_id, key_serialized_arguments, status
        )

    def _set_invocation_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: "InvocationStatus",
    ) -> None:
        self.redis_cache.set_status(invocation, status)
        self.app.logger.debug(f"Set status of invocation {invocation} to {status}")

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

    def increase_retries(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        self.redis_cache.increment_invocation_retries(invocation)

    def get_invocation_retries(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> int:
        return self.redis_cache.get_invocation_retries(invocation)

    def increment_invocation_retries(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        self.redis_cache.increment_invocation_retries(invocation)

    def purge(self) -> None:
        """Remove all invocations from the orchestrator"""
        self.redis_cache.purge()
        self.cycle_control.purge()
        self.blocking_control.purge()
