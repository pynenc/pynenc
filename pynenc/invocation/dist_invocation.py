from __future__ import annotations

import json
import time
from collections.abc import AsyncGenerator, Iterator
from typing import TYPE_CHECKING, Any, cast

from pynenc import context, exceptions
from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.invocation.base_invocation import (
    BaseInvocation,
    BaseInvocationGroup,
    InvocationIdentity,
)
from pynenc.invocation.status import InvocationStatus
from pynenc.types import Params, Result
from pynenc.util.asyncio_helper import run_task_sync
from pynenc.workflow.exceptions import WorkflowPauseError
from pynenc.workflow.identity import WorkflowIdentity

if TYPE_CHECKING:
    from pynenc.workflow import WorkflowContext

    from ..app import Pynenc


# Create a context variable to store current invocation
class DistributedInvocation(BaseInvocation[Params, Result]):
    """
    Represents a distributed invocation of a task call in the system.

    This class is a specific implementation of `BaseInvocation` for use in distributed environments. It extends the base invocation with additional features necessary for handling task executions in a distributed manner.

    In the distributed system, `DistributedInvocation` handles the execution of a task across different nodes or processes. It includes mechanisms for tracking and managing the state of an invocation in a distributed context.

    :param Call[Params, Result] call:
        Inherits the `call` attribute from `BaseInvocation`, representing the specific task call that this invocation executes.
    :param DistributedInvocation | None parent_invocation:
        A reference to a parent invocation, if this invocation is part of a nested call structure.
        This attribute is used to maintain the invocation hierarchy in complex task workflows.
    :param Optional[str] _invocation_id:
        A unique identifier for the invocation. This ID is crucial for tracking and orchestrating the invocation
        across the distributed system. It's assigned internally and used by the orchestration mechanism.
    :param bool _disable_upsert:
        A flag to disable the upsert operation during initialization. This flag is used to prevent unnecessary
        updates to the state backend when creating invocations in certain scenarios.
    """

    def __init__(
        self,
        call: Call[Params, Result],
        parent_invocation: DistributedInvocation | None = None,
        invocation_id: str | None = None,
        stored_in_backend: bool = False,
        workflow: WorkflowIdentity | None = None,
    ) -> None:
        # Call parent init
        super().__init__(call, parent_invocation, invocation_id, workflow)

        # Initialize additional state
        self._result: Result | None = None
        self._num_retries: int = 0

        # Initialize state fields
        self._cached_status: InvocationStatus | None = None
        self._cached_status_time: float = 0.0

        # Store in state backend
        self._stored_in_backend: bool = stored_in_backend
        self.store_in_backend()

    @property
    def parent_invocation(self) -> DistributedInvocation | None:
        """
        :return: the parent invocation of this invocation
        """
        return (
            cast(DistributedInvocation, self.identity.parent_invocation)
            if self.identity.parent_invocation
            else None
        )

    @property
    def wf(self) -> WorkflowContext:
        """Access workflow functionality for this invocation task."""
        return self.task.wf

    def store_in_backend(self) -> None:
        """Store the invocation in the state backend."""
        if not self._stored_in_backend:
            self._stored_in_backend = True
            self.app.state_backend.upsert_invocation(self)

    @property
    def num_retries(self) -> int:
        """:return: number of times the invocation got retried"""
        return self.app.orchestrator.get_invocation_retries(self)

    def update_status_cache(self, status: InvocationStatus) -> None:
        """Update the cached status and timestamp."""
        self._cached_status = status
        self._cached_status_time = time.time()

    @property
    def status(self) -> InvocationStatus:
        """:return: status of the invocation from the orchestrator"""
        # return self.app.orchestrator.get_invocation_status(self)
        # First check cache for final statuses - they never change
        if self._cached_status and self._cached_status.is_final():
            return self._cached_status
        # For non-final statuses, use a short cache (100ms) to avoid hammering Redis
        cache_ttl = self.app.conf.cached_status_time
        if self._cached_status and (time.time() - self._cached_status_time < cache_ttl):
            return self._cached_status
        # check status and update cache
        status = self.app.orchestrator.get_invocation_status(self)
        self.update_status_cache(status)
        return status

    def to_json(self) -> str:
        """:return: The serialized invocation"""
        inv_dict: dict = {}
        inv_dict["invocation_id"] = self.invocation_id
        inv_dict["call"] = self.call.to_json()
        inv_dict["workflow"] = self.workflow.to_json()
        if self.parent_invocation:
            inv_dict["parent_invocation"] = self.parent_invocation.to_json()
        inv_dict["_stored_in_backend"] = self._stored_in_backend
        return json.dumps(inv_dict)

    @classmethod
    def from_json(cls, app: Pynenc, serialized: str) -> DistributedInvocation:
        """:return: a new invocation from a serialized invocation"""
        inv_dict = json.loads(serialized)
        parent_invocation: DistributedInvocation | None = None
        if "parent_invocation" in inv_dict:
            parent_invocation = cls.from_json(app, inv_dict["parent_invocation"])
        return cls(
            call=Call.from_json(app, inv_dict["call"]),
            parent_invocation=parent_invocation,
            invocation_id=inv_dict["invocation_id"],
            stored_in_backend=inv_dict["_stored_in_backend"],
            workflow=WorkflowIdentity.from_json(inv_dict["workflow"]),
        )

    def __getstate__(self) -> dict:
        """Return a serialized state dict for pickling."""
        state: dict = {}
        state["identity"] = {
            "invocation_id": self.invocation_id,
            "call": self.call,
            "parent_invocation": self.parent_invocation,
        }
        state["state"] = {
            "cached_status": self._cached_status,
            "cached_status_time": self._cached_status_time,
            "stored_in_backend": self._stored_in_backend,
            "num_retries": self._num_retries,
            "result": self._result,
        }
        state["workflow"] = self.workflow.to_json()
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore state from a pickled dict."""
        # Recreate identity object (it's immutable, needs to be created properly)
        identity_data = state["identity"]
        self.identity = InvocationIdentity(
            call=identity_data["call"],
            invocation_id=identity_data["invocation_id"],
            parent_invocation=identity_data["parent_invocation"],
        )
        self.workflow = WorkflowIdentity.from_json(state["workflow"])
        # Restore mutable state directly
        state_data = state["state"]
        self._cached_status = state_data["cached_status"]
        self._cached_status_time = state_data["cached_status_time"]
        self._stored_in_backend = state_data["stored_in_backend"]
        self._num_retries = state_data["num_retries"]
        self._result = state_data["result"]
        # Initialize logger if needed
        self.init_logger()

    def swap_context(self) -> DistributedInvocation | None:
        """
        Swap the current invocation context with this invocation.

        This method is responsible for setting the current invocation context to this invocation.
        It uses the `context` module to manage the invocation context and ensure that the current invocation is tracked correctly.

        The method is used to manage the invocation context when executing the task associated with this invocation.
        It ensures that the current invocation is correctly set and tracked in the distributed environment.
        """
        return context.swap_dist_invocation_context(self.app.app_id, self)

    def reset_context(
        self, previous_invocation_context: DistributedInvocation | None
    ) -> None:
        """
        Reset the invocation context to a previous state.

        This method is responsible for resetting the current invocation context to a previous state.
        It uses the `context` module to manage the invocation context and ensures that the previous invocation context is restored correctly.

        :param Any previous_invocation_context:
            The previous invocation context to restore. This value is returned by the `swap_context` method.
        """
        context.swap_dist_invocation_context(
            self.app.app_id, previous_invocation_context
        )

    def run(self, runner_args: dict[str, Any] | None = None) -> None:
        """
        Execute the task associated with this invocation in a distributed environment.

        This method is responsible for running the task's function with its arguments, handling retriable exceptions,
        and updating the task's state in the orchestrator. It manages the invocation context and communicates with
        the orchestrator to set the invocation's run state and result.

        :param dict[str, Any] | None runner_args:
            Optional arguments passed from/to the runner. These arguments can be used for synchronization
            in subprocesses or other runner-specific tasks. Default is None.

        The method updates the orchestrator with the status of the invocation (`InvocationStatus`) and
        logs the execution process. In case of exceptions, especially retriable ones, it follows the defined
        retry logic as per the task's configuration.

        The return value or any raised exception is stored in the orchestrator and can be retrieved by the caller.

        ```{note}
            - If a retriable exception occurs and the number of retries has not been exceeded, the method
            will set the invocation for a retry and log a warning.
            - If the maximum retries are reached or a non-retriable exception occurs, the exception will be raised
            after updating the orchestrator.
        ```

        The invocation's context is managed to ensure the correct tracking of the current and parent invocations.

        :raises Exception:
            Raises the original exception if a non-retriable exception occurs or the maximum retries are reached for a retriable exception.
        """
        # runner_args are passed from/to the runner (e.g. used to sync subprocesses)
        context.runner_args = runner_args
        try:
            self.task.logger.info("Invocation STARTED")
            previous_invocation_context = self.swap_context()
            if not self.app.orchestrator.is_authorize_to_run_by_concurrency_control(
                self
            ):
                self.app.orchestrator.reroute_invocations({self})
            self.app.orchestrator.set_invocation_run(self.parent_invocation, self)
            result = run_task_sync(self.task.func, **self.arguments.kwargs)
            self.app.orchestrator.set_invocation_result(self, result)
            self.task.logger.info("Invocation FINISHED")
        except WorkflowPauseError as ex:
            self.task.logger.warning(
                f"Workflow pause requested but not implemented yet: {ex.reason}"
            )
        except self.task.retriable_exceptions as ex:
            if self.num_retries >= self.task.conf.max_retries:
                self.app.logger.exception("Invocation MAX-RETRY")
                self.app.orchestrator.set_invocation_exception(self, ex)
                raise ex
            self.app.orchestrator.set_invocation_retry(self, ex)
            self.task.logger.warning(f"Invocation WILL-RETRY {ex=}")
        except Exception as ex:
            self.app.logger.exception("Invocation EXCEPTION")
            self.app.orchestrator.set_invocation_exception(self, ex)
            raise ex
        finally:
            self.reset_context(previous_invocation_context)

    @property
    def result(self) -> Result:
        """
        Retrieve the result of the task execution.

        This property method is responsible for obtaining the final result of the task associated with this invocation.
        If the task is not yet completed (i.e., its status is not final), it enters a waiting state. The method ensures
        that it waits for the task to reach a final state before returning the result.

        The waiting mechanism involves communicating with the orchestrator and potentially the runner, depending on the
        task's current state and execution context. If the task is part of a nested call structure, it also considers the
        parent invocation's state.

        Once the task reaches a final state, the method retrieves and returns the final result of the task execution.

        :return: The result of the task execution. The exact type of `Result` depends on the task's implementation.

        ```{note}
            - This method will block until the task execution is complete and the result is available.
            - If called on an invocation that is not yet in a final state, it will wait (potentially indefinitely)
            for the task to complete.
        ```
        """
        self.app.logger.debug(f"ini waiting for invocation {self.invocation_id} result")
        if not self.status.is_final():
            self.app.orchestrator.waiting_for_results(self.parent_invocation, [self])

        while not self.status.is_final():
            self.app.runner.waiting_for_results(
                self.parent_invocation, [self], context.runner_args
            )
        self.app.logger.debug(f"end waiting for invocation {self.invocation_id} result")
        return self.get_final_result()

    async def async_result(self) -> Result:
        # Assuming an async_waiting_for_results will be implemented in the runner:
        if not self.status.is_final():
            self.app.orchestrator.waiting_for_results(self.parent_invocation, [self])
        while not self.status.is_final():
            await self.app.runner.async_waiting_for_results(
                self.parent_invocation, [self], context.runner_args
            )
        return self.get_final_result()

    def get_final_result(self) -> Result:
        """
        Retrieve the final result of the task execution if the invocation is in a final state.

        This method checks if the invocation has reached a final state. If it has, the method then retrieves and returns
        the result of the task execution from the state backend. In case the invocation is in a failed state, it raises
        an exception with the details of the failure.

        :return: The final result of the task execution.
        :rtype: Result

        :raises exceptions.InvocationError: If the invocation is not in a final state when this method is called.
        """
        if not self.status.is_final():
            raise exceptions.InvocationError(
                self.invocation_id, "Invocation is not final"
            )
        if self.status == InvocationStatus.FAILED:
            raise self.app.state_backend.get_exception(self)
        return self.app.state_backend.get_result(self)


class DistributedInvocationGroup(
    BaseInvocationGroup[Params, Result, DistributedInvocation]
):
    """
    A group of distributed invocations for a specific task in a distributed environment.

    This class extends `BaseInvocationGroup` to handle groups of `DistributedInvocation` instances.

    :param Task task:
        The task associated with these invocations.
    :param list[DistributedInvocation] invocations:
        A list of distributed invocations, each an instance of `DistributedInvocation`.

    The `DistributedInvocationGroup` is specifically designed for use in distributed environments, where task executions are spread across multiple nodes or processes.
    """

    @property
    def results(self) -> Iterator[Result]:
        """
        An iterator over the results of the invocations in the group.

        This property method iterates over the `DistributedInvocation` instances in the group,
        yielding the result of each invocation once it reaches a final state.
        If an invocation has not yet completed, it waits for the result to become available.

        The method ensures that the orchestrator is notified about the waiting invocations
        and communicates with the runner to handle the waiting state efficiently.

        :return:
            An iterator over the results of each invocation in the group.
        :rtype:
            Iterator[Result]

        ```{note}
            This method will block until all invocations in the group have completed and their results are available.
        ```
        """
        waiting_invocations = self.invocations.copy()
        if not waiting_invocations:
            return
        parent_invocation = waiting_invocations[0].parent_invocation
        notified_orchestrator = False
        while waiting_invocations:
            for final_inv in self.app.orchestrator.filter_final(waiting_invocations):
                waiting_invocations.remove(final_inv)
                yield final_inv.result
            if not waiting_invocations:
                break
            if not notified_orchestrator:
                self.app.orchestrator.waiting_for_results(
                    parent_invocation, waiting_invocations
                )
                notified_orchestrator = True
            self.app.runner.waiting_for_results(
                parent_invocation, waiting_invocations, context.runner_args
            )

    async def async_results(self) -> AsyncGenerator[Result, None]:
        waiting_invocations = self.invocations.copy()
        if not waiting_invocations:
            return
        parent_invocation = waiting_invocations[0].parent_invocation
        notified_orchestrator = False
        while waiting_invocations:
            for final_inv in self.app.orchestrator.filter_final(waiting_invocations):
                waiting_invocations.remove(final_inv)
                yield final_inv.result
            if not waiting_invocations:
                break
            if not notified_orchestrator:
                self.app.orchestrator.waiting_for_results(
                    parent_invocation, waiting_invocations
                )
                notified_orchestrator = True
            await self.app.runner.async_waiting_for_results(
                parent_invocation, waiting_invocations, context.runner_args
            )


class ReusedInvocation(DistributedInvocation):
    """
    A specialized invocation that reuses an existing `DistributedInvocation`.

    This class is used for scenarios where an existing invocation is reused, possibly with some differences
    in arguments. It adds an attribute to track these argument differences.

    :ivar Arguments | None diff_arg:
        An optional `Arguments` object representing any differences in arguments compared to the original invocation.
        If `None`, it indicates no differences in arguments.
    """

    def __init__(
        self,
        call: Call[Params, Result],
        parent_invocation: DistributedInvocation | None = None,
        invocation_id: str | None = None,
        diff_arg: Arguments | None = None,
    ):
        # Call parent init
        super().__init__(call, parent_invocation, invocation_id)
        self.diff_arg = diff_arg

    @classmethod
    def from_existing(
        cls, invocation: DistributedInvocation, diff_arg: Arguments | None = None
    ) -> ReusedInvocation:
        """
        Create a `ReusedInvocation` instance from an existing `DistributedInvocation`.

        This method constructs a new `ReusedInvocation` based on an existing distributed invocation. It reuses
        the invocation ID and other relevant attributes from the original invocation and allows specifying
        differences in arguments.

        :param DistributedInvocation invocation:
            The existing invocation to reuse.
        :param Arguments | None diff_arg:
            Optional argument differences for the new invocation. If provided, these arguments will be used to
            distinguish the new invocation from the original. Default is None.
        :return:
            A new instance of `ReusedInvocation` based on the provided existing invocation.
        :rtype:
            ReusedInvocation
        """
        new_invc = cls(
            call=Call(invocation.task, invocation.arguments),
            parent_invocation=invocation.parent_invocation,
            # we reuse invocation_id from original invocation
            invocation_id=invocation.invocation_id,
            diff_arg=diff_arg,
        )
        return new_invc
