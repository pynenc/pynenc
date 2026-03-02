from __future__ import annotations

from collections.abc import AsyncGenerator, Iterator
import time
from typing import TYPE_CHECKING, Any

from pynenc import context, exceptions
from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.identifiers.invocation_id import InvocationId, generate_invocation_id
from pynenc.invocation.base_invocation import (
    BaseInvocation,
    BaseInvocationGroup,
)
from pynenc.invocation.status import InvocationStatus
from pynenc.models.invocation_dto import InvocationDTO
from pynenc.types import Params, Result
from pynenc.util.asyncio_helper import run_task_sync
from pynenc.workflow.workflow_exceptions import WorkflowPauseError
from pynenc.workflow.workflow_identity import WorkflowIdentity

if TYPE_CHECKING:
    from pynenc.runner import RunnerContext


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
    :param InvocationId | None _invocation_id:
        A unique identifier for the invocation. This ID is crucial for tracking and orchestrating the invocation
        across the distributed system. It's assigned internally and used by the orchestration mechanism.
    :param bool _disable_upsert:
        A flag to disable the upsert operation during initialization. This flag is used to prevent unnecessary
        updates to the state backend when creating invocations in certain scenarios.
    """

    def __init__(
        self,
        call: Call[Params, Result],
        invocation_id: InvocationId,
        parent_invocation_id: InvocationId | None,
        workflow: WorkflowIdentity,
        stored_in_backend: bool,
    ) -> None:
        # Call parent init
        super().__init__(call, invocation_id)
        self.parent_invocation_id = parent_invocation_id
        self._workflow = workflow

        # Initialize additional state
        self._result: Result | None = None
        self._num_retries: int = 0

        # Initialize state fields
        self._cached_status: InvocationStatus | None = None
        self._cached_status_time: float = 0.0

        # Store in state backend
        self._stored_in_backend = stored_in_backend
        self.store_in_backend()

    @classmethod
    def isolated(cls, call: Call[Params, Result]) -> DistributedInvocation:
        """Create a new DistributedInvocation for a given call ignoring any existing invocation context."""
        return cls.from_parent(call, parent_invocation=None)

    @classmethod
    def from_parent(
        cls,
        call: Call[Params, Result],
        parent_invocation: DistributedInvocation | None = None,
    ) -> DistributedInvocation:
        """Create a new invocation as a child of an existing invocation.

        :param Call[Params, Result] call:
            The call that this invocation will execute.
        :param DistributedInvocation | None parent_invocation:
            The parent invocation that this new invocation will be a child of. If None, the new invocation will be a main workflow task.
        """
        new_invocation_id = generate_invocation_id()
        if parent_invocation is None:
            workflow = WorkflowIdentity.new_workflow(
                invocation_id=new_invocation_id, task_id=call.task.task_id
            )
            call.app.logger.info(f"Creating a new main {workflow=}")
        elif call.task.conf.force_new_workflow:
            workflow = WorkflowIdentity.new_subworkflow(
                invocation_id=new_invocation_id,
                task_id=call.task.task_id,
                parent_workflow_id=parent_invocation.workflow.workflow_id,
            )
            call.app.logger.info(f"Creating a new sub-workflow {workflow=}")
        else:
            workflow = parent_invocation.workflow
            call.app.logger.debug(
                f"Inheriting workflow from parent {workflow=} for {new_invocation_id=}"
            )
        return cls(
            call=call,
            invocation_id=new_invocation_id,
            parent_invocation_id=parent_invocation.invocation_id
            if parent_invocation
            else None,
            workflow=workflow,
            stored_in_backend=False,
        )

    @property
    def workflow(self) -> WorkflowIdentity:
        """Get the workflow identity for this invocation."""
        return self._workflow

    def store_in_backend(self) -> None:
        """Store the invocation in the state backend."""
        if not self._stored_in_backend:
            self._stored_in_backend = True
            self.app.state_backend.upsert_invocations([self])

    @property
    def num_retries(self) -> int:
        """:return: number of times the invocation got retried"""
        return self.app.orchestrator.get_invocation_retries(self.invocation_id)

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
        # For non-final statuses, use a short cache (100ms) to reduce orchestrator load
        cache_ttl = self.app.conf.cached_status_time
        if self._cached_status and (time.time() - self._cached_status_time < cache_ttl):
            return self._cached_status
        # check status and update cache
        status = self.app.orchestrator.get_invocation_status(self.invocation_id)
        self.update_status_cache(status)
        return status

    def to_dto(self) -> InvocationDTO:
        """Create a side-effect-free DTO with invocation-level fields only.

        The DTO carries identity and workflow data. It does NOT embed
        call/argument data — the state backend pairs this with a separate
        ``CallDTO`` when persisting.

        :return: An InvocationDTO with invocation identity fields.
        """
        return InvocationDTO(
            invocation_id=self.invocation_id,
            call_id=self.call.call_id,
            parent_invocation_id=self.parent_invocation_id,
            workflow=self.workflow,
        )

    @classmethod
    def from_dto(cls, dto: InvocationDTO, call: Call) -> DistributedInvocation:
        """Reconstruct a DistributedInvocation from a DTO and a pre-built call.

        The caller (state backend) is responsible for constructing the call
        (typically a ``LazyCall``) from the corresponding ``CallDTO``. Parent
        invocation is loaded from the state backend by ID (flat lookup).

        :param InvocationDTO dto: The DTO with invocation identity fields.
        :param Call call: The call for this invocation (usually a LazyCall).
        :return: A fully reconstructed DistributedInvocation.
        """
        return cls(
            call=call,
            invocation_id=dto.invocation_id,
            parent_invocation_id=dto.parent_invocation_id,
            workflow=dto.workflow,
            stored_in_backend=True,
        )

    def __getstate__(self) -> dict:
        """Return a serialized state dict for pickling."""
        state: dict = {}
        state["call"] = self._call
        state["invocation_id"] = self._invocation_id
        state["parent_invocation_id"] = self.parent_invocation_id
        state["workflow"] = self._workflow
        state["state"] = {
            "cached_status": self._cached_status,
            "cached_status_time": self._cached_status_time,
            "stored_in_backend": self._stored_in_backend,
            "num_retries": self._num_retries,
            "result": self._result,
        }
        return state

    def __setstate__(self, state: dict) -> None:
        """Restore state from a pickled dict."""
        # Recreate identity object (it's immutable, needs to be created properly)
        self._call = state["call"]
        self._invocation_id = state["invocation_id"]
        self.parent_invocation_id = state["parent_invocation_id"]
        self._workflow = state["workflow"]
        # Restore mutable state directly
        state_data = state["state"]
        self._cached_status = state_data["cached_status"]
        self._cached_status_time = state_data["cached_status_time"]
        self._stored_in_backend = state_data["stored_in_backend"]
        self._num_retries = state_data["num_retries"]
        self._result = state_data["result"]

    def is_main_workflow_task(self) -> bool:
        """Check if the task is the main workflow task.

        :return: True if the task is the main workflow task, False otherwise

        ```{note}
            All tasks run within a workflow, the main workflow task is just the first task in the workflow.
            To determine that, we check if the task_id of the workflow task is the same as the task_id of the current task.
        ```
        """
        return self.workflow.workflow_type == self.task.task_id

    def _register_workflow_run(self) -> None:
        """Register workflow tracking for this invocation start."""
        if self.is_main_workflow_task():
            self.app.state_backend.store_workflow_run(self.workflow)
            if self.workflow.parent_workflow_id:
                self.app.state_backend.store_workflow_sub_invocation(
                    self.workflow.parent_workflow_id,
                    self.workflow.workflow_id,
                )
            return

        self.app.state_backend.store_workflow_sub_invocation(
            self.workflow.workflow_id,
            self.invocation_id,
        )

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

    def run(
        self, runner_ctx: RunnerContext, runner_args: dict[str, Any] | None = None
    ) -> None:
        """
        Execute the task associated with this invocation in a distributed environment.

        This method is responsible for running the task's function with its arguments, handling retriable exceptions,
        and updating the task's state in the orchestrator. It manages the invocation context and communicates with
        the orchestrator to set the invocation's run state and result.

        :param RunnerContext runner_ctx: The context of the runner executing this invocation.
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
        # Always set runner_args in thread-local storage so nested invocations can access them
        context.set_runner_args(runner_args)
        # Set the current app so core tasks can access it
        context.set_current_app(self.app)
        # Set the runner context for this thread so any sub-invocations
        # registered from within this task use the correct runner context
        # (log.py reads directly from context.py, so this is the single source of truth)
        context.set_runner_context(self.app.app_id, runner_ctx)
        previous_invocation_context = self.swap_context()
        try:
            if not self.app.orchestrator.is_authorize_to_run_by_concurrency_control(
                self
            ):
                self.app.orchestrator.reroute_invocations(
                    {self.invocation_id}, runner_ctx
                )
            self.app.orchestrator.set_invocation_status(
                self.invocation_id, InvocationStatus.RUNNING, runner_ctx
            )
            self._register_workflow_run()
            result = run_task_sync(self.task.func, **self.arguments.kwargs)
            self.app.orchestrator.set_invocation_result(self, result, runner_ctx)
        except WorkflowPauseError as ex:
            self.task.logger.warning(
                f"Workflow pause requested but not implemented yet: {ex.reason}"
            )
        except self.task.retriable_exceptions as ex:
            if self.num_retries >= self.task.conf.max_retries:
                self.app.logger.exception("Invocation MAX-RETRY")
                self.app.orchestrator.set_invocation_exception(self, ex, runner_ctx)
                raise ex
            self.app.orchestrator.set_invocation_retry(
                self.invocation_id, ex, runner_ctx
            )
            self.task.logger.warning(f"Invocation WILL-RETRY {ex=}")
        except exceptions.InvocationStatusTransitionError as ex:
            # Expected race condition: another runner already picked up this invocation
            self.task.logger.warning(
                f"Status transition conflict (another runner took ownership): {ex}"
            )
        except exceptions.InvocationStatusOwnershipError as ex:
            # Expected race condition: we lost ownership to another runner
            self.task.logger.warning(f"Lost ownership to another runner: {ex}")
        except Exception as ex:
            self.app.logger.exception("Invocation EXCEPTION")
            self.app.orchestrator.set_invocation_exception(self, ex, runner_ctx)
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
            self.app.orchestrator.waiting_for_results(
                self.parent_invocation_id, [self.invocation_id]
            )

        while not self.status.is_final():
            self.app.runner.waiting_for_results(
                self.parent_invocation_id,
                [self.invocation_id],
                context.get_runner_args(),
            )
        self.app.logger.debug(f"end waiting for invocation {self.invocation_id} result")
        return self.get_final_result()

    async def async_result(self) -> Result:
        # Assuming an async_waiting_for_results will be implemented in the runner:
        if not self.status.is_final():
            self.app.orchestrator.waiting_for_results(
                self.parent_invocation_id, [self.invocation_id]
            )
        while not self.status.is_final():
            await self.app.runner.async_waiting_for_results(
                self.parent_invocation_id,
                [self.invocation_id],
                context.get_runner_args(),
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
            raise self.app.state_backend.get_exception(self.invocation_id)
        return self.app.state_backend.get_result(self.invocation_id)


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
        waiting_invocation_ids = list(self.invocation_map.keys())
        if not waiting_invocation_ids:
            return
        parent_invocation_id = self.invocation_map[
            waiting_invocation_ids[0]
        ].parent_invocation_id
        notified_orchestrator = False
        while waiting_invocation_ids:
            for final_inv_id in self.app.orchestrator.filter_final(
                waiting_invocation_ids
            ):
                waiting_invocation_ids.remove(final_inv_id)
                yield self.invocation_map[final_inv_id].result
            if not waiting_invocation_ids:
                break
            if not notified_orchestrator:
                self.app.orchestrator.waiting_for_results(
                    parent_invocation_id, waiting_invocation_ids
                )
                notified_orchestrator = True
            self.app.runner.waiting_for_results(
                parent_invocation_id, waiting_invocation_ids, context.get_runner_args()
            )

    async def async_results(self) -> AsyncGenerator[Result, None]:
        waiting_invocation_ids = list(self.invocation_map.keys())
        if not waiting_invocation_ids:
            return
        parent_invocation_id = self.invocation_map[
            waiting_invocation_ids[0]
        ].parent_invocation_id
        notified_orchestrator = False
        while waiting_invocation_ids:
            for final_inv_id in self.app.orchestrator.filter_final(
                waiting_invocation_ids
            ):
                waiting_invocation_ids.remove(final_inv_id)
                yield self.invocation_map[final_inv_id].result
            if not waiting_invocation_ids:
                break
            if not notified_orchestrator:
                self.app.orchestrator.waiting_for_results(
                    parent_invocation_id, waiting_invocation_ids
                )
                notified_orchestrator = True
            await self.app.runner.async_waiting_for_results(
                parent_invocation_id, waiting_invocation_ids, context.get_runner_args()
            )


class ReusedInvocation(DistributedInvocation):
    """
    A specialized invocation that reuses an existing `DistributedInvocation`.

    This class is used for scenarios where an existing invocation is reused, possibly with some differences
    in arguments. It adds an attribute to track these argument differences.

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

    def __init__(
        self,
        existing_invocation: DistributedInvocation,
        diff_arg: Arguments | None = None,
    ):
        if not existing_invocation._stored_in_backend:
            raise ValueError(
                "Cannot reuse an invocation that is not stored in the backend"
            )
        super().__init__(
            existing_invocation.call,
            existing_invocation.invocation_id,
            existing_invocation.parent_invocation_id,
            existing_invocation.workflow,
            existing_invocation._stored_in_backend,
        )
        self.diff_arg = diff_arg

    def run(
        self, runner_ctx: RunnerContext, runner_args: dict[str, Any] | None = None
    ) -> None:
        """ReusedInvocation cannot be run directly, it's just a wrapper around an existing invocation."""
        self.app.logger.error(
            f"Attempted to run a ReusedInvocation, with {runner_ctx=} {runner_args=}."
        )
        raise NotImplementedError(
            "ReusedInvocation cannot be run directly, it's just a wrapper around an existing invocation"
        )
