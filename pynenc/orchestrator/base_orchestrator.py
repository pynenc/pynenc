from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import datetime
from functools import cached_property
from time import time
from typing import TYPE_CHECKING, Any, Optional

from pynenc import context
from pynenc.conf.config_orchestrator import ConfigOrchestrator
from pynenc.conf.config_task import ConcurrencyControlType
from pynenc.exceptions import (
    InvocationConcurrencyWithDifferentArgumentsError,
    TaskParallelProcessingError,
    InvocationStatusError,
)
from pynenc.invocation.dist_invocation import DistributedInvocation, ReusedInvocation
from pynenc.invocation.status import InvocationStatus, InvocationStatusRecord
from pynenc.orchestrator.atomic_service import can_run_atomic_service

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.call import Call, PreSerializedCall, CallId
    from pynenc.identifiers.invocation_id import InvocationId
    from pynenc.orchestrator.atomic_service import ActiveRunnerInfo
    from pynenc.runner.runner_context import RunnerContext
    from pynenc.task import Task, TaskId
    from pynenc.types import Params, Result


class BaseBlockingControl(ABC):
    """
    A component of the orchestrator to implement blocking control functionalities.

    This abstract base class defines the interface for managing blocking behavior in distributed task executions.
    """

    @abstractmethod
    def release_waiters(self, waited_invocation_id: "InvocationId") -> None:
        """
        Releases any invocations that are waiting on the specified invocation.

        :param InvocationId waited_invocation_id: The ID of the invocation that has finished and can release its waiters.
        """

    @abstractmethod
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

    @abstractmethod
    def get_blocking_invocations(
        self, max_num_invocations: int
    ) -> Iterator["InvocationId"]:
        """
        Retrieves invocation IDs that are blocking others but are not blocked themselves.

        :param int max_num_invocations: The maximum number of blocking invocation IDs to retrieve.
        :return: An iterator over unblocked, blocking invocation IDs, **ordered by age (oldest first)**.
        :rtype: Iterator[InvocationId]
        """


class BaseOrchestrator(ABC):
    """
    Abstract base class defining the orchestrator's interface in a distributed task system.

    The orchestrator is responsible for managing task invocations, including tracking their status,
    handling retries, and implementing blocking control.

    :param Pynenc app: The Pynenc application instance.
    """

    def __init__(self, app: "Pynenc") -> None:
        self.app = app

    @cached_property
    def conf(self) -> ConfigOrchestrator:
        return ConfigOrchestrator(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @abstractmethod
    def _register_new_invocations(
        self,
        invocations: list["DistributedInvocation[Params, Result]"],
        runner_id: str | None = None,
    ) -> InvocationStatusRecord:
        """
        Register new invocations with status Register if they don't exist yet.

        :param list[DistributedInvocation[Params, Result]] invocations: The invocations to be registered.
        :param str | None runner_id: The owner ID for ownership validation
        :return: The status record of the registered invocation.
        """

    @abstractmethod
    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_serialized_arguments: dict[str, str] | None = None,
        statuses: "list[InvocationStatus] | None" = None,
    ) -> Iterator["InvocationId"]:
        """
        Retrieves existing invocation IDs based on task, arguments, and status.

        :param Task[Params, Result] task: The task for which to retrieve invocations.
        :param dict[str, str] | None key_serialized_arguments: Serialized arguments to filter invocations.
        :param Optional[list[InvocationStatus]] statuses: The statuses to filter invocations.
        :return: An iterator over the matching invocation IDs.
        :rtype: Iterator[InvocationId]
        """

    @abstractmethod
    def get_task_invocation_ids(self, task_id: "TaskId") -> Iterator["InvocationId"]:
        """
        Retrieves all invocation IDs associated with a specific task ID.

        :param TaskId task_id: The task ID to filter invocations.
        :return: List of invocation IDs for the specified task.
        """

    @abstractmethod
    def get_invocation_ids_paginated(
        self,
        task_id: Optional["TaskId"] = None,
        statuses: "list[InvocationStatus] | None" = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list["InvocationId"]:
        """
        Retrieves invocation IDs with pagination support.

        This method provides efficient pagination for large datasets by using
        LIMIT/OFFSET semantics. Results are ordered by registration time (newest first).

        :param Optional[TaskId] task_id: Optional task ID to filter by.
        :param list[InvocationStatus] | None statuses: Optional statuses to filter by.
        :param int limit: Maximum number of results to return.
        :param int offset: Number of results to skip.
        :return: List of matching invocation IDs.
        """

    @abstractmethod
    def count_invocations(
        self,
        task_id: Optional["TaskId"] = None,
        statuses: "list[InvocationStatus] | None" = None,
    ) -> int:
        """
        Counts invocations matching the given filters.

        :param str | None task_id: Optional task ID to filter by.
        :param list[InvocationStatus] | None statuses: Optional statuses to filter by.
        :return: The total count of matching invocations.
        """

    @abstractmethod
    def get_call_invocation_ids(self, call_id: "CallId") -> Iterator["InvocationId"]:
        """
        Retrieves all invocation IDs associated with a specific call ID.

        :param CallId call_id: The call ID to filter invocations.
        :return: List of invocation IDs for the specified call.
        """

    @abstractmethod
    def _atomic_status_transition(
        self,
        invocation_id: "InvocationId",
        status: InvocationStatus,
        runner_id: str | None = None,
    ) -> InvocationStatusRecord:
        """
        Atomically validates and transitions invocation status.

        Backend implementations must:
        1. Read current status record
        2. Validate transition using status_record_transition()
        3. Atomically update only if validation passes
        4. Return the new status record

        All validation and state changes happen within a single atomic operation.

        :param InvocationId invocation_id: The ID of the invocation to update
        :param InvocationStatus status: The target status
        :param str | None runner_id: The owner ID for ownership validation
        :return: The new status record after successful transition
        :rtype: InvocationStatusRecord
        :raises InvocationStatusTransitionError: If transition is not allowed
        :raises InvocationStatusOwnershipError: If ownership rules are violated
        :raises KeyError: If invocation does not exist
        """
        # Example implementation pattern (varies by backend):
        #
        # def _atomic_status_transition(self, invocation_id, status, runner_id):
        #     # PostgreSQL with transaction:
        #     with transaction:
        #         current = get_invocation_status_record(invocation_id)
        #         new_record = status_record_transition(current, status, runner_id)
        #         UPDATE ... WHERE invocation_id = ? AND status = current.status
        #         if not updated: raise race condition
        #         return new_record
        #
        #     # Redis with Lua script:
        #     lua_script = """
        #         local current = get_status(invocation_id)
        #         -- validate in Lua or return data for Python validation
        #         if valid then set_status(new_status) end
        #     """
        #     return execute_lua(lua_script)
        #
        #     # MongoDB findAndModify:
        #     current = find_one(invocation_id)
        #     new_record = status_record_transition(current, status, runner_id)
        #     result = find_and_modify(
        #         query={id: invocation_id, status: current.status},
        #         update={status: new_record}
        #     )
        #     if not result: raise race condition
        #     return new_record

    @abstractmethod
    def index_arguments_for_concurrency_control(
        self,
        invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """
        Caches the required data to implement concurrency control.

        :param DistributedInvocation[Params, Result] invocation: The invocation to be cached.
        """

    @abstractmethod
    def set_up_invocation_auto_purge(self, invocation_id: "InvocationId") -> None:
        """
        Sets up automatic purging for an invocation after a defined period.
        ```{note}
            Set auto purge period with `app.conf.orchestrator_auto_final_invocation_purge_hours`
        ```

        :param InvocationId invocation_id: The invocation to set up for auto purge.
        """

    @abstractmethod
    def auto_purge(self) -> None:
        """
        Automatically purges all invocations in a final state that are older than a defined time period.
        ```{note}
            Set auto purge period with `app.conf.orchestrator_auto_final_invocation_purge_hours`
        ```
        """

    def get_invocation_status(
        self, invocation_id: "InvocationId"
    ) -> "InvocationStatus":
        """
        Retrieves the status of a specific invocation id.

        note::
            This method should be cached specially final status that cannot change.
            For filtering by status, use `filter_by_status` instead.

        :param InvocationId invocation_id: The id of the invocation whose status is to be retrieved.
        :return: The current status of the invocation.
        :rtype: InvocationStatus
        :raises KeyError: If the invocation ID does not exist.
        """
        status_record = self.get_invocation_status_record(invocation_id)
        return status_record.status

    @abstractmethod
    def get_invocation_status_record(
        self, invocation_id: "InvocationId"
    ) -> "InvocationStatusRecord":
        """
        Retrieves the status record of a specific invocation id.

        :param InvocationId invocation_id: The id of the invocation whose status is to be retrieved.
        :return: The current status record of the invocation.
        :rtype: InvocationStatusRecord
        :raises KeyError: If the invocation ID does not exist.
        """

    @abstractmethod
    def increment_invocation_retries(self, invocation_id: "InvocationId") -> None:
        """
        Increments the retry count of a specific invocation.

        :param InvocationId invocation_id: The id of the invocation for which to increment retries.
        """

    @abstractmethod
    def get_invocation_retries(self, invocation_id: "InvocationId") -> int:
        """
        Retrieves the number of retries for a specific invocation.

        :param InvocationId invocation_id: The id of the invocation whose retry count is to be retrieved.
        :return: The number of retries for the invocation.
        :rtype: int
        """

    @abstractmethod
    def filter_by_status(
        self,
        invocation_ids: list["InvocationId"],
        status_filter: frozenset["InvocationStatus"],
    ) -> list["InvocationId"]:
        """
        Filters a list of invocation ids by their status in an optimized way.

        This method allows efficient batch filtering of invocations by status,
        reducing the number of individual status checks needed.

        :param list[InvocationId] invocations: The invocation ids to filter
        :param list[InvocationStatus] | None status_filter: The statuses to filter by.
            If None, defaults to final statuses.
        :return: List of invocation ids matching the status filter
        :rtype: list[InvocationId]
        """
        pass

    def filter_final(
        self, invocation_ids: list["InvocationId"]
    ) -> list["InvocationId"]:
        """
        Returns invocation ids that have reached a final status.

        :param list[InvocationId] invocations: The invocation ids to check
        :return: List of invocation ids that have reached a final status
        :rtype: list[InvocationId]
        """
        return self.filter_by_status(
            invocation_ids, InvocationStatus.get_final_statuses()
        )

    @abstractmethod
    def purge(self) -> None:
        """
        Purges all the orchestrator data for the current self.app.app_id.
        ```{important}
            This should only be used for testing purposes.
        ```
        """

    #############################################
    # blocking sub functionalities
    @property
    @abstractmethod
    def blocking_control(self) -> BaseBlockingControl:
        """
        Property to access the blocking control component of the orchestrator.

        :return: The blocking control component.
        :rtype: BaseBlockingControl
        """

    def release_waiters(self, waited_id: "InvocationId") -> None:
        """
        Releases other invocations that are waiting on the completion of the specified invocation.

        :param InvocationId waited_id: The ID of the invocation that has completed.
        """
        if self.app.orchestrator.conf.blocking_control:
            self.blocking_control.release_waiters(waited_id)

    def waiting_for_results(
        self,
        caller_invocation_id: "InvocationId | None",
        result_invocation_ids: list["InvocationId"],
    ) -> None:
        """
        Notifies the system that an invocation is waiting for the results of other invocations.

        :param InvocationId | None caller_invocation_id: The ID of the waiting invocation.
        :param list[InvocationId] result_invocation_ids: The IDs of the invocations being waited on.
        """
        if not result_invocation_ids:
            self.app.logger.warning(
                f"Unnecessary call to waiting_for_results, invocation:{caller_invocation_id} is not waiting for any results"
            )
            return
        if self.app.orchestrator.conf.blocking_control and caller_invocation_id:
            self.blocking_control.waiting_for_results(
                caller_invocation_id, result_invocation_ids
            )

    def get_blocking_invocations(
        self, max_num_invocation_ids: int
    ) -> Iterator["InvocationId"]:
        """
        Retrieves invocation IDs that are blocking others but are not blocked themselves.

        :param int max_num_invocation_ids: The maximum number of blocking invocation IDs to retrieve.
        :return: An iterator over unblocked, blocking invocation IDs.
        :rtype: Iterator[InvocationId]

        ```{note}
            The order of the returned invocation IDs is **oldest first**.
        ```
        """
        if self.app.orchestrator.conf.blocking_control:
            yield from self.blocking_control.get_blocking_invocations(
                max_num_invocation_ids
            )

    #############################################

    def set_invocation_status(
        self,
        invocation_id: "InvocationId",
        status: "InvocationStatus",
        runner_ctx: "RunnerContext",
    ) -> None:
        """
        Sets the status of a specific invocation.

        :param InvocationId invocation_id: The ID of the invocation to update.
        :param InvocationStatus status: The new status to set for the invocation.

        :raises InvocationStatusTransitionError: If transition is not allowed
        :raises InvocationStatusOwnershipError: If ownership rules are violated
        :raises InvocationStatusRaceConditionError: If a race condition is detected during status update
        """
        new_status_record = self._atomic_status_transition(
            invocation_id, status, runner_ctx.runner_id
        )
        if status.is_final():
            self.release_waiters(invocation_id)
            self.set_up_invocation_auto_purge(invocation_id)
        self.app.state_backend.add_history(invocation_id, new_status_record, runner_ctx)
        self.app.trigger.report_tasks_status([invocation_id], status)
        self.app.logger.info(f"invocation:{invocation_id} status:{status.value}")

    def set_invocation_result(
        self,
        invocation: "DistributedInvocation",
        result: Any,
        runner_ctx: "RunnerContext",
    ) -> None:
        """
        Sets the result for a completed invocation.

        :param DistributedInvocation invocation: The invocation that has completed.
        :param Any result: The result of the invocation's execution.
        """
        self.app.state_backend.set_result(invocation.invocation_id, result)
        self.app.orchestrator.set_invocation_status(
            invocation.invocation_id, InvocationStatus.SUCCESS, runner_ctx
        )
        self.app.trigger.report_invocation_result(invocation, result)

    def set_invocation_exception(
        self,
        invocation: "DistributedInvocation",
        exception: Exception,
        runner_ctx: "RunnerContext",
    ) -> None:
        """
        Sets an exception for an invocation that finished with an error.

        :param DistributedInvocation invocation: The invocation that encountered an exception.
        :param Exception exception: The exception that occurred during the invocation's execution.
        """
        self.app.state_backend.set_exception(invocation.invocation_id, exception)
        # TODO! on previous fail, this should still change status
        # eg. on case of interruption from a kubernetes pod (SIGTERM, SIGKILL)
        #     it should try to finish all the calls in this function
        self.app.orchestrator.set_invocation_status(
            invocation.invocation_id, InvocationStatus.FAILED, runner_ctx
        )
        self.app.trigger.report_invocation_failure(invocation, exception)

    def set_invocation_retry(
        self,
        invocation_id: "InvocationId",
        exception: Exception,
        runner_ctx: "RunnerContext",
    ) -> None:
        """
        Sets an invocation for retry in case of a retriable exception.

        :param InvocationId invocation_id: The ID of the invocation to be retried.
        :param Exception exception: The exception that triggered the retry.
        """
        # TODO! on previous fail, this should still change status
        # eg. on case of interruption from a kubernetes pod (SIGTERM, SIGKILL)
        #     it should try to finish all the calls in this function

        # TODO store Retry exception on Retry status
        self.app.orchestrator.set_invocation_status(
            invocation_id, InvocationStatus.RETRY, runner_ctx
        )
        self.app.orchestrator.increment_invocation_retries(invocation_id)
        self.app.broker.route_invocation(invocation_id)

    def is_candidate_to_run_by_concurrency_control(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> bool:
        """
        Checks if an invocation can be candidate to run based on the task's concurrency control configuration.

        :param DistributedInvocation invocation: The invocation to check for authorization.
        :return: True if the invocation is authorized to be a running candidate, False otherwise.
        """
        return self._is_authorize_by_concurrency_control(
            invocation, [InvocationStatus.PENDING, InvocationStatus.RUNNING]
        )

    def is_authorize_to_run_by_concurrency_control(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> bool:
        """
        Checks if an invocation can be candidate to run based on the task's concurrency control configuration.

        :param DistributedInvocation invocation: The invocation to check for authorization.
        :return: True if the invocation is authorized to be a running candidate, False otherwise.
        """
        return self._is_authorize_by_concurrency_control(
            invocation, [InvocationStatus.RUNNING]
        )

    def _is_authorize_by_concurrency_control(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        statuses: list["InvocationStatus"],
    ) -> bool:
        """
                Checks if an invocation is authorized to run based on the task's concurrency control configuration.

                ```{important}
                The authorization is determined by the task's running_concurrency setting:
                - If ConcurrencyControlType.DISABLED, the invocation is always authorized to run.
                - If ConcurrencyControlType.TASK, it checks if there are any other running invocations of the same task.
                If there are, the invocation is not authorized to run.
                - If ConcurrencyControlType.ARGUMENTS, it checks for any running invocation of the same task with the same arguments.
                If there are, the invocation is not authorized to run.
                - If ConcurrencyControlType.KEYS, it checks for any running invocation with the same (key) arguments.
                If any are found, the invocation is not authorized to run.
                ```

                ```{note}
                The function call.serialized_args_for_concurrency_check is used to determine the arguments
                that are relevant for checking existing running invocations based on the task's running_concurrency option.
                ```

                :param DistributedInvocation invocation: The invocation to check for authorization.
                :param list[InvocationStatus] statuses: The statuses to check for existing invocations.
        :return: True if the invocation is authorized, False otherwise.
        """
        if invocation.task.conf.running_concurrency == ConcurrencyControlType.DISABLED:
            return True
        running_invocation = next(
            self.get_existing_invocations(
                task=invocation.call.task,
                key_serialized_arguments=invocation.call.serialized_args_for_concurrency_check,
                statuses=statuses,
            ),
            None,
        )
        if not running_invocation:
            return True
        invocation.task.logger.error(
            f"invocation:{invocation.invocation_id} cannot run because invocation:{running_invocation} is already in status:{statuses}"
        )
        return False

    def get_blocking_invocations_to_run(
        self,
        max_num_invocations: int,
        blocking_invocation_ids: set["InvocationId"],
        runner_ctx: "RunnerContext",
    ) -> Iterator["InvocationId"]:
        """
        Retrieves invocation IDs that are blocking others but are not themselves blocked, up to a maximum number.

        :param int max_num_invocations: The maximum number of blocking invocations to retrieve.
        :param set["InvocationId"] blocking_invocation_ids: A set of invocation IDs that are already identified as blocking.
        :return: An iterator over the blocking invocation IDs.
        :rtype: Iterator["InvocationId"]
        """
        for blocking_invocation_id in self.get_blocking_invocations(
            max_num_invocations
        ):
            current_status = self.get_invocation_status(blocking_invocation_id)
            # Skip silently if the invocation is already past the point where PENDING makes sense
            # (e.g. already RUNNING, SUCCESS, FAILED) — another worker already claimed it
            if not current_status.can_transition_to(InvocationStatus.PENDING):
                continue
            blocking_invocation = self.app.state_backend.get_invocation(
                blocking_invocation_id
            )
            if not self.is_candidate_to_run_by_concurrency_control(blocking_invocation):
                continue
            try:
                self.set_invocation_status(
                    blocking_invocation_id, InvocationStatus.PENDING, runner_ctx
                )
                blocking_invocation_ids.add(blocking_invocation_id)
                yield blocking_invocation_id
            except InvocationStatusError as ex:
                # Race condition: another worker claimed it between our status check and transition
                self.app.logger.debug(
                    f"Could not set blocking invocation:{blocking_invocation_id} to status:pending: {ex}"
                )
                continue

    def get_additional_invocations_to_run(
        self,
        missing_invocations: int,
        blocking_invocation_ids: set["InvocationId"],
        invocations_to_reroute: set["InvocationId"],
        runner_ctx: "RunnerContext",
    ) -> Iterator["DistributedInvocation"]:
        """
        Retrieves additional invocations to run, considering those not blocked or already identified as blocking.

        :param int missing_invocations: The number of additional invocations needed.
        :param set["InvocationId"] blocking_invocation_ids: IDs of invocations already identified as blocking.
        :param set["InvocationId"] invocations_to_reroute: A set to collect invocations that need rerouting.
        :return: An iterator over the additional invocations to run.
        :rtype: Iterator["DistributedInvocation"]
        """
        while missing_invocations > 0:
            if invocation_id := self.app.broker.retrieve_invocation():
                if invocation_id not in blocking_invocation_ids:
                    invocation_status = self.get_invocation_status(invocation_id)
                    if invocation_status.is_available_for_run():
                        invocation = self.app.state_backend.get_invocation(
                            invocation_id
                        )
                        if not self.is_candidate_to_run_by_concurrency_control(
                            invocation
                        ):
                            if invocation.task.conf.reroute_on_concurrency_control:
                                self.set_invocation_status(
                                    invocation_id,
                                    InvocationStatus.CONCURRENCY_CONTROLLED,
                                    runner_ctx,
                                )
                                invocations_to_reroute.add(invocation_id)
                            else:
                                self.set_invocation_status(
                                    invocation_id,
                                    InvocationStatus.CONCURRENCY_CONTROLLED_FINAL,
                                    runner_ctx,
                                )
                            continue
                        try:
                            self.set_invocation_status(
                                invocation_id,
                                InvocationStatus.PENDING,
                                runner_ctx,
                            )
                            missing_invocations -= 1
                            yield invocation
                        except InvocationStatusError as ex:
                            self.app.logger.warning(
                                f"Could not set invocation:{invocation_id} to status:pending: {ex}"
                            )
                            continue
            else:
                break

    def reroute_invocations(
        self,
        invocations_to_reroute: set["InvocationId"],
        runner_ctx: "RunnerContext",
    ) -> None:
        """
        Reroutes the specified invocations , typically when they are not authorized to run.

        :param set["InvocationId"] invocations_to_reroute: The invocations to be rerouted.
        """
        for invocation_id in invocations_to_reroute:
            self.set_invocation_status(
                invocation_id, InvocationStatus.REROUTED, runner_ctx
            )
            self.app.broker.route_invocation(invocation_id)

    def get_invocations_to_run(
        self, max_num_invocations: int, runner_ctx: "RunnerContext"
    ) -> Iterator["DistributedInvocation"]:
        """
        Retrieves a set of invocations to run, considering blocking and concurrency control.

        :param int max_num_invocations: The maximum number of invocations to retrieve.
        :return: An iterator over invocations that are ready to run.
        :rtype: Iterator["DistributedInvocation"]
        """
        blocking_invocation_ids: set[InvocationId] = set()
        # Get blocking invocations as IDs but still need to yield actual invocations
        for blocking_invocation_id in self.get_blocking_invocations_to_run(
            max_num_invocations, blocking_invocation_ids, runner_ctx
        ):
            if invocation := self.app.state_backend.get_invocation(
                blocking_invocation_id
            ):
                yield invocation

        invocations_to_reroute: set[InvocationId] = set()
        missing_invocations = max_num_invocations - len(blocking_invocation_ids)
        yield from self.get_additional_invocations_to_run(
            missing_invocations,
            blocking_invocation_ids,
            invocations_to_reroute,
            runner_ctx,
        )
        self.reroute_invocations(invocations_to_reroute, runner_ctx)

    def register_new_invocations(
        self, invocations: list["DistributedInvocation[Params, Result]"]
    ) -> None:
        """
        Registers a new invocation in the state backend and the orchestrator.

        :param DistributedInvocation[Params, Result] invocation: The invocation to register.
        :param str | None runner_id: The owner ID for ownership validation
        """
        runner_ctx = context.get_or_create_runner_context(self.app.app_id)
        runner_id = runner_ctx.runner_id
        self.app.state_backend.upsert_invocations(invocations)
        # This should add the status registered to the backend
        status_record = self._register_new_invocations(invocations, runner_id)
        inv_ids = [invocation.invocation_id for invocation in invocations]
        self.app.state_backend.add_histories(invocations, status_record, runner_ctx)
        self.app.trigger.report_tasks_status(inv_ids, status_record.status)
        self.app.broker.route_invocations(inv_ids)
        for invocation in invocations:
            task_key = invocation.call.task.task_id.key
            inv_id = invocation.invocation_id
            self.app.logger.info(
                f"NEW task:{task_key} invocation:{inv_id} REGISTERED and ROUTED"
            )

    def _route_new_call_invocation(
        self, call: "Call[Params, Result]", runner_id: str | None = None
    ) -> "DistributedInvocation[Params, Result]":
        """
        Routes a new call invocation within the distributed task system.

        This method creates and routes a new `DistributedInvocation` for the provided call.
        It is primarily used when the task does not have single invocation options set.

        :param Call[Params, Result] call: The task call to be routed.
        :return: The newly created `DistributedInvocation` for the call.
        :rtype: DistributedInvocation[Params, Result]
        """
        parent_invocation = context.get_dist_invocation_context(self.app.app_id)
        new_invocation = DistributedInvocation.from_parent(call, parent_invocation)
        self.register_new_invocations([new_invocation])
        if (
            call.task.conf.registration_concurrency != ConcurrencyControlType.DISABLED
            or call.task.conf.running_concurrency != ConcurrencyControlType.DISABLED
        ):
            self.index_arguments_for_concurrency_control(new_invocation)
        self.app.logger.info(f"invocation:{new_invocation.invocation_id} ROUTED")
        return new_invocation

    def route_call(self, call: "Call") -> "DistributedInvocation":
        """
        Routes a task call in the distributed task system, considering single invocation options.

        This method handles the routing of a task call.

        ```{important}
            Note the different behavior depending on the task's registration_concurrency option.
            A task is Registered when it is created but not yet Running, preventing over-queueing:
            - If ConcurrencyControlType.DISABLED,
                It always creates a new invocation.
            - If ConcurrencyControlType.TASK,
                It checks for any existing invocation of the same task regardless the arguments.
                If any is found, it reuses it, otherwise it creates a new invocation.
            - If ConcurrencyControlType.ARGUMENTS,
                It checks for any existing invocation with the same arguments.
                If any is found, it reuses it, otherwise it creates a new invocation.
            - If ConcurrencyControlType.KEYS,
                It checks for any existing invocation with the same key arguments.
                If any is found and the non-key arguments are the same, it always reuses it,
                IF the non-key arguments are differents and on_diff_non_key_args_raise is set to True,
                it raises an error, otherwise it reuses it with the new non-key arguments.
                If no invocation is found, it creates a new invocation.

        ```
        ```{note}
            The function call.serialized_args_for_concurrency_check is used to get the arguments
            that are used to check for existing invocations based on the task's registration_concurrency option.
        ```

        :param Call call: The task call to be routed.
        :return: A `DistributedInvocation` object, which could be either a new or reused invocation.
        :rtype: DistributedInvocation
        :raises InvocationConcurrencyWithDifferentArgumentsError: If an invocation with different arguments exists
            and the task's configuration specifies to raise an error in such cases.
        """
        if call.task.conf.registration_concurrency == ConcurrencyControlType.DISABLED:
            call.task.logger.debug(f"New invocation for call {call}")
            return self._route_new_call_invocation(call)

        # Handleling registred task concurrency control
        invocation_id = next(
            self.get_existing_invocations(
                task=call.task,
                # we filter here by TASK, all Arguments, or defined keys based in the config
                # TODO Make it explicit
                key_serialized_arguments=call.serialized_args_for_concurrency_check,
                statuses=[InvocationStatus.REGISTERED],
            ),
            None,
        )
        if not invocation_id:
            call.task.logger.debug(
                f"No matching registered invocation found for {call.task} "
                f"and key args {call.serialized_args_for_concurrency_check}"
            )
            return self._route_new_call_invocation(call)

        invocation = self.app.state_backend.get_invocation(invocation_id)
        if not invocation:
            call.task.logger.warning(
                f"invocation:{invocation_id} not found in state backend for call {call}, routing new invocation"
            )
            return self._route_new_call_invocation(call)

        if invocation.call.call_id == call.call_id:
            call.task.logger.debug(
                f"Reusing invocation:{invocation.invocation_id} for call {call}"
            )
            return ReusedInvocation(invocation)
        if call.task.conf.on_diff_non_key_args_raise:
            raise InvocationConcurrencyWithDifferentArgumentsError.from_call_mismatch(
                existing_invocation=invocation, new_call=call
            )
        # TODO: review this code, we are reusing an invocation with different non-key arguments, should we still reuse it?
        return ReusedInvocation(invocation, call.arguments)

    def route_calls(
        self, calls: list["PreSerializedCall[Params, Result]"]
    ) -> list["DistributedInvocation[Params, Result]"]:
        """
        Routes multiple calls at once for improved performance.

        This method is specifically for batch processing tasks with disabled concurrency control.

        :param list[PreSerializedCall[Params, Result]] calls: The calls to be routed.
        :return: The list of routed invocations.
        :rtype: list[DistributedInvocation[Params, Result]]
        :raises TaskParallelProcessingError: If concurrency control is enabled for any of the calls.
        """
        if not calls:
            self.app.logger.debug("No calls to route")
            return []

        if (
            calls[0].task.conf.registration_concurrency
            != ConcurrencyControlType.DISABLED
        ):
            raise TaskParallelProcessingError(
                calls[0].task.task_id,
                "Batch processing is only supported with ConcurrencyControlType.DISABLED",
            )

        parent_invocation = context.get_dist_invocation_context(self.app.app_id)
        invocations: list[DistributedInvocation[Params, Result]] = [
            DistributedInvocation.from_parent(call, parent_invocation) for call in calls
        ]
        self.register_new_invocations(invocations)
        return invocations

    @abstractmethod
    def register_runner_heartbeats(
        self,
        runner_ids: list[str],
        can_run_atomic_service: bool = False,
    ) -> None:
        """
        Register or update heartbeat timestamps for one or more runners.

        This unified method handles both:
        - Parent/atomic service runners registering themselves (can_run_atomic_service=True)
        - Worker runners registering themselves (can_run_atomic_service=False, default)
        - Parent runners reporting their children's heartbeats (can_run_atomic_service=False)

        For runners that already exist, only updates the heartbeat timestamp.
        For new runners, creates a new record with the given atomic service eligibility.

        :param list[str] runner_ids: List of runner_ids to register/update heartbeats for.
        :param bool can_run_atomic_service: Whether these runners are eligible to run atomic services.
        """

    @abstractmethod
    def _get_active_runners(
        self, timeout_seconds: float, can_run_atomic_service: bool | None
    ) -> list["ActiveRunnerInfo"]:
        """
        Retrieve runners that are considered active based on heartbeat activity.

        A runner is considered "active" if it has sent a heartbeat within the timeout period.
        This is used for atomic service scheduling to determine which runners are eligible
        to participate in time slot distribution.

        :param float timeout_seconds: Heartbeat timeout in seconds (typically from runner_considered_dead_after_minutes config)
        :param bool | None can_run_atomic_service: If specified, filters runners based on their eligibility to run atomic services
        :return: List of active runners ordered by creation time (oldest first)
        :rtype: list["ActiveRunnerInfo"]
        """

    def get_active_runners(
        self, can_run_atomic_service: bool | None = None
    ) -> list["ActiveRunnerInfo"]:
        """
        Retrieve runners that are considered active based on heartbeat activity.

        A runner is considered "active" if it has sent a heartbeat within the timeout period.
        This is used for atomic service scheduling to determine which runners are eligible
        to participate in time slot distribution.

        :param bool | None can_run_atomic_service: If specified, filters runners based on their eligibility to run atomic services
        :return: List of active runners ordered by creation time (oldest first)
        :rtype: list["ActiveRunnerInfo"]
        """
        timeout_seconds = self.app.conf.runner_considered_dead_after_minutes * 60
        return self._get_active_runners(timeout_seconds, can_run_atomic_service)

    @abstractmethod
    def get_pending_invocations_for_recovery(self) -> Iterator["InvocationId"]:
        """
        Retrieve invocation IDs stuck in PENDING status beyond the allowed time.

        :return: Iterator of invocation IDs that need recovery.
        :rtype: Iterator["InvocationId"]
        """

    @abstractmethod
    def _get_running_invocations_for_recovery(
        self, timeout_seconds: float
    ) -> Iterator["InvocationId"]:
        """
        Retrieve invocation IDs in RUNNING status owned by inactive runners.

        An inactive runner is one that hasn't sent a heartbeat within the
        configured timeout period. Invocations owned by such runners are
        considered stuck and need recovery.

        :param float timeout_seconds: Heartbeat timeout in seconds
        :return: Iterator of invocation IDs that need recovery.
        :rtype: Iterator["InvocationId"]
        """

    def get_running_invocations_for_recovery(self) -> Iterator["InvocationId"]:
        """
        Retrieve invocation IDs in RUNNING status owned by inactive runners.

        An inactive runner is one that hasn't sent a heartbeat within the
        configured timeout period. Invocations owned by such runners are
        considered stuck and need recovery.

        :param float timeout_seconds: Heartbeat timeout in seconds
        :return: Iterator of invocation IDs that need recovery.
        :rtype: Iterator["InvocationId"]
        """
        timeout_seconds = self.app.conf.runner_considered_dead_after_minutes * 60
        return self._get_running_invocations_for_recovery(timeout_seconds)

    def should_run_atomic_service(self, runner_ctx: "RunnerContext") -> bool:
        """
        Determine if the current runner should execute atomic global services.

        This method has a side effect: it registers a heartbeat for the runner with can_run_atomic_service=True.
        This ensures that only runners actively checking for atomic service eligibility are considered for atomic service distribution.

        Uses runner count and timing to distribute service execution across runners,
        preventing simultaneous execution and race conditions.

        :param RunnerContext runner_ctx: The context of the current runner.
        :return: True if this runner should execute services now.
        :rtype: bool
        """
        self.register_runner_heartbeats(
            [runner_ctx.runner_id], can_run_atomic_service=True
        )
        active_runners = self.get_active_runners(can_run_atomic_service=True)

        return can_run_atomic_service(
            runner_id=runner_ctx.runner_id,
            active_runners=active_runners,
            current_time=time(),
            service_interval_minutes=self.app.conf.atomic_service_interval_minutes,
            spread_margin_minutes=self.app.conf.atomic_service_spread_margin_minutes,
        )

    @abstractmethod
    def record_atomic_service_execution(
        self, runner_id: str, start_time: datetime, end_time: datetime
    ) -> None:
        """
        Record the latest atomic service execution window for a runner.

        Replaces any previous execution record for this runner with the current one.
        Used for diagnostics and detecting potential collisions.

        :param str runner_id: The runner that executed the service
        :param datetime start_time: When execution started (UTC timezone-aware)
        :param datetime end_time: When execution ended (UTC timezone-aware)
        """
