from abc import ABC, abstractmethod
from collections.abc import Iterator
from functools import cached_property
from typing import TYPE_CHECKING, Any, Optional

from pynenc import context
from pynenc.conf.config_orchestrator import ConfigOrchestrator
from pynenc.conf.config_task import ConcurrencyControlType
from pynenc.exceptions import (
    InvocationConcurrencyWithDifferentArgumentsError,
    PendingInvocationLockError,
    TaskParallelProcessingError,
)
from pynenc.invocation.dist_invocation import DistributedInvocation, ReusedInvocation
from pynenc.invocation.status import InvocationStatus

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.call import Call, PreSerializedCall
    from pynenc.runner import RunnerContext
    from pynenc.task import Task
    from pynenc.types import Params, Result


class BaseCycleControl(ABC):
    """
    A component of the orchestrator to implement cycle control functionalities.

    This abstract base class defines the interface for cycle control in a distributed task system.
    It is intended to prevent the formation of call cycles between tasks.
    """

    @abstractmethod
    def add_call_and_check_cycles(
        self, caller: "DistributedInvocation", callee: "DistributedInvocation"
    ) -> None:
        """
        Adds a new call relationship between invocations and checks for potential cycles.

        :param DistributedInvocation[Params, Result] caller: The invocation calling another task.
        :param DistributedInvocation[Params, Result] callee: The invocation being called.
        :raises CycleDetectedError: If adding the call creates a cycle.
        """
        # TODO async store of call dependencies in state backend

    @abstractmethod
    def get_callees(self, caller_call_id: str) -> Iterator[str]:
        """
        Returns an iterator of direct callee call_ids for the given caller_call_id.

        :param str caller_call_id: The call_id of the caller invocation.
        :return: Iterator of callee call_ids.
        :rtype: Iterator[str]
        """

    @abstractmethod
    def clean_up_invocation_cycles(self, invocation_id: str) -> None:
        """
        Cleans up any cycle-related data when an invocation is finished.

        :param str invocation_id: The ID of the invocation that has finished.
        """


class BaseBlockingControl(ABC):
    """
    A component of the orchestrator to implement blocking control functionalities.

    This abstract base class defines the interface for managing blocking behavior in distributed task executions.
    """

    @abstractmethod
    def release_waiters(self, waited_invocation_id: str) -> None:
        """
        Releases any invocations that are waiting on the specified invocation.

        :param str waited_invocation_id: The ID of the invocation that has finished and can release its waiters.
        """

    @abstractmethod
    def waiting_for_results(
        self, caller_invocation_id: str, result_invocation_ids: list[str]
    ) -> None:
        """
        Notifies the system that an invocation is waiting for the results of other invocations.

        :param str caller_invocation_id: The ID of the invocation that is waiting.
        :param list[str] result_invocation_ids: The IDs of the invocations being waited on.
        """

    @abstractmethod
    def get_blocking_invocations(self, max_num_invocations: int) -> Iterator[str]:
        """
        Retrieves invocation IDs that are blocking others but are not blocked themselves.

        :param int max_num_invocations: The maximum number of blocking invocation IDs to retrieve.
        :return: An iterator over unblocked, blocking invocation IDs, **ordered by age (oldest first)**.
        :rtype: Iterator[str]
        """


class BaseOrchestrator(ABC):
    """
    Abstract base class defining the orchestrator's interface in a distributed task system.

    The orchestrator is responsible for managing task invocations, including tracking their status,
    handling retries, and implementing cycle and blocking controls.

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
        self, invocations: list["DistributedInvocation[Params, Result]"]
    ) -> None:
        """
        Register new invocations with status Register if they don't exist yet.

        :param list[DistributedInvocation[Params, Result]] invocations: The invocations to be registered.
        """

    @abstractmethod
    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_serialized_arguments: Optional[dict[str, str]] = None,
        statuses: Optional[list["InvocationStatus"]] = None,
    ) -> Iterator[str]:
        """
        Retrieves existing invocation IDs based on task, arguments, and status.

        :param Task[Params, Result] task: The task for which to retrieve invocations.
        :param Optional[dict[str, str]] key_serialized_arguments: Serialized arguments to filter invocations.
        :param Optional[list[InvocationStatus]] statuses: The statuses to filter invocations.
        :return: An iterator over the matching invocation IDs.
        :rtype: Iterator[str]
        """

    @abstractmethod
    def get_task_invocation_ids(self, task_id: str) -> Iterator[str]:
        """
        Retrieves all invocation IDs associated with a specific task ID.

        :param str task_id: The task ID to filter invocations.
        :return: List of invocation IDs for the specified task.
        """

    @abstractmethod
    def get_call_invocation_ids(self, call_id: str) -> Iterator[str]:
        """
        Retrieves all invocation IDs associated with a specific call ID.

        :param str call_id: The call ID to filter invocations.
        :return: List of invocation IDs for the specified call.
        """

    @abstractmethod
    def get_invocation_call_id(self, invocation_id: str) -> str:
        """
        Retrieves the call ID associated with a specific invocation ID.

        :param str invocation_id: The invocation ID to look up.
        :return: The call ID associated with the invocation
        """

    @abstractmethod
    def any_non_final_invocations(self, call_id: str) -> bool:
        """
        Checks if there are any non-final invocations for a specific call ID.

        :param str call_id: The call ID to check for non-final invocations.
        :return: True if there are non-final invocations, False otherwise.
        """

    @abstractmethod
    def _set_invocation_status(
        self,
        invocation_id: str,
        status: "InvocationStatus",
    ) -> None:
        """
        Sets the status of a specific invocation.

        :param str invocation_id: The ID of the invocation to update.
        :param InvocationStatus status: The new status to set for the invocation.
        """

    @abstractmethod
    def _set_invocations_status(
        self, invocation_ids: list[str], status: InvocationStatus
    ) -> None:
        """
        Set the status of multiple invocations at once.

        Default implementation sets status for each invocation sequentially.
        Subclasses should override this with more efficient batch implementations.

        :param list[str] invocation_ids: The invocations to update.
        :param InvocationStatus status: The status to set.
        """

    @abstractmethod
    def _set_invocation_pending_status(self, invocation_id: str) -> None:
        """
        Sets the status of an invocation to pending.
        ```{note}
            Pending can only be set by the orchestrator
        ```
        :param str invocation_id: The ID of the invocation to update.
        """

    def set_invocation_pending_status(self, invocation_id: str) -> None:
        """
        Marks an invocation as pending and updates its history in the state backend.
        ```{note}
            Pending can only be set by the orchestrator
        ```
        :param str invocation_id: The ID of the invocation to mark as pending.
        """
        self._set_invocation_pending_status(invocation_id)
        self.app.state_backend.add_histories(
            [invocation_id], status=InvocationStatus.PENDING
        )

    @abstractmethod
    def get_invocation_pending_timer(self, invocation_id: str) -> Optional[float]:
        """
        Retrieves the pending timer for a specific invocation.

        :param str invocation_id: The ID of the invocation to look up.
        :return: The pending timer value, or None if not set.
        :rtype: Optional[float]
        """

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
    def set_up_invocation_auto_purge(self, invocation_id: str) -> None:
        """
        Sets up automatic purging for an invocation after a defined period.
        ```{note}
            Set auto purge period with `app.conf.orchestrator_auto_final_invocation_purge_hours`
        ```

        :param DistributedInvocation[Params, Result] invocation: The invocation to set up for auto purge.
        """

    @abstractmethod
    def auto_purge(self) -> None:
        """
        Automatically purges all invocations in a final state that are older than a defined time period.
        ```{note}
            Set auto purge period with `app.conf.orchestrator_auto_final_invocation_purge_hours`
        ```
        """

    @abstractmethod
    def get_invocation_status(self, invocation_id: str) -> "InvocationStatus":
        """
        Retrieves the status of a specific invocation id.

        :param str invocation_id: The id of the invocation whose status is to be retrieved.
        :return: The current status of the invocation.
        :rtype: InvocationStatus
        """

    @abstractmethod
    def increment_invocation_retries(self, invocation_id: str) -> None:
        """
        Increments the retry count of a specific invocation.

        :param str invocation_id: The id of the invocation for which to increment retries.
        """

    @abstractmethod
    def get_invocation_retries(self, invocation_id: str) -> int:
        """
        Retrieves the number of retries for a specific invocation.

        :param str invocation_id: The id of the invocation whose retry count is to be retrieved.
        :return: The number of retries for the invocation.
        :rtype: int
        """

    @abstractmethod
    def filter_by_status(
        self, invocation_ids: list[str], status_filter: set["InvocationStatus"]
    ) -> list[str]:
        """
        Filters a list of invocation ids by their status in an optimized way.

        This method allows efficient batch filtering of invocations by status,
        reducing the number of individual status checks needed.

        :param list[str] invocations: The invocation ids to filter
        :param list[InvocationStatus] | None status_filter: The statuses to filter by.
            If None, defaults to final statuses.
        :return: List of invocation ids matching the status filter
        :rtype: list[str]
        """
        pass

    def filter_final(self, invocation_ids: list[str]) -> list[str]:
        """
        Returns invocation ids that have reached a final status.

        :param list[str] invocations: The invocation ids to check
        :return: List of invocation ids that have reached a final status
        :rtype: list[str]
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
    # cycle sub functionalities
    @property
    @abstractmethod
    def cycle_control(self) -> BaseCycleControl:
        """
        Property to access the cycle control component of the orchestrator.

        :return: The cycle control component.
        :rtype: BaseCycleControl
        """

    def add_call_and_check_cycles(
        self,
        caller_invocation: "DistributedInvocation[Params, Result]",
        callee_invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """
        Adds a call relationship between two invocations and checks for potential cycles.

        :param DistributedInvocation[Params, Result] caller_invocation: The calling invocation.
        :param DistributedInvocation[Params, Result] callee_invocation: The called invocation.
        """
        if self.conf.cycle_control:
            self.cycle_control.add_call_and_check_cycles(
                caller_invocation, callee_invocation
            )
        # TODO async store of call dependencies in state backend

    def clean_up_invocation_cycles(self, invocation_id: str) -> None:
        """
        Cleans up data related to invocation cycles when an invocation finishes.

        :param str invocation_id: The ID of the invocation that has finished.
        """
        if self.conf.cycle_control:
            self.cycle_control.clean_up_invocation_cycles(invocation_id)

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

    def release_waiters(self, waited_id: str) -> None:
        """
        Releases other invocations that are waiting on the completion of the specified invocation.

        :param str waited_id: The ID of the invocation that has completed.
        """
        if self.app.orchestrator.conf.blocking_control:
            self.blocking_control.release_waiters(waited_id)

    def waiting_for_results(
        self, caller_invocation_id: Optional[str], result_invocation_ids: list[str]
    ) -> None:
        """
        Notifies the system that an invocation is waiting on the results of other invocations.

        :param Optional[str] caller_invocation_id: The ID of the waiting invocation.
        :param list[str] result_invocation_ids: The IDs of the invocations being waited on.
        """
        if not result_invocation_ids:
            self.app.logger.warning(
                f"Unnecessary call to waiting_for_results, {caller_invocation_id=} is not waiting for any results"
            )
            return
        if self.app.orchestrator.conf.blocking_control and caller_invocation_id:
            self.blocking_control.waiting_for_results(
                caller_invocation_id, result_invocation_ids
            )

    def get_blocking_invocations(self, max_num_invocation_ids: int) -> Iterator[str]:
        """
        Retrieves invocation IDs that are blocking others but not blocked themselves.

        :param int max_num_invocation_ids: The maximum number of blocking invocation IDs to retrieve.
        :return: An iterator over unblocked, blocking invocation IDs.
        :rtype: Iterator[str]

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
        invocation_id: str,
        status: "InvocationStatus",
        runner_ctx: Optional["RunnerContext"] = None,
    ) -> None:
        """
        Sets the status of a specific invocation.

        :param DistributedInvocation[Params, Result] invocation: The invocation to update.
        :param InvocationStatus status: The new status to set for the invocation.
        """
        if status == InvocationStatus.PENDING:
            self._set_invocation_pending_status(invocation_id)
        else:
            if status.is_final():
                self.release_waiters(invocation_id)
                self.clean_up_invocation_cycles(invocation_id)
                self.set_up_invocation_auto_purge(invocation_id)
            # TODO! on previous fail, this should still change status to running
            self._set_invocation_status(invocation_id, status)
        self.app.state_backend.add_histories([invocation_id], status, runner_ctx)
        self.app.trigger.report_tasks_status([invocation_id], status)

    def set_invocations_status(
        self,
        invocation_ids: list[str],
        status: "InvocationStatus",
    ) -> None:
        """
        Sets the status for a list of invocations.

        :param list[str] invocation_ids: The invocation_ids to update.
        :param InvocationStatus status: The new status to set for the invocations.
        """
        self._set_invocations_status(invocation_ids, status)
        self.app.trigger.report_tasks_status(invocation_ids, status)

    def set_invocation_run(
        self,
        caller: Optional["DistributedInvocation[Params, Result]"],
        callee: "DistributedInvocation[Params, Result]",
        runner_ctx: "RunnerContext",
    ) -> None:
        """
        Marks an invocation as running and checks for call cycles if a caller is specified.

        :param Optional[DistributedInvocation[Params, Result]] caller: The calling invocation, if any.
        :param DistributedInvocation[Params, Result] callee: The invocation that is being marked as running.
        """
        if caller:
            self.add_call_and_check_cycles(caller, callee)
        # TODO! on previous fail, this should still change status
        self.set_invocation_status(
            callee.invocation_id, InvocationStatus.RUNNING, runner_ctx
        )
        callee.wf.register_task_run(caller)

    def set_invocation_result(
        self, invocation: "DistributedInvocation", result: Any
    ) -> None:
        """
        Sets the result for a completed invocation.

        :param DistributedInvocation invocation: The invocation that has completed.
        :param Any result: The result of the invocation's execution.
        """
        self.app.state_backend.set_result(invocation.invocation_id, result)
        self.app.orchestrator.set_invocation_status(
            invocation.invocation_id, InvocationStatus.SUCCESS
        )
        self.app.trigger.report_invocation_result(invocation, result)

    def set_invocation_exception(
        self, invocation: "DistributedInvocation", exception: Exception
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
            invocation.invocation_id, InvocationStatus.FAILED
        )
        self.app.trigger.report_invocation_failure(invocation, exception)

    def set_invocation_retry(
        self, invocation: "DistributedInvocation", exception: Exception
    ) -> None:
        """
        Sets an invocation for retry in case of a retriable exception.

        :param DistributedInvocation invocation: The invocation to be retried.
        :param Exception exception: The exception that triggered the retry.
        """
        # TODO! on previous fail, this should still change status
        # eg. on case of interruption from a kubernetes pod (SIGTERM, SIGKILL)
        #     it should try to finish all the calls in this function
        self.app.orchestrator.set_invocation_status(
            invocation.invocation_id, InvocationStatus.RETRY
        )
        self.app.orchestrator.increment_invocation_retries(invocation.invocation_id)
        self.app.broker.route_invocation(invocation)

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
        if not invocation:
            return False
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
            f"{invocation.invocation_id=} cannot run because {running_invocation} is already in {statuses} status"
        )
        return False

    def get_blocking_invocations_to_run(
        self, max_num_invocations: int, blocking_invocation_ids: set[str]
    ) -> Iterator[str]:
        """
        Retrieves invocation IDs that are blocking others but are not themselves blocked, up to a maximum number.

        :param int max_num_invocations: The maximum number of blocking invocations to retrieve.
        :param set[str] blocking_invocation_ids: A set of invocation IDs that are already identified as blocking.
        :return: An iterator over the blocking invocation IDs.
        :rtype: Iterator[str]
        """
        for blocking_invocation_id in self.get_blocking_invocations(
            max_num_invocations
        ):
            blocking_invocation = self.app.state_backend.get_invocation(
                blocking_invocation_id
            )
            if not self.is_candidate_to_run_by_concurrency_control(blocking_invocation):
                continue
            blocking_invocation_ids.add(blocking_invocation_id)
            try:
                self.set_invocation_pending_status(blocking_invocation_id)
                yield blocking_invocation_id
            except PendingInvocationLockError:
                continue

    def get_additional_invocations_to_run(
        self,
        missing_invocations: int,
        blocking_invocation_ids: set[str],
        invocations_to_reroute: set["DistributedInvocation"],
    ) -> Iterator["DistributedInvocation"]:
        """
        Retrieves additional invocations to run, considering those not blocked or already identified as blocking.

        :param int missing_invocations: The number of additional invocations needed.
        :param set[str] blocking_invocation_ids: IDs of invocations already identified as blocking.
        :param set[DistributedInvocation] invocations_to_reroute: A set to collect invocations that need rerouting.
        :return: An iterator over the additional invocations to run.
        :rtype: Iterator[DistributedInvocation]
        """
        while missing_invocations > 0:
            if invocation := self.app.broker.retrieve_invocation():
                if invocation.invocation_id not in blocking_invocation_ids:
                    invocation_status = self.get_invocation_status(
                        invocation.invocation_id
                    )
                    if invocation_status.is_available_for_run():
                        if not self.is_candidate_to_run_by_concurrency_control(
                            invocation
                        ):
                            invocations_to_reroute.add(invocation)
                            continue
                        try:
                            self.set_invocation_pending_status(invocation.invocation_id)
                            missing_invocations -= 1
                            yield invocation
                        except PendingInvocationLockError:
                            # invocations_to_reroute.add(invocation)
                            continue
            else:
                break

    def reroute_invocations(
        self, invocations_to_reroute: set["DistributedInvocation"]
    ) -> None:
        """
        Reroutes the specified invocations, typically when they are not authorized to run.

        :param set[DistributedInvocation] invocations_to_reroute: The invocations to be rerouted.
        """
        for invocation in invocations_to_reroute:
            invocation.task.logger.debug(
                f"Rerouting invocation {invocation.invocation_id} because it was not authorized to run"
            )
            self.set_invocation_status(
                invocation.invocation_id, InvocationStatus.REROUTED
            )
            self.app.broker.route_invocation(invocation)

    def get_invocations_to_run(
        self, max_num_invocations: int
    ) -> Iterator["DistributedInvocation"]:
        """
        Retrieves a set of invocations to run, considering blocking and concurrency control.

        :param int max_num_invocations: The maximum number of invocations to retrieve.
        :return: An iterator over invocations that are ready to run.
        :rtype: Iterator[DistributedInvocation]
        """
        blocking_invocation_ids: set[str] = set()
        # Get blocking invocations as IDs but still need to yield actual invocations
        for blocking_invocation_id in self.get_blocking_invocations_to_run(
            max_num_invocations, blocking_invocation_ids
        ):
            if invocation := self.app.state_backend.get_invocation(
                blocking_invocation_id
            ):
                yield invocation

        invocations_to_reroute: set[DistributedInvocation] = set()
        missing_invocations = max_num_invocations - len(blocking_invocation_ids)
        yield from self.get_additional_invocations_to_run(
            missing_invocations, blocking_invocation_ids, invocations_to_reroute
        )
        self.reroute_invocations(invocations_to_reroute)

    def register_new_invocations(
        self, invocations: list["DistributedInvocation[Params, Result]"]
    ) -> None:
        """
        Registers a new invocation in the state backend and the orchestrator.

        :param DistributedInvocation[Params, Result] invocation: The invocation to register.
        """
        self.app.state_backend.upsert_invocations(invocations)
        # This should add the status registered to the backend
        self._register_new_invocations(invocations)
        inv_ids = [invocation.invocation_id for invocation in invocations]
        self.app.state_backend.add_histories(inv_ids, InvocationStatus.REGISTERED)
        self.app.trigger.report_tasks_status(inv_ids, InvocationStatus.REGISTERED)
        self.app.broker.route_invocations(invocations)

    def _route_new_call_invocation(
        self, call: "Call[Params, Result]"
    ) -> "DistributedInvocation[Params, Result]":
        """
        Routes a new call invocation within the distributed task system.

        This method creates and routes a new `DistributedInvocation` for the provided call.
        It is primarily used when the task does not have single invocation options set.mems

        :param Call[Params, Result] call: The task call to be routed.
        :return: The newly created `DistributedInvocation` for the call.
        :rtype: DistributedInvocation[Params, Result]
        """
        parent_invocation = context.get_dist_invocation_context(self.app.app_id)
        new_invocation = DistributedInvocation(call, parent_invocation)
        self.app.logger.info(f"routing {new_invocation=}")
        self.register_new_invocations([new_invocation])
        if (
            call.task.conf.registration_concurrency != ConcurrencyControlType.DISABLED
            or call.task.conf.running_concurrency != ConcurrencyControlType.DISABLED
        ):
            self.index_arguments_for_concurrency_control(new_invocation)
        self.app.logger.info(
            f"routed task {call.task.task_id} with invocation {new_invocation.invocation_id}"
        )
        return new_invocation

    def route_call(self, call: "Call") -> "DistributedInvocation":
        """
        Routes a task call in the distributed task system, considering single invocation options.

        This method handles the routing of a task call.

        ```{important}
            Note the different behavior depending on the task's registration_concurrency option.
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
            return self._route_new_call_invocation(call)

        if invocation.serialized_arguments == call.serialized_arguments:
            call.task.logger.debug(
                f"Reusing invocation {invocation.invocation_id} for call {call}"
            )
            return ReusedInvocation.from_existing(invocation)
        if call.task.conf.on_diff_non_key_args_raise:
            raise InvocationConcurrencyWithDifferentArgumentsError.from_call_mismatch(
                existing_invocation=invocation, new_call=call
            )
        return ReusedInvocation.from_existing(invocation, call.arguments)

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
            DistributedInvocation(call, parent_invocation=parent_invocation)  # type: ignore
            for call in calls
        ]
        self.register_new_invocations(invocations)
        return invocations
