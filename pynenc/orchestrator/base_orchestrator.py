from abc import ABC, abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING, Any, Iterator, Optional

from pynenc import context
from pynenc.conf.config_orchestrator import ConfigOrchestrator
from pynenc.conf.config_task import ConcurrencyControlType
from pynenc.exceptions import (
    InvocationConcurrencyWithDifferentArgumentsError,
    PendingInvocationLockError,
)
from pynenc.invocation.dist_invocation import DistributedInvocation, ReusedInvocation
from pynenc.invocation.status import InvocationStatus

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.call import Call
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
        self,
        caller_invocation: "DistributedInvocation[Params, Result]",
        callee_invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """
        Adds a new call relationship between invocations and checks for potential cycles.

        :param DistributedInvocation[Params, Result] caller_invocation: The invocation calling another task.
        :param DistributedInvocation[Params, Result] callee_invocation: The invocation being called.
        :raises CycleDetectedError: If adding the call creates a cycle.
        """
        # TODO async store of call dependencies in state backend

    @abstractmethod
    def clean_up_invocation_cycles(self, invocation: "DistributedInvocation") -> None:
        """
        Cleans up any cycle-related data when an invocation is finished.

        :param DistributedInvocation invocation: The invocation that has finished.
        """


class BaseBlockingControl(ABC):
    """
    A component of the orchestrator to implement blocking control functionalities.

    This abstract base class defines the interface for managing blocking behavior in distributed task executions.
    """

    @abstractmethod
    def release_waiters(self, waited: "DistributedInvocation") -> None:
        """
        Releases any invocations that are waiting on the specified invocation.

        :param DistributedInvocation waited: The invocation that has finished and can release its waiters.
        """

    @abstractmethod
    def waiting_for_results(
        self,
        caller_invocation: "DistributedInvocation[Params, Result]",
        result_invocations: list["DistributedInvocation[Params, Result]"],
    ) -> None:
        """
        Notifies the system that an invocation is waiting for the results of other invocations.

        :param DistributedInvocation[Params, Result] caller_invocation: The invocation that is waiting.
        :param list[DistributedInvocation[Params, Result]] result_invocations: The invocations being waited on.
        """

    @abstractmethod
    def get_blocking_invocations(
        self, max_num_invocations: int
    ) -> Iterator["DistributedInvocation"]:
        """
        Retrieves invocations that are blocking others but are not blocked themselves.

        :param int max_num_invocations: The maximum number of blocking invocations to retrieve.
        :return: An iterator over unblocked, blocking invocations, **ordered by age (oldest first)**.
        :rtype: Iterator[DistributedInvocation]
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
    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_serialized_arguments: Optional[dict[str, str]] = None,
        statuses: Optional[list["InvocationStatus"]] = None,
    ) -> Iterator["DistributedInvocation"]:
        """
        Retrieves existing invocations based on task, arguments, and status.

        :param Task[Params, Result] task: The task for which to retrieve invocations.
        :param Optional[dict[str, str]] key_serialized_arguments: Serialized arguments to filter invocations.
        :param Optional[list[InvocationStatus]] status: The status to filter invocations.
        :return: An iterator over the matching invocations.
        :rtype: Iterator[DistributedInvocation]
        """

    @abstractmethod
    def _set_invocation_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: "InvocationStatus",
    ) -> None:
        """
        Sets the status of a specific invocation.

        :param DistributedInvocation[Params, Result] invocation: The invocation to update.
        :param InvocationStatus status: The new status to set for the invocation.
        """

    @abstractmethod
    def _set_invocation_pending_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """
        Sets the status of an invocation to pending.
        ```{note}
            Pending can only be set by the orchestrator
        ```
        :param DistributedInvocation[Params, Result] invocation: The invocation to update.
        """

    def _set_pending(
        self,
        invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """
        Marks an invocation as pending and updates its history in the state backend.
        ```{note}
            Pending can only be set by the orchestrator
        ```
        :param DistributedInvocation[Params, Result] invocation: The invocation to mark as pending.
        """
        self._set_invocation_pending_status(invocation)
        self.app.state_backend.add_history(invocation, status=InvocationStatus.PENDING)

    @abstractmethod
    def set_up_invocation_auto_purge(
        self,
        invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
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
    def get_invocation_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "InvocationStatus":
        """
        Retrieves the status of a specific invocation.

        :param DistributedInvocation[Params, Result] invocation: The invocation whose status is to be retrieved.
        :return: The current status of the invocation.
        :rtype: InvocationStatus
        """
        # TODO if invocation does not exists try in state backend (and cache it up)
        #      before raising an exception!!!

    @abstractmethod
    def increment_invocation_retries(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        """
        Increments the retry count of a specific invocation.

        :param DistributedInvocation[Params, Result] invocation: The invocation for which to increment retries.
        """

    @abstractmethod
    def get_invocation_retries(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> int:
        """
        Retrieves the number of retries for a specific invocation.

        :param DistributedInvocation[Params, Result] invocation: The invocation whose retry count is to be retrieved.
        :return: The number of retries for the invocation.
        :rtype: int
        """

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

    def clean_up_invocation_cycles(self, invocation: "DistributedInvocation") -> None:
        """
        Cleans up data related to invocation cycles when an invocation finishes.

        :param DistributedInvocation invocation: The invocation that has finished.
        """
        if self.conf.cycle_control:
            self.cycle_control.clean_up_invocation_cycles(invocation)

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

    def release_waiters(self, waited: "DistributedInvocation") -> None:
        """
        Releases other invocations that are waiting on the completion of the specified invocation.

        :param DistributedInvocation waited: The invocation that has completed.
        """
        if self.app.orchestrator.conf.blocking_control:
            self.blocking_control.release_waiters(waited)

    def waiting_for_results(
        self,
        caller_invocation: Optional["DistributedInvocation[Params, Result]"],
        result_invocations: list["DistributedInvocation[Params, Result]"],
    ) -> None:
        """
        Notifies the system that an invocation is waiting on the results of other invocations.

        :param Optional[DistributedInvocation[Params, Result]] caller_invocation: The waiting invocation.
        :param list[DistributedInvocation[Params, Result]] result_invocations: The invocations being waited on.
        """
        if self.app.orchestrator.conf.blocking_control and caller_invocation:
            self.blocking_control.waiting_for_results(
                caller_invocation, result_invocations
            )

    def get_blocking_invocations(
        self, max_num_invocations: int
    ) -> Iterator["DistributedInvocation"]:
        """
        Retrieves invocations that are blocking others but not blocked themselves.

        :param int max_num_invocations: The maximum number of blocking invocations to retrieve.
        :return: An iterator over unblocked, blocking invocations.
        :rtype: Iterator[DistributedInvocation]

        ```{note}
            The order of the returned invocations is **oldest first**.
        ```
        """
        if self.app.orchestrator.conf.blocking_control:
            yield from self.blocking_control.get_blocking_invocations(
                max_num_invocations
            )

    #############################################

    def set_invocation_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: "InvocationStatus",
    ) -> None:
        """
        Sets the status of a specific invocation.

        :param DistributedInvocation[Params, Result] invocation: The invocation to update.
        :param InvocationStatus status: The new status to set for the invocation.
        """
        if status == InvocationStatus.PENDING:
            self._set_invocation_pending_status(invocation)
        else:
            # TODO async store of status in state backend
            if status.is_final():
                self.release_waiters(invocation)
                self.clean_up_invocation_cycles(invocation)
                self.set_up_invocation_auto_purge(invocation)
            # TODO! on previous fail, this should still change status to running
            self._set_invocation_status(invocation, status)
        self.app.state_backend.add_history(invocation, status)

    def set_invocations_status(
        self,
        invocations: list["DistributedInvocation[Params, Result]"],
        status: "InvocationStatus",
    ) -> None:
        """
        Sets the status for a list of invocations.

        :param list[DistributedInvocation[Params, Result]] invocations: The invocations to update.
        :param InvocationStatus status: The new status to set for the invocations.
        """
        for invocation in invocations:
            self.set_invocation_status(invocation, status)

    def set_invocation_run(
        self,
        caller: Optional["DistributedInvocation[Params, Result]"],
        callee: "DistributedInvocation[Params, Result]",
    ) -> None:
        """
        Marks an invocation as running and checks for call cycles if a caller is specified.

        :param Optional[DistributedInvocation[Params, Result]] caller: The calling invocation, if any.
        :param DistributedInvocation[Params, Result] callee: The invocation that is being marked as running.
        """
        if caller:
            self.add_call_and_check_cycles(caller, callee)
        # TODO! on previous fail, this should still change status
        self.set_invocation_status(callee, InvocationStatus.RUNNING)

    def set_invocation_result(
        self, invocation: "DistributedInvocation", result: Any
    ) -> None:
        """
        Sets the result for a completed invocation.

        :param DistributedInvocation invocation: The invocation that has completed.
        :param Any result: The result of the invocation's execution.
        """
        self.app.state_backend.set_result(invocation, result)
        # TODO! on previous fail, this should still change status
        self.app.orchestrator.set_invocation_status(
            invocation, InvocationStatus.SUCCESS
        )

    def set_invocation_exception(
        self, invocation: "DistributedInvocation", exception: Exception
    ) -> None:
        """
        Sets an exception for an invocation that finished with an error.

        :param DistributedInvocation invocation: The invocation that encountered an exception.
        :param Exception exception: The exception that occurred during the invocation's execution.
        """
        self.app.state_backend.set_exception(invocation, exception)
        # TODO! on previous fail, this should still change status
        # eg. on case of interruption from a kubernetes pod (SIGTERM, SIGKILL)
        #     it should try to finish all the calls in this function
        self.app.orchestrator.set_invocation_status(invocation, InvocationStatus.FAILED)

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
        self.app.orchestrator.set_invocation_status(invocation, InvocationStatus.RETRY)
        self.app.orchestrator.increment_invocation_retries(invocation)
        self.app.broker.route_invocation(invocation)

    def is_candidate_to_run_by_concurrency_control(
        self, invocation: "DistributedInvocation"
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
        self, invocation: "DistributedInvocation"
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
        self, invocation: "DistributedInvocation", statuses: list["InvocationStatus"]
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
        invocation.task.logger.debug(
            f"{invocation=} cannot run because {running_invocation} is already in {statuses} status"
        )
        return False

    def get_blocking_invocations_to_run(
        self, max_num_invocations: int, blocking_invocation_ids: set[str]
    ) -> Iterator["DistributedInvocation"]:
        """
        Retrieves invocations that are blocking others but are not themselves blocked, up to a maximum number.

        :param int max_num_invocations: The maximum number of blocking invocations to retrieve.
        :param set[str] blocking_invocation_ids: A set of invocation IDs that are already identified as blocking.
        :return: An iterator over the blocking invocations.
        :rtype: Iterator[DistributedInvocation]
        """
        for blocking_invocation in self.get_blocking_invocations(max_num_invocations):
            if not self.is_candidate_to_run_by_concurrency_control(blocking_invocation):
                continue
            blocking_invocation_ids.add(blocking_invocation.invocation_id)
            try:
                self._set_pending(blocking_invocation)
                yield blocking_invocation
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
                    if self.get_invocation_status(invocation).is_available_for_run():
                        if not self.is_candidate_to_run_by_concurrency_control(
                            invocation
                        ):
                            invocations_to_reroute.add(invocation)
                            continue
                        missing_invocations -= 1
                        try:
                            self._set_pending(invocation)
                            yield invocation
                        except PendingInvocationLockError:
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
            self.set_invocation_status(invocation, InvocationStatus.REROUTED)
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
        yield from self.get_blocking_invocations_to_run(
            max_num_invocations, blocking_invocation_ids
        )
        invocations_to_reroute: set["DistributedInvocation"] = set()
        missing_invocations = max_num_invocations - len(blocking_invocation_ids)
        yield from self.get_additional_invocations_to_run(
            missing_invocations, blocking_invocation_ids, invocations_to_reroute
        )
        self.reroute_invocations(invocations_to_reroute)

    def _route_new_call_invocation(
        self, call: "Call[Params, Result]"
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
        new_invocation = DistributedInvocation(call, parent_invocation)
        self.set_invocation_status(new_invocation, InvocationStatus.REGISTERED)
        self.app.broker.route_invocation(new_invocation)
        return new_invocation

    def route_call(self, call: "Call") -> "DistributedInvocation[Params, Result]":
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
        :rtype: DistributedInvocation[Params, Result]
        :raises InvocationConcurrencyWithDifferentArgumentsError: If an invocation with different arguments exists
            and the task's configuration specifies to raise an error in such cases.
        """
        if call.task.conf.registration_concurrency == ConcurrencyControlType.DISABLED:
            call.task.logger.debug(f"New invocation for call {call}")
            return self._route_new_call_invocation(call)
        # Handleling registred task concurrency control
        invocation = next(
            self.get_existing_invocations(
                task=call.task,
                key_serialized_arguments=call.serialized_args_for_concurrency_check,
                statuses=[InvocationStatus.REGISTERED],
            ),
            None,
        )
        if not invocation:
            call.task.logger.debug(
                f"No matching registered invocation found for {call.task} "
                f"and key args {call.serialized_args_for_concurrency_check}"
            )
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
