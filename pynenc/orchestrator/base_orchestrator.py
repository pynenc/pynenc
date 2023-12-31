from abc import ABC, abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING, Any, Iterator, Optional

from ..conf.config_orchestrator import ConfigOrchestrator
from ..conf.config_task import ConcurrencyControlType
from ..context import dist_inv_context
from ..exceptions import (
    InvocationConcurrencyWithDifferentArgumentsError,
    PendingInvocationLockError,
)
from ..invocation import DistributedInvocation, InvocationStatus, ReusedInvocation

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..call import Call
    from ..task import Task
    from ..types import Params, Result


class BaseCycleControl(ABC):
    """Sub component of the orchestrator to implement cycle control functionalities"""

    @abstractmethod
    def add_call_and_check_cycles(
        self,
        caller_invocation: "DistributedInvocation[Params, Result]",
        callee_invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """Adds a new call between invocations and raise an exception to prevent the formation of a call cycle"""
        # TODO async store of call dependencies in state backend

    @abstractmethod
    def clean_up_invocation_cycles(self, invocation: "DistributedInvocation") -> None:
        """Called when an invocation is finished and therefore cannot be part of a cycle anymore"""


class BaseBlockingControl(ABC):
    """Sub component of the orchestrator to implement blocking control functionalities"""

    @abstractmethod
    def release_waiters(self, waited: "DistributedInvocation") -> None:
        """Called when an invocation is finished and therefore cannot block other invocations anymore"""

    @abstractmethod
    def waiting_for_results(
        self,
        caller_invocation: "DistributedInvocation[Params, Result]",
        result_invocations: list["DistributedInvocation[Params, Result]"],
    ) -> None:
        """Called when an Optional[invocation] is waiting in the result result of another invocation."""

    @abstractmethod
    def get_blocking_invocations(
        self, max_num_invocations: int
    ) -> Iterator["DistributedInvocation"]:
        """Returns an iterator of invocations that are blocking other invocations
        but are not getting blocked by any invocation.
        order by age, the oldest invocation first.
        """


class BaseOrchestrator(ABC):
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
        status: Optional["InvocationStatus"] = None,
    ) -> Iterator["DistributedInvocation"]:
        ...

    @abstractmethod
    def _set_invocation_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: "InvocationStatus",
    ) -> None:
        ...

    @abstractmethod
    def _set_invocation_pending_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """Pending can only be set by the orchestrator"""
        ...

    def _set_pending(
        self,
        invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """Pending can only be set by the orchestrator"""
        self._set_invocation_pending_status(invocation)
        self.app.state_backend.add_history(invocation, status=InvocationStatus.PENDING)

    @abstractmethod
    def set_up_invocation_auto_purge(
        self,
        invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """set up the invocation to be auto purgue after app.conf.orchestrator_auto_final_invocation_purge_hours"""

    @abstractmethod
    def auto_purge(self) -> None:
        """Purge all invocations in final state that are older than app.conf.orchestrator_auto_final_invocation_purge_hours"""

    @abstractmethod
    def get_invocation_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "InvocationStatus":
        ...
        # TODO if invocation does not exists try in state backend (and cache it up)
        #      before raising an exception!!!

    @abstractmethod
    def increment_invocation_retries(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        ...

    @abstractmethod
    def get_invocation_retries(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> int:
        ...

    @abstractmethod
    def purge(self) -> None:
        ...

    #############################################
    # cycle sub functionalities
    @property
    @abstractmethod
    def cycle_control(self) -> BaseCycleControl:
        ...

    def add_call_and_check_cycles(
        self,
        caller_invocation: "DistributedInvocation[Params, Result]",
        callee_invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """Adds a new call between invocations and raise an exception to prevent the formation of a call cycle"""
        if self.conf.cycle_control:
            self.cycle_control.add_call_and_check_cycles(
                caller_invocation, callee_invocation
            )
        # TODO async store of call dependencies in state backend

    def clean_up_invocation_cycles(self, invocation: "DistributedInvocation") -> None:
        """Called when an invocation is finished and therefore cannot be part of a cycle anymore"""
        if self.conf.cycle_control:
            self.cycle_control.clean_up_invocation_cycles(invocation)

    #############################################
    # blocking sub functionalities
    @property
    @abstractmethod
    def blocking_control(self) -> BaseBlockingControl:
        ...

    def release_waiters(self, waited: "DistributedInvocation") -> None:
        """Called when an invocation is finished and therefore cannot block other invocations anymore"""
        if self.app.orchestrator.conf.blocking_control:
            self.blocking_control.release_waiters(waited)

    def waiting_for_results(
        self,
        caller_invocation: Optional["DistributedInvocation[Params, Result]"],
        result_invocations: list["DistributedInvocation[Params, Result]"],
    ) -> None:
        """Called when an Optional[invocation] is waiting in the result result of another invocation."""
        if self.app.orchestrator.conf.blocking_control and caller_invocation:
            self.blocking_control.waiting_for_results(
                caller_invocation, result_invocations
            )

    def get_blocking_invocations(
        self, max_num_invocations: int
    ) -> Iterator["DistributedInvocation"]:
        """Returns an iterator of invocations that are blocking other invocations
        but are not getting blocked by any invocation.
        order by age, the oldest invocation first.
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
        for invocation in invocations:
            self.set_invocation_status(invocation, status)

    def set_invocation_run(
        self,
        caller: Optional["DistributedInvocation[Params, Result]"],
        callee: "DistributedInvocation[Params, Result]",
    ) -> None:
        """Called when an invocation is started"""
        if caller:
            self.add_call_and_check_cycles(caller, callee)
        # TODO! on previous fail, this should still change status
        self.set_invocation_status(callee, InvocationStatus.RUNNING)

    def set_invocation_result(
        self, invocation: "DistributedInvocation", result: Any
    ) -> None:
        """Called when an invocation is finished successfully"""
        self.app.state_backend.set_result(invocation, result)
        # TODO! on previous fail, this should still change status
        self.app.orchestrator.set_invocation_status(
            invocation, InvocationStatus.SUCCESS
        )

    def set_invocation_exception(
        self, invocation: "DistributedInvocation", exception: Exception
    ) -> None:
        """Called when an invocation is finished with an exception"""
        self.app.state_backend.set_exception(invocation, exception)
        # TODO! on previous fail, this should still change status
        # eg. on case of interruption from a kubernetes pod (SIGTERM, SIGKILL)
        #     it should try to finish all the calls in this function
        self.app.orchestrator.set_invocation_status(invocation, InvocationStatus.FAILED)

    def set_invocation_retry(
        self, invocation: "DistributedInvocation", exception: Exception
    ) -> None:
        """Called when an invocation is finished with an exception and should be retried"""
        # TODO! on previous fail, this should still change status
        # eg. on case of interruption from a kubernetes pod (SIGTERM, SIGKILL)
        #     it should try to finish all the calls in this function
        self.app.orchestrator.set_invocation_status(invocation, InvocationStatus.RETRY)
        self.app.orchestrator.increment_invocation_retries(invocation)
        self.app.broker.route_invocation(invocation)

    def is_authorize_to_run_by_concurrency_control(
        self, invocation: "DistributedInvocation"
    ) -> bool:
        """Checks if the invocation can run based on task concurrency configuration"""
        if invocation.task.conf.running_concurrency == ConcurrencyControlType.DISABLED:
            return True
        running_invocation = next(
            self.get_existing_invocations(
                task=invocation.call.task,
                key_serialized_arguments=invocation.call.serialized_args_for_concurrency_check,
                status=InvocationStatus.RUNNING,
            ),
            None,
        )
        if not running_invocation:
            return True
        invocation.task.logger.debug(
            f"{invocation=} cannot run because {running_invocation} is already running"
        )
        return False

    def get_blocking_invocations_to_run(
        self, max_num_invocations: int, blocking_invocation_ids: set[str]
    ) -> Iterator["DistributedInvocation"]:
        for blocking_invocation in self.get_blocking_invocations(max_num_invocations):
            if not self.is_authorize_to_run_by_concurrency_control(blocking_invocation):
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
        while missing_invocations > 0:
            if invocation := self.app.broker.retrieve_invocation():
                if invocation.invocation_id not in blocking_invocation_ids:
                    if self.get_invocation_status(invocation).is_available_for_run():
                        if not self.is_authorize_to_run_by_concurrency_control(
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
        for invocation in invocations_to_reroute:
            invocation.task.logger.debug(
                f"Rerouting invocation {invocation.invocation_id} because it was not authorized to run"
            )
            self.set_invocation_status(invocation, InvocationStatus.REROUTED)
            self.app.broker.route_invocation(invocation)

    def get_invocations_to_run(
        self, max_num_invocations: int
    ) -> Iterator["DistributedInvocation"]:
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
        new_invocation = DistributedInvocation(
            call, parent_invocation=dist_inv_context.get(self.app.app_id)
        )
        self.set_invocation_status(new_invocation, InvocationStatus.REGISTERED)
        self.app.broker.route_invocation(new_invocation)
        return new_invocation

    def route_call(self, call: "Call") -> "DistributedInvocation[Params, Result]":
        """
        Routes a task call in a distributed task system based on single invocation options.

        If the task has no single invocation option set, it routes a new call invocation.
        For tasks with single invocation, it checks if an existing invocation with the same or different arguments exists.
        If an invocation with the same arguments exists, it reuses the invocation.
        If an invocation with different arguments exists, it raises an error or reuses the invocation based on the 'on_diff_args_raise' flag.

        :param call: The task call to be routed.
        :type call: Call
        :return: The resulting DistributedInvocation object, which could be a new or reused invocation.
        :rtype: DistributedInvocation[Params, Result]
        """
        if call.task.conf.registration_concurrency == ConcurrencyControlType.DISABLED:
            call.task.logger.debug(f"New invocation for call {call}")
            return self._route_new_call_invocation(call)
        # Handleling registred task concurrency control
        invocation = next(
            self.get_existing_invocations(
                task=call.task,
                key_serialized_arguments=call.serialized_args_for_concurrency_check,
                status=InvocationStatus.REGISTERED,
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
