from abc import ABC, abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING, Any, Iterator, Optional

from ..conf.config_orchestrator import ConfigOrchestrator
from ..context import invocation_context
from ..exceptions import (
    PendingInvocationLockError,
    SingleInvocationWithDifferentArgumentsError,
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
        self.app.orchestrator.set_invocation_status(invocation, InvocationStatus.FAILED)

    def get_invocations_to_run(
        self, max_num_invocations: int
    ) -> Iterator["DistributedInvocation"]:
        """Returns an iterator of max_num_invocations that are ready for running"""
        # first get the blocking invocations
        blocking_invocation_ids: set[str] = set()
        for blocking_invocation in self.get_blocking_invocations(max_num_invocations):
            blocking_invocation_ids.add(blocking_invocation.invocation_id)
            try:
                self._set_invocation_pending_status(blocking_invocation)
                yield blocking_invocation
            except PendingInvocationLockError:
                continue
        missing_invocations = max_num_invocations - len(blocking_invocation_ids)
        # then get the rest from the broker
        while missing_invocations > 0:
            if invocation := self.app.broker.retrieve_invocation():
                if invocation.invocation_id not in blocking_invocation_ids:
                    if self.get_invocation_status(invocation).is_available_for_run():
                        missing_invocations -= 1
                        try:
                            self._set_invocation_pending_status(invocation)
                            yield invocation
                        except PendingInvocationLockError:
                            continue
            else:
                break

    def _route_new_call_invocation(
        self, call: "Call[Params, Result]"
    ) -> "DistributedInvocation[Params, Result]":
        new_invocation = DistributedInvocation(
            call, parent_invocation=invocation_context.get(self.app.app_id)
        )
        self.set_invocation_status(new_invocation, InvocationStatus.REGISTERED)
        self.app.broker.route_invocation(new_invocation)
        return new_invocation

    def route_call(self, call: "Call") -> "DistributedInvocation[Params, Result]":
        if not call.task.options.single_invocation:
            return self._route_new_call_invocation(call)
        # Handleling single invocation routings
        invocation = next(
            self.get_existing_invocations(
                task=call.task,
                key_serialized_arguments=call.task.options.single_invocation.get_key_arguments(
                    call.serialized_arguments
                ),
                status=InvocationStatus.REGISTERED,
            ),
            None,
        )
        if not invocation:
            return self._route_new_call_invocation(call)
        if invocation.serialized_arguments == call.serialized_arguments:
            return ReusedInvocation.from_existing(invocation)
        if call.task.options.single_invocation.on_diff_args_raise:
            raise SingleInvocationWithDifferentArgumentsError.from_call_mismatch(
                existing_invocation=invocation, new_call=call
            )
        return ReusedInvocation.from_existing(invocation, call.arguments)
