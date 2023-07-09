from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Iterator, Any

from ..invocation import InvocationStatus, ReusedInvocation
from ..exceptions import SingleInvocationWithDifferentArgumentsError

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..call import Call
    from ..task import Task
    from ..invocation import DistributedInvocation
    from ..types import Params, Result, Args


class BaseOrchestrator(ABC):
    def __init__(self, app: "Pynenc") -> None:
        self.app = app

    @abstractmethod
    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_arguments: Optional["Args"] = None,
        status: Optional["InvocationStatus"] = None,
    ) -> Iterator["DistributedInvocation"]:
        ...

    @abstractmethod
    def set_invocation_status(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: "InvocationStatus",
    ) -> None:
        ...

    @abstractmethod
    def set_invocations_status(
        self,
        invocations: list["DistributedInvocation[Params, Result]"],
        status: "InvocationStatus",
    ) -> None:
        ...

    @abstractmethod
    def get_invocation_status(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "InvocationStatus":
        ...

    @abstractmethod
    def check_for_call_cycle(
        self,
        caller_invocation: "DistributedInvocation[Params, Result]",
        callee_invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """Raises an exception if there is a cycle in the invocation graph"""

    @abstractmethod
    def waiting_for_result(
        self,
        caller_invocation: Optional["DistributedInvocation[Params, Result]"],
        result_invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """Called when an Optional[invocation] is waiting in the result result of another invocation."""

    def set_invocation_run(
        self,
        caller: Optional["DistributedInvocation[Params, Result]"],
        callee: "DistributedInvocation[Params, Result]",
    ) -> None:
        """Called when an invocation is started"""
        if caller:
            self.check_for_call_cycle(caller, callee)
        self.set_invocation_status(callee, InvocationStatus.RUNNING)

    def set_invocation_result(
        self, invocation: "DistributedInvocation", result: Any
    ) -> None:
        """Called when an invocation is finished successfully"""
        self.app.state_backend.set_result(invocation, result)
        self.app.orchestrator.set_invocation_status(
            invocation, InvocationStatus.SUCCESS
        )

    def set_invocation_exception(
        self, invocation: "DistributedInvocation", exception: Exception
    ) -> None:
        """Called when an invocation is finished with an exception"""
        self.app.state_backend.set_exception(invocation, exception)
        self.app.orchestrator.set_invocation_status(invocation, InvocationStatus.FAILED)

    def get_invocations_to_run(
        self, max_num_invocations: int
    ) -> Iterator["DistributedInvocation"]:
        """Returns an iterator of max_num_invocations that are ready for running"""
        # TODO check priorities based in graph waiting_for_result
        # TODO check for cycles based in graph
        for _ in range(max_num_invocations):
            if invocation := self.app.broker.retrieve_invocation():
                yield invocation
            else:
                break

    def route_call(self, call: "Call") -> "DistributedInvocation[Params, Result]":
        if not call.task.options.single_invocation:
            return self.app.broker.route_call(call)
        # Handleling single invocation routings
        invocation = next(
            self.get_existing_invocations(
                task=call.task,
                key_arguments=call.task.options.single_invocation.get_key_arguments(
                    call.arguments
                ),
                status=InvocationStatus.REGISTERED,
            ),
            None,
        )
        if not invocation:
            return self.app.broker.route_call(call)
        if invocation.arguments == call.arguments:
            return ReusedInvocation.from_existing(invocation)
        if call.task.options.single_invocation.on_diff_args_raise:
            raise SingleInvocationWithDifferentArgumentsError(
                call.task, invocation, call.arguments
            )
        return ReusedInvocation.from_existing(invocation, call.arguments)
