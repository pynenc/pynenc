from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Iterator

from ..invocation import InvocationStatus, ReusedInvocation
from ..exceptions import SingleInvocationWithDifferentArgumentsError

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..arguments import Arguments
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

    def waiting_for_result(
        self,
        context_invocation: Optional["DistributedInvocation[Params, Result]"],
        result_invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """Called when an Optional[invocation] is waiting in the result result of another invocation."""

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

    def route_task(
        self, task: "Task", arguments: "Arguments"
    ) -> "DistributedInvocation[Params, Result]":
        if not task.options.single_invocation:
            return self.app.broker.route_task(task, arguments)
        # Handleling single invocation routings
        invocation = next(
            self.get_existing_invocations(
                task=task,
                key_arguments=task.options.single_invocation.get_key_arguments(
                    arguments
                ),
                status=InvocationStatus.REGISTERED,
            ),
            None,
        )
        if not invocation:
            return self.app.broker.route_task(task, arguments)
        if invocation.arguments == arguments:
            return ReusedInvocation.from_existing(invocation)
        if task.options.single_invocation.on_diff_args_raise:
            raise SingleInvocationWithDifferentArgumentsError(
                task, invocation, arguments
            )
        return ReusedInvocation.from_existing(invocation, arguments)
