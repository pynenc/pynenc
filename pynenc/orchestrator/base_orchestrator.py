from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Iterator

from ..invocation import InvocationStatus, ReusedInvocation
from ..exceptions import SingleInvocationWithDifferentArgumentsError

if TYPE_CHECKING:
    from ..app import Pynenc
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

    def route_task(
        self, task: "Task", arguments: "Args"
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
