from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Iterator

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..task import Task
    from ..invocation import InvocationStatus, DistributedInvocation
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
    ) -> Iterator["DistributedInvocation[Result]"]:
        ...

    @abstractmethod
    def set_invocation_status(
        self,
        invocation: "DistributedInvocation",
        status: "InvocationStatus",
    ) -> None:
        ...

    def route_task(
        self, task: "Task", arguments: "Args"
    ) -> "DistributedInvocation[Result]":
        if task.options.single_pending:
            if invocation := next(
                self.get_existing_invocations(
                    task=task,
                    key_arguments=task.options.single_pending.get_key_arguments(
                        arguments
                    ),
                    status=InvocationStatus.PENDING,
                ),
                None,
            ):
                return invocation
        return self.app.task_broker.route_task(task, arguments)
