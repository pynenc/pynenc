from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Iterator, Generic, TypeVar

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..task import BaseTask
    from ..invocation import InvocationStatus, DistributedInvocation

R = TypeVar("R")


class BaseOrchestrator(ABC, Generic[R]):
    def __init__(self, app: "Pynenc") -> None:
        self.app = app

    @abstractmethod
    def get_existing_invocations(
        self,
        task: "BaseTask",
        key_arguments: Optional[dict[str, Any]] = None,
        status: Optional["InvocationStatus"] = None,
    ) -> Iterator["DistributedInvocation[R]"]:
        ...

    @abstractmethod
    def set_invocation_status(
        self, invocation: "DistributedInvocation[R]", status: "InvocationStatus"
    ) -> None:
        ...

    def route_task(
        self, task: "BaseTask", arguments: dict[str, Any]
    ) -> "DistributedInvocation":
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
