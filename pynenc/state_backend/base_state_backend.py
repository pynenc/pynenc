from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
import threading
from typing import TYPE_CHECKING, Any, Optional

from ..exceptions import InvocationNotFoundError

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..invocation import DistributedInvocation
    from ..types import Params, Result
    from ..invocation import DistributedInvocation, InvocationStatus


@dataclass
class InvocationHistory:
    invocation_id: str
    _timestamp: datetime = field(init=False, default_factory=lambda: datetime.utcnow())
    status: Optional["InvocationStatus"] = None
    execution_context: Optional[Any] = None  # Todo on Runners

    @property
    def timestamp(self) -> datetime:
        return self._timestamp


class BaseStateBackend(ABC):
    def __init__(self, app: "Pynenc") -> None:
        self.app = app
        self.threads: list[threading.Thread] = []

    @abstractmethod
    def _upsert_invocation(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        ...

    @abstractmethod
    def _get_invocation(self, invocation_id: str) -> Optional["DistributedInvocation"]:
        ...

    @abstractmethod
    def _insert_history(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        invocation_history: InvocationHistory,
    ) -> None:
        ...

    @abstractmethod
    def _get_history(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> list[InvocationHistory]:
        ...

    @abstractmethod
    def _insert_result(
        self, invocation: "DistributedInvocation[Params, Result]", result: "Result"
    ) -> None:
        ...

    @abstractmethod
    def _get_result(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "Result":
        ...

    def upsert_invocation(self, invocation: "DistributedInvocation") -> None:
        thread = threading.Thread(target=self._upsert_invocation, args=(invocation,))
        thread.start()
        self.threads.append(thread)

    def get_invocation(self, invocation_id: str) -> "DistributedInvocation":
        if invocation := self._get_invocation(invocation_id):
            return invocation
        raise InvocationNotFoundError(invocation_id, "The invocation wasn't stored")

    def insert_history(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: Optional["InvocationStatus"] = None,
        execution_context: Optional["Any"] = None,
    ) -> None:
        invocation_history = InvocationHistory(
            invocation.invocation_id, status, execution_context
        )
        thread = threading.Thread(
            target=self._insert_history, args=(invocation, invocation_history)
        )
        self.threads.append(thread)
        thread.start()

    def get_history(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> list[InvocationHistory]:
        # todo fork open threads
        return self._get_history(invocation)

    def insert_result(
        self, invocation: "DistributedInvocation", result: "Result"
    ) -> None:
        self._insert_result(invocation, result)

    def get_result(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "Result":
        # insert result is block, no need for thread control
        return self._get_result(invocation)
