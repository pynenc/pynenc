import json
import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any, Optional

from ..conf.config_state_backend import ConfigStateBackend
from ..exceptions import InvocationNotFoundError
from ..invocation import InvocationStatus

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..invocation import DistributedInvocation
    from ..types import Params, Result


@dataclass
class InvocationHistory:
    invocation_id: str
    _timestamp: datetime = field(init=False, default_factory=lambda: datetime.utcnow())
    status: Optional["InvocationStatus"] = None
    execution_context: Optional[Any] = None  # Todo on Runners

    @property
    def timestamp(self) -> datetime:
        return self._timestamp

    def to_json(self) -> str:
        return json.dumps(
            {
                "invocation_id": self.invocation_id,
                "_timestamp": self._timestamp.isoformat(),
                "status": self.status.value if self.status else None,
                "execution_context": self.execution_context,  # TODO
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> "InvocationHistory":
        hist_dict = json.loads(json_str)
        history = cls(hist_dict["invocation_id"])
        history._timestamp = datetime.fromisoformat(hist_dict["_timestamp"])
        history.status = InvocationStatus(hist_dict["status"])
        history.execution_context = hist_dict["execution_context"]
        return history


class BaseStateBackend(ABC):
    def __init__(self, app: "Pynenc") -> None:
        self.app = app
        self.invocation_threads: dict[str, list[threading.Thread]] = defaultdict(list)

    @cached_property
    def conf(self) -> ConfigStateBackend:
        return ConfigStateBackend(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def wait_for_all_async_operations(self) -> None:
        """Blocks until all asynchronous status operations are finished."""
        for invocation_id in self.invocation_threads:
            self.wait_for_invocation_async_operations(invocation_id)

    def wait_for_invocation_async_operations(self, invocation_id: str) -> None:
        """Blocks until all asynchronous operations for a specific invocation are finished."""
        for thread in self.invocation_threads[invocation_id]:
            thread.join()

    @abstractmethod
    def purge(self) -> None:
        ...

    @abstractmethod
    def _upsert_invocation(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        ...

    @abstractmethod
    def _get_invocation(self, invocation_id: str) -> Optional["DistributedInvocation"]:
        ...

    @abstractmethod
    def _add_history(
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
    def _get_result(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "Result":
        ...

    @abstractmethod
    def _set_result(
        self, invocation: "DistributedInvocation[Params, Result]", result: "Result"
    ) -> None:
        ...

    @abstractmethod
    def _get_exception(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "Exception":
        ...

    @abstractmethod
    def _set_exception(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        exception: "Exception",
    ) -> None:
        ...

    def upsert_invocation(self, invocation: "DistributedInvocation") -> None:
        thread = threading.Thread(target=self._upsert_invocation, args=(invocation,))
        thread.start()
        self.invocation_threads[invocation.invocation_id].append(thread)

    def get_invocation(self, invocation_id: str) -> "DistributedInvocation":
        if invocation := self._get_invocation(invocation_id):
            return invocation
        raise InvocationNotFoundError(invocation_id, "The invocation wasn't stored")

    def add_history(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: Optional["InvocationStatus"] = None,
        execution_context: Optional["Any"] = None,
    ) -> None:
        invocation_history = InvocationHistory(
            invocation.invocation_id, status, execution_context
        )
        thread = threading.Thread(
            target=self._add_history, args=(invocation, invocation_history)
        )
        self.invocation_threads[invocation.invocation_id].append(thread)
        thread.start()

    def get_history(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> list[InvocationHistory]:
        # todo fork open threads
        return self._get_history(invocation)

    def set_result(self, invocation: "DistributedInvocation", result: "Result") -> None:
        self._set_result(invocation, result)

    def get_result(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "Result":
        # insert result is block, no need for thread control
        return self._get_result(invocation)

    def set_exception(
        self, invocation: "DistributedInvocation", exception: "Exception"
    ) -> None:
        self._set_exception(invocation, exception)

    def get_exception(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> Exception:
        # todo
        return self._get_exception(invocation)
