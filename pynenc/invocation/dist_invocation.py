from __future__ import annotations
from contextvars import ContextVar
from dataclasses import dataclass
import json
from typing import Iterator, Optional, TYPE_CHECKING

from ..arguments import Arguments
from ..call import Call
from ..exceptions import InvocationError
from .status import InvocationStatus
from .base_invocation import BaseInvocation, BaseInvocationGroup
from ..types import Params, Result

if TYPE_CHECKING:
    from ..app import Pynenc


# Create a context variable to store current invocation
running_invocation: ContextVar[DistributedInvocation] = ContextVar("context_invocation")


@dataclass(frozen=True, eq=False)
class DistributedInvocation(BaseInvocation[Params, Result]):
    """"""

    parent_invocation: Optional[DistributedInvocation]

    def __post_init__(self) -> None:
        self.app.state_backend.upsert_invocation(self)

    @property
    def status(self) -> "InvocationStatus":
        """Get the status of the invocation"""
        return self.app.orchestrator.get_invocation_status(self)

    def to_json(self) -> str:
        """Returns a string with the serialized invocation"""
        inv_dict = {"invocation_id": self.invocation_id, "call": self.call.to_json()}
        if self.parent_invocation:
            inv_dict["parent_invocation_id"] = self.parent_invocation.invocation_id
        return json.dumps(inv_dict)

    @classmethod
    def from_json(cls, app: "Pynenc", serialized: str) -> "DistributedInvocation":
        """Returns a new invocation from a serialized invocation"""
        inv_dict = json.loads(serialized)
        call = Call.from_json(app, inv_dict["call"])
        parent_invocation = None
        if "parent_invocation_id" in inv_dict:
            parent_invocation = app.state_backend.get_invocation(
                inv_dict["parent_invocation_id"]
            )
        invocation = cls(call, parent_invocation)
        cls._set_frozen_attr(
            invocation=invocation, invocation_id=inv_dict["invocation_id"]
        )
        return invocation

    def run(self) -> None:
        # Set current invocation
        previous_invocation_context = self.app.invocation_context
        try:
            self.app.invocation_context = self
            self.app.orchestrator.set_invocation_run(self.parent_invocation, self)
            result = self.task.func(**self.arguments.kwargs)
            self.app.orchestrator.set_invocation_result(self, result)
        except Exception as ex:
            self.app.orchestrator.set_invocation_exception(self, ex)
        finally:
            self.app.invocation_context = previous_invocation_context

    @property
    def result(self) -> "Result":
        if not self.status.is_final():
            self.app.orchestrator.waiting_for_results(self.parent_invocation, [self])

        while not self.status.is_final():
            self.app.runner.waiting_for_results(self.parent_invocation, [self])
        return self.get_final_result()

    def get_final_result(self) -> "Result":
        if not self.status.is_final():
            raise InvocationError(self.invocation_id, "Invocation is not final")
        if self.status == InvocationStatus.FAILED:
            raise self.app.state_backend.get_exception(self)
        return self.app.state_backend.get_result(self)


class DistributedInvocationGroup(
    BaseInvocationGroup[Params, Result, DistributedInvocation]
):
    @property
    def results(self) -> Iterator[Result]:
        waiting_invocations = self.invocations.copy()
        if not waiting_invocations:
            return
        parent_invocation = waiting_invocations[0].parent_invocation
        notified_orchestrator = False
        while waiting_invocations:
            for invocation in waiting_invocations:
                if invocation.status.is_final():
                    waiting_invocations.remove(invocation)
                    yield invocation.result
            if not notified_orchestrator:
                self.app.orchestrator.waiting_for_results(
                    parent_invocation, waiting_invocations
                )
            self.app.runner.waiting_for_results(parent_invocation, waiting_invocations)


@dataclass(frozen=True)
class ReusedInvocation(DistributedInvocation):
    """This is an invocation referencing an older one"""

    # Due to single invocation functionality
    # keeps existing invocation + new argument if any change
    diff_arg: Optional["Arguments"] = None

    @classmethod
    def from_existing(
        cls, invocation: DistributedInvocation, diff_arg: Optional["Arguments"] = None
    ) -> "ReusedInvocation":
        # Create a new instance with the same fields as the existing invocation, but with the added field
        new_invc = cls(
            Call(invocation.task, invocation.arguments),
            invocation.parent_invocation,
            diff_arg,
        )
        # Because the class is frozen, we can't ordinarily set attributes
        # So, we use object.__setattr__() to bypass this
        cls._set_frozen_attr(
            invocation=new_invc,
            app=invocation.app,
            invocation_id=invocation.invocation_id,
        )
        return new_invc
