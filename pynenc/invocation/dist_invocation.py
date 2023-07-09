from __future__ import annotations
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional, Any

from ..arguments import Arguments
from ..call import Call
from .status import InvocationStatus
from .base_invocation import BaseInvocation
from ..types import Params, Result


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
            self.app.orchestrator.waiting_for_result(self.parent_invocation, self)

        while not self.status.is_final():
            self.app.runner.waiting_for_result(self.parent_invocation, self)
        if self.status == InvocationStatus.FAILED:
            raise self.app.state_backend.get_exception(self)
        return self.app.state_backend.get_result(self)


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
        object.__setattr__(new_invc.call, "app", invocation.app)
        object.__setattr__(new_invc, "invocation_id", invocation.invocation_id)
        return new_invc
