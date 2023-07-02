from __future__ import annotations
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional, Any

from ..arguments import Arguments
from .status import InvocationStatus
from .base_invocation import BaseInvocation
from ..types import Params, Result


# Create a context variable to store current invocation
context_invocation: ContextVar[DistributedInvocation] = ContextVar("context_invocation")


class DistributedInvocation(BaseInvocation[Params, Result]):
    """"""

    def __post_init__(self) -> None:
        self.app.state_backend.upsert_invocation(self)

    @property
    def status(self) -> "InvocationStatus":
        """Get the status of the invocation"""
        return self.app.orchestrator.get_invocation_status(self)

    def run(self) -> None:
        # Set current invocation
        token = context_invocation.set(self)
        try:
            self.app.orchestrator.set_invocation_status(self, InvocationStatus.RUNNING)
            result = self.task.func(**self.arguments.kwargs)
            self.app.state_backend.set_result(self, result)
            self.app.orchestrator.set_invocation_status(self, InvocationStatus.SUCCESS)
        except Exception as ex:
            self.app.state_backend.set_exception(self, ex)
            self.app.orchestrator.set_invocation_status(self, InvocationStatus.FAILED)
        finally:
            # Reset the current invocation to the previous context
            context_invocation.reset(token)

    @property
    def result(self) -> "Result":
        current_invocation = context_invocation.get(None)
        if not self.status.is_final():
            self.app.orchestrator.waiting_for_result(current_invocation, self)

        while not self.status.is_final():
            self.app.runner.waiting_for_result(current_invocation, self)
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
        new_invc = cls(invocation.task, invocation.arguments, diff_arg)
        # Because the class is frozen, we can't ordinarily set attributes
        # So, we use object.__setattr__() to bypass this
        object.__setattr__(new_invc, "app", invocation.app)
        object.__setattr__(new_invc, "invocation_id", invocation.invocation_id)
        return new_invc
