from __future__ import annotations
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, Optional

from .base_invocation import BaseInvocation
from ..types import Params, Result, Args


class DistributedInvocation(BaseInvocation[Params, Result]):
    """"""

    def __post_init__(self) -> None:
        self.app.state_backend.upsert_invocation(self)

    @property
    def result(self) -> "Result":
        raise NotImplementedError("TODO")
        # self.app.state_backend...
        # HERE SPECIFIC WAITING BEHAVIOUR!!!


@dataclass(frozen=True)
class ReusedInvocation(DistributedInvocation):
    """This is an invocation referencing an older one"""

    # Due to single invocation functionality
    # keeps existing invocation + new argument if any change
    diff_arg: Optional[Args] = None

    @classmethod
    def from_existing(
        cls, invocation: DistributedInvocation, diff_arg: Optional[Args] = None
    ) -> "ReusedInvocation":
        # Create a new instance with the same fields as the existing invocation, but with the added field
        new_invc = cls(invocation.task, invocation.arguments, diff_arg)
        # Because the class is frozen, we can't ordinarily set attributes
        # So, we use object.__setattr__() to bypass this
        object.__setattr__(new_invc, "app", invocation.app)
        object.__setattr__(new_invc, "invocation_id", invocation.invocation_id)
        return new_invc
