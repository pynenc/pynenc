from __future__ import annotations

import json
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any, Iterator

from .. import context
from ..arguments import Arguments
from ..call import Call
from ..exceptions import InvocationError
from ..types import Params, Result
from .base_invocation import BaseInvocation, BaseInvocationGroup
from .status import InvocationStatus

if TYPE_CHECKING:
    from ..app import Pynenc


# Create a context variable to store current invocation
@dataclass(frozen=True, eq=False)
class DistributedInvocation(BaseInvocation[Params, Result]):
    """"""

    parent_invocation: DistributedInvocation | None
    _invocation_id: str | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        self.app.state_backend.upsert_invocation(self)

    @cached_property
    def invocation_id(self) -> str:
        """on deserialization allows to set the invocation_id"""
        return self._invocation_id or super().invocation_id

    @property
    def num_retries(self) -> int:
        """Get the number of times the invocation got retried"""
        return self.app.orchestrator.get_invocation_retries(self)

    @property
    def status(self) -> InvocationStatus:
        """Get the status of the invocation"""
        return self.app.orchestrator.get_invocation_status(self)

    def to_json(self) -> str:
        """Returns a string with the serialized invocation"""
        inv_dict = {"invocation_id": self.invocation_id, "call": self.call.to_json()}
        if self.parent_invocation:
            inv_dict["parent_invocation_id"] = self.parent_invocation.invocation_id
        return json.dumps(inv_dict)

    def __getstate__(self) -> dict:
        # Return state as a dictionary and a secondary value as a tuple
        state = self.__dict__.copy()
        state["invocation_id"] = self.invocation_id
        return state

    def __setstate__(self, state: dict) -> None:
        # Restore instance attributes
        for key, value in state.items():
            object.__setattr__(self, key, value)

    @classmethod
    def from_json(cls, app: Pynenc, serialized: str) -> DistributedInvocation:
        """Returns a new invocation from a serialized invocation"""
        inv_dict = json.loads(serialized)
        call = Call.from_json(app, inv_dict["call"])
        parent_invocation = None
        if "parent_invocation_id" in inv_dict:
            parent_invocation = app.state_backend.get_invocation(
                inv_dict["parent_invocation_id"]
            )
        return cls(call, parent_invocation, inv_dict["invocation_id"])

    def run(self, runner_args: dict[str, Any] | None = None) -> None:
        # runner_args are passed from/to the runner (e.g. used to sync subprocesses)
        context.runner_args = runner_args
        # Set current invocation
        previous_invocation_context = context.dist_inv_context.get(self.app.app_id)
        try:
            self.app.logger.info(f"Invocation {self.invocation_id} started")
            context.dist_inv_context[self.app.app_id] = self
            self.app.orchestrator.set_invocation_run(self.parent_invocation, self)
            result = self.task.func(**self.arguments.kwargs)
            self.app.orchestrator.set_invocation_result(self, result)
            self.app.logger.info(f"Invocation {self.invocation_id} finished")
        except self.task.retriable_exceptions as ex:
            if self.num_retries >= self.task.conf.max_retries:
                self.task.logger.exception("Max retries reached")
                self.app.orchestrator.set_invocation_exception(self, ex)
                raise ex
            self.app.orchestrator.set_invocation_retry(self, ex)
            self.task.logger.warning("Retrying invocation")
        except Exception as ex:
            self.app.orchestrator.set_invocation_exception(self, ex)
            raise ex
        finally:
            context.dist_inv_context[self.app.app_id] = previous_invocation_context

    @property
    def result(self) -> Result:
        if not self.status.is_final():
            self.app.orchestrator.waiting_for_results(self.parent_invocation, [self])

        while not self.status.is_final():
            self.app.runner.waiting_for_results(
                self.parent_invocation, [self], context.runner_args
            )
        return self.get_final_result()

    def get_final_result(self) -> Result:
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
    diff_arg: Arguments | None = None

    @classmethod
    def from_existing(
        cls, invocation: DistributedInvocation, diff_arg: Arguments | None = None
    ) -> ReusedInvocation:
        # Create a new instance with the same fields as the existing invocation, but with the added field
        new_invc = cls(
            call=Call(invocation.task, invocation.arguments),
            parent_invocation=invocation.parent_invocation,
            # we reuse invocation_id from original invocation
            _invocation_id=invocation.invocation_id,
            diff_arg=diff_arg,
        )
        return new_invc
