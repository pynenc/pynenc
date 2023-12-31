from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Iterator

from .. import context
from ..exceptions import PynencError
from ..types import Params, Result
from .base_invocation import BaseInvocation, BaseInvocationGroup
from .status import InvocationStatus

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..call import Call


class SynchronousInvocation(BaseInvocation[Params, Result]):
    def __init__(self, call: Call[Params, Result]) -> None:
        super().__init__(call)
        self._num_retries = 0
        self._status = InvocationStatus.REGISTERED

    @property
    def status(self) -> InvocationStatus:
        """Get the status of the invocation"""
        return self._status

    @cached_property
    def result(self) -> Result:
        previous_invocation_context = context.sync_inv_context.get(self.app.app_id)
        try:
            self._status = InvocationStatus.RUNNING
            context.sync_inv_context[self.app.app_id] = self
            result = self.task.func(**self.arguments.kwargs)
            self._status = InvocationStatus.SUCCESS
            return result
        except self.task.retriable_exceptions as exc:
            if self._num_retries >= self.task.conf.max_retries:
                self._status = InvocationStatus.FAILED
                raise exc
            self._status = InvocationStatus.RETRY
            self._num_retries += 1
            return self.result
        except Exception as exc:
            self._status = InvocationStatus.FAILED
            raise exc
        finally:
            context.sync_inv_context[self.app.app_id] = previous_invocation_context

    @property
    def num_retries(self) -> int:
        """Get the number of times the invocation got retried"""
        return self._num_retries

    def to_json(self) -> str:
        raise PynencError("SynchronousInvocation cannot be serialized")

    @classmethod
    def from_json(cls, app: Pynenc, serialized: str) -> SynchronousInvocation:
        del app, serialized
        raise PynencError("SynchronousInvocation cannot be deserialized")


class SynchronousInvocationGroup(
    BaseInvocationGroup[Params, Result, SynchronousInvocation]
):
    @property
    def results(self) -> Iterator[Result]:
        for invocation in self.invocations:
            yield invocation.result
