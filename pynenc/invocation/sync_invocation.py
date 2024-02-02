from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Iterator

from pynenc import context
from pynenc.exceptions import PynencError
from pynenc.invocation.base_invocation import BaseInvocation, BaseInvocationGroup
from pynenc.invocation.status import InvocationStatus
from pynenc.types import Params, Result

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..call import Call


class SynchronousInvocation(BaseInvocation[Params, Result]):
    """
    A synchronous implementation of a task invocation.

    This class represents an invocation of a task in a synchronous context.

    :param Call[Params, Result] call: The specific call instance that this invocation represents.

    ```{danger}
        Use only for testing purposes where distributed processing is not required.
    ```
    """

    def __init__(self, call: Call[Params, Result]) -> None:
        super().__init__(call)
        self._num_retries = 0
        self._status = InvocationStatus.REGISTERED

    @property
    def status(self) -> InvocationStatus:
        """
        Get the status of the invocation.

        :return: The current status of the invocation.
        :rtype: InvocationStatus
        """
        return self._status

    @cached_property
    def result(self) -> Result:
        """
        Execute the task call and return its result.

        This method runs the task synchronously and returns the result.
        It handles retries for retriable exceptions as per the task's configuration.

        :return: The result of the task execution.
        :rtype: Result
        :raises Exception: Raised if the task execution results in an unhandled exception.
        """
        previous_invocation_context = context.sync_inv_context.get(self.app.app_id)
        try:
            self.task.logger.info(f"Sync Invocation {self.invocation_id} started")
            self._status = InvocationStatus.RUNNING
            context.sync_inv_context[self.app.app_id] = self
            result = self.task.func(**self.arguments.kwargs)
            self._status = InvocationStatus.SUCCESS
            self.task.logger.info(f"Sync Invocation {self.invocation_id} finished")
            return result
        except self.task.retriable_exceptions as exc:
            if self._num_retries >= self.task.conf.max_retries:
                self.task.logger.exception("Max retries reached")
                self._status = InvocationStatus.FAILED
                raise exc
            self._status = InvocationStatus.RETRY
            self._num_retries += 1
            self.task.logger.warning("Retrying invocation")
            return self.result
        except Exception as exc:
            self._status = InvocationStatus.FAILED
            raise exc
        finally:
            context.sync_inv_context[self.app.app_id] = previous_invocation_context

    @property
    def num_retries(self) -> int:
        """
        Get the number of retries for the invocation.

        :return: The number of times the invocation has been retried.
        :rtype: int
        """
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
    """
    A group of synchronous invocations for a specific task.

    This class extends `BaseInvocationGroup` to handle groups of `SynchronousInvocation` instances.
    It is designed for scenarios where multiple synchronous invocations of a task are managed or processed together.

    :param Task task: The task associated with these invocations.
    :param list[SynchronousInvocation] invocations: A list of synchronous invocations.

    ```{danger}
        Use only for testing purposes where distributed processing is not required.
    ```
    """

    @property
    def results(self) -> Iterator[Result]:
        """
        An iterator over the results of the invocations in the group.

        This property method iterates over the `SynchronousInvocation` instances in the group,
        yielding the result of each invocation.

        :return: An iterator over the results of each invocation in the group.
        :rtype: Iterator[Result]
        """
        for invocation in self.invocations:
            yield invocation.result
