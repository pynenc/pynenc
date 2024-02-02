from __future__ import annotations

import json
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any, Iterator

from pynenc import context, exceptions
from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.invocation.base_invocation import BaseInvocation, BaseInvocationGroup
from pynenc.invocation.status import InvocationStatus
from pynenc.types import Params, Result

if TYPE_CHECKING:
    from ..app import Pynenc


# Create a context variable to store current invocation
@dataclass(frozen=True, eq=False)
class DistributedInvocation(BaseInvocation[Params, Result]):
    """
    Represents a distributed invocation of a task call in the system.

    This class is a specific implementation of `BaseInvocation` for use in distributed environments. It extends the base invocation with additional features necessary for handling task executions in a distributed manner.

    In the distributed system, `DistributedInvocation` handles the execution of a task across different nodes or processes. It includes mechanisms for tracking and managing the state of an invocation in a distributed context.

    :param Call[Params, Result] call:
        Inherits the `call` attribute from `BaseInvocation`, representing the specific task call that this invocation executes.
    :param Optional[DistributedInvocation] parent_invocation:
        A reference to a parent invocation, if this invocation is part of a nested call structure.
        This attribute is used to maintain the invocation hierarchy in complex task workflows.
    :param Optional[str] _invocation_id:
        A unique identifier for the invocation. This ID is crucial for tracking and orchestrating the invocation
        across the distributed system. It's assigned internally and used by the orchestration mechanism.
    """

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
        """:return: number of times the invocation got retried"""
        return self.app.orchestrator.get_invocation_retries(self)

    @property
    def status(self) -> InvocationStatus:
        """:return: status of the invocation from the orchestrator"""
        return self.app.orchestrator.get_invocation_status(self)

    def to_json(self) -> str:
        """:return: The serialized invocation"""
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
        """:return: a new invocation from a serialized invocation"""
        inv_dict = json.loads(serialized)
        call = Call.from_json(app, inv_dict["call"])
        parent_invocation = None
        if "parent_invocation_id" in inv_dict:
            parent_invocation = app.state_backend.get_invocation(
                inv_dict["parent_invocation_id"]
            )
        return cls(call, parent_invocation, inv_dict["invocation_id"])

    def swap_context(self) -> DistributedInvocation | None:
        """
        Swap the current invocation context with this invocation.

        This method is responsible for setting the current invocation context to this invocation.
        It uses the `context` module to manage the invocation context and ensure that the current invocation is tracked correctly.

        The method is used to manage the invocation context when executing the task associated with this invocation.
        It ensures that the current invocation is correctly set and tracked in the distributed environment.
        """
        return context.swap_dist_invocation_context(self.app.app_id, self)

    def reset_context(
        self, previous_invocation_context: DistributedInvocation | None
    ) -> None:
        """
        Reset the invocation context to a previous state.

        This method is responsible for resetting the current invocation context to a previous state.
        It uses the `context` module to manage the invocation context and ensures that the previous invocation context is restored correctly.

        :param Any previous_invocation_context:
            The previous invocation context to restore. This value is returned by the `swap_context` method.
        """
        context.swap_dist_invocation_context(
            self.app.app_id, previous_invocation_context
        )

    def run(self, runner_args: dict[str, Any] | None = None) -> None:
        """
        Execute the task associated with this invocation in a distributed environment.

        This method is responsible for running the task's function with its arguments, handling retriable exceptions,
        and updating the task's state in the orchestrator. It manages the invocation context and communicates with
        the orchestrator to set the invocation's run state and result.

        :param Optional[dict[str, Any]] runner_args:
            Optional arguments passed from/to the runner. These arguments can be used for synchronization
            in subprocesses or other runner-specific tasks. Default is None.

        The method updates the orchestrator with the status of the invocation (`InvocationStatus`) and
        logs the execution process. In case of exceptions, especially retriable ones, it follows the defined
        retry logic as per the task's configuration.

        The return value or any raised exception is stored in the orchestrator and can be retrieved by the caller.

        ```{note}
            - If a retriable exception occurs and the number of retries has not been exceeded, the method
            will set the invocation for a retry and log a warning.
            - If the maximum retries are reached or a non-retriable exception occurs, the exception will be raised
            after updating the orchestrator.
        ```

        The invocation's context is managed to ensure the correct tracking of the current and parent invocations.

        :raises Exception:
            Raises the original exception if a non-retriable exception occurs or the maximum retries are reached for a retriable exception.
        """
        # runner_args are passed from/to the runner (e.g. used to sync subprocesses)
        context.runner_args = runner_args
        try:
            self.task.logger.info(f"Invocation {self.invocation_id} started")
            previous_invocation_context = self.swap_context()
            if not self.app.orchestrator.is_authorize_to_run_by_concurrency_control(
                self
            ):
                self.app.orchestrator.reroute_invocations({self})
            self.app.orchestrator.set_invocation_run(self.parent_invocation, self)
            result = self.task.func(**self.arguments.kwargs)
            self.app.orchestrator.set_invocation_result(self, result)
            self.task.logger.info(f"Invocation {self.invocation_id} finished")
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
            self.reset_context(previous_invocation_context)

    @property
    def result(self) -> Result:
        """
        Retrieve the result of the task execution.

        This property method is responsible for obtaining the final result of the task associated with this invocation.
        If the task is not yet completed (i.e., its status is not final), it enters a waiting state. The method ensures
        that it waits for the task to reach a final state before returning the result.

        The waiting mechanism involves communicating with the orchestrator and potentially the runner, depending on the
        task's current state and execution context. If the task is part of a nested call structure, it also considers the
        parent invocation's state.

        Once the task reaches a final state, the method retrieves and returns the final result of the task execution.

        :return: The result of the task execution. The exact type of `Result` depends on the task's implementation.

        ```{note}
            - This method will block until the task execution is complete and the result is available.
            - If called on an invocation that is not yet in a final state, it will wait (potentially indefinitely)
            for the task to complete.
        ```
        """
        if not self.status.is_final():
            self.app.orchestrator.waiting_for_results(self.parent_invocation, [self])

        while not self.status.is_final():
            self.app.runner.waiting_for_results(
                self.parent_invocation, [self], context.runner_args
            )
        return self.get_final_result()

    def get_final_result(self) -> Result:
        """
        Retrieve the final result of the task execution if the invocation is in a final state.

        This method checks if the invocation has reached a final state. If it has, the method then retrieves and returns
        the result of the task execution from the state backend. In case the invocation is in a failed state, it raises
        an exception with the details of the failure.

        :return: The final result of the task execution.
        :rtype: Result

        :raises exceptions.InvocationError: If the invocation is not in a final state when this method is called.
        """
        if not self.status.is_final():
            raise exceptions.InvocationError(
                self.invocation_id, "Invocation is not final"
            )
        if self.status == InvocationStatus.FAILED:
            raise self.app.state_backend.get_exception(self)
        return self.app.state_backend.get_result(self)


class DistributedInvocationGroup(
    BaseInvocationGroup[Params, Result, DistributedInvocation]
):
    """
    A group of distributed invocations for a specific task in a distributed environment.

    This class extends `BaseInvocationGroup` to handle groups of `DistributedInvocation` instances.

    :param Task task:
        The task associated with these invocations.
    :param list[DistributedInvocation] invocations:
        A list of distributed invocations, each an instance of `DistributedInvocation`.

    The `DistributedInvocationGroup` is specifically designed for use in distributed environments, where task executions are spread across multiple nodes or processes.
    """

    @property
    def results(self) -> Iterator[Result]:
        """
        An iterator over the results of the invocations in the group.

        This property method iterates over the `DistributedInvocation` instances in the group,
        yielding the result of each invocation once it reaches a final state.
        If an invocation has not yet completed, it waits for the result to become available.

        The method ensures that the orchestrator is notified about the waiting invocations
        and communicates with the runner to handle the waiting state efficiently.

        :return:
            An iterator over the results of each invocation in the group.
        :rtype:
            Iterator[Result]

        ```{note}
            This method will block until all invocations in the group have completed and their results are available.
        ```
        """
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
    """
    A specialized invocation that reuses an existing `DistributedInvocation`.

    This class is used for scenarios where an existing invocation is reused, possibly with some differences
    in arguments. It adds an attribute to track these argument differences.

    :ivar Arguments | None diff_arg:
        An optional `Arguments` object representing any differences in arguments compared to the original invocation.
        If `None`, it indicates no differences in arguments.
    """

    # Due to single invocation functionality
    # keeps existing invocation + new argument if any change
    diff_arg: Arguments | None = None

    @classmethod
    def from_existing(
        cls, invocation: DistributedInvocation, diff_arg: Arguments | None = None
    ) -> ReusedInvocation:
        """
        Create a `ReusedInvocation` instance from an existing `DistributedInvocation`.

        This method constructs a new `ReusedInvocation` based on an existing distributed invocation. It reuses
        the invocation ID and other relevant attributes from the original invocation and allows specifying
        differences in arguments.

        :param DistributedInvocation invocation:
            The existing invocation to reuse.
        :param Arguments | None diff_arg:
            Optional argument differences for the new invocation. If provided, these arguments will be used to
            distinguish the new invocation from the original. Default is None.
        :return:
            A new instance of `ReusedInvocation` based on the provided existing invocation.
        :rtype:
            ReusedInvocation
        """
        new_invc = cls(
            call=Call(invocation.task, invocation.arguments),
            parent_invocation=invocation.parent_invocation,
            # we reuse invocation_id from original invocation
            _invocation_id=invocation.invocation_id,
            diff_arg=diff_arg,
        )
        return new_invc
