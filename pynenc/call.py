"""
Task call representations with optimized argument handling.

This module defines the core call abstractions for Pynenc tasks, optimized
for different construction contexts:
- Call: Standard client-side construction with raw arguments
- LazyCall: State backend construction with deferred deserialization
- PreSerializedCall: Batch construction with shared pre-serialized arguments

Key components:
- CallId: Structured identifier combining task and argument identity
- CallDTO: Serialization-ready data transfer object
- compute_args_id: Deterministic hashing from serialized arguments
"""

import hashlib
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic

from pynenc.arguments import Arguments
from pynenc.exceptions import SerializationError
from pynenc.models.call_dto import CallDTO
from pynenc.conf.config_task import ConcurrencyControlType
from pynenc.identifiers.call_id import CallId
from pynenc.types import Params, Result

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.task import Task


def compute_args_id(serialized_args: dict[str, str]) -> str:
    """Compute deterministic argument hash from serialized form."""
    if not serialized_args:
        return "no_args"
    sorted_items = sorted(serialized_args.items())
    args_str = "".join([f"{k}:{v}" for k, v in sorted_items])
    return hashlib.sha256(args_str.encode()).hexdigest()


class Call(Generic[Params, Result]):
    """Standard task call with raw arguments.

    Created client-side when invoking tasks. Arguments are stored as Python
    objects and serialized on-demand for distribution.

    :param Task[Params, Result] task: Associated task definition
    :param Arguments arguments: Raw Python argument values
    """

    def __init__(
        self,
        task: "Task[Params, Result]",
        arguments: Arguments | None = None,
        _serialized_arguments: dict[str, str] | None = None,
    ) -> None:
        self.task = task
        self._arguments = arguments
        self._serialized_arguments = _serialized_arguments
        self._args_id: str | None = None
        self._call_id: CallId | None = None

    @property
    def app(self) -> "Pynenc":
        return self.task.app

    @property
    def arguments(self) -> "Arguments":
        """
        Get the arguments for this call.
        This property allows subclasses to override argument handling.

        :return: Arguments object containing call arguments
        """
        if self._arguments is None:
            self._arguments = Arguments()
        return self._arguments

    @property
    def serialized_arguments(self) -> dict[str, str]:
        """Serialize arguments with external storage for large values.

        :return: Mapping of argument names to serialized values or storage keys
        :raises SerializationError: If an argument cannot be serialized,
            enriched with task context.
        """
        if self._serialized_arguments is None:
            try:
                self._serialized_arguments = (
                    self.app.client_data_store.serialize_arguments(
                        self.arguments.kwargs, self.task.conf.disable_cache_args
                    )
                )
            except SerializationError as exc:
                raise SerializationError(
                    f"Task '{self.task.task_id.key}': {exc}"
                ) from exc
        return self._serialized_arguments

    @property
    def args_id(self) -> str:
        """Compute argument identity hash.

        :return: SHA256 hash of serialized arguments
        """
        if self._args_id is None:
            self._args_id = compute_args_id(self.serialized_arguments)
        return self._args_id

    @property
    def call_id(self) -> CallId:
        """Compute composite call identifier.

        :return: CallId combining task and argument identity
        """
        if self._call_id is None:
            self._call_id = CallId(task_id=self.task.task_id, args_id=self.args_id)
        return self._call_id

    @cached_property
    def arg_keys(self) -> set[str]:
        """Set of argument keys for this call."""
        return set(self.arguments.kwargs.keys())

    @property
    def serialized_args_for_concurrency_check(self) -> dict[str, str] | None:
        """
        Determines the call arguments required for the task concurrency check.

        :return: A dictionary of serialized argument strings required for concurrency control, or None if concurrency control is disabled.
        """
        if self.task.conf.registration_concurrency == ConcurrencyControlType.DISABLED:
            return None
        if self.task.conf.registration_concurrency == ConcurrencyControlType.TASK:
            return None
        if self.task.conf.registration_concurrency == ConcurrencyControlType.ARGUMENTS:
            return self.serialized_arguments
        if self.task.conf.registration_concurrency == ConcurrencyControlType.KEYS:
            # TODO cache serialization of each argument independently?
            # So we do not serialize all the arguments for the key if not needed
            return {
                key: self.serialized_arguments[key]
                for key in self.task.conf.key_arguments
            }
        return None

    def to_dto(self) -> CallDTO:
        """Create serialization-ready DTO.

        :return: CallDTO with pre-serialized data
        """
        return CallDTO(
            call_id=self.call_id,
            serialized_arguments=self.serialized_arguments,
        )

    def __str__(self) -> str:
        return (
            f"Call(task={self.task.task_id}, arguments={self.arguments.kwargs.keys()})"
        )

    def __repr__(self) -> str:
        return self.__str__()

    def __hash__(self) -> int:
        return hash(self.call_id)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Call):
            return False
        return self.call_id == other.call_id


class LazyCall(Call[Params, Result]):
    """Task call with deferred argument deserialization.

    Created by state backends when loading persisted invocations. Arguments
    remain serialized until accessed, avoiding deserialization cost for
    identity-only operations (concurrency checks, status queries).

    Must be constructed via from_dto factory method - do not instantiate directly.

    :param Task[Params, Result] task: Associated task definition
    :param dict[str, str] _serialized_arguments: Pre-serialized argument mapping
    :param str _args_id: Pre-computed argument hash
    """

    def __init__(
        self,
        task: "Task[Params, Result]",
        _serialized_arguments: dict[str, str],
        _call_id: CallId,
    ) -> None:
        super().__init__(
            task=task,
            arguments=None,  # Arguments will be lazily deserialized
            _serialized_arguments=_serialized_arguments,
        )
        self._args_id = _call_id.args_id
        self._call_id = _call_id

    @property
    def arguments(self) -> "Arguments":
        """Lazily deserialize arguments on first access."""
        if self._arguments is None:
            self._arguments = Arguments()
            self._arguments.kwargs = self.app.client_data_store.deserialize_arguments(
                self.serialized_arguments
            )
        return self._arguments

    @cached_property
    def arg_keys(self) -> set[str]:
        return set(self.serialized_arguments.keys())

    @classmethod
    def from_dto(cls, app: "Pynenc", dto: CallDTO) -> "LazyCall[Params, Result]":
        """Construct LazyCall from DTO without deserialization.

        Primary factory method for state backend usage.

        :param Pynenc app: Pynenc application instance
        :param CallDTO dto: Data transfer object with serialized data
        :return: LazyCall with deferred deserialization
        """
        from pynenc.task import Task

        task = Task.from_id(app, dto.call_id.task_id)
        return cls(
            task=task,
            _serialized_arguments=dto.serialized_arguments,
            _call_id=dto.call_id,
        )

    def __str__(self) -> str:
        return f"LazyCall(task={self.task.task_id}, args_id={self.args_id})"


class PreSerializedCall(Call[Params, Result]):
    """Task call optimized for batch operations with shared arguments.

    Used when distributing many similar tasks with large common arguments.
    Common arguments are pre-serialized once; unique arguments serialized
    per-call. Enables efficient batch routing without redundant serialization.

    :param Task[Params, Result] task: Associated task definition
    :param dict[str, str] pre_serialized_args: Shared pre-serialized arguments
    :param dict[str, Any] other_args: Call-specific raw arguments
    """

    def __init__(
        self,
        task: "Task[Params, Result]",
        common_args: dict[str, Any] | None = None,
        common_serialized_args: dict[str, str] | None = None,
        other_args: dict[str, Any] | None = None,
    ) -> None:
        # Store raw arguments without calling parent __init__
        self.task = task
        self.common_args = common_args or {}
        self.common_serialized_args = common_serialized_args or {}
        self.other_args = other_args or {}
        self._serialized_arguments: dict[str, str] | None = None
        self._arguments = Arguments(kwargs={**self.common_args, **self.other_args})
        self._args_id: str | None = None
        self._call_id: CallId | None = None

    @property
    def serialized_arguments(self) -> dict[str, str]:
        """Combine pre-serialized and freshly serialized arguments.

        :return: Complete serialized argument mapping
        """
        if self._serialized_arguments is None:
            # Only serialize other_args not already in pre_serialized_args
            other_only = {
                k: v
                for k, v in self.other_args.items()
                if k not in self.common_serialized_args
            }
            serialized_other = self.app.client_data_store.serialize_arguments(
                other_only, self.task.conf.disable_cache_args
            )
            self._serialized_arguments = {
                **self.common_serialized_args,
                **serialized_other,
            }
        return self._serialized_arguments

    @property
    def serialized_args_for_concurrency_check(self) -> dict[str, str] | None:
        raise NotImplementedError(
            "RoutingParallelCall does not support serialized_args_for_concurrency_check "
            "(intended for batch routing only)"
        )

    def __str__(self) -> str:
        other_args = set(self.other_args.keys())
        common_args = set(self.common_args.keys())
        return f"PreSerializedCall(task={self.task}, {other_args=}, {common_args=})"
