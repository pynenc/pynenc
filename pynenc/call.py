import json
from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic

from pynenc.arguments import Arguments
from pynenc.conf.config_task import ConcurrencyControlType
from pynenc.types import Params, Result

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.task import Task


@dataclass
class Call(Generic[Params, Result]):
    """
    Base class for task calls with common functionality.

    :param Task[Params, Result] task: The task associated with the call.
    """

    task: "Task[Params, Result]"
    _arguments: "Arguments" = field(default_factory=Arguments, repr=False)
    _serialized_arguments: dict[str, str] | None = None

    @property
    def app(self) -> "Pynenc":
        """
        Gets the Pynenc application instance associated with the task.

        :return: The Pynenc application instance.
        """
        return self.task.app

    @property
    def arguments(self) -> "Arguments":
        """
        Get the arguments for this call.
        This property allows subclasses to override argument handling.

        :return: Arguments object containing call arguments
        """
        return self._arguments

    @property
    def call_id(self) -> str:
        """
        Generates a unique identifier for the call based on the task ID and the arguments.

        :return: A string representing the unique identifier of the call.
        """
        return "#task_id#" + self.task.task_id + "#args_id#" + self.arguments.args_id

    @cached_property
    def serialized_arguments(self) -> dict[str, str]:
        """
        Serializes the call arguments into strings.

        :return: A dictionary of serialized argument strings.
        """
        if self._serialized_arguments:
            return self._serialized_arguments
        disable_cache = "*" in self.task.conf.disable_cache_args
        return {
            k: self.app.arg_cache.serialize(
                v, disable_cache or k in self.task.conf.disable_cache_args
            )
            for k, v in self.arguments.kwargs.items()
        }

    @cached_property
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
            return {
                key: self.serialized_arguments[key]
                for key in self.task.conf.key_arguments
            }
        return None

    def deserialize_arguments(self, serialized_arguments: dict[str, str]) -> Arguments:
        """
        Deserializes the given serialized arguments.

        :param dict[str, str] serialized_arguments: The serialized arguments to deserialize.
        :return: An Arguments object representing the deserialized arguments.
        """
        return Arguments(
            {
                k: self.app.arg_cache.deserialize(v)
                for k, v in serialized_arguments.items()
            }
        )

    def to_json(self) -> str:
        """
        Serializes the call into a JSON string.

        :return: A JSON string representing the serialized call.
        """
        return json.dumps(
            {"task": self.task.to_json(), "arguments": self.serialized_arguments}
        )

    def __getstate__(self) -> dict:
        """
        Gets the state of the Call object for serialization purposes.

        :return: A dictionary representing the state of the Call object.
        """
        return {"task": self.task, "arguments": self.serialized_arguments}

    def __setstate__(self, state: dict) -> None:
        """
        Sets the state of the Call object from the provided dictionary.

        :param dict state: A dictionary representing the state to set.
        """
        object.__setattr__(self, "task", state["task"])
        arguments = self.deserialize_arguments(state["arguments"])
        object.__setattr__(self, "_arguments", arguments)

    @classmethod
    def from_json(cls, app: "Pynenc", serialized: str) -> "Call":
        """
        Creates a Call object from a serialized JSON string.

        :param Pynenc app: The Pynenc application instance.
        :param str serialized: The serialized JSON string representing the call.
        :return: A Call object created from the serialized data.
        """
        from pynenc.task import Task

        call_dict = json.loads(serialized)
        return cls(
            task=Task.from_json(app, call_dict["task"]),
            _arguments=Arguments(
                {
                    k: app.arg_cache.deserialize(v)
                    for k, v in call_dict["arguments"].items()
                }
            ),
            _serialized_arguments=call_dict["arguments"],
        )

    def __str__(self) -> str:
        return f"Call(call_id={self.call_id}, task={self.task}, arguments={self.arguments})"

    def __repr__(self) -> str:
        return self.__str__()

    def __hash__(self) -> int:
        return hash(self.call_id)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Call):
            return False
        return self.call_id == other.call_id


@dataclass
class PreSerializedCall(Call[Params, Result]):
    """
    Represents a call optimized for parallel routing with pre-serialized arguments.
    This call type is used for batch processing tasks with disabled concurrency control,
    where some arguments are pre-serialized (e.g., large shared data).

    :param Task[Params, Result] task: The task associated with the call.
    :param dict[str, Any] other_args: Unique arguments for this specific call.
    :param dict[str, str] pre_serialized_args: Pre-serialized common arguments.
    """

    other_args: dict[str, Any] = field(default_factory=dict)
    pre_serialized_args: dict[str, str] = field(default_factory=dict)
    _call: Call[Params, Result] | None = field(default=None, repr=False)
    _cached_arguments: Arguments | None = field(default=None, repr=False)
    _serialized_arguments: dict[str, str] | None = field(default=None, repr=False)
    _args_hash: str | None = field(default=None, repr=False)

    @property
    def call(self) -> "Call[Params, Result]":
        if self._call is None:
            self.app.logger.warning(
                "Generating a regular Call object from a RoutingParallelCall "
                "is inefficient and should be avoided if possible. "
            )
            self._call = Call(
                task=self.task,
                _arguments=self.deserialize_arguments(self.serialized_arguments),
            )
        return self._call

    @property
    def arguments(self) -> "Arguments":
        if not self._cached_arguments:
            self._cached_arguments = self.deserialize_arguments(
                self.serialized_arguments
            )
        return self._cached_arguments

    @property
    def call_id(self) -> str:
        """
        Get the call_id from the underlying Call object.

        :return: A string representing the unique identifier of the call.
        """
        return self.call.call_id

    @property
    def serialized_arguments(self) -> dict[str, str]:
        """
        Serializes the call arguments into strings.

        :return: A dictionary of serialized argument strings.
        """
        if self._serialized_arguments is None:
            disable_cache = "*" in self.task.conf.disable_cache_args
            serialized = {
                k: self.app.arg_cache.serialize(
                    v, disable_cache or k in self.task.conf.disable_cache_args
                )
                for k, v in self.other_args.items()
                if k not in self.pre_serialized_args
            }
            self._serialized_arguments = {**serialized, **self.pre_serialized_args}
        return self._serialized_arguments

    @property
    def serialized_args_for_concurrency_check(self) -> dict[str, str] | None:
        raise NotImplementedError(
            "RoutingParallelCall does not support serialized_args_for_concurrency_check "
            "(intended for batch routing only)"
        )

    def __getstate__(self) -> dict:
        return {
            "task": self.task,
            "other_args": {
                k: v
                for k, v in self.serialized_arguments.items()
                if k in self.other_args
            },
            "pre_serialized_args": self.pre_serialized_args,
        }

    def __setstate__(self, state: dict) -> None:
        object.__setattr__(self, "task", state["task"])
        other_args = self.deserialize_arguments(state["other_args"]).kwargs
        object.__setattr__(self, "other_args", other_args)
        object.__setattr__(self, "pre_serialized_args", state["pre_serialized_args"])

    @classmethod
    def from_json(cls, app: "Pynenc", serialized: str) -> "Call":
        """
        PreSerializedCall doesn't support from_json as it's meant for batch routing.

        :raises NotImplementedError: This method is not implemented for PreSerializedCall.
        """
        raise NotImplementedError(
            "PreSerializedCall does not support from_json method "
            "(Use a regular Call object for serialization)"
        )

    def __str__(self) -> str:
        return f"PreSerializedCall(task={self.task}, other_args={self.other_args}, pre_serialized_args={list(self.pre_serialized_args.keys())})"

    def __hash__(self) -> int:
        raise NotImplementedError(
            "RoutingParallelCall does not support __hash__ (not intended for sets/dicts)"
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Call):
            return False
        if isinstance(other, PreSerializedCall):
            return self.call == other.call
        return self.call == other
