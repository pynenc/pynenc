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


@dataclass(frozen=True)
class Call(Generic[Params, Result]):
    """
    Represents a specific call of a task. A call is unique per task and its arguments.

    :param Task[Params, Result] task: The task associated with the call.
    :param Arguments arguments: The arguments used in the call. Defaults to an empty Arguments object.
    """

    task: "Task[Params, Result]"
    arguments: "Arguments" = field(default_factory=Arguments)

    @cached_property
    def app(self) -> "Pynenc":
        """
        Gets the Pynenc application instance associated with the task.

        :return: The Pynenc application instance.
        """
        return self.task.app

    @cached_property
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
        return {
            k: self.app.serializer.serialize(v)
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

    def deserialize_arguments(self, serialized_arguments: dict[str, str]) -> Arguments:
        """
        Deserializes the given serialized arguments.

        :param dict[str, str] serialized_arguments: The serialized arguments to deserialize.
        :return: An Arguments object representing the deserialized arguments.
        """
        return Arguments(
            {
                k: self.app.serializer.deserialize(v)
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
        object.__setattr__(self, "arguments", arguments)

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
            arguments=Arguments(
                {
                    k: app.serializer.deserialize(v)
                    for k, v in call_dict["arguments"].items()
                }
            ),
        )

    def __str__(self) -> str:
        return f"Call(call_id={self.call_id}, task={self.task}, arguments={self.arguments})"

    def __repr__(self) -> str:
        return self.__str__()

    def __hash__(self) -> int:
        return hash(self.call_id)

    def __eq__(self, other: Any) -> bool:
        # TODO equality based in task and arguments or in invocation_id?
        if not isinstance(other, Call):
            return False
        return self.call_id == other.call_id
