import json
from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic

from .arguments import Arguments
from .conf.config_task import ConcurrencyControlType
from .types import Params, Result

if TYPE_CHECKING:
    from .app import Pynenc
    from .task import Task


@dataclass(frozen=True)
class Call(Generic[Params, Result]):
    """A specific call of a task

    A call is unique per task and arguments"""

    task: "Task[Params, Result]"
    arguments: "Arguments" = field(default_factory=Arguments)

    @cached_property
    def app(self) -> "Pynenc":
        return self.task.app

    @cached_property
    def call_id(self) -> str:
        """Returns a unique id for each task and arguments"""
        return "#task_id#" + self.task.task_id + "#args_id#" + self.arguments.args_id

    @cached_property
    def serialized_arguments(self) -> dict[str, str]:
        """Returns a dictionary with each of the call arguments serialized into a string"""
        return {
            k: self.app.serializer.serialize(v)
            for k, v in self.arguments.kwargs.items()
        }

    @cached_property
    def serialized_args_for_concurrency_check(self) -> dict[str, str] | None:
        """Returns a dictionary with the call arguments required for the task concurrency check"""
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
        """Returns an Arguments instance with the deserialized arguments"""
        return Arguments(
            {
                k: self.app.serializer.deserialize(v)
                for k, v in serialized_arguments.items()
            }
        )

    def to_json(self) -> str:
        """Returns a string with the serialized call"""
        return json.dumps(
            {"task": self.task.to_json(), "arguments": self.serialized_arguments}
        )

    def __getstate__(self) -> dict:
        # Return state as a dictionary and a secondary value as a tuple
        return {"task": self.task, "arguments": self.serialized_arguments}

    def __setstate__(self, state: dict) -> None:
        object.__setattr__(self, "task", state["task"])
        arguments = self.deserialize_arguments(state["arguments"])
        object.__setattr__(self, "arguments", arguments)

    @classmethod
    def from_json(cls, app: "Pynenc", serialized: str) -> "Call":
        """Returns a new call from a serialized call"""
        from .task import Task

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
