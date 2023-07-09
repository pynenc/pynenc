from dataclasses import dataclass, field
from functools import cached_property
from typing import TYPE_CHECKING, Generic, Any

from .types import Params, Result
from .arguments import Arguments

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
