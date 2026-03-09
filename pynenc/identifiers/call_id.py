from dataclasses import dataclass
from functools import cached_property

from pynenc.identifiers.task_id import TaskId


@dataclass(frozen=True)
class CallId:
    """Identifier for a task call.

    :param TaskId task_id: Fully qualified task identifier (module.function)
    :param str args_id: Hash-based identifier for the argument set
    """

    task_id: TaskId
    args_id: str

    @cached_property
    def key(self) -> str:
        """Composite key for storage and lookup."""
        return self.task_id.key + ":" + self.args_id

    @classmethod
    def from_key(cls, key: str) -> "CallId":
        """Parse a CallId from its composite key."""
        try:
            task_id_str, args_id = key.rsplit(":", 1)
        except ValueError as ex:
            raise ValueError(f"Invalid CallId key format: {key}") from ex
        return cls(task_id=TaskId.from_key(task_id_str), args_id=args_id)

    def __str__(self) -> str:
        return self.key
