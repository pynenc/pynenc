from dataclasses import dataclass
from functools import cached_property


@dataclass(frozen=True)
class TaskId:
    """Structured identifier for a task.

    :param str module: The module where the task function is defined.
    :param str func_name: The name of the task function.
    """

    module: str
    func_name: str

    @cached_property
    def key(self) -> str:
        return self.module + ":" + self.func_name

    @cached_property
    def config_key(self) -> str:
        """Key used for config file lookups, using '.' as separator.

        Config files (YAML/JSON) use dot notation for task-specific settings,
        e.g. ``module_name.task_name``.
        """
        return f"{self.module}.{self.func_name}"

    @classmethod
    def from_key(cls, key: str) -> "TaskId":
        try:
            module, func_name = key.split(":")
        except ValueError as ex:
            raise ValueError(f"Invalid TaskId key format: {key}") from ex
        return cls(module, func_name)

    def __str__(self) -> str:
        return self.key
