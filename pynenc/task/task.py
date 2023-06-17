from dataclasses import dataclass
from typing import Type, ClassVar

from .base_task import BaseOptions, BaseTask


@dataclass
class TaskOptions(BaseOptions):
    """Specific Options for the implementation of the BaseOption"""

    sub_task_option: int = 0


class Task(BaseTask):
    """Specific Task implementation"""

    options_cls: ClassVar[Type[TaskOptions]] = TaskOptions
