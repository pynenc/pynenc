from __future__ import annotations
from dataclasses import dataclass
from functools import cached_property
import importlib
import json
from typing import TYPE_CHECKING, Generic, Any, Optional

from .arguments import Arguments
from .call import Call
from .conf.base_config_option import BaseConfigOption
from .conf.single_invocation_pending import SingleInvocation
from .invocation import BaseInvocation, SynchronousInvocation
from .types import Params, Result, Func

if TYPE_CHECKING:
    from .app import Pynenc


@dataclass
class TaskOptions:
    """The options common to any implementation of BaseTask"""

    #: If True, only one request will be routed by the broker.
    #: Use this option for tasks that make no sense to execute multiple times in parallel or to avoid generating too much unnecessary tasks in the system.
    single_invocation: Optional[SingleInvocation] = None

    #: Profiling will take care of storing profiling information for the task (this is a todo, will require further options).
    profiling: Optional[str] = None

    def __post_init__(self) -> None:
        for attr, value in self.__dict__.items():
            if isinstance(value, dict):
                self.__dict__[attr] = BaseConfigOption.from_dict(value)
        self.validate()

    def validate(self) -> None:
        for attr, value in self.__dict__.items():
            if isinstance(value, (str, int, bool, type(None))):
                continue
            if not issubclass(type(value), BaseConfigOption):
                raise TypeError(
                    f"Attribute {attr} must be a basic type or a subclass of BaseConfigOption"
                )

    def to_dict(self) -> dict[str, Any]:
        """Returns a dictionary with the options"""
        options_dict = {**self.__dict__}
        for attr, value in options_dict.items():
            if isinstance(value, BaseConfigOption):
                options_dict[attr] = value.to_dict()
        return options_dict

    def to_json(self) -> str:
        """Returns a string with the serialized options"""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, options_dict: dict[str, Any]) -> "TaskOptions":
        """Returns a new options from a dictionary"""
        for attr, value in options_dict.items():
            if isinstance(value, dict):
                options_dict[attr] = BaseConfigOption.from_dict(value)
        return cls(**options_dict)

    @classmethod
    def from_json(cls, serialized: str) -> "TaskOptions":
        return cls.from_dict(json.loads(serialized))


class Task(Generic[Params, Result]):
    """
    A task in the Pynenc library that represents a function that can be distributed.

    Parameters
    ----------
    app : Pynenc
        A reference to the Pynenc application.
    func : Callable
        The function to be run distributed.
    options : dict
        The options to apply.

    The `BaseTask` can be called normally and will return an instance of `BaseResult`.
    The result will be an `AsyncResult` when running normally but can be `SyncResult`
    when running eagerly in development with the `pynenc` app's `dev_mode_force_sync_tasks`
    option set to `True` (or the 'DEV_MODE_FORCE_SYNC_TASK' environment variable set).
    The option `dev_mode_force_sync_tasks` should only be used in development.

    Although it is possible to create a `BaseTask` instance directly, it is recommended to
    use the decorator provided in the `pynenc` application, i.e., `@app.task(options...)`.
    This is the expected way of instantiating a class and registering it in the app.

    Examples
    --------
    >>> @app.task(options)
    ... def func():
    ...     pass
    ...
    >>> result = func()
    """

    def __init__(self, app: Pynenc, func: Func, options: dict[str, Any]) -> None:
        self.task_id = f"{func.__module__}.{func.__name__}"
        self.app = app
        self.func = func
        self.options: TaskOptions = TaskOptions(**options)

    def to_json(self) -> str:
        """Returns a string with the serialized task"""
        return json.dumps({"task_id": self.task_id, "options": self.options.to_json()})

    @classmethod
    def from_json(cls, app: Pynenc, serialized: str) -> "Task":
        """Returns a new task from a serialized task"""
        task_dict = json.loads(serialized)
        task_id = task_dict["task_id"]
        module_name, function_name = task_id.rsplit(".", 1)
        module = importlib.import_module(module_name)
        function = getattr(module, function_name)
        options_dict = TaskOptions.from_json(task_dict["options"]).to_dict()
        return cls(app, function.func, options_dict)

    @cached_property
    def task_id(self) -> str:
        """The id of the task, which is the module and function name."""
        return f"{self.func.__module__}.{self.func.__name__}"

    def __str__(self) -> str:
        return f"Task(func={self.func.__name__})"

    def __repr__(self) -> str:
        return self.__str__()

    def __call__(
        self, *args: Params.args, **kwargs: Params.kwargs
    ) -> "BaseInvocation[Params, Result]":
        """"""
        arguments = Arguments.from_call(self.func, *args, **kwargs)
        if self.app.conf.dev_mode_force_sync_tasks:
            return SynchronousInvocation(
                call=Call(self, arguments),
                result=self.func(*args, **kwargs),
            )
        return self.app.orchestrator.route_call(Call(self, arguments))
