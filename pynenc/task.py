from __future__ import annotations

import importlib
import json
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic, Iterable

from . import context
from .arguments import Arguments
from .call import Call
from .conf.config_task import ConfigTask
from .exceptions import InvalidTaskOptionsError, RetryError
from .invocation import (
    BaseInvocation,
    BaseInvocationGroup,
    DistributedInvocationGroup,
    SynchronousInvocation,
    SynchronousInvocationGroup,
)
from .types import Func, Params, Result
from .util.log import TaskLoggerAdapter

if TYPE_CHECKING:
    from .app import Pynenc


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
    option set to `True` (or the 'PYNENC_DEV_MODE_FORCE_SYNC_TASK' environment variable set).
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
        self.logger = TaskLoggerAdapter(self.app.logger, self.task_id)
        self.func = func
        self.options = options
        self.validate_options()

    def validate_options(self) -> None:
        """
        validate that all the option fields exists in the config_fields
        it will raise an exception with all the invalid options
        """
        invalid_options = []
        for option in self.options:
            if option not in ConfigTask.config_fields():
                invalid_options.append(option)
        if invalid_options:
            raise InvalidTaskOptionsError(
                self.task_id, f"Invalid options: {invalid_options}"
            )

    @cached_property
    def conf(self) -> ConfigTask:
        return ConfigTask(
            task_id=self.task_id,
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
            task_options=self.options,
        )

    @property
    def invocation(self) -> BaseInvocation:
        """The invocation of the task"""
        if dist_inv := context.dist_inv_context.get(self.app.app_id):
            return dist_inv
        if sync_inv := context.sync_inv_context.get(self.app.app_id):
            return sync_inv
        raise RuntimeError("Task has not been invoked yet")

    def to_json(self) -> str:
        """Returns a string with the serialized task"""
        return json.dumps(
            {"task_id": self.task_id, "options": self.conf.options_to_json()}
        )

    def __getstate__(self) -> dict:
        # Return state as a dictionary and a secondary value as a tuple
        return {"app": self.app, "task_json": self.to_json()}

    def __setstate__(self, state: dict) -> None:
        # Restore instance attributes
        self.app = state["app"]
        serialized = state["task_json"]
        task_id, func, options = Task._from_json(self.app, serialized)
        # Restore the cached property
        self.task_id = task_id
        self.app = self.app
        self.func = func
        self.options = options

    @staticmethod
    def _from_json(app: Pynenc, serialized: str) -> tuple[str, Func, dict[str, Any]]:
        """Returns a function and options from a serialized task"""
        task_dict = json.loads(serialized)
        task_id = task_dict["task_id"]
        module_name, function_name = task_id.rsplit(".", 1)
        module = importlib.import_module(module_name)
        function = getattr(module, function_name)
        options = ConfigTask.options_from_json(task_dict["options"])
        return task_id, function.func, options

    @classmethod
    def from_json(cls, app: Pynenc, serialized: str) -> Task:
        """Returns a new task from a serialized task"""
        _, func, options = cls._from_json(app, serialized)
        return cls(app, func, options)

    @cached_property
    def retriable_exceptions(self) -> tuple[type[Exception], ...]:
        if self.conf.retry_for is None:
            return (RetryError,)
        return self.conf.retry_for + (RetryError,)

    @cached_property
    def task_id(self) -> str:
        """The id of the task, which is the module and function name."""
        return f"{self.func.__module__}.{self.func.__name__}"

    def __str__(self) -> str:
        return f"Task(func={self.func.__name__})"

    def __repr__(self) -> str:
        return self.__str__()

    def args(self, *args: Params.args, **kwargs: Params.kwargs) -> Arguments:
        """Returns an Arguments instance from the given args and kwargs"""
        return Arguments.from_call(self.func, *args, **kwargs)

    def __call__(
        self, *args: Params.args, **kwargs: Params.kwargs
    ) -> BaseInvocation[Params, Result]:
        """Handles a call to the task"""
        arguments = Arguments.from_call(self.func, *args, **kwargs)
        return self._call(arguments)

    def _call(self, arguments: Arguments) -> BaseInvocation[Params, Result]:
        """Route the call to the orchestrator if not in dev mode, otherwise run synchronously"""
        if self.app.conf.dev_mode_force_sync_tasks:
            return SynchronousInvocation(call=Call(self, arguments))
        return self.app.orchestrator.route_call(Call(self, arguments))

    def parallelize(
        self, param_iter: Iterable[tuple | dict | Arguments]
    ) -> BaseInvocationGroup:
        """
        iterable of calls to the task,
        will accept a tuple positional arguments,
        a dict of keyword arguments,
        or an Arguments instance, eg: task.args(``*args``, ``**kwargs``))
        """
        group_cls: type[BaseInvocationGroup]
        if self.app.conf.dev_mode_force_sync_tasks:
            group_cls = SynchronousInvocationGroup
        else:
            group_cls = DistributedInvocationGroup

        def get_args(params: tuple | dict | Arguments) -> Arguments:
            if isinstance(params, tuple):
                return self.args(*params)
            if isinstance(params, dict):
                return self.args(**params)
            return params

        invocations = []
        for params in param_iter:
            if invocation := self._call(get_args(params)):
                invocations.append(invocation)
        return group_cls(self, invocations)
