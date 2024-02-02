from __future__ import annotations

import importlib
import json
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic, Iterable

from pynenc import context
from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.conf.config_task import ConfigTask
from pynenc.exceptions import InvalidTaskOptionsError, RetryError
from pynenc.invocation.base_invocation import BaseInvocation, BaseInvocationGroup
from pynenc.invocation.dist_invocation import DistributedInvocationGroup
from pynenc.invocation.sync_invocation import (
    SynchronousInvocation,
    SynchronousInvocationGroup,
)
from pynenc.types import Func, Params, Result
from pynenc.util.log import TaskLoggerAdapter

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class Task(Generic[Params, Result]):
    """
    **A task in the Pynenc library that represents a function that can be distributed.**

    :param Pynenc app: A reference to the Pynenc application.
    :param Callable func: The function to be run distributed.
    :param dict[str, Any] **options: The options to apply.

    The `BaseTask` can be called normally and will return an instance of `BaseResult`.
    The result will be an `AsyncResult` when running normally but can be `SyncResult`
    when running eagerly in development with the `pynenc` app's `dev_mode_force_sync_tasks`
    option set to `True` (or the 'PYNENC_DEV_MODE_FORCE_SYNC_TASK' environment variable set).
    The option `dev_mode_force_sync_tasks` should only be used in development.

    ```{hint}
    Although it is possible to create a `BaseTask` instance directly,
    it is recommended to use the decorator provided in the `pynenc` application, i.e., `@app.task(options...)`.
    This is the expected way of instantiating a class and registering it in the app.
    ```

    ### Limitations
    ```{attention}
    This implementation does not support the creation of tasks
    from functions defined in modules intended to run as standalone scripts.
    ```
    This applies to any module executed directly, where its `__name__` attribute becomes `"__main__"`.
    This is not exclusive to modules with `if __name__ == "__main__"`
    sections but includes any module run as the main program.
    In such situations, `func.__module__` being `"__main__"`
    poses a challenge for task instantiation and serialization.
    When a task is executed in the initiator script, it is identified as `__main__.task_name`.

    However, in a Pynenc worker's distributed environment, `__main__` refers to the worker itself.
    As a result, the task identified as `__main__.task_name` cannot be found,
    since the worker's `__main__` differs from that of the initiator script.
    **To ensure simplicity and robustness in task management,
    tasks defined in modules run as the main program are not supported.**

    ### Examples
    ```{code-block} python
    @app.task(options)
    def func():
        pass

    result = func()
    ```

    ### Raises
    - **RuntimeError:** If an attempt is made to create a task from a function in the `__main__` module.
    """

    def __init__(self, app: Pynenc, func: Func, options: dict[str, Any]) -> None:
        if "__main__" in func.__module__:
            raise RuntimeError(
                "Cannot create a task from a function in the __main__ module"
            )
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
        if dist_inv := context.get_dist_invocation_context(self.app.app_id):
            return dist_inv
        if sync_inv := context.sync_inv_context.get(self.app.app_id):
            return sync_inv
        raise RuntimeError("Task has not been invoked yet")

    def to_json(self) -> str:
        """:return: The serialized task"""
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
        task_id, func, options = Task._from_json(serialized)
        # Restore the cached property
        self.task_id = task_id
        self.app = self.app
        self.func = func
        self.options = options
        self.logger = TaskLoggerAdapter(self.app.logger, self.task_id)

    @staticmethod
    def _from_json(serialized: str) -> tuple[str, Func, dict[str, Any]]:
        """:return: a function and options from a serialized task"""
        task_dict = json.loads(serialized)
        task_id = task_dict["task_id"]
        module_name, function_name = task_id.rsplit(".", 1)
        module = importlib.import_module(module_name)
        function = getattr(module, function_name)
        options = ConfigTask.options_from_json(task_dict["options"])
        return task_id, function.func, options

    @classmethod
    def from_json(cls, app: Pynenc, serialized: str) -> Task:
        """:return: a new task from a serialized task"""
        _, func, options = cls._from_json(serialized)
        return cls(app, func, options)

    @cached_property
    def retriable_exceptions(self) -> tuple[type[Exception], ...]:
        """
        Retrieve a tuple of exception types that should trigger a retry of the task.

        This method provides a list of exception types, indicating which exceptions will cause the task to be retried.
        The `RetryError` exception type, specific to the Pynenc system, is always included to ensure that internal retry
        mechanisms are accounted for.

        :return: A tuple of retriable exceptions.
        """
        if not self.conf.retry_for:
            return (RetryError,)
        if RetryError in self.conf.retry_for:
            return self.conf.retry_for
        return self.conf.retry_for + (RetryError,)

    def __str__(self) -> str:
        return f"Task(func={self.func.__name__})"

    def __repr__(self) -> str:
        return self.__str__()

    def args(self, *args: Params.args, **kwargs: Params.kwargs) -> Arguments:
        """:return: an Arguments instance from the given args and kwargs"""
        return Arguments.from_call(self.func, *args, **kwargs)

    def __call__(
        self, *args: Params.args, **kwargs: Params.kwargs
    ) -> BaseInvocation[Params, Result]:
        """Handles a call to the task"""
        arguments = Arguments.from_call(self.func, *args, **kwargs)
        return self._call(arguments)

    def _call(self, arguments: Arguments) -> BaseInvocation[Params, Result]:
        """
        Route the call to the orchestrator if not in dev mode, otherwise run synchronously
        :return: the invocation
        """
        if self.app.conf.dev_mode_force_sync_tasks:
            return SynchronousInvocation(call=Call(self, arguments))
        return self.app.orchestrator.route_call(Call(self, arguments))

    def parallelize(
        self, param_iter: Iterable[tuple | dict | Arguments]
    ) -> BaseInvocationGroup:
        """
        Parallelize the execution of a task with different sets of parameters.

        This method allows for concurrent execution of the same task with varying parameters.
        It accepts an iterable where each element represents a set of parameters for a separate task invocation.
        ```{note}
        These parameters can be specified in different formats:
        - As a tuple: Interpreted as positional arguments for the task.
        - As a dictionary: Interpreted as keyword arguments for the task.
        - As an `Arguments` instance: Created using `task.args(*args, **kwargs)`.
        ```

        :param Iterable[tuple | dict | Arguments] param_iter: An iterable of parameters for each call.
            Each element in the iterable is used to invoke the task separately.

        :return: A group of task invocations, allowing the task to be run in parallel with different parameters.
            The type of group (synchronous or distributed) depends on the application's configuration.

        ```{important}
        Depending on the configuration, this method creates a group of either synchronous or distributed invocations.
        In development mode, where `dev_mode_force_sync_tasks` is enabled, it creates synchronous invocations.
        Otherwise, it creates distributed invocations for parallel processing.
        ```

        ### Examples
        ```{code-block} python
        app = Pynenc()

        @app.task
        def add(x: int, y: int) -> int:
            return x + y

        # Example usage of parallelize
        invocation_group = add.parallelize([(1, 1), add.args(1, 2), {"x": 2, "y": 3}])
        print(list(invocation_group.results))
        # prints [2, 3, 5]
        ```
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
