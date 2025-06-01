from __future__ import annotations

import importlib
import json
import time
from collections.abc import Iterable
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Generic, overload

from pynenc import context
from pynenc.arguments import Arguments
from pynenc.call import Call, PreSerializedCall
from pynenc.conf.config_task import ConcurrencyControlType, ConfigTask
from pynenc.exceptions import InvalidTaskOptionsError, RetryError
from pynenc.invocation.base_invocation import BaseInvocation, BaseInvocationGroup
from pynenc.invocation.conc_invocation import (
    ConcurrentInvocation,
    ConcurrentInvocationGroup,
)
from pynenc.invocation.dist_invocation import DistributedInvocationGroup
from pynenc.types import Func, Params, Result
from pynenc.util.log import TaskLoggerAdapter
from pynenc.workflow.context import WorkflowContext

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

    def is_main_workflow_task(self) -> bool:
        """Check if the task is the main workflow task.

        :return: True if the task is the main workflow task, False otherwise

        ```{note}
            All tasks run within a workflow, the main workflow task is just the first task in the workflow.
            To determine that, we check if the task_id of the workflow task is the same as the task_id of the current task.
        ```
        """
        return self.invocation.workflow.workflow_task_id == self.task_id

    @cached_property
    def wf(self) -> WorkflowContext:
        """
        Access workflow functionality for this task.

        Provides methods for workflow state management, deterministic operations,
        and durability features like pause/resume and continue-as-new.

        :return: A helper object with workflow functionality

        Example:
        ```python
        @app.task
        def main_wf_task(data: dict) -> str:
            # Save workflow state
            state = main_wf_task.wf.get_state({"step": 0})

            # Use deterministic random
            if main_wf_task.wf.random() > 0.5:
                state["path"] = "A"
            else:
                state["path"] = "B"

            # Save updated state
            main_wf_task.wf.save_state(state)

            # Conditionally pause workflow
            if needs_human_approval(data):
                main_wf_task.wf.pause("Waiting for approval")

            return f"Completed via path {state['path']}"
        ```
        """
        return WorkflowContext(self)

    def to_json(self) -> str:
        """:return: The serialized task"""
        return json.dumps(
            {"task_id": self.task_id, "options": self.conf.options_to_json()}
        )

    def __getstate__(self) -> dict:
        # Return state as a dictionary and a secondary value as a tuple
        return {
            "app": self.app,
            "task_json": self.to_json(),
        }

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
    def _get_from_task_id(task_id: str) -> Task | Callable:
        try:
            module_name, function_name = task_id.rsplit(".", 1)
        except ValueError as e:
            raise ValueError(
                f"Invalid task_id format: {task_id}. Expected format: module_name.function_name"
            ) from e
        module = importlib.import_module(module_name)
        return getattr(module, function_name)

    @staticmethod
    def _from_json(serialized: str) -> tuple[str, Func, dict[str, Any]]:
        """:return: a function and options from a serialized task"""
        task_dict = json.loads(serialized)
        task_id = task_dict["task_id"]
        function = Task._get_from_task_id(task_id)
        options = ConfigTask.options_from_json(task_dict["options"])
        # Check if the function is a Task (from @task) or a plain function (from @direct_task)
        if isinstance(function, Task):
            return task_id, function.func, options
        # For direct_task, return the function itself
        return task_id, function.__inner_function__, options  # type: ignore

    @classmethod
    def from_json(cls, app: Pynenc, serialized: str) -> Task:
        """:return: a new task from a serialized task"""
        _, func, options = cls._from_json(serialized)
        return cls(app, func, options)

    @classmethod
    def from_id(cls, app: Pynenc, task_id: str) -> Task:
        """:return: a new task from a task ID"""
        function = Task._get_from_task_id(task_id)
        if isinstance(function, Task):
            return function
        raise ValueError("Cannot reference direct_task by ID")

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
            return ConcurrentInvocation(call=Call(self, arguments))
        call = Call(self, arguments)
        return self.app.orchestrator.route_call(call)

    @overload
    def parallelize(
        self,
        param_iter: Iterable[tuple | dict | Arguments],
        common_args: None = None,
    ) -> BaseInvocationGroup:
        ...

    @overload
    def parallelize(
        self,
        param_iter: Iterable[dict],
        common_args: dict,
    ) -> BaseInvocationGroup:
        ...

    def parallelize(
        self,
        param_iter: Iterable[tuple | dict | Arguments],
        common_args: dict | None = None,
    ) -> BaseInvocationGroup:
        """
        Parallelize the execution of a task with different sets of parameters.

        This method allows for concurrent execution of the same task with varying parameters.
        It accepts an iterable where each element represents a set of parameters for a separate task invocation.
        When `common_args` is provided, `param_iter` must be an iterable of dictionaries, and common arguments
        are pre-serialized for efficiency.
        ```{note}
        Without common_args param_iter can be specified in different formats:
        - As a tuple: Interpreted as positional arguments for the task.
        - As a dictionary: Interpreted as keyword arguments for the task.
        - As an `Arguments` instance: Created using `task.args(*args, **kwargs)`.
        ```
        ```{important}
        common_args is intended for optimize parallelization of huge arguments that will be cached by the arg_cache.
        if the arguments are small or the arg_cache is disabled, it will not provide any major improvement.
        However, for big arguments, it will provide massive time and memory improvements.
        ```


        :param Iterable[tuple | dict | Arguments] param_iter: An iterable of parameters for each call.
            Each element in the iterable is used to invoke the task separately.

        :param common_args: Optional dictionary of common arguments to pre-serialize and share across calls.

        :return: A group of task invocations, allowing the task to be run in parallel with different parameters.
            The type of group (synchronous or distributed) depends on the application's configuration.

        ```{important}
        Depending on the configuration, this method creates a group of either synchronous or distributed invocations.
        In development mode, where `dev_mode_force_sync_tasks` is enabled, it creates synchronous invocations.
        Otherwise, it creates distributed invocations for parallel processing.
        ```

        ### Examples
        Parallelization with tuples, dicts and arguments:
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

        Parallelization with common_args:
        ```python
        @app.task(registration_concurrency=ConcurrencyControlType.DISABLED)
        def process(large_data: str, index: int) -> int:
            return len(large_data) + index

        # With common_args
        common = {"large_data": "huge_string"}
        params = [{"index": i} for i in range(3)]
        invocation_group = process.parallelize(params, common)
        print(list(invocation_group.results))  # [len("huge_string") + i for i in range(3)]
        ```
        """
        self.logger.info(f"parallelizing {self.task_id}")

        # Convert param_iter to a list to allow multiple iterations and length checking
        param_list = list(param_iter)

        if common_args is not None and not all(
            isinstance(p, dict) for p in param_list or {}
        ):
            raise ValueError(
                "When using common_args, param_iter must contain only dictionaries"
            )

        # Choose distribution strategy based on whether batch processing is available
        if can_batch_process(self, len(param_list)):
            return distribute_batch_calls(self, param_list, common_args)
        return distribute_calls(self, param_list, common_args)


def can_batch_process(task: Task, num_calls: int) -> bool:
    """
    Determine if a task can be processed in batches.

    A task can be batch processed when:
    1. The application is not in development mode
    2. Registration concurrency is disabled
    3. There are multiple calls to process
    4. The task has a valid parallel_batch_size

    :param Task task: The task to check
    :param int num_calls: The number of calls to process
    :return: True if the task can be batch processed, False otherwise
    """
    return (
        not task.app.conf.dev_mode_force_sync_tasks
        and task.conf.registration_concurrency == ConcurrencyControlType.DISABLED
        and num_calls > 1
        and task.conf.parallel_batch_size > 0
    )


def prepare_arguments(
    task: Task,
    param_iter: Iterable[tuple | dict | Arguments],
    common_args: dict | None = None,
) -> list[Arguments]:
    """
    Convert various parameter formats to a list of Arguments objects,
    merging with common_args when provided.

    :param Task task: The task to prepare arguments for
    :param Iterable[tuple | dict | Arguments] param_iter: Iterable of parameters
    :param dict | None common_args: Optional common arguments to merge with each parameter
    :return: A list of Arguments objects with common_args merged in
    """
    result = []

    for params in param_iter:
        if common_args and not isinstance(params, dict):
            raise ValueError(
                "common_args can only be used with an iterable of dictionaries"
            )
        if isinstance(params, tuple):
            args_obj = task.args(*params)
        elif isinstance(params, dict):
            if common_args:
                # Create a new dict with common_args as base, then update with specific params
                merged_kwargs = common_args.copy()
                merged_kwargs.update(params)
                args_obj = task.args(**merged_kwargs)
            else:
                args_obj = task.args(**params)
        else:
            args_obj = params

        result.append(args_obj)

    return result


def distribute_calls(
    task: Task,
    param_list: list[tuple | dict | Arguments],
    common_args: dict | None = None,
) -> BaseInvocationGroup:
    """
    Distribute calls individually without batch processing.

    :param Task task: The task to process
    :param list[tuple | dict | Arguments] param_list: List of parameters
    :param dict | None common_args: Optional common arguments to merge with each parameter
    :return: A list of created invocations
    """
    # Prepare arguments with common_args merged in
    all_args = prepare_arguments(task, param_list, common_args)

    # Standard processing - distribute calls normally
    invocations = []
    for args in all_args:
        invocation = task._call(args)
        if invocation:
            invocations.append(invocation)

    group_cls: type[BaseInvocationGroup]
    if task.app.conf.dev_mode_force_sync_tasks:
        group_cls = ConcurrentInvocationGroup
    else:
        group_cls = DistributedInvocationGroup

    return group_cls(task, invocations)


def distribute_batch_calls(
    task: Task[Params, Result],
    param_list: list[tuple | dict | Arguments],
    common_args: dict | None = None,
) -> DistributedInvocationGroup:
    """
    Process a list of parameters in batches using PreSerializedCall.
    Handles pre-serialization of common arguments for efficient distribution.

    :param Task task: The task to process
    :param list[tuple | dict | Arguments] param_list: The arguments to process
    :param dict | None common_args: Optional common arguments to be pre-serialized once
    :return: An invocation group for the distributed calls
    """
    # Pre-serialize common arguments if provided
    pre_serialized_args = {}
    other_args: list[dict[str, Any]] = []
    if common_args:
        task.logger.info("Pre-serializing common arguments for batch parallelization")
        pre_serialized_args = {
            k: task.app.arg_cache.serialize(v) for k, v in common_args.items()
        }
        task.logger.debug(f"Pre-serialized {len(pre_serialized_args)} common arguments")
        other_args = param_list  # type: ignore
    else:
        other_args = [a.kwargs for a in prepare_arguments(task, param_list)]

    invocations = []
    batch_size = task.conf.parallel_batch_size

    task.logger.info(f"Processing {len(other_args)} calls in batches of {batch_size}")
    start_time = time.time()

    # Process in batches
    for i in range(0, len(other_args), batch_size):
        batch_args = other_args[i : i + batch_size]
        batch_calls = [
            PreSerializedCall(
                task, other_args=args, pre_serialized_args=pre_serialized_args
            )
            for args in batch_args
        ]
        batch_invocations = task.app.orchestrator.route_calls(batch_calls)
        invocations.extend(batch_invocations)
        if i + batch_size < len(other_args):
            task.logger.info(
                f"Processed batch {i//batch_size + 1}, "
                f"{len(invocations)}/{len(other_args)} invocations"
            )

    elapsed = time.time() - start_time
    task.logger.info(f"Batch processed {len(invocations)} calls in {elapsed:.2f}s")

    return DistributedInvocationGroup(task, invocations)
