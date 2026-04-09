import importlib
import json
from enum import StrEnum, auto
from typing import Any, TypeVar, TYPE_CHECKING

from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase
from pynenc.conf.constants import ENV_PREFIX, ENV_SEPARATOR
from pynenc.exceptions import RetryError

if TYPE_CHECKING:
    from cistell.root import ConfigRoot

    from pynenc.identifiers.task_id import TaskId


class ConcurrencyControlType(StrEnum):
    """
    Type of concurrency control.

    :cvar DISABLED:
        Concurrency control is disabled. This means there are no concurrency checks.
    :cvar TASK:
        Concurrency is checked per task.
        Only one instance of each task can be in the determined state at a time.
    :cvar ARGUMENTS:
        Concurrency is checked for each task's arguments.
        Only one task with the same arguments can be in the determined state at a time.
    :cvar KEYS:
        Concurrency is checked for each task's key arguments.
        Only one task with the same key arguments can be in the determined state at a time.
    """

    DISABLED = auto()
    TASK = auto()
    ARGUMENTS = auto()
    KEYS = auto()


DEFAULT_KEY_ARGS: ConfigField[tuple[str, ...]] = ConfigField(())


class TaskOptionsJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, type) and issubclass(obj, Exception):
            return f"{obj.__module__}.{obj.__name__}"
        if isinstance(obj, StrEnum):
            return obj.value
        return super().default(obj)


def options_deserializer(serialized_pairs: list[tuple[Any, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in serialized_pairs:
        if key == "retry_for":
            result[key] = exception_mapper(value)
        elif key in ("running_concurrency", "registration_concurrency"):
            result[key] = ConcurrencyControlType(value)
        elif key == "key_arguments":
            result[key] = tuple(value)
        elif key == "disable_cache_args":
            result[key] = tuple(value)
        else:
            result[key] = value
    return result


def exception_mapper(
    value: list[str] | list[type[Exception]],
) -> tuple[type[Exception], ...]:
    exceptions = []
    for _exception in value:
        if isinstance(_exception, str):
            try:
                module_name, class_name = _exception.rsplit(".", 1)
                module = importlib.import_module(module_name)
                exception_class = getattr(module, class_name)
                if not issubclass(exception_class, Exception):
                    raise TypeError(
                        f"Expected a subclass of Exception, got {exception_class}"
                    )
            except (AttributeError, ModuleNotFoundError) as ex:
                raise TypeError(f"Invalid exception class: {_exception}") from ex
            exceptions.append(exception_class)
        elif isinstance(_exception, type) and issubclass(_exception, Exception):
            exceptions.append(_exception)
        else:
            raise TypeError(
                f"Expected str or Exception subclass, got {type(_exception).__name__}"
            )
    return tuple(exceptions)


T = TypeVar("T")


def exception_config_mapper(value: list[str], expected_type: type[T]) -> T:
    exceptions = exception_mapper(value)
    if not isinstance(exceptions, expected_type):
        raise TypeError(f"Expected {expected_type}, got {type(exceptions)}")
    return exceptions


class ConfigTask(ConfigPynencBase):
    """
    Provides task-specific configuration settings for the distributed task system.

    This subclass of `ConfigPynencBase` adds task-level configuration options, allowing for
    fine-grained control over the behavior of individual tasks. Configuration can be
    specified globally for all tasks, or individually for each task using environment
    variables, configuration files, or the `@task` decorator.

    :cvar ConfigField[int] parallel_batch_size:
        If set to 100, when parallelizing a task, we will route batches of 100 tasks.
        0 means that each parallel task will be routed individually.

    :cvar ConfigField[tuple] retry_for:
        A tuple of exceptions for which the task should be retried.

    :cvar ConfigField[int] max_retries:
        Defines the maximum number of retries for a task. This limit ensures that a task does
        not retry indefinitely.

    :cvar ConfigField[ConcurrencyControlType] running_concurrency:
        Controls the concurrency behavior of the task. This option prevents the task from being
        in a running state, managing and limiting concurrent execution of the same task.

    :cvar ConfigField[ConcurrencyControlType] registration_concurrency:
        Manages the registration concurrency for the task, ensuring unique task registration
        based on the configuration. Useful for tasks that should not execute multiple times in
        parallel or to avoid generating too much unnecessary tasks in the system.

    :cvar ConfigField[str] key_arguments:
        Specifies key arguments for concurrency control, relevant when concurrency control is
        set to key-based. This option determines which arguments are used to identify unique
        task invocations.

    :cvar ConfigField[bool] on_diff_non_key_args_raise:
        If set to True, raises an exception when a task invocation with matching key arguments
        but different non-key arguments is encountered. This option is used to handle
        concurrency at the key level.

    :cvar ConfigField[bool] call_result_cache:
        If set to True, enables caching for the task. This option is useful for tasks that
        perform expensive computations and can benefit from caching results.

    :cvar ConfigField[tuple[str, ...]] disable_cache_args:
        Specifies arguments to exclude from caching. This option is useful for tasks that
        should not cache results based on certain arguments.
        It can be set to `("*",)` to disable caching for all arguments.

    :cvar ConfigField[bool] force_new_workflow:
        If True, this task will always create a new workflow when invoked.
        Even when called from within another workflow, it creates a subworkflow
        that maintains a reference to its parent workflow.

    :cvar ConfigField[bool] reroute_on_concurrency_control:
        If True, tasks blocked by concurrency control will be automatically rerouted.
        If False (default), they will be marked as CONCURRENCY_CONTROLLED_FINAL and never run.

        ```{warning}
        Setting this to True can cause system saturation if tasks are repeatedly triggered
        (e.g., by cron jobs) while a running instance blocks new invocations. The blocked
        tasks will continuously reroute, creating an ever-growing queue of pending work.
        Only enable this if you have safeguards against unbounded task accumulation.
        ```

    Examples
    --------
    Using environment variables to configure tasks:

    .. code-block:: python

        # Set global auto parallel batch size
        os.environ["PYNENC__CONFIGTASK__PARALLEL_BATCH_SIZE"] = "2"

        # Set auto parallel batch size specifically for 'my_module.my_task'
        os.environ["PYNENC__CONFIGTASK__MY_MODULE#MY_TASK__PARALLEL_BATCH_SIZE"] = "3"

    Loading configuration from a YAML file:

    .. code-block:: yaml

        task:
            parallel_batch_size: 4
            max_retries: 10
            module_name.task_name:
                max_retries: 5

    .. code-block:: python

        # Create and load a config file for tasks
        config = ConfigTask(task_id="my_module.my_task", config_filepath="path/to/config.yaml")

    .. note::

        - When specifying task-specific settings using environment variables, the separator
          between the module name and task name is `#`, not `__`. For example, use
          `MY_MODULE#MY_TASK__AUTO_PARALLEL` to specify the task-specific setting.

    The above examples demonstrate how to configure tasks both globally and on a per-task basis,
    offering flexibility and precise control over the behavior of tasks in the system.
    """

    parallel_batch_size = ConfigField(100)
    retry_for = ConfigField((RetryError,), mapper=exception_config_mapper)
    max_retries = ConfigField(0)
    running_concurrency = ConfigField(ConcurrencyControlType.DISABLED)
    registration_concurrency = ConfigField(ConcurrencyControlType.DISABLED)
    key_arguments = DEFAULT_KEY_ARGS
    on_diff_non_key_args_raise = ConfigField(False)
    call_result_cache = ConfigField(False)
    disable_cache_args: ConfigField[tuple[str, ...]] = ConfigField(())
    force_new_workflow = ConfigField(False)
    reroute_on_concurrency_control = ConfigField(False)

    def __init__(
        self,
        task_id: "TaskId",
        config_values: dict[str, Any] | None = None,
        config_filepath: str | None = None,
        task_options: dict[str, Any] | None = None,
    ) -> None:
        self.task_id = task_id
        config_values = dict(config_values or {})
        self.task_options = task_options or {}
        if task_options:
            config_values.update(task_options)
        super().__init__(config_values, config_filepath)

    def options_to_json(self) -> str:
        """:return: the serialized options"""
        return json.dumps(self.task_options, cls=TaskOptionsJSONEncoder)

    @staticmethod
    def options_from_json(options_json: str) -> dict[str, Any]:
        """:return: a new options from a dictionary"""
        return json.loads(options_json, object_pairs_hook=options_deserializer)

    def get_extra_qualifiers(
        self, config_cls: type["ConfigRoot"]
    ) -> list[str] | None:
        return [self.task_id.config_key]

    def get_extra_env_keys(
        self, field_name: str, config_cls: type["ConfigRoot"]
    ) -> list[str] | None:
        task_env_key = (
            f"{ENV_PREFIX}{ENV_SEPARATOR}"
            f"{self.__class__.__name__.upper()}{ENV_SEPARATOR}"
            f"{self.task_id.module.upper()}#{self.task_id.func_name.upper()}"
            f"{ENV_SEPARATOR}{field_name.upper()}"
        )
        return [task_env_key]
