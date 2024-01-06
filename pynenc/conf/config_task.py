import importlib
import json
import os
from enum import StrEnum, auto
from typing import Any, Optional, Type, TypeVar

from ..exceptions import RetryError
from .config_base import ConfigBase, ConfigField
from .constants import ENV_PREFIX, ENV_SEP


class ConcurrencyControlType(StrEnum):
    """Type of concurrency control"""

    #: Concurrency control is disabled
    DISABLED = auto()

    #: Concurrency would be check per each task, only one task can be in the determined state at a time
    TASK = auto()

    #: Concurrency would be check for each task arguments,
    #: only one task with the same arguments can be in the determined state at a time
    ARGUMENTS = auto()

    #: Concurrency would be check for each task key arguments,
    #: only one task with the same key arguments can be in the determined state at a time
    KEYS = auto()


DEFAULT_KEY_ARGS: ConfigField[tuple[str, ...]] = ConfigField(())


class TaskOptionsJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if issubclass(obj, Exception):
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
        else:
            result[key] = value
    return result


def exception_mapper(
    value: list[str] | list[type[Exception]],
) -> tuple[type[Exception], ...]:
    exceptions = []
    for _exception in value:
        if isinstance(_exception, str):
            # Handle the case where _exception is a string
            module_name, class_name = _exception.rsplit(".", 1)
            module = importlib.import_module(module_name)
            exception_class = getattr(module, class_name)
            if not issubclass(exception_class, Exception):
                raise TypeError(
                    f"Expected a subclass of Exception, got {exception_class}"
                )
            exceptions.append(exception_class)
        elif issubclass(_exception, Exception):
            # Handle the case where _exception is already a class
            exceptions.append(_exception)
        else:
            raise TypeError(
                f"Expected str or Exception subclass, got {type(_exception)}"
            )
    return tuple(exceptions)


T = TypeVar("T")


def exception_config_mapper(value: list[str], expected_type: Type[T]) -> T:
    exceptions = exception_mapper(value)
    if not isinstance(exceptions, expected_type):
        raise TypeError(f"Expected {expected_type}, got {type(exceptions)}")
    return exceptions


class ConfigTask(ConfigBase):
    """
    Provides task-specific configuration settings for the distributed task system.

    This subclass of `ConfigBase` adds task-level configuration options, allowing for
    fine-grained control over the behavior of individual tasks. Configuration can be
    specified globally for all tasks, or individually for each task using environment
    variables, configuration files, or the `@task` decorator.

    Attributes
    ----------
    auto_parallel_batch_size : ConfigField[int]
        If set to 0, auto parallelization is disabled. If greater than 0, tasks with iterable
        arguments are automatically split into chunks, with each chunk potentially processed
        by a different worker. If the task arguments are not an iterable, nothing happens.

    retry_for : ConfigField[tuple]
        A tuple of exceptions for which the task should be retried.

    max_retries : ConfigField[int]
        Defines the maximum number of retries for a task. This limit ensures that a task does
        not retry indefinitely.

    running_concurrency : ConfigField[ConcurrencyControlType]
        Controls the concurrency behavior of the task. This option prevents the task from being
        in a running state, managing and limiting concurrent execution of the same task.

    registration_concurrency : ConfigField[ConcurrencyControlType]
        Manages the registration concurrency for the task, ensuring unique task registration
        based on the configuration. Useful for tasks that should not execute multiple times in
        parallel or to avoid generating too much unnecessary tasks in the system.

    key_arguments : ConfigField[str]
        Specifies key arguments for concurrency control, relevant when concurrency control is
        set to key-based. This option determines which arguments are used to identify unique
        task invocations.

    on_diff_non_key_args_raise : ConfigField[bool]
        If set to True, raises an exception when a task invocation with matching key arguments
        but different non-key arguments is encountered. This option is used to handle
        concurrency at the key level.

    Examples
    --------
    Using environment variables to configure tasks:

    .. code-block:: python

        # Set global auto parallel batch size
        os.environ["PYNENC__CONFIGTASK__AUTO_PARALLEL_BATCH_SIZE"] = "2"

        # Set auto parallel batch size specifically for 'my_module.my_task'
        os.environ["PYNENC__CONFIGTASK__MY_MODULE__MY_TASK__AUTO_PARALLEL_BATCH_SIZE"] = "3"

    Loading configuration from a YAML file:

    .. code-block:: yaml

        task:
            auto_parallel_batch_size: 4
            max_retries: 10
            module_name.task_name:
                max_retries: 5

    .. code-block:: python

        # Create and load a config file for tasks
        config = ConfigTask(task_id="my_module.my_task", config_filepath="path/to/config.yaml")

    The above examples demonstrate how to configure tasks both globally and on a per-task basis,
    offering flexibility and precise control over the behavior of tasks in the system.
    """

    auto_parallel_batch_size = ConfigField(0)
    retry_for = ConfigField((RetryError,), mapper=exception_config_mapper)
    max_retries = ConfigField(0)
    running_concurrency = ConfigField(ConcurrencyControlType.DISABLED)
    registration_concurrency = ConfigField(ConcurrencyControlType.DISABLED)
    key_arguments = DEFAULT_KEY_ARGS
    on_diff_non_key_args_raise = ConfigField(False)

    def __init__(
        self,
        task_id: str,
        config_values: Optional[dict[str, Any]] = None,
        config_filepath: Optional[str] = None,
        task_options: Optional[dict[str, Any]] = None,
    ) -> None:
        self.task_id = task_id
        config_values = config_values or {}
        self.task_options = task_options or {}
        if task_options:
            config_values.update(task_options)
        super().__init__(config_values, config_filepath)

    def options_to_json(self) -> str:
        """Returns a string with the serialized options"""
        return json.dumps(self.task_options, cls=TaskOptionsJSONEncoder)

    @staticmethod
    def options_from_json(options_json: str) -> dict[str, Any]:
        """Returns a new options from a dictionary"""
        return json.loads(options_json, object_pairs_hook=options_deserializer)

    # SPECIFIC CONFIG FOR TASK OPTIONS, SO IT CAN BE INCLUDED IN ENV VARS, CONFIG FILES or TASK decorator

    def init_config_value_key_from_mapping(
        self, source: str, config_id: str, key: str, mapping: dict, conf_mapping: dict
    ) -> None:
        super().init_config_value_key_from_mapping(
            source, config_id, key, mapping, conf_mapping
        )
        task_key = f"{source}##{config_id}##{self.task_id}##{key}"
        # task_id specific mapping always within task config level
        if task_key not in self._mapped_keys and self.task_id in conf_mapping:
            if key in conf_mapping[self.task_id]:
                setattr(self, key, conf_mapping[self.task_id][key])
                self._mapped_keys.add(task_key)

    def init_config_value_from_env_vars(self, config_cls: type[ConfigBase]) -> None:
        super().init_config_value_from_env_vars(config_cls)
        # specific env vars for task options
        config_key = f"{ENV_PREFIX}{ENV_SEP}{self.__class__.__name__.upper()}{ENV_SEP}"
        task_key = config_key + self.task_id.upper().replace(".", ENV_SEP)
        for key in self.config_cls_to_fields.get(config_cls.__name__, []):
            env_key = f"{task_key}{ENV_SEP}{key.upper()}"
            if env_key in os.environ:
                setattr(self, key, os.environ[env_key])
