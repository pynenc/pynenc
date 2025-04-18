from typing import Any, Literal, Optional, Union

from pynenc import Pynenc
from pynenc.conf.config_pynenc import ArgumentPrintMode
from pynenc.conf.config_task import ConcurrencyControlType


class PynencBuilder:
    """
    A builder pattern implementation for creating and configuring Pynenc applications.

    This builder simplifies the configuration process by providing intuitive method chaining
    to set up various components of a Pynenc application, including Redis connections,
    runners, serializers, and performance tuning parameters.

    :example:
    ```python
    # Create a Pynenc application with Redis and MultiThreadRunner
    pynenc_app = (
        PynencBuilder()
        .app_id("my_application")
        .serializer("pickle")
        .redis(url="redis://localhost:6379/14")
        .multi_thread_runner(min_threads=1, max_threads=4)
        .logging_level("info")
        .build()
    )
    ```
    """

    _VALID_LOG_LEVELS = ["debug", "info", "warning", "error", "critical"]
    _SERIALIZER_MAP = {
        "json": "JsonSerializer",
        "pickle": "PickleSerializer",
    }
    _MEMORY_ARG_CACHE = "MemArgCache"
    _REDIS_ARG_CACHE = "RedisArgCache"
    _DISABLED_ARG_CACHE = "DisabledArgCache"
    _MEMORY_TRIGGER = "MemTrigger"
    _REDIS_TRIGGER = "RedisTrigger"
    _DISABLED_TRIGGER = "DisabledTrigger"
    _VALID_CONCURRENCY_MODES = {"DISABLED", "TASK", "ARGUMENTS", "KEYS"}

    def __init__(self) -> None:
        """
        Initialize a new PynencBuilder with an empty configuration dictionary.
        """
        self._config: dict[str, Any] = {}
        self._using_memory_components = False
        self._using_redis_components = False

    def app_id(self, app_id: str) -> "PynencBuilder":
        """
        Set the application ID for the Pynenc application.

        The application ID uniquely identifies this Pynenc application instance
        and is used in logging, monitoring, and component configuration.

        :param str app_id:
            The unique identifier for this application.

        :return: The builder instance for method chaining.
        """
        self._config["app_id"] = app_id
        return self

    def redis(self, url: str | None = None, db: int | None = None) -> "PynencBuilder":
        """
        Configure Redis components for the Pynenc application.

        This sets up all Redis-related components (orchestrator, broker, state backend,
        and argument cache) to use Redis as their backend.

        :param str url:
            The Redis URL to connect to.
        :param Optional[int] db:
            The Redis database number to use. If provided, it will be appended to the URL.

        :return: The builder instance for method chaining.
        """
        if url or db:
            self._config["redis_url"] = f"{url}/{db}" if db else url
        self._config.update(
            {
                "orchestrator_cls": "RedisOrchestrator",
                "broker_cls": "RedisBroker",
                "state_backend_cls": "RedisStateBackend",
                "arg_cache_cls": self._REDIS_ARG_CACHE,
                "trigger_cls": self._REDIS_TRIGGER,
            }
        )
        self._using_redis_components = True
        self._using_memory_components = False
        return self

    def memory(self) -> "PynencBuilder":
        """
        Configure in-memory components for the Pynenc application.

        This sets up all components (orchestrator, broker, state backend,
        and argument cache) to use in-memory backends. This is primarily
        for testing and development purposes.

        Note: In-memory components are only compatible with certain runners.

        :return: The builder instance for method chaining.
        """
        self._config.update(
            {
                "orchestrator_cls": "MemOrchestrator",
                "broker_cls": "MemBroker",
                "state_backend_cls": "MemStateBackend",
                "arg_cache_cls": self._MEMORY_ARG_CACHE,
                "trigger_cls": self._MEMORY_TRIGGER,
            }
        )
        self._using_memory_components = True
        self._using_redis_components = False
        return self

    def arg_cache(
        self,
        mode: Literal["redis", "memory", "disabled"] = "redis",
        min_size_to_cache: int = 1024,  # Default from ConfigArgCache
        local_cache_size: int = 1024,  # Default from ConfigArgCache
    ) -> "PynencBuilder":
        """
        Configure argument caching behavior.

        :param mode:
            "redis": Use Redis for argument caching (requires redis() to be called)
            "memory": Use in-memory argument caching (for testing/development)
            "disabled": Disable argument caching completely
        :param int min_size_to_cache:
            Minimum string length (in characters) required to cache an argument.
            Arguments smaller than this size will be passed directly.
            Default is 1024 characters (roughly 1KB).
        :param int local_cache_size:
            Maximum number of items to cache locally.
            Default is 1024.

        :return: The builder instance for method chaining.
        :raises ValueError: If "redis" mode is selected without prior redis() configuration
        """
        if mode == "disabled":
            self._config["arg_cache_cls"] = self._DISABLED_ARG_CACHE
        elif mode == "memory":
            self._config["arg_cache_cls"] = self._MEMORY_ARG_CACHE
            self._using_memory_components = True
        elif mode == "redis":
            self._config["arg_cache_cls"] = self._REDIS_ARG_CACHE
            if not self._using_redis_components and "redis_url" not in self._config:
                raise ValueError(
                    "Redis arg cache requires redis configuration. Call redis() first."
                )

        # Set the cache configuration values
        self._config["min_size_to_cache"] = min_size_to_cache
        self._config["local_cache_size"] = local_cache_size

        return self

    def trigger(
        self,
        mode: Literal["redis", "memory", "disabled"] = "redis",
        scheduler_interval_seconds: int = 60,  # Default from ConfigTrigger
        enable_scheduler: bool = True,  # Default from ConfigTrigger
    ) -> "PynencBuilder":
        """
        Configure trigger behavior.

        :param mode:
            "redis": Use Redis for trigger system (requires redis() to be called)
            "memory": Use in-memory trigger system (for testing/development)
            "disabled": Disable trigger functionality completely
        :param int scheduler_interval_seconds:
            Interval in seconds for the scheduler to check for time-based triggers.
            Default is 60 seconds (1 minute).
        :param bool enable_scheduler:
            Whether to enable the scheduler for time-based triggers.
            Default is True.

        :return: The builder instance for method chaining.
        :raises ValueError: If "redis" mode is selected without prior redis() configuration
        """
        if mode == "disabled":
            self._config["trigger_cls"] = self._DISABLED_TRIGGER
        elif mode == "memory":
            self._config["trigger_cls"] = self._MEMORY_TRIGGER
            self._using_memory_components = True
        elif mode == "redis":
            self._config["trigger_cls"] = self._REDIS_TRIGGER
            if not self._using_redis_components and "redis_url" not in self._config:
                raise ValueError(
                    "Redis trigger requires redis configuration. Call redis() first."
                )

        # Set the trigger configuration values
        self._config["scheduler_interval_seconds"] = scheduler_interval_seconds
        self._config["enable_scheduler"] = enable_scheduler

        return self

    def multi_thread_runner(
        self,
        min_threads: int = 1,
        max_threads: int = 1,
        enforce_max_processes: bool = False,
    ) -> "PynencBuilder":
        """
        Configure the MultiThreadRunner for concurrent task execution.

        The MultiThreadRunner uses threads to execute tasks concurrently within
        the same process, providing efficient parallel execution with shared memory.

        :param int min_threads:
            The minimum number of threads to keep in the thread pool.
        :param int max_threads:
            The maximum number of threads allowed in the thread pool.
        :param bool enforce_max_processes:
            If True, enforces the maximum number of processes that can run concurrently.

        :return: The builder instance for method chaining.
        """
        self._config["runner_cls"] = "MultiThreadRunner"
        self._config.update(
            {
                "min_threads": min_threads,
                "max_threads": max_threads,
                "enforce_max_processes": enforce_max_processes,
            }
        )
        return self

    def persistent_process_runner(self, num_processes: int = 0) -> "PynencBuilder":
        """
        Configure the PersistentProcessRunner for concurrent task execution.

        The PersistentProcessRunner maintains a pool of persistent processes
        for task execution, providing true parallel execution across multiple
        CPU cores, with isolated memory spaces.

        :param int num_processes:
            The number of processes to create in the process pool.
            Default 0 will use the number of CPU cores.

        :return: The builder instance for method chaining.
        """
        self._config["runner_cls"] = "PersistentProcessRunner"
        self._config["num_processes"] = num_processes
        return self

    def thread_runner(
        self, min_threads: int = 1, max_threads: int = 0
    ) -> "PynencBuilder":
        """
        Configure the ThreadRunner for task execution.

        The ThreadRunner uses a thread pool to execute tasks concurrently within
        the same process, providing efficient parallel execution with shared memory.

        :param int min_threads:
            The minimum number of threads to keep in the thread pool.
        :param int max_threads:
            The maximum number of threads allowed in the thread pool.
            Default 0 will use the number of CPU cores.

        :return: The builder instance for method chaining.
        """
        self._config["runner_cls"] = "ThreadRunner"
        self._config.update(
            {
                "min_threads": min_threads,
                "max_threads": max_threads,
            }
        )
        return self

    def process_runner(self) -> "PynencBuilder":
        """
        Configure the ProcessRunner for task execution.

        The ProcessRunner creates a new process for each task execution,
        providing isolated execution context for each task.

        :return: The builder instance for method chaining.
        """
        self._config["runner_cls"] = "ProcessRunner"
        return self

    def dummy_runner(self) -> "PynencBuilder":
        """
        Configure the DummyRunner for task execution.

        The DummyRunner executes tasks in the main thread of the application.
        This is useful for testing and debugging purposes.

        :return: The builder instance for method chaining.
        """
        self._config["runner_cls"] = "DummyRunner"
        return self

    def dev_mode(self, force_sync_tasks: bool = True) -> "PynencBuilder":
        """
        Enable development mode for easier debugging.

        In development mode, tasks can be forced to run synchronously,
        making debugging and testing easier.

        :param bool force_sync_tasks:
            If True, forces all tasks to run synchronously in the same process.

        :return: The builder instance for method chaining.
        """
        self._config["dev_mode_force_sync_tasks"] = force_sync_tasks
        return self

    def logging_level(self, level: str) -> "PynencBuilder":
        """
        Set the logging level for the application.

        :param str level:
            The logging level to use ("debug", "info", "warning", "error", "critical").

        :return: The builder instance for method chaining.
        :raises: ValueError if an invalid logging level is provided
        """
        level = level.lower()
        if level not in self._VALID_LOG_LEVELS:
            raise ValueError(
                f"Invalid logging level: {level}. Valid options are: {', '.join(self._VALID_LOG_LEVELS)}"
            )
        self._config["logging_level"] = level
        return self

    def runner_tuning(
        self,
        runner_loop_sleep_time_sec: float = 0.01,
        invocation_wait_results_sleep_time_sec: float = 0.01,
        min_parallel_slots: int = 1,
    ) -> "PynencBuilder":
        """
        Configure runner performance tuning parameters.

        These parameters control various aspects of the runner behavior,
        allowing for fine-tuning of performance characteristics.

        :param float runner_loop_sleep_time_sec:
            Sleep time between runner loop iterations in seconds.
        :param float invocation_wait_results_sleep_time_sec:
            Sleep time when waiting for invocation results in seconds.
        :param int min_parallel_slots:
            Minimum number of parallel execution slots for tasks.

        :return: The builder instance for method chaining.
        """
        self._config.update(
            {
                "runner_loop_sleep_time_sec": runner_loop_sleep_time_sec,
                "invocation_wait_results_sleep_time_sec": invocation_wait_results_sleep_time_sec,
                "min_parallel_slots": min_parallel_slots,
            }
        )
        return self

    def task_control(
        self,
        cycle_control: bool = False,
        blocking_control: bool = False,
        queue_timeout_sec: float = 0.1,
    ) -> "PynencBuilder":
        """
        Configure task control parameters.

        These parameters control various aspects of task dependency management
        and execution control.

        :param bool cycle_control:
            Whether to enable cycle control for task dependencies.
        :param bool blocking_control:
            Whether to enable blocking control for concurrent tasks.
        :param float queue_timeout_sec:
            Timeout for queue operations in seconds.

        :return: The builder instance for method chaining.
        """
        self._config.update(
            {
                "cycle_control": cycle_control,
                "blocking_control": blocking_control,
                "queue_timeout_sec": queue_timeout_sec,
            }
        )
        return self

    def serializer(self, serializer: str) -> "PynencBuilder":
        """
        Configure the serializer for task arguments and results.

        The serializer is responsible for converting Python objects to and from
        a serialized format for storage and transmission.

        :param str serializer:
            The serializer to use. Can be a shortname ("json", "pickle") or
            the full class name (e.g., "JsonSerializer", "PickleSerializer").

        :return: The builder instance for method chaining.
        :raises: ValueError if an invalid serializer name is provided
        """
        if serializer.lower() in self._SERIALIZER_MAP:
            cls_name = self._SERIALIZER_MAP[serializer.lower()]
        elif serializer.endswith("Serializer"):
            cls_name = serializer
        else:
            raise ValueError(
                f"Invalid serializer: {serializer}. Valid options are: {', '.join(self._SERIALIZER_MAP.keys())} "
                f"or a full class name ending with 'Serializer'"
            )
        self._config["serializer_cls"] = cls_name
        return self

    def concurrency_control(
        self,
        running_concurrency: Optional[Union[str, ConcurrencyControlType]] = None,
        registration_concurrency: Optional[Union[str, ConcurrencyControlType]] = None,
    ) -> "PynencBuilder":
        """
        Configure concurrency control default behaviors for all tasks.
        A task can override these settings by specifying the concurrency control
        mode in the task decorator.

        Concurrency control determines how tasks are scheduled and executed when
        multiple instances of the same task are invoked concurrently.

        :param Optional[Union[str, ConcurrencyControlType]] running_concurrency:
            Controls the concurrency behavior of tasks at runtime. Can be a string
            ("DISABLED", "TASK", "ARGUMENTS", "KEYS") or a ConcurrencyControlType enum value.
        :param Optional[Union[str, ConcurrencyControlType]] registration_concurrency:
            Controls the concurrency behavior of tasks at registration time. Can be a string
            ("DISABLED", "TASK", "ARGUMENTS", "KEYS") or a ConcurrencyControlType enum value.

        :return: The builder instance for method chaining.
        """
        if running_concurrency is not None:
            if isinstance(running_concurrency, ConcurrencyControlType):
                self._config["running_concurrency"] = running_concurrency
            else:
                mode_upper = running_concurrency.upper()
                self._config["running_concurrency"] = ConcurrencyControlType[mode_upper]

        if registration_concurrency is not None:
            if isinstance(registration_concurrency, ConcurrencyControlType):
                self._config["registration_concurrency"] = registration_concurrency
            else:
                mode_upper = registration_concurrency.upper()
                self._config["registration_concurrency"] = ConcurrencyControlType[
                    mode_upper
                ]

        return self

    def max_pending_seconds(self, seconds: float) -> "PynencBuilder":
        """
        Set the maximum time a task can remain in PENDING state.

        :param float seconds:
            Maximum time in seconds a task can remain in PENDING state before it expires.

        :return: The builder instance for method chaining.
        """
        self._config["max_pending_seconds"] = seconds
        return self

    def argument_print_mode(
        self, mode: Union[str, ArgumentPrintMode], truncate_length: int = 32
    ) -> "PynencBuilder":
        """
        Configure how task arguments are printed in logs.

        :param Union[str, ArgumentPrintMode] mode:
            The print mode to use. Can be a string
            ("HIDDEN", "KEYS", "FULL", "TRUNCATED") or an ArgumentPrintMode enum value.
        :param int truncate_length:
            Maximum length for printed argument values when using TRUNCATED mode.
            Default is 32.

        :return: The builder instance for method chaining.
        """
        if isinstance(mode, ArgumentPrintMode):
            arg_print_mode = mode
        else:
            mode_upper = mode.upper()
            arg_print_mode = ArgumentPrintMode[mode_upper]

        self._config["argument_print_mode"] = arg_print_mode

        # Configure related settings based on the mode
        if arg_print_mode == ArgumentPrintMode.HIDDEN:
            self._config["print_arguments"] = False
        else:
            self._config["print_arguments"] = True

        if arg_print_mode == ArgumentPrintMode.TRUNCATED:
            if truncate_length <= 0:
                raise ValueError("truncate_length must be greater than 0")
            self._config["truncate_arguments_length"] = truncate_length

        return self

    def hide_arguments(self) -> "PynencBuilder":
        """
        Configure logs to hide all task arguments.

        Sets print_arguments=False, resulting in "<arguments hidden>" in logs.

        :return: The builder instance for method chaining.
        """
        return self.argument_print_mode(ArgumentPrintMode.HIDDEN)

    def show_argument_keys(self) -> "PynencBuilder":
        """
        Configure logs to show only argument names.

        Results in "args(key1, key2)" in logs.

        :return: The builder instance for method chaining.
        """
        return self.argument_print_mode(ArgumentPrintMode.KEYS)

    def show_full_arguments(self) -> "PynencBuilder":
        """
        Configure logs to show complete argument values without truncation.

        Results in "args(key1=value1, key2=value2)" in logs.

        :return: The builder instance for method chaining.
        """
        return self.argument_print_mode(ArgumentPrintMode.FULL)

    def show_truncated_arguments(self, truncate_length: int = 32) -> "PynencBuilder":
        """
        Configure logs to show truncated argument values.

        Results in "args(key1=trunc_value1, key2=trunc_value2)" in logs, with truncation
        based on the specified length.

        :param int truncate_length:
            Maximum length for printed argument values. Must be greater than 0.
            Default is 32.
        :return: The builder instance for method chaining.
        """
        return self.argument_print_mode(ArgumentPrintMode.TRUNCATED, truncate_length)

    def custom_config(self, **kwargs: Any) -> "PynencBuilder":
        """
        Add arbitrary configuration values.

        This method allows adding any custom configuration values that are not
        covered by the specialized methods.

        For common configuration values, prefer using the dedicated methods
        (like app_id(), logging_level(), etc.) instead of this generic method.

        :param Any kwargs:
            Custom configuration values to add to the configuration.

        :return: The builder instance for method chaining.
        """
        self._config.update(kwargs)
        return self

    def _validate_memory_compatibility(self) -> None:
        """
        Validate that the selected runner is compatible with memory components if they are being used.

        :raises: ValueError if memory components are used with an incompatible runner
        """
        if not self._using_memory_components:
            return
        runner_cls = self._config.get("runner_cls")
        if not runner_cls:
            return
        memory_compatible_runners = ["DummyRunner", "ThreadRunner"]
        if runner_cls not in memory_compatible_runners:
            raise ValueError(
                f"Runner '{runner_cls}' is not compatible with in-memory components. "
                f"Use one of these runners instead: {', '.join(memory_compatible_runners)} "
                f"or configure Redis components using the redis() method."
            )

    def build(self) -> Pynenc:
        """
        Build and return a configured Pynenc instance.

        This method creates a new Pynenc instance using the configuration
        values that have been set through the builder methods.

        :return: A configured Pynenc instance ready for use.
        :raises: ValueError if the configuration is invalid
        """
        self._validate_memory_compatibility()
        return Pynenc(config_values=self._config)
