"""
Builder pattern implementation for configuring Pynenc applications with plugin support.

This module provides a fluent, chainable builder interface that can be extended by plugins
through Python's entry points system. Plugins automatically register their builder methods
when installed, enabling seamless integration of backend-specific functionality.

Key components:
- PynencBuilder: Core builder class with plugin support
- Plugin registration system via entry points
- Dynamic method resolution for plugin-provided methods
- Validation system for plugin configurations
"""

import warnings
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Optional, Union

from pynenc.conf.config_pynenc import ArgumentPrintMode
from pynenc.conf.config_task import ConcurrencyControlType

# Entry points imports
try:
    from importlib.metadata import entry_points
except ImportError:
    entry_points = None  # type: ignore[assignment]

try:
    import pkg_resources
except ImportError:
    pkg_resources = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from pynenc import Pynenc


class PynencBuilder:
    """
    A builder pattern implementation for creating and configuring Pynenc applications.

    This builder simplifies the configuration process by providing intuitive method chaining
    to set up various components of a Pynenc application. Backend-specific methods are
    provided by installed plugins through Python's entry points system.

    The plugin system automatically discovers and registers methods from installed plugins,
    enabling backend-specific configuration methods to become available when plugins are installed.

    :example:
    ```python
    # With backend plugins installed
    pynenc_app = (
        PynencBuilder()
        .app_id("my_application")
        .serializer("pickle")
        .multi_thread_runner(min_threads=1, max_threads=4)
        .logging_level("info")
        .build()
    )

    # Memory-based configuration for development
    pynenc_app = (
        PynencBuilder()
        .app_id("my_application")
        .memory()  # Built-in memory backend
        .mem_arg_cache(min_size_to_cache=1024)
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
    _DISABLED_ARG_CACHE = "DisabledArgCache"
    _MEMORY_TRIGGER = "MemTrigger"
    _DISABLED_TRIGGER = "DisabledTrigger"
    _VALID_CONCURRENCY_MODES = {"DISABLED", "TASK", "ARGUMENTS", "KEYS"}

    # Plugin registration system
    _plugin_methods: dict[str, Callable] = {}
    _plugin_validators: list[Callable] = []
    _plugins_loaded = False

    def __init__(self) -> None:
        """
        Initialize a new PynencBuilder with plugin discovery and empty configuration.

        Automatically discovers and loads plugin methods via entry points on first instantiation.
        """
        self._config: dict[str, Any] = {}
        self._using_memory_components = False
        self._plugin_components: set[str] = set()

        # Auto-discover plugins on first builder instantiation
        self._load_plugins()

    @classmethod
    def _load_plugins(cls) -> None:
        """
        Load and register plugin methods via Python entry points system.

        Discovers all plugins registered under 'pynenc.plugins' entry point group
        and registers their builder methods for dynamic resolution.
        """
        if cls._plugins_loaded:
            return

        cls._plugins_loaded = True

        # Try to load plugins using available entry points mechanism
        if entry_points is not None:
            cls._load_plugins_importlib()
        elif pkg_resources is not None:
            cls._load_plugins_pkg_resources()
        # If neither is available, continue without plugins

    @classmethod
    def _load_plugins_importlib(cls) -> None:
        """Load plugins using importlib.metadata."""
        if entry_points is None:
            return

        try:
            # Try Python 3.10+ API first
            try:
                plugin_entries = list(entry_points(group="pynenc.plugins"))
            except TypeError:
                # Fallback to older API
                eps = entry_points()
                if hasattr(eps, "select"):
                    plugin_entries = list(eps.select(group="pynenc.plugins"))
                else:
                    # For Python 3.8-3.9, entry_points() returns a dict-like object
                    plugin_entries = list(eps.get("pynenc.plugins", []))  # type: ignore[attr-defined]

            for ep in plugin_entries:
                cls._register_plugin_from_entry_point(ep)
        except Exception:
            # Silently continue if plugin loading fails
            pass

    @classmethod
    def _load_plugins_pkg_resources(cls) -> None:
        """Load plugins using pkg_resources."""
        if pkg_resources is None:
            return

        try:
            for entry_point in pkg_resources.iter_entry_points("pynenc.plugins"):
                cls._register_plugin_from_entry_point(entry_point)
        except Exception:
            # Silently continue if plugin loading fails
            pass

    @classmethod
    def _register_plugin_from_entry_point(cls, entry_point: Any) -> None:
        """Register a single plugin from an entry point."""
        try:
            plugin_class = entry_point.load()
            # Plugins should have a register_builder_methods class method
            if hasattr(plugin_class, "register_builder_methods"):
                plugin_class.register_builder_methods(cls)
        except Exception as e:
            # Log plugin loading errors but don't fail the builder
            warnings.warn(
                f"Failed to load plugin {entry_point.name}: {e}",
                UserWarning,
                stacklevel=2,
            )

    @classmethod
    def register_plugin_method(cls, method_name: str, method_func: Callable) -> None:
        """
        Register a plugin method to be available on PynencBuilder instances.

        This allows plugins to extend the builder with their own configuration methods.
        Plugins should call this during their registration process.

        :param str method_name: Name of the method to register (e.g., 'redis', 'mongodb')
        :param Callable method_func: The method function that takes builder as first argument
        :raises ValueError: If method name conflicts with existing core methods
        """
        # Check for conflicts with core methods (but allow plugin method overrides)
        if hasattr(cls, method_name) and method_name not in cls._plugin_methods:
            raise ValueError(
                f"Cannot register plugin method '{method_name}': conflicts with core method"
            )

        cls._plugin_methods[method_name] = method_func

    @classmethod
    def register_plugin_validator(cls, validator_func: Callable) -> None:
        """
        Register a plugin validator function that validates configuration before build().

        Validators should raise ValueError if the configuration is invalid.

        :param Callable validator_func: Function that takes (config: dict) and raises ValueError if invalid
        """
        cls._plugin_validators.append(validator_func)

    def __getattr__(self, name: str) -> Callable:
        """
        Dynamic method resolution for plugin-registered methods.

        This enables plugins to add methods that are automatically
        available when the plugin is installed.

        :param str name: Method name being accessed
        :return: Bound method from plugin
        :raises AttributeError: If method not found in plugins with helpful error message
        """
        if name in self._plugin_methods:
            # Bind the plugin method to this instance
            plugin_method = self._plugin_methods[name]
            return lambda *args, **kwargs: plugin_method(self, *args, **kwargs)

        # Provide helpful error message suggesting plugin installation
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'. "
            f"This method may be provided by a plugin. Check available plugins and ensure they are installed."
        )

    def app_id(self, app_id: str) -> "PynencBuilder":
        """
        Set the application ID for the Pynenc application.

        The application ID uniquely identifies this Pynenc application instance
        and is used in logging, monitoring, and component configuration.

        :param str app_id: The unique identifier for this application
        :return: The builder instance for method chaining
        """
        self._config["app_id"] = app_id
        return self

    def memory(self) -> "PynencBuilder":
        """
        Configure in-memory components for the Pynenc application.

        This sets up all components (orchestrator, broker, state backend,
        and argument cache) to use in-memory backends. This is primarily
        for testing and development purposes.

        Note: In-memory components are only compatible with certain runners.

        :return: The builder instance for method chaining
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
        self._plugin_components.clear()  # Clear any plugin components
        return self

    def mem_arg_cache(
        self,
        min_size_to_cache: int = 1024,
        local_cache_size: int = 1024,
    ) -> "PynencBuilder":
        """
        Configure memory-based argument caching.

        This sets up in-memory argument caching for development and testing purposes.
        Arguments larger than the specified threshold will be cached locally.

        :param int min_size_to_cache: Minimum string length required to cache an argument
        :param int local_cache_size: Maximum number of items to cache locally
        :return: The builder instance for method chaining
        """
        self._config.update(
            {
                "arg_cache_cls": self._MEMORY_ARG_CACHE,
                "min_size_to_cache": min_size_to_cache,
                "local_cache_size": local_cache_size,
            }
        )
        self._using_memory_components = True
        return self

    def disable_arg_cache(self) -> "PynencBuilder":
        """
        Disable argument caching completely.

        This turns off all argument caching functionality, which may be useful
        for debugging or when caching is not desired.

        :return: The builder instance for method chaining
        """
        self._config["arg_cache_cls"] = self._DISABLED_ARG_CACHE
        return self

    def mem_trigger(
        self,
        scheduler_interval_seconds: int = 60,
        enable_scheduler: bool = True,
    ) -> "PynencBuilder":
        """
        Configure memory-based trigger system.

        This sets up in-memory triggers for development and testing purposes.
        Time-based triggers will be checked at the specified interval.

        :param int scheduler_interval_seconds: Interval for the scheduler to check time-based triggers
        :param bool enable_scheduler: Whether to enable the scheduler for time-based triggers
        :return: The builder instance for method chaining
        """
        self._config.update(
            {
                "trigger_cls": self._MEMORY_TRIGGER,
                "scheduler_interval_seconds": scheduler_interval_seconds,
                "enable_scheduler": enable_scheduler,
            }
        )
        self._using_memory_components = True
        return self

    def disable_trigger(self) -> "PynencBuilder":
        """
        Disable trigger functionality completely.

        This turns off all trigger functionality, preventing any scheduled or
        event-driven task execution.

        :return: The builder instance for method chaining
        """
        self._config["trigger_cls"] = self._DISABLED_TRIGGER
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

        :param int min_threads: The minimum number of threads to keep in the thread pool
        :param int max_threads: The maximum number of threads allowed in the thread pool
        :param bool enforce_max_processes: If True, enforces the maximum number of processes
        :return: The builder instance for method chaining
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

        :param int num_processes: The number of processes to create in the process pool
        :return: The builder instance for method chaining
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

        :param int min_threads: The minimum number of threads to keep in the thread pool
        :param int max_threads: The maximum number of threads allowed in the thread pool
        :return: The builder instance for method chaining
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

        :return: The builder instance for method chaining
        """
        self._config["runner_cls"] = "ProcessRunner"
        return self

    def dummy_runner(self) -> "PynencBuilder":
        """
        Configure the DummyRunner for task execution.

        The DummyRunner executes tasks in the main thread of the application.
        This is useful for testing and debugging purposes.

        :return: The builder instance for method chaining
        """
        self._config["runner_cls"] = "DummyRunner"
        return self

    def dev_mode(self, force_sync_tasks: bool = True) -> "PynencBuilder":
        """
        Enable development mode for easier debugging.

        In development mode, tasks can be forced to run synchronously,
        making debugging and testing easier.

        :param bool force_sync_tasks: If True, forces all tasks to run synchronously
        :return: The builder instance for method chaining
        """
        self._config["dev_mode_force_sync_tasks"] = force_sync_tasks
        return self

    def logging_level(self, level: str) -> "PynencBuilder":
        """
        Set the logging level for the application.

        :param str level: The logging level ("debug", "info", "warning", "error", "critical")
        :return: The builder instance for method chaining
        :raises ValueError: If an invalid logging level is provided
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

        :param float runner_loop_sleep_time_sec: Sleep time between runner loop iterations
        :param float invocation_wait_results_sleep_time_sec: Sleep time when waiting for results
        :param int min_parallel_slots: Minimum number of parallel execution slots
        :return: The builder instance for method chaining
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

        :param bool cycle_control: Whether to enable cycle control for task dependencies
        :param bool blocking_control: Whether to enable blocking control for concurrent tasks
        :param float queue_timeout_sec: Timeout for queue operations in seconds
        :return: The builder instance for method chaining
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

        :param str serializer: Serializer name ("json", "pickle") or full class name
        :return: The builder instance for method chaining
        :raises ValueError: If an invalid serializer name is provided
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

        :param Optional[Union[str, ConcurrencyControlType]] running_concurrency: Controls runtime concurrency behavior
        :param Optional[Union[str, ConcurrencyControlType]] registration_concurrency: Controls registration concurrency behavior
        :return: The builder instance for method chaining
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

        :param float seconds: Maximum time in seconds a task can remain in PENDING state
        :return: The builder instance for method chaining
        """
        self._config["max_pending_seconds"] = seconds
        return self

    def argument_print_mode(
        self, mode: Union[str, ArgumentPrintMode], truncate_length: int = 32
    ) -> "PynencBuilder":
        """
        Configure how task arguments are printed in logs.

        :param Union[str, ArgumentPrintMode] mode: The print mode to use
        :param int truncate_length: Maximum length for printed argument values in TRUNCATED mode
        :return: The builder instance for method chaining
        :raises ValueError: If an invalid mode string is provided
        """
        if isinstance(mode, ArgumentPrintMode):
            arg_print_mode = mode
        else:
            mode_upper = mode.upper()
            try:
                arg_print_mode = ArgumentPrintMode[mode_upper]
            except KeyError as ex:
                valid_modes = [m.name for m in ArgumentPrintMode]
                raise ValueError(
                    f"Invalid argument print mode: {mode}. Valid options are: {', '.join(valid_modes)}"
                ) from ex

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

        :return: The builder instance for method chaining
        """
        return self.argument_print_mode(ArgumentPrintMode.HIDDEN)

    def show_argument_keys(self) -> "PynencBuilder":
        """
        Configure logs to show only argument names.

        :return: The builder instance for method chaining
        """
        return self.argument_print_mode(ArgumentPrintMode.KEYS)

    def show_full_arguments(self) -> "PynencBuilder":
        """
        Configure logs to show complete argument values without truncation.

        :return: The builder instance for method chaining
        """
        return self.argument_print_mode(ArgumentPrintMode.FULL)

    def show_truncated_arguments(self, truncate_length: int = 32) -> "PynencBuilder":
        """
        Configure logs to show truncated argument values.

        :param int truncate_length: Maximum length for printed argument values
        :return: The builder instance for method chaining
        """
        return self.argument_print_mode(ArgumentPrintMode.TRUNCATED, truncate_length)

    def custom_config(self, **kwargs: Any) -> "PynencBuilder":
        """
        Add arbitrary configuration values.

        For common configuration values, prefer using the dedicated methods
        instead of this generic method.

        :param Any kwargs: Custom configuration values to add
        :return: The builder instance for method chaining
        """
        self._config.update(kwargs)
        return self

    def _validate_plugin_compatibility(self) -> None:
        """
        Validate plugin configurations before building.

        Runs all registered plugin validators to ensure the configuration is valid.
        """
        for validator in self._plugin_validators:
            try:
                validator(self._config)
            except Exception as e:
                raise ValueError(f"Plugin configuration validation failed: {e}") from e

    def _validate_memory_compatibility(self) -> None:
        """
        Validate that the selected runner is compatible with memory components.

        :raises ValueError: If memory components are used with an incompatible runner
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
                f"or configure distributed components using an appropriate plugin."
            )

    def build(self) -> "Pynenc":
        """
        Build and return a configured Pynenc instance.

        This method creates a new Pynenc instance using the configuration
        values that have been set through the builder methods.

        :return: A configured Pynenc instance ready for use
        :raises ValueError: If the configuration is invalid
        """
        from pynenc import Pynenc

        self._validate_memory_compatibility()
        self._validate_plugin_compatibility()
        return Pynenc(config_values=self._config)
