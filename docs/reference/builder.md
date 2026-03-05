# PynencBuilder Reference

Reference for the `PynencBuilder` fluent API for programmatic application configuration.

## Overview

`PynencBuilder` provides a method-chaining interface to configure a Pynenc application. All methods return the builder instance, allowing fluent configuration:

```python
from pynenc.builder import PynencBuilder

app = (
    PynencBuilder()
    .app_id("my_application")
    .memory()
    .thread_runner()
    .dev_mode()
    .build()
)
```

## Methods

### Application Identity

#### `app_id(app_id: str) -> PynencBuilder`

Set the application identifier. Defaults to `"pynenc"`.

```python
builder.app_id("my_app")
```

### Backend Configuration

#### `memory() -> PynencBuilder`

Configure all components to use in-memory backends. Suitable for development and testing. Only compatible with `ThreadRunner` and `DummyRunner`.

```python
builder.memory()
```

#### `sqlite(sqlite_db_path: str | None = None) -> PynencBuilder`

Configure all components to use SQLite backends. Suitable for single-host testing with process runners.

```python
builder.sqlite(sqlite_db_path="/tmp/pynenc.db")
```

### Runner Configuration

#### `thread_runner(min_threads: int = 1, max_threads: int = 0) -> PynencBuilder`

Configure `ThreadRunner` with a thread pool. Memory-compatible. `max_threads=0` defaults to CPU count.

```python
builder.thread_runner(min_threads=2, max_threads=8)
```

#### `multi_thread_runner(min_threads: int = 1, max_threads: int = 1, enforce_max_processes: bool = False) -> PynencBuilder`

Configure `MultiThreadRunner` with process and thread pools.

| Parameter               | Default | Description                                  |
| ----------------------- | ------- | -------------------------------------------- |
| `min_threads`           | `1`     | Threads per child process                    |
| `max_threads`           | `1`     | Threads per child process                    |
| `enforce_max_processes` | `False` | If `True`, always maintain maximum processes |

```python
builder.multi_thread_runner(min_threads=2, max_threads=4, enforce_max_processes=True)
```

#### `process_runner() -> PynencBuilder`

Configure `ProcessRunner` that spawns a new process per task.

```python
builder.process_runner()
```

#### `persistent_process_runner(num_processes: int = 0) -> PynencBuilder`

Configure `PersistentProcessRunner` with a persistent process pool. `num_processes=0` defaults to CPU count.

```python
builder.persistent_process_runner(num_processes=4)
```

#### `dummy_runner() -> PynencBuilder`

Configure `DummyRunner` (non-executable placeholder).

```python
builder.dummy_runner()
```

### Trigger Configuration

#### `mem_trigger(scheduler_interval_seconds: int = 60, enable_scheduler: bool = True) -> PynencBuilder`

Configure memory-based trigger system for development.

```python
builder.mem_trigger(scheduler_interval_seconds=30)
```

### Client Data Store

#### `mem_client_data_store(min_size_to_cache: int = 1024, local_cache_size: int = 1024) -> PynencBuilder`

Configure memory-based client data store for large argument caching.

| Parameter           | Default | Description                                         |
| ------------------- | ------- | --------------------------------------------------- |
| `min_size_to_cache` | `1024`  | Minimum string length required to cache an argument |
| `local_cache_size`  | `1024`  | Maximum number of items in local cache              |

```python
builder.mem_client_data_store(min_size_to_cache=512, local_cache_size=100)
```

#### `disable_client_data_store() -> PynencBuilder`

Disable the client data store completely.

```python
builder.disable_client_data_store()
```

### Serializer Configuration

#### `serializer_json_pickle() -> PynencBuilder`

Configure the JSON-Pickle hybrid serializer (default). Preserves Python object types but requires trusted data.

#### `serializer_json() -> PynencBuilder`

Configure the pure JSON serializer. Safe for untrusted data but requires JSON-serializable types or `JsonSerializable` protocol.

#### `serializer_pickle() -> PynencBuilder`

Configure the Pickle serializer. Maximum type support but least portable and not human-readable.

### Task Control

#### `concurrency_control(running_concurrency: str | ConcurrencyControlType | None = None, registration_concurrency: str | ConcurrencyControlType | None = None) -> PynencBuilder`

Configure default concurrency control for all tasks.

| Parameter                  | Values                                          | Description                      |
| -------------------------- | ----------------------------------------------- | -------------------------------- |
| `running_concurrency`      | `"disabled"`, `"task"`, `"arguments"`, `"keys"` | Runtime concurrency control      |
| `registration_concurrency` | `"disabled"`, `"task"`, `"arguments"`, `"keys"` | Registration concurrency control |

```python
builder.concurrency_control(running_concurrency="task", registration_concurrency="arguments")
```

#### `task_control(blocking_control: bool = False, queue_timeout_sec: float = 0.1) -> PynencBuilder`

Configure task control parameters.

| Parameter           | Default | Description                                  |
| ------------------- | ------- | -------------------------------------------- |
| `blocking_control`  | `False` | Enable blocking control for concurrent tasks |
| `queue_timeout_sec` | `0.1`   | Timeout for queue operations (seconds)       |

```python
builder.task_control(blocking_control=True, queue_timeout_sec=0.5)
```

#### `max_pending_seconds(seconds: float) -> PynencBuilder`

Set maximum time a task can remain in `PENDING` state before recovery.

```python
builder.max_pending_seconds(10.0)
```

### Logging and Display

#### `logging_level(level: str) -> PynencBuilder`

Set the logging level. Accepts `"debug"`, `"info"`, `"warning"`, `"error"`, `"critical"`.

```python
builder.logging_level("debug")
```

#### `dev_mode(force_sync_tasks: bool = True) -> PynencBuilder`

Enable development mode. When `force_sync_tasks` is `True`, tasks execute synchronously in the calling thread.

```python
builder.dev_mode()
builder.dev_mode(force_sync_tasks=False)  # Enable dev mode without sync tasks
```

#### `argument_print_mode(mode: str | ArgumentPrintMode, truncate_length: int = 32) -> PynencBuilder`

Configure how task arguments appear in logs.

| Mode          | Description                     |
| ------------- | ------------------------------- |
| `"FULL"`      | Show complete argument values   |
| `"KEYS"`      | Show only argument names        |
| `"TRUNCATED"` | Show truncated values (default) |
| `"HIDDEN"`    | Hide all arguments              |

```python
builder.argument_print_mode("TRUNCATED", truncate_length=64)
```

#### `hide_arguments() -> PynencBuilder`

Shortcut to hide all task arguments in logs.

#### `show_argument_keys() -> PynencBuilder`

Shortcut to show only argument names in logs.

#### `show_full_arguments() -> PynencBuilder`

Shortcut to show complete argument values in logs.

#### `show_truncated_arguments(truncate_length: int = 32) -> PynencBuilder`

Shortcut to show truncated argument values in logs.

### Runner Tuning

#### `runner_tuning(runner_loop_sleep_time_sec: float = 0.01, invocation_wait_results_sleep_time_sec: float = 0.01, min_parallel_slots: int = 1) -> PynencBuilder`

Configure runner performance parameters.

| Parameter                                | Default | Description                               |
| ---------------------------------------- | ------- | ----------------------------------------- |
| `runner_loop_sleep_time_sec`             | `0.01`  | Sleep time between runner loop iterations |
| `invocation_wait_results_sleep_time_sec` | `0.01`  | Sleep time between result polling checks  |
| `min_parallel_slots`                     | `1`     | Minimum parallel execution slots          |

### Advanced Configuration

#### `trigger_task_modules(modules: Iterable[str]) -> PynencBuilder`

Declare modules containing trigger-dependent tasks. These modules are imported when the runner starts to register trigger definitions.

```python
builder.trigger_task_modules(["myapp.triggers", "myapp.scheduled_tasks"])
```

#### `custom_config(**kwargs) -> PynencBuilder`

Add arbitrary configuration values that map to `ConfigField` names.

```python
builder.custom_config(cached_status_time=0.5, compact_log_context=False)
```

### Build

#### `build() -> Pynenc`

Build and return a fully configured `Pynenc` instance. Runs all registered validators before returning.

```python
app = builder.build()
```

## Plugin Extensions

Backend plugins can register additional builder methods via `PynencBuilder.register_plugin_method()`. For example, after installing `pynenc-redis`:

```python
app = (
    PynencBuilder()
    .app_id("my_app")
    .redis(url="redis://localhost:6379")  # Added by pynenc-redis plugin
    .process_runner()
    .build()
)
```

Plugin methods are discovered automatically when the plugin package is installed.

See {doc}`../overview` for how the plugin system works.
