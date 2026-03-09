# Configuration System

Reference for all Pynenc configuration options, sources, and resolution order.

## Configuration Sources

Configuration values resolve from these sources in priority order (highest first):

1. **Direct assignment** in the config instance
2. **Environment variables** (`PYNENC__FIELD_NAME` or `PYNENC__CLASS__FIELD_NAME`)
3. **Configuration file** specified by `PYNENC__FILEPATH` environment variable
4. **Configuration file** path passed to `Pynenc(config_filepath=...)` (YAML, TOML, or JSON)
5. **`pyproject.toml`** under `[tool.pynenc]`
6. **Default values** defined in the `ConfigField`

## Environment Variables

Two naming conventions are supported:

```bash
# Default for all config classes
PYNENC__FIELD_NAME="value"

# Specific to a config class
PYNENC__CONFIGPYNENC__FIELD_NAME="value"
```

The specific form takes precedence over the default form.

## Configuration Files

### `pyproject.toml`

```toml
[tool.pynenc]
app_id = "my_application"
orchestrator_cls = "RedisOrchestrator"
broker_cls = "RedisBroker"
state_backend_cls = "RedisStateBackend"
runner_cls = "MultiThreadRunner"
serializer_cls = "JsonPickleSerializer"

[tool.pynenc.orchestrator]
max_pending_seconds = 300

[tool.pynenc.runner]
min_threads = 2
max_threads = 8

[tool.pynenc.task]
running_concurrency = "task"
```

### YAML

```yaml
app_id: my_application
orchestrator_cls: RedisOrchestrator

orchestrator:
  max_pending_seconds: 300

runner:
  min_threads: 4
  max_threads: 16
```

Load with:

```python
from pynenc import Pynenc
app = Pynenc(config_filepath="/path/to/pynenc.yaml")
```

### JSON

```json
{
  "app_id": "my_application",
  "orchestrator_cls": "RedisOrchestrator"
}
```

## ConfigPynenc Fields

Main application configuration (`pynenc.conf.config_pynenc.ConfigPynenc`).

| Field                                   | Type    | Default                  | Description                                                     |
| --------------------------------------- | ------- | ------------------------ | --------------------------------------------------------------- |
| `app_id`                                | `str`   | `"pynenc"`               | Application identifier                                          |
| `orchestrator_cls`                      | `str`   | `"MemOrchestrator"`      | Orchestrator implementation class name                          |
| `trigger_cls`                           | `str`   | `"MemTrigger"`           | Trigger implementation class name                               |
| `broker_cls`                            | `str`   | `"MemBroker"`            | Broker implementation class name                                |
| `state_backend_cls`                     | `str`   | `"MemStateBackend"`      | State backend implementation class name                         |
| `serializer_cls`                        | `str`   | `"JsonPickleSerializer"` | Serializer implementation class name                            |
| `client_data_store_cls`                 | `str`   | `"MemClientDataStore"`   | Client data store implementation class name                     |
| `runner_cls`                            | `str`   | `"DummyRunner"`          | Runner implementation class name                                |
| `trigger_task_modules`                  | `set`   | `set()`                  | Modules containing trigger-dependent tasks                      |
| `dev_mode_force_sync_tasks`             | `bool`  | `False`                  | Execute tasks synchronously in calling thread                   |
| `logging_level`                         | `str`   | `"info"`                 | Logging level (`debug`, `info`, `warning`, `error`, `critical`) |
| `print_arguments`                       | `bool`  | `True`                   | Print task arguments in log messages                            |
| `truncate_arguments_length`             | `int`   | `32`                     | Maximum printed argument length                                 |
| `argument_print_mode`                   | `str`   | `"TRUNCATED"`            | Argument display mode: `FULL`, `KEYS`, `TRUNCATED`, `HIDDEN`    |
| `cached_status_time`                    | `float` | `0.1`                    | Invocation status cache TTL (seconds)                           |
| `compact_log_context`                   | `bool`  | `True`                   | Truncate IDs in log context for readability                     |
| `atomic_service_interval_minutes`       | `float` | `5.0`                    | Cycle interval for atomic recovery services                     |
| `atomic_service_spread_margin_minutes`  | `float` | `1.0`                    | Safety margin for time-slot allocation                          |
| `atomic_service_check_interval_minutes` | `float` | `0.5`                    | Runner check interval for atomic services                       |
| `recover_pending_invocations_cron`      | `str`   | `"*/5 * * * *"`          | Cron expression for pending invocation recovery                 |
| `max_pending_seconds`                   | `float` | `5.0`                    | Maximum time an invocation can remain PENDING                   |
| `recover_running_invocations_cron`      | `str`   | `"*/15 * * * *"`         | Cron expression for running invocation recovery                 |
| `runner_considered_dead_after_minutes`  | `float` | `10.0`                   | Heartbeat timeout before runner is considered dead              |

## ConfigTask Fields

Per-task configuration (`pynenc.conf.config_task.ConfigTask`). Configurable globally or per-task.

| Field                            | Type    | Default         | Description                                                               |
| -------------------------------- | ------- | --------------- | ------------------------------------------------------------------------- |
| `parallel_batch_size`            | `int`   | `100`           | Batch size for `task.parallelize()` routing                               |
| `retry_for`                      | `tuple` | `(RetryError,)` | Exception types that trigger a retry                                      |
| `max_retries`                    | `int`   | `0`             | Maximum retry attempts (0 = no retries)                                   |
| `running_concurrency`            | `str`   | `"DISABLED"`    | Runtime concurrency control: `DISABLED`, `TASK`, `ARGUMENTS`, `KEYS`      |
| `registration_concurrency`       | `str`   | `"DISABLED"`    | Registration concurrency control: `DISABLED`, `TASK`, `ARGUMENTS`, `KEYS` |
| `key_arguments`                  | `tuple` | `()`            | Arguments used for `KEYS` concurrency checks                              |
| `on_diff_non_key_args_raise`     | `bool`  | `False`         | Raise error when non-key arguments differ in concurrency check            |
| `call_result_cache`              | `bool`  | `False`         | Cache results by call arguments                                           |
| `disable_cache_args`             | `tuple` | `()`            | Arguments to exclude from cache key                                       |
| `force_new_workflow`             | `bool`  | `False`         | Always create a new workflow context                                      |
| `reroute_on_concurrency_control` | `bool`  | `False`         | Reroute blocked tasks instead of marking final                            |

### Per-Task Configuration

Override settings for specific tasks using environment variables or config files:

**Environment variables**:

```bash
# Global task setting
PYNENC__CONFIGTASK__MAX_RETRIES="3"

# Task-specific (module#task separator)
PYNENC__CONFIGTASK__MYMODULE#MY_TASK__MAX_RETRIES="5"
```

```{note}
Use `#` (not `__`) to separate the module name from the task name in environment variables.
```

**Configuration files**:

```yaml
task:
  max_retries: 3
  mymodule.my_task:
    max_retries: 5
```

**Task decorator**:

```python
@app.task(max_retries=5, running_concurrency="task")
def my_task(x: int) -> int:
    return x * 2
```

## ConfigRunner Fields

Base runner configuration (`pynenc.conf.config_runner.ConfigRunner`).

| Field                                    | Type    | Default | Description                               |
| ---------------------------------------- | ------- | ------- | ----------------------------------------- |
| `invocation_wait_results_sleep_time_sec` | `float` | `0.1`   | Sleep time between result polling checks  |
| `runner_loop_sleep_time_sec`             | `float` | `0.1`   | Sleep time between runner loop iterations |
| `min_parallel_slots`                     | `int`   | `1`     | Minimum parallel execution slots          |

### ThreadRunner Configuration

| Field         | Type  | Default | Description                                |
| ------------- | ----- | ------- | ------------------------------------------ |
| `min_threads` | `int` | `1`     | Minimum thread pool size                   |
| `max_threads` | `int` | `0`     | Maximum thread pool size (`0` = CPU count) |

### MultiThreadRunner Configuration

| Field                      | Type   | Default | Description                                        |
| -------------------------- | ------ | ------- | -------------------------------------------------- |
| `min_threads`              | `int`  | `1`     | Threads per child process                          |
| `max_threads`              | `int`  | `1`     | Threads per child process                          |
| `min_processes`            | `int`  | `1`     | Minimum worker processes                           |
| `max_processes`            | `int`  | `0`     | Maximum worker processes (`0` = CPU count)         |
| `idle_timeout_process_sec` | `int`  | `4`     | Seconds idle before a process is terminated        |
| `enforce_max_processes`    | `bool` | `True`  | Always maintain `max_processes` regardless of load |

### PersistentProcessRunner Configuration

| Field           | Type  | Default   | Description                           |
| --------------- | ----- | --------- | ------------------------------------- |
| `num_processes` | `int` | CPU count | Number of persistent worker processes |

## Plugin Configuration

Backend plugins add their own configuration sections. For example:

### Redis Plugin

```toml
[tool.pynenc.redis]
redis_host = "localhost"
redis_port = 6379
redis_db = 0
```

### MongoDB Plugin

```toml
[tool.pynenc.mongodb]
connection_string = "mongodb://localhost:27017"
database_name = "pynenc"
```

## Hierarchical Resolution

Pynenc supports hierarchical configuration classes with inheritance. The most specific (child) configuration takes precedence:

```toml
[tool.pynenc]
test_field = "default"

[tool.pynenc.child]
test_field = "child_override"
```

See {doc}`../overview` for how configuration fits within the architecture.
See {doc}`../reference/builder` for programmatic configuration with `PynencBuilder`.
