# Runners

Reference for all Pynenc runner implementations, their configuration, and execution models.

## Runner Hierarchy

```
BaseRunner (ABC)
├── ThreadRunner
├── MultiThreadRunner
├── ProcessRunner
├── PersistentProcessRunner
├── DummyRunner
│   └── ExternalRunner
```

All runners are in `pynenc.runner` and can be selected via configuration:

```toml
[tool.pynenc]
runner_cls = "MultiThreadRunner"
```

Or the builder:

```python
PynencBuilder().multi_thread_runner(min_threads=2, max_threads=8).build()
```

## Common Configuration

These fields apply to all runner types via `ConfigRunner`:

| Field                                    | Type    | Default | Description                                                       |
| ---------------------------------------- | ------- | ------- | ----------------------------------------------------------------- |
| `invocation_wait_results_sleep_time_sec` | `float` | `0.1`   | Delay between polls when waiting for dependent invocation results |
| `runner_loop_sleep_time_sec`             | `float` | `0.1`   | Sleep between main loop iterations                                |
| `min_parallel_slots`                     | `int`   | `1`     | Minimum number of concurrent execution slots                      |

## Choosing a Runner

```{image} ../_static/pynenc_runners_timeline_detail.png
:alt: Pynmon timeline comparing all four runners processing short-lived tasks over a 2-second window
:width: 100%
```

_The timeline above shows all four runners processing identical short-lived tasks. Orange = pending (waiting), green = completed (ran successfully). See {doc}`../faq` for a detailed breakdown of each runner's behavior in this scenario._

- **Development/testing**: `ThreadRunner` with memory backends, or `dev_mode_force_sync_tasks=True` with no runner
- **Production I/O-bound**: `MultiThreadRunner` — scales processes and threads
- **Production CPU-bound**: `PersistentProcessRunner` — avoids process spawn overhead per task
- **Fine-grained process isolation**: `ProcessRunner` — one process per invocation with OS-level pause/resume

See {doc}`../configuration/index` for full configuration details.
See {doc}`builder` for programmatic runner selection with the builder API.

## ThreadRunner

Executes tasks in threads within a single process. Suitable for I/O-bound workloads and development.

**`mem_compatible`**: `True` — works with in-memory backends.

### Configuration

| Field         | Type  | Default | Description                            |
| ------------- | ----- | ------- | -------------------------------------- |
| `min_threads` | `int` | `1`     | Minimum thread count                   |
| `max_threads` | `int` | `0`     | Maximum thread count (`0` = CPU count) |

### Behavior

- Spawns daemon threads for each invocation
- When a task waits on another invocation's result, the waiting thread is tracked separately so it does not consume a concurrency slot
- On shutdown, alive threads are killed and their invocations rerouted

### Usage

```toml
[tool.pynenc]
runner_cls = "ThreadRunner"

[tool.pynenc.runner]
min_threads = 2
max_threads = 8
```

```python
PynencBuilder().thread_runner(min_threads=2, max_threads=8).build()
```

## MultiThreadRunner

Spawns separate processes, each running a `ThreadRunner` internally. Scales processes up and down based on broker queue depth.

**`mem_compatible`**: `False` — requires a distributed backend.

### Configuration

| Field                      | Type   | Default | Description                                        |
| -------------------------- | ------ | ------- | -------------------------------------------------- |
| `min_threads`              | `int`  | `1`     | Threads per child process                          |
| `max_threads`              | `int`  | `1`     | Threads per child process                          |
| `min_processes`            | `int`  | `1`     | Minimum worker processes                           |
| `max_processes`            | `int`  | `0`     | Maximum worker processes (`0` = CPU count)         |
| `idle_timeout_process_sec` | `int`  | `4`     | Seconds idle before a process is terminated        |
| `enforce_max_processes`    | `bool` | `True`  | Always maintain `max_processes` regardless of load |

### Behavior

- Parent process manages child `ThreadRunner` processes
- Children report active thread count and idle state via shared memory
- When `enforce_max_processes` is `True`, always runs `max_processes` workers
- When `False`, scales based on pending invocation count in the broker
- Idle processes beyond `min_processes` are terminated after `idle_timeout_process_sec`
- Dead processes are automatically cleaned up

### Usage

```toml
[tool.pynenc]
runner_cls = "MultiThreadRunner"

[tool.pynenc.runner]
min_processes = 2
max_processes = 8
min_threads = 1
max_threads = 4
```

```python
PynencBuilder().multi_thread_runner(min_threads=1, max_threads=4).build()
```

## ProcessRunner

Executes each invocation in its own process. Uses OS signals (`SIGSTOP`/`SIGCONT`) to pause and resume processes waiting on dependency results.

**`mem_compatible`**: `False` — requires a distributed backend.

### Configuration

Uses the base `ConfigRunner` fields only. Maximum parallelism equals CPU count.

### Behavior

- One process per invocation, up to `cpu_count()` concurrent
- When an invocation depends on another's result, the process is paused with `SIGSTOP`
- When the dependency resolves, the process is resumed with `SIGCONT`
- On shutdown, child processes are killed with `SIGKILL` and invocations rerouted

### Usage

```toml
[tool.pynenc]
runner_cls = "ProcessRunner"
```

```python
PynencBuilder().process_runner().build()
```

## PersistentProcessRunner

Maintains a fixed pool of long-lived worker processes. Each worker loops, fetching and executing invocations sequentially.

**`mem_compatible`**: `False` — requires a distributed backend.

### Configuration

| Field           | Type  | Default | Description                                  |
| --------------- | ----- | ------- | -------------------------------------------- |
| `num_processes` | `int` | `0`     | Number of worker processes (`0` = CPU count) |

Also inherits `ConfigThreadRunner` fields.

### Behavior

- Spawns `num_processes` persistent workers at startup
- Each worker runs a loop: fetch one invocation → execute → repeat
- Dead workers are automatically respawned to maintain pool size
- On shutdown, workers receive `SIGTERM`, then `SIGKILL` after 5 seconds
- When waiting on dependencies, the worker sleeps (no pause/resume mechanism)
- Forces `spawn` start method on macOS

### Usage

```toml
[tool.pynenc]
runner_cls = "PersistentProcessRunner"

[tool.pynenc.runner]
num_processes = 4
```

```python
PynencBuilder().persistent_process_runner(num_processes=4).build()
```

## DummyRunner

Non-executable placeholder used when the app is defined outside a runner context (scripts that route tasks but don't execute them). All execution methods raise `RunnerNotExecutableError`.

## ExternalRunner

Extends `DummyRunner` with hostname and PID tracking. Generates a `runner_id` of `ExternalRunner@{hostname}-{pid}`. Used to track which external process registered an invocation.

## Comparison

| Runner                      | Execution Model        | Memory Compatible | Parallelism         | Auto-Scaling       | Wait Strategy      |
| --------------------------- | ---------------------- | ----------------- | ------------------- | ------------------ | ------------------ |
| **ThreadRunner**            | Threads in one process | Yes               | `max_threads`       | No                 | Free thread slot   |
| **MultiThreadRunner**       | Processes × threads    | No                | Processes × threads | Yes                | Delegated to child |
| **ProcessRunner**           | One process per task   | No                | CPU count           | No                 | SIGSTOP/SIGCONT    |
| **PersistentProcessRunner** | Fixed process pool     | No                | `num_processes`     | No (respawns dead) | Sleep polling      |
| **DummyRunner**             | None                   | N/A               | N/A                 | N/A                | N/A                |
