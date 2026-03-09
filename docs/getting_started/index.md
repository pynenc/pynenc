# Getting Started

This tutorial walks you through installing Pynenc, defining a task, executing it, and retrieving its result. By the end you will have a working distributed task running locally.

## Prerequisites

- Python 3.11 or later
- pip (or any Python package manager)

## Step 1: Install Pynenc

```bash
pip install pynenc
```

This installs the core package with the built-in Memory and SQLite backends.

## Step 2: Define a Task

Create a file named `tasks.py`:

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    return x + y
```

```{important}
Tasks cannot be defined in modules that run as `__main__` (e.g., scripts executed with `python tasks.py`).
Define tasks in importable modules and reference the `app` object from elsewhere.
See the {doc}`../faq` for details on this limitation.
```

## Step 3: Run Synchronously (Dev Mode)

The fastest way to test is synchronous execution. Set the environment variable:

```bash
export PYNENC__DEV_MODE_FORCE_SYNC_TASKS=True
```

Then in a Python session or script (separate from `tasks.py`):

```python
from tasks import add

result = add(1, 2).result
print(result)  # 3
```

The task runs in the calling thread. No runner process is needed.

## Step 4: Run with a Runner

For actual distributed execution, start a runner in one terminal:

```bash
pynenc --app=tasks.app runner start
```

In another terminal or script:

```python
from tasks import add

invocation = add(1, 2)
print(invocation.result)  # blocks until the runner executes the task
```

The runner picks up the task from the broker, executes it, and stores the result in the state backend.

## Step 5: Use the Builder for Programmatic Setup

Instead of environment variables, configure Pynenc with the `PynencBuilder`:

```python
from pynenc.builder import PynencBuilder

app = (
    PynencBuilder()
    .app_id("my_app")
    .memory()
    .dev_mode(force_sync_tasks=True)
    .build()
)

@app.task
def multiply(x: int, y: int) -> int:
    return x * y

result = multiply(3, 4).result
print(result)  # 12
```

## Step 6: Switch to a Production Backend

Install a backend plugin and update your configuration. Example with Redis:

```bash
pip install pynenc-redis
```

```toml
# pyproject.toml
[tool.pynenc]
app_id = "my_app"
orchestrator_cls = "RedisOrchestrator"
broker_cls = "RedisBroker"
state_backend_cls = "RedisStateBackend"
runner_cls = "MultiThreadRunner"
```

Or with the builder:

```python
app = (
    PynencBuilder()
    .app_id("my_app")
    .redis(url="redis://localhost:6379")
    .multi_thread_runner(min_threads=2, max_threads=8)
    .build()
)
```

Start the runner:

```bash
pynenc --app=tasks.app runner start
```

Your tasks now route through Redis.

## Step 7: Launch the Monitor

Install the monitoring extra and start Pynmon:

```bash
pip install pynenc[monitor]
pynenc --app=tasks.app monitor
```

Open `http://localhost:8000` to see invocations, runners, and task timelines.

## Next Steps

- {doc}`../usage_guide/use_case_003_concurrency_control` — Control parallel task execution
- {doc}`../usage_guide/use_case_004_auto_orchestration` — Automatic task dependency resolution
- {doc}`../usage_guide/use_case_010_trigger_system` — Schedule tasks with cron and events
- {doc}`../usage_guide/use_case_011_workflow_system` — Deterministic multi-step workflows
- {doc}`../configuration/index` — Full configuration reference
- {doc}`../reference/builder` — Builder API reference
