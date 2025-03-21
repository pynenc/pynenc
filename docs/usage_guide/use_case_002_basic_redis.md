# Basic Redis

## Overview

This guide provides a concise overview of using `pynenc` with Redis for distributed task processing. For detailed setup and execution, refer to the full [basic_redis_example](https://github.com/pynenc/samples/tree/main/basic_redis_example).

## Key Concepts

### Initialization and Task Definition

Define tasks in `tasks.py` using `pynenc` decorators. Below are two ways to initialize the app: directly with `Pynenc()` or using `PynencBuilder`.

### Option 1: Direct Initialization

```python
import time
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    add.logger.info(f"{add.task_id=} Adding {x} + {y}")
    return x + y

@app.task
def sleep(x: int) -> int:
    sleep.logger.info(f"{sleep.task_id=} Sleeping for {x} seconds")
    time.sleep(x)
    sleep.logger.info(f"{sleep.task_id=} Done sleeping for {x} seconds")
    return x
```

### Option 2: Using PynencBuilder

```python
import time
from pynenc import Pynenc
from pynenc.builder import PynencBuilder

app = (
    PynencBuilder()
    .redis(url="redis://redis:6379")  # Matches docker-compose default
    .process_runner()
    .serializer("json")
    .custom_config(app_id="app_basic_redis_example")
    .build()
)

@app.task
def add(x: int, y: int) -> int:
    add.logger.info(f"{add.task_id=} Adding {x} + {y}")
    return x + y

@app.task
def sleep(x: int) -> int:
    sleep.logger.info(f"{sleep.task_id=} Sleeping for {x} seconds")
    time.sleep(x)
    sleep.logger.info(f"{sleep.task_id=} Done sleeping for {x} seconds")
    return x
```

## Configuration with Redis

You can configure `pynenc` to use Redis in two ways:

### Using pyproject.toml

Configure `pynenc` in `pyproject.toml` with the `ProcessRunner`:

```toml
[tool.pynenc]
app_id = "app_basic_redis_example"
orchestrator_cls = "RedisOrchestrator"
broker_cls = "RedisBroker"
state_backend_cls = "RedisStateBackend"
serializer_cls = "JsonSerializer"
runner_cls = "ProcessRunner"

[tool.pynenc.redis]
redis_host = "redis"
```

### Using PynencBuilder

As shown in Option 2 above, use `PynencBuilder` in your code to configure Redis, the runner, and other settings programmatically. The example uses `redis://redis:6379` to match a typical Docker setup, but you can adjust the URL (e.g., `redis://localhost:6379` for local testing).

## Executing Tasks

In `sample.py`, demonstrate different ways to execute the `add` task:

### Running the Worker

Start a `pynenc` worker with:

```bash
pynenc --app=tasks.app runner start
```

### Triggering Tasks

- Run `sample.py` to trigger the tasks:

```bash
python sample.py
```

- If you haven’t set the Redis host in `pyproject.toml` or via `PynencBuilder`, specify it inline:

```bash
PYNENC__CONFIGREDIS__REDIS_HOST=localhost python sample.py
```

Execute tasks:

```python
invocation = tasks.add(4, 4)
assert invocation.result == 8
```

Run tasks in parallel with `parallelize`:

```python
sleep_args = [1, 2, 3]  # Example sleep durations
invocation_group = tasks.sleep.parallelize([(i,) for i in sleep_args])
results = list(invocation_group.results)
```

## Redis and Docker

For a containerized setup using Docker, the `Dockerfile` and `docker-compose.yml` are configured to include Redis and the Python environment. The `PynencBuilder` example above uses `redis://redis:6379`, which aligns with the default Redis service name in `docker-compose.yml`.

## Development Mode

Run in development mode without Redis by setting:

```bash
PYNENC__DEV_MODE_FORCE_SYNC_TASKS=True python sample.py
```

This mode is useful for debugging and testing, regardless of whether you use `Pynenc()` or `PynencBuilder`.

## Conclusion

This guide highlights the essential steps to use `pynenc` with Redis, offering flexibility with both `pyproject.toml` and `PynencBuilder` configuration approaches. For a comprehensive understanding, including Docker setup and advanced configurations, refer to the [full example](https://github.com/pynenc/samples/tree/main/basic_redis_example).
