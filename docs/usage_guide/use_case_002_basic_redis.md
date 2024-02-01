# Basic Redis Example Guide

## Overview

This guide provides a concise overview of using `pynenc` with Redis for distributed task processing. For detailed setup and execution, refer to the full [basic_redis_example](https://github.com/pynenc/samples/tree/main/basic_redis_example).

## Key Concepts

### Initialization and Dask Definition

Define tasks in `tasks.py` using `pynenc` decorators:

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
    add.logger.info(f"{sleep.task_id=} Sleeping for {x} seconds")
    time.sleep(x)
    add.logger.info(f"{sleep.task_id=} Done sleeping for {x} seconds")
    return x

```

### Configuration with Redis

Configure `pynenc` to use Redis in `pyproject.toml` and the `ProcessRunner`:

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

## Executing Tasks

In `sample.py`, we demonstrate different ways to execute the add task:

### Running the Worker

Start a `pynenc` worker with `pynenc --app=app.app runner start`.

### Triggering Tasks

- Run `sample.py` to trigger the tasks: `python sample.py`.
- Note: If you haven't set the Redis host in step 3 or in `pyproject.toml`, you can specify it inline when running the script:
  ```bash
  PYNENC__CONFIGREDIS__REDIS_HOST=localhost python sample.py
  ```

Execute tasks:

```python
    invocation = tasks.add(4, 4)
    assert invocation.result == 8
```

Run tasks in parallel with parallelize:

```python
    invocation_group = tasks.sleep.parallelize([(i,) for i in sleep_args])
    results = list(invocation_group.results)
```

## Redis and Docker

For a containerized setup using Docker, the `Dockerfile` and `docker-compose.yml` are configured to include Redis and the Python environment.

## Development Mode

Run in development mode without Redis:

```bash
PYNENC__DEV_MODE_FORCE_SYNC_TASKS=True python sample.py
```

This mode is useful for debugging and testing.

## Conclusion

This guide highlights the essential steps to use `pynenc` with Redis. For a comprehensive understanding, including Docker setup and advanced configurations, refer to the [full example](https://github.com/pynenc/samples/tree/main/basic_redis_example).
