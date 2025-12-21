# Getting Started

This section covers the basics of getting Pynenc installed and running in your environment.

## Installation

Installing Pynenc is straightforward and can be accomplished using pip. Simply run the following command in your terminal:

```bash
pip install pynenc
```

This command will download and install Pynenc along with its necessary dependencies. Once the installation is complete, you are ready to start using Pynenc in your Python projects.

For more detailed instructions and advanced installation options, refer to the `Installation` section in the Pynenc Documentation.

## Quick Start

To get a quick feel of Pynenc, here's a basic example of creating and executing a distributed task.

### 1. Define a Task

Create a file named `tasks.py` and define a simple addition task:

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    add.logger.info(f"{add.task_id=} Adding {x} + {y}")
    return x + y

@app.direct_task
def direct_add(x: int, y: int) -> int:
    return x + y
```

### 2. Start Your Runner or Run Synchronously

Before executing the task, decide if you want to run it asynchronously with a runner or synchronously for testing or development purposes.

- **Asynchronously**: Start a runner in a separate terminal or script:

  ```bash
  pynenc --app=tasks.app runner start
  ```

  Check out the [basic_redis_example](https://github.com/pynenc/samples/tree/main/basic_redis_example)

- **Synchronously**:
  For test or local demonstration, to try synchronous execution, you can set the environment variable:

  ```bash
  export PYNENC__DEV_MODE_FORCE_SYNC_TASKS=True
  ```

### 3. Execute the Task

```python
result = add(1, 2).result
print(result)  # This will output the result of 1 + 2

print(direct_add(1, 2))  # Directly waits for result
```

## Configuration Methods

Pynenc supports multiple configuration methods. Choose the one that fits your workflow:

### 1. `pyproject.toml` (Recommended for Default Configuration)

The most common approach for production setups. Define your defaults in `pyproject.toml`:

**Basic Production Setup with Plugins:**

```toml
[tool.pynenc]
app_id = "my_application"
orchestrator_cls = "MongoOrchestrator"
broker_cls = "RabbitMqBroker"
state_backend_cls = "MongoStateBackend"
serializer_cls = "JsonPickleSerializer"
runner_cls = "PersistentProcessRunner"
trigger_cls = "MongoTrigger"
```

**Redis-based Setup:**

```toml
[tool.pynenc]
app_id = "my_application"
orchestrator_cls = "RedisOrchestrator"
broker_cls = "RedisBroker"
state_backend_cls = "RedisStateBackend"
runner_cls = "MultiThreadRunner"
```

**Component-Specific Configuration with Subconfigs:**

Pynenc supports hierarchical configuration for fine-grained control:

```toml
[tool.pynenc]
app_id = "my_application"
orchestrator_cls = "RedisOrchestrator"

[tool.pynenc.orchestrator]
max_pending_seconds = 300

[tool.pynenc.runner]
min_threads = 2
max_threads = 8

[tool.pynenc.task]
running_concurrency = "task"
```

### 2. Environment Variables (Production Clusters)

Ideal for deploying across different environments (staging, production, different clusters). Environment variables override `pyproject.toml` settings:

```bash
# Override app_id per environment
export PYNENC__APP_ID="my_app_production"

# Database credentials (never commit these!)
export PYNENC__MONGO_USERNAME="prod_user"
export PYNENC__MONGO_PASSWORD="secret"
export PYNENC__MONGO_AUTH_SOURCE="admin"
export PYNENC__MONGO_HOST="mongo.production.internal"

# Redis configuration
export PYNENC__REDIS_URL="redis://redis.production.internal:6379"

# Override runner settings per cluster
export PYNENC__RUNNER__MAX_THREADS="16"
```

This allows you to:

- Keep secrets out of version control
- Deploy the same codebase to different environments
- Scale runner configuration per cluster

### 3. Configuration Files (JSON/YAML)

For complex configurations or when you need to inject config files:

**YAML (`pynenc.yaml`):**

```yaml
app_id: my_application
orchestrator_cls: RedisOrchestrator
broker_cls: RedisBroker

orchestrator:
  max_pending_seconds: 300

runner:
  min_threads: 4
  max_threads: 16
```

**JSON (`pynenc.json`):**

```json
{
  "app_id": "my_application",
  "orchestrator_cls": "RedisOrchestrator",
  "orchestrator": {
    "max_pending_seconds": 300
  }
}
```

Load with:

```python
from pynenc import Pynenc

app = Pynenc(config_filepath="/path/to/pynenc.yaml")
```

### 4. `PynencBuilder` (Programmatic Configuration)

For dynamic configuration or when you need programmatic control:

```python
from pynenc.builder import PynencBuilder

app = (
    PynencBuilder()
    .app_id("my_application")
    .redis(url="redis://localhost:6379")
    .multi_thread_runner(min_threads=2, max_threads=8)
    .build()
)
```

**Development/Testing Setup:**

```python
app = (
    PynencBuilder()
    .app_id("my_application")
    .memory()  # In-memory backends, no external dependencies
    .dev_mode(force_sync_tasks=True)
    .build()
)
```

The builder provides methods for:

- **Backends**: `.memory()`, `.sqlite()`, `.redis()` (plugin), `.mongodb()` (plugin)
- **Runners**: `.thread_runner()`, `.multi_thread_runner()`, `.process_runner()`, `.persistent_process_runner()`
- **Serializers**: `.serializer_json()`, `.serializer_pickle()`, `.serializer_json_pickle()`
- **Concurrency**: `.concurrency_control()`, `.task_control()`
- **Advanced**: `.runner_tuning()`, `.max_pending_seconds()`, `.custom_config()`

### Configuration Precedence

When multiple configuration sources are used, they are applied in this order (later overrides earlier):

1. Default values in code
2. `pyproject.toml` or config file
3. Environment variables
4. Programmatic configuration (Builder)

This allows you to set sensible defaults in `pyproject.toml` and override specific values per environment using environment variables.

For a more comprehensive guide on setting up and running this example, visit our [Basic Redis Example on GitHub](https://github.com/pynenc/samples/tree/main/basic_redis_example).

```{important}
   Note that in Pynenc, tasks cannot be defined in Python modules intended to run as standalone scripts
   (where `__name__` is set to `"__main__"`). This includes modules executed directly or using the
   `python -m module` command. To learn more about this limitation and its implications, refer to the
   {doc}`../faq` or the detailed explanation in the {doc}`../usage_guide/index`.
```

## Requirements

Pynenc supports multiple backend options through its plugin system:

- **Memory Backend**: Built-in, no additional requirements (for development/testing, single-host only)
- **SQLite Backend**: Built-in, no additional requirements (for single-host testing with process runners)
- **Redis Backend**: Requires `pynenc-redis` plugin and a Redis server
- **MongoDB Backend**: Requires `pynenc-mongodb` plugin and a MongoDB server
- **RabbitMQ Backend**: Requires `pynenc-rabbitmq` plugin and a RabbitMQ server

Future Updates:

- Development plans include extending support to additional databases and message queues to broaden Pynenc's compatibility in various distributed systems.
