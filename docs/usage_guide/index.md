# Usage Guide

This Usage Guide is designed to provide you with detailed instructions and practical examples to harness the full potential of Pynenc in various scenarios. Whether you're a beginner or an advanced user, this guide aims to help you navigate through the features and functionalities of Pynenc with ease.

```{toctree}
:hidden:
:maxdepth: 2
:caption: Detailed Use Cases

./use_case_001_basic_local_threaded
./use_case_002_basic_redis
./use_case_003_concurrency_control
./use_case_004_auto_orchestration
./use_case_005_sync_unit_testing
./use_case_006_mem_unit_testing
./use_case_007_json_serializable
./use_case_008_direct_task
./use_case_009_client_data_store
./use_case_010_trigger_system
./use_case_011_workflow_system
./invocation_status
```

## Getting Started with Pynenc

Before diving into specific use cases, ensure that you have Pynenc installed and configured correctly in your environment. Refer to the _Getting Started_ section for installation instructions and initial setup.

## Best Practices for Task Definition

```{important}
    When defining tasks in Pynenc, it is crucial to avoid creating tasks in modules that are designed to run
    as standalone scripts. In Python, if a module is run directly (either as a script or via `python -m module`),
    its `__name__` attribute is set to `"__main__"`. This can cause issues in a distributed environment like Pynenc,
    where the `__main__` module refers to the worker process, leading to difficulties in task identification and execution.
    For more information on this limitation and how to structure your tasks correctly, refer to the [FAQ section](faq.rst).
```

## Use Case Scenarios

Each section below gives a self-contained summary with a key code snippet.
Use the cards to jump to any full step-by-step guide, or follow the quick link
directly beneath each section heading.

::::{grid} 1 2 2 2
:gutter: 2

:::{grid-item-card} 1 · Basic Local Threaded
:link: use_case_001_basic_local_threaded
:link-type: doc
:shadow: sm
Run your first task with `ThreadRunner` — no infrastructure required.
:::

:::{grid-item-card} 2 · Distributed with Redis
:link: use_case_002_basic_redis
:link-type: doc
:shadow: sm
Scale out with `pynenc-redis` and `ProcessRunner` in a Docker environment.
:::

:::{grid-item-card} 3 · Concurrency Control
:link: use_case_003_concurrency_control
:link-type: doc
:shadow: sm
Prevent duplicate work with `TASK`-level and `DISABLED` concurrency modes.
:::

:::{grid-item-card} 4 · Auto Orchestration
:link: use_case_004_auto_orchestration
:link-type: doc
:shadow: sm
Fibonacci via recursive tasks — Pynenc resolves dependencies automatically.
:::

:::{grid-item-card} 5 · Sync Unit Testing
:link: use_case_005_sync_unit_testing
:link-type: doc
:shadow: sm
One flag forces synchronous execution for simple `assertEqual` tests.
:::

:::{grid-item-card} 6 · Mem Mode Testing
:link: use_case_006_mem_unit_testing
:link-type: doc
:shadow: sm
Full in-memory stack with `ThreadRunner` — CI-friendly, zero infrastructure.
:::

:::{grid-item-card} 7 · JSON Serialization
:link: use_case_007_json_serializable
:link-type: doc
:shadow: sm
Add `to_json` / `from_json` to any class for `JsonSerializer` compatibility.
:::

:::{grid-item-card} 8 · Direct Task
:link: use_case_008_direct_task
:link-type: doc
:shadow: sm
Distribute existing functions with `@app.direct_task` — no call-site changes.
:::

:::{grid-item-card} 9 · Client Data Store
:link: use_case_009_client_data_store
:link-type: doc
:shadow: sm
Automatically cache large arguments to reduce serialization overhead at scale.
:::

:::{grid-item-card} 10 · Trigger System
:link: use_case_010_trigger_system
:link-type: doc
:shadow: sm
Schedule tasks or react to events, status changes, results, and exceptions.
:::

:::{grid-item-card} 11 · Workflow System
:link: use_case_011_workflow_system
:link-type: doc
:shadow: sm
Deterministic, replayable, stateful workflows with automatic failure recovery.
:::

:::{grid-item-card} Invocation Status
:link: invocation_status
:link-type: doc
:shadow: sm
State machine lifecycle, ownership tracking, and recovery mechanics.
:::
::::

## Use Case 1: Basic Local Threaded Demonstration

📖 {doc}`Full step-by-step guide <./use_case_001_basic_local_threaded>`

Learn the basics of setting up and executing tasks using Pynenc in a local, non-distributed environment. This use case is ideal for understanding the fundamental workings of Pynenc, especially for development and testing purposes.

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    add.logger.info(f"{add.task_id=} Adding {x} + {y}")
    return x + y

```

For a detailed guide and example, see {doc}`./use_case_001_basic_local_threaded`.

## Use Case 2: Distributed System with Redis and Process Runner

📖 {doc}`Full step-by-step guide <./use_case_002_basic_redis>`

Explore setting up a distributed task processing system using `pynenc` with Redis. This use case demonstrates how to configure and run tasks in a distributed environment, leveraging the Redis plugin for task queuing and the ProcessRunner for executing tasks across multiple processes.

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

**Prerequisites**: This use case requires the Redis plugin:

```bash
pip install pynenc-redis
```

Configuration is key to integrating Redis with `pynenc`, as shown in the `pyproject.toml` setup. This setup enables tasks to be queued and processed in a truly distributed manner.

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

Tasks are executed through a simple Python script that triggers them. Running the `pynenc` worker and executing tasks can be done in a local development environment or within Docker for a more isolated setup.

Execute tasks directly or in parallel to understand the power of distributed task processing with `pynenc` and Redis. Also, explore running the system in development mode for debugging and testing without the need for Redis.

For a detailed guide and example, see {doc}`./use_case_002_basic_redis`.

## Use Case 3: Concurrency Control

📖 {doc}`Full step-by-step guide <./use_case_003_concurrency_control>`

Dive into the mechanics of concurrency control within Pynenc. This use case demonstrates various settings for concurrency control, such as disabling concurrent execution or enforcing task-level concurrency, to ensure tasks are executed according to specific requirements.

```python
from pynenc import Pynenc, ConcurrencyControlType

app = Pynenc()

@app.task(registration_concurrency=ConcurrencyControlType.DISABLED)
def get_own_invocation_id() -> str:
    return get_own_invocation_id.invocation.invocation_id

@app.task(registration_concurrency=ConcurrencyControlType.TASK)
def get_own_invocation_id_registration_concurrency() -> str:
    return get_own_invocation_id_registration_concurrency.invocation.invocation_id

@app.task(running_concurrency=ConcurrencyControlType.DISABLED)
def sleep_without_running_concurrency(seconds: float) -> SleepResult:
    start = time.time()
    time.sleep(seconds)
    return SleepResult(start=start, end=time.time())

@app.task(running_concurrency=ConcurrencyControlType.TASK)
def sleep_with_running_concurrency(seconds: float) -> SleepResult:
    start = time.time()
    time.sleep(seconds)
    return SleepResult(start=start, end=time.time())
```

Through practical examples, see how tasks behave differently under various concurrency controls.

For a detailed guide and examples, see {doc}`./use_case_003_concurrency_control`.

## Use Case 4: Automatic Orchestration

📖 {doc}`Full step-by-step guide <./use_case_004_auto_orchestration>`

Delve into the advanced features of Pynenc with the Automatic Orchestration use case, showcasing the library's ability to manage task dependencies. This scenario uses the well-known Fibonacci sequence to illustrate how Pynenc automatically orchestrates the execution of dependent tasks, ensuring that tasks are executed in the correct sequence without manual intervention.

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def fibonacci(n: int) -> int:
    fibonacci.logger.info(f"Calculating fibonacci({n})")
    if n <= 1:
        return n
    else:
        return fibonacci(n - 1).result + fibonacci(n - 2).result
```

This use case highlights how tasks that depend on the results of previous tasks can be executed seamlessly, showcasing Pynenc's capability to pause and resume tasks as needed based on their dependencies. It's an ideal demonstration for understanding Pynenc's orchestration mechanisms in scenarios with complex task dependencies.

For a detailed guide and examples, see {doc}`./use_case_004_auto_orchestration`.

## Use Case 5: Unit Testing with Synchronous Mode

📖 {doc}`Full step-by-step guide <./use_case_005_sync_unit_testing>`

Discover the simplicity of unit testing Pynenc tasks using the synchronous execution mode. This approach facilitates testing by executing tasks sequentially within the test process, allowing for straightforward assertion of task outcomes without the need for an asynchronous execution environment.

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    add.logger.info(f"{add.task_id=} Adding {x} + {y}")
    return x + y
```

The synchronous execution mode is particularly useful for testing tasks in isolation, ensuring that their logic functions as expected without external dependencies or the complexity of an asynchronous runtime.

To enable synchronous mode during testing, you can directly configure the Pynenc application within your test setup or use environment variables to adjust the runtime behavior.

```python
import unittest
from unittest.mock import patch

import tasks

class TestAddTask(unittest.TestCase):
    def setUp(self) -> None:
        # Enable synchronous task execution
        tasks.app.conf.dev_mode_force_sync_tasks = True

    def test_add_functionality(self) -> None:
        # Test the add task
        result = tasks.add(1, 2).result
        self.assertEqual(result, 3)
```

This use case is instrumental in demonstrating how Pynenc's design accommodates unit testing, promoting testability and reliability of task-based applications. It underscores Pynenc's adaptability to development workflows, ensuring tasks can be thoroughly tested in a simplified execution context.

For a detailed guide and examples, see {doc}`./use_case_005_sync_unit_testing`.

## Use Case 6: Unit Testing with Mem Mode

📖 {doc}`Full step-by-step guide <./use_case_006_mem_unit_testing>`

Explore the efficiency of unit testing Pynenc tasks using the `Mem` mode, where all operational components such as broker, orchestrator, and state backend utilize in-memory implementations. This approach enables fast, isolated testing without the reliance on external infrastructure, making it ideal for rapid development cycles and CI/CD pipelines.

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    add.logger.info(f"{add.task_id=} Adding {x} + {y}")
    return x + y
```

Leveraging `Mem` mode for unit testing streamlines the testing process, ensuring tasks are executed in a controlled, predictable manner. This method is particularly beneficial for validating task logic and behavior under various conditions without the overhead of configuring external services or dealing with asynchronous execution complexities.

```python
import unittest
import tasks

class TestAddTaskMemMode(unittest.TestCase):
    def setUp(self) -> None:
        # Set Pynenc to use in-memory components for testing
        tasks.app.conf.dev_mode_force_sync_tasks = False
        tasks.app.conf.orchestrator_cls = 'MemOrchestrator'
        tasks.app.conf.broker_cls = 'MemBroker'
        tasks.app.conf.state_backend_cls = 'MemStateBackend'
        tasks.app.conf.runner_cls = 'ThreadRunner'

        # Start the runner in a separate thread for asynchronous task execution
        self.thread = threading.Thread(target=tasks.app.runner.run, daemon=True)
        self.thread.start()

    def tearDown(self):
        # Ensure the runner is stopped after tests
        tasks.app.runner.stop_runner_loop()
        self.thread.join()

    def test_add_in_mem_mode(self) -> None:
        # Test the add task under Mem mode
        result = tasks.add(1, 2).result
        self.assertEqual(result, 3)
```

Explore unit testing of Pynenc tasks using `Mem` mode, which employs in-memory components for brokers, orchestrators, and state backends. This mode offers a swift and isolated testing approach, devoid of external dependencies, perfect for CI/CD pipelines and swift development cycles.

This use case also demonstrates configuring Pynenc through environment variables for in-memory testing, providing an alternative method to adjust runtime behavior for tests.

For a detailed guide and examples, see {doc}`./use_case_006_mem_unit_testing`.

## Use Case 7: Custom JSON Serialization with `JsonSerializable`

📖 {doc}`Full step-by-step guide <./use_case_007_json_serializable>`

When using `JsonSerializer`, any task argument or return value that is not a JSON primitive
will raise a `TypeError` — unless the class implements the `JsonSerializable` protocol.
This protocol is a lightweight alternative to switching serializers: add two methods to
your domain object and Pynenc handles the rest automatically.

```python
from pynenc.serializer import JsonSerializable

class Money:
    def __init__(self, amount: float, currency: str) -> None:
        self.amount = amount
        self.currency = currency

    def to_json(self) -> dict:
        return {"amount": self.amount, "currency": self.currency}

    @classmethod
    def from_json(cls, data: dict) -> "Money":
        return cls(data["amount"], data["currency"])
```

With both methods in place, any Pynenc task can accept and return `Money` values
without further configuration:

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def calculate_total(unit_price: Money, quantity: int) -> Money:
    return Money(unit_price.amount * quantity, unit_price.currency)
```

The serializer embeds the class's module and qualified name alongside the data so
that deserialization reconstructs the exact original type — not a plain `dict`.

| Method                         | Direction     | Purpose                                               |
| ------------------------------ | ------------- | ----------------------------------------------------- |
| `to_json(self) -> Any`         | object → JSON | Return a JSON-native value (`dict`, `list`, `str`, …) |
| `from_json(cls, data) -> Self` | JSON → object | Reconstruct from the value `to_json` returned         |

Because `JsonSerializable` is `@runtime_checkable`, compliance can be verified at
runtime with a standard `isinstance` check.

For a detailed guide and examples, see {doc}`./use_case_007_json_serializable`.

## Use Case: Direct Task — Distribute Without Refactoring

📖 {doc}`Full step-by-step guide <./use_case_008_direct_task>`

`@app.direct_task` lets you distribute existing Python functions across workers
without changing their call sites. The decorated function still returns its value
directly — no `Invocation`, no `.result`. Toggle sync execution with
`PYNENC__DEV_MODE_FORCE_SYNC_TASKS=True` to make the decorator transparent during
local development.

```python
from pynenc import Pynenc

app = Pynenc()

@app.direct_task
def analyze_item(item: str) -> dict:
    return {"item": item, "length": len(item)}

# Caller is unchanged — returns the dict directly
result = analyze_item("apple")
```

For caller-side parallelism, use a `ThreadPoolExecutor`. For single-call fan-out,
declare `parallel_func` and `aggregate_func` on the decorator.

For a detailed guide and examples, see {doc}`./use_case_008_direct_task`.

## Use Case 8: Customizing Data Serialization

Pynenc provides built-in support for common serialization formats through its serializer classes:

- **JsonPickleSerializer** (default): Preserves Python object types using the `jsonpickle` library. Best for internal persistence with trusted data.
- **JsonSerializer**: Pure JSON serialization for interoperability.
- **PickleSerializer**: Native Python pickle serialization for complex objects.

```{warning}
The `jsonpickle` serializer can reconstruct arbitrary Python objects on deserialization — use it only for trusted, internal persistence (local state backends).
```

### Creating Custom Serializers

You can create a custom serializer to handle any specific requirements of your tasks. This could be necessary when dealing with complex data types that are not natively supported by JSON or Pickle, or if you need to integrate with external systems that use a different data format.

To create a custom serializer, you need to subclass the `BaseSerializer` and implement the required serialization and deserialization methods. Here's a simplified example:

```{code-block} python
    from pynenc.serializer import BaseSerializer

    class CustomSerializer(BaseSerializer):
        def serialize(self, obj):
            # Implement custom serialization logic
            return serialized_obj

        def deserialize(self, serialized_obj):
            # Implement custom deserialization logic
            return obj
```

### Configuring Your Custom Serializer

Once your custom serializer is implemented, you can configure Pynenc to use it:

Using the builder:

```{code-block} python
    from pynenc.builder import PynencBuilder

    app = PynencBuilder().custom_config(serializer_cls="path.to.CustomSerializer").build()
```

Or using environment variables:

```{code-block} bash
    PYNENC__SERIALIZER_CLS="path.to.CustomSerializer"
```

For more details on configuration options, refer to the {doc}`../configuration/index`.

## Use Case 9: Client Data Store

📖 {doc}`Full step-by-step guide <./use_case_009_client_data_store>`

Pynenc's client data store optimizes task execution by efficiently handling large serialized arguments.

```python
from pynenc import Pynenc
import numpy as np

app = Pynenc()

@app.task
def process_array(data: np.ndarray) -> float:
    """Process a large numpy array with automatic argument caching."""
    return float(data.mean())

# Large arrays will be automatically cached based on size threshold
large_array = np.random.rand(1000000)
result = process_array(large_array)
```

The argument caching system offers several key features:

- Automatic caching of large arguments based on configurable size thresholds
- Multiple backend options (Redis for distributed, Memory for local development)
- Process-safe shared caching through runner-level storage
- Smart detection to prevent redundant serialization
- Fine-grained control over caching behavior per task and argument
- LRU cache management for optimal memory usage

Configure the caching behavior through simple configuration settings:

```toml
[tool.pynenc.client_data_store]
min_size_to_cache = 1024  # Cache arguments larger than 1KB
local_cache_size = 1000   # Keep 1000 most recent entries
```

This use case demonstrates how Pynenc's argument caching can significantly improve performance in distributed systems by reducing network traffic and serialization overhead.

For a detailed guide and examples, see {doc}`./use_case_009_client_data_store`.

## Use Case 10: Trigger System

📖 {doc}`Full step-by-step guide <./use_case_010_trigger_system>`

Explore Pynenc's trigger system, which enables declarative task scheduling and event-driven execution. A trigger belongs to the task that reacts, so the upstream caller does not have to wire callbacks or build chains every time it enqueues work.

```python
from pynenc import Pynenc
from pynenc.invocation.status import InvocationStatus
from pynenc.trigger.trigger_builder import TriggerBuilder

app = Pynenc()

@app.task
def source_task(x: int) -> str:
    return f"Processed {x}"

# Define a task that runs when source_task completes successfully.
@app.task(
    triggers=TriggerBuilder().on_status(
        source_task, statuses=[InvocationStatus.SUCCESS]
    )
)
def notification_task() -> str:
    return "Source task completed successfully"
```

The trigger system provides a comprehensive framework for automating workflows with features including:

- Diverse trigger conditions (cron expressions, task statuses, results, exceptions, events)
- Flexible argument handling with providers that can generate task arguments dynamically
- Conditional execution with filters based on arguments, results, or payload content
- Composite conditions using AND/OR logic for complex triggering rules

This use case demonstrates how to create reactive task graphs that respond to system events and task outcomes, reducing the need for manual orchestration. For a runnable end-to-end version, see the external [`trigger_demo` sample](https://github.com/pynenc/samples/tree/main/trigger_demo), which runs with SQLite and no external services.

For a detailed guide and examples, see {doc}`./use_case_010_trigger_system`.

## Use Case 11: Workflow System

📖 {doc}`Full step-by-step guide <./use_case_011_workflow_system>`

Discover Pynenc's workflow system for deterministic, stateful task orchestration. Workflows are explicit `@app.workflow` functions with shared workflow data, root-only deterministic operations, sub-workflow boundaries, and child invocations that can be reused when the workflow invocation is retried.

```python
from typing import Any

from pynenc import Pynenc

app = Pynenc()

@app.workflow
def process_order_workflow(order_id: str) -> dict[str, Any]:
    payment_id = process_order_workflow.wf.root.uuid()
    tracking_number = (
        f"TRK-{order_id}-"
        f"{int(process_order_workflow.wf.root.random() * 100000):05d}"
    )
    payment_result = process_order_workflow.wf.root.execute_task(
        process_payment, order_id, payment_id
    )

    process_order_workflow.wf.set_data("tracking_number", tracking_number)
    process_order_workflow.wf.set_data("payment_id", payment_result.result["payment_id"])
    process_order_workflow.wf.set_data("status", "paid")

    return {
        "order_id": order_id,
        "tracking_number": tracking_number,
        "payment_status": payment_result.result["status"],
        "workflow_id": str(process_order_workflow.wf.identity.workflow_id),
    }


@app.task
def process_payment(order_id: str, payment_id: str) -> dict[str, str]:
    return {
        "payment_id": payment_id,
        "status": "approved",
    }
```

The workflow system provides the core pieces needed for durable orchestration:

- **Deterministic execution**: `wf.root.random()`, `wf.root.uuid()`, and `wf.root.utc_now()` replay stored values when the workflow invocation is retried
- **Workflow identity**: explicit workflow invocations define workflow runs, with optional parent workflow tracking for sub-workflows
- **Workflow data**: `wf.set_data()` and `wf.get_data()` persist workflow-scoped milestones
- **Durable child calls**: `wf.root.execute_task()` records child invocations by task and arguments
- **Workflow boundaries**: `@app.workflow` creates explicit main workflows or sub-workflows

This use case demonstrates how to build stateful workflows that can retry without duplicating child work that already completed.

For a practical guide, see {doc}`./use_case_011_workflow_system`.
For the deeper model, see {doc}`../workflows/index`.

## Invocation Status System

📖 {doc}`Full reference guide <./invocation_status>`

Pynenc uses a declarative, type-safe state machine to manage the lifecycle of task invocations. This system provides:

- **Ownership Tracking**: Each invocation is owned by a specific runner during execution
- **Valid State Transitions**: The state machine enforces which transitions are allowed
- **Automatic Recovery**: Stuck invocations are automatically recovered when runners become inactive
- **Concurrency Control Integration**: Status transitions integrate with concurrency control rules

Key status categories include:

- **Available for Run**: `REGISTERED`, `REROUTED`, `RETRY`
- **Owned by Runner**: `PENDING`, `RUNNING`, `PAUSED`
- **Recovery**: `PENDING_RECOVERY`, `RUNNING_RECOVERY`
- **Final**: `SUCCESS`, `FAILED`, `CONCURRENCY_CONTROLLED_FINAL`

For a detailed guide on the status system, state diagram, and recovery mechanisms, see {doc}`./invocation_status`.
