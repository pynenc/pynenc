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
./use_case_009_argument_caching
./use_case_010_trigger_system
./use_case_011_workflow_system
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

Each use case in this guide represents a typical scenario where Pynenc can significantly streamline your task management and orchestration processes.

## Use Case 1: Basic Local Threaded Demonstration

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

Explore setting up a distributed task processing system using `pynenc` with Redis. This use case demonstrates how to configure and run tasks in a distributed environment, leveraging Redis for task queuing and the ProcessRunner for executing tasks across multiple processes.

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

## Use Case 7: Extending Pynenc for Your Needs

Pynenc's modular design is aimed at providing a flexible framework that can be easily extended to meet your specific requirements. Whether you have a unique use case or need to integrate Pynenc with other systems, this section will guide you through customizing and extending its capabilities.

### Creating Custom Components

Pynenc is built with extensibility in mind, allowing you to create custom components to suit your specific needs. This can be done by subclassing the base classes provided by Pynenc:

- **Custom Orchestrators**: Create a subclass of `BaseOrchestrator` or `RedisOrchestrator` to implement custom orchestration logic.
- **Custom State Backends**: Develop a state backend that aligns with your data storage and retrieval needs by subclassing the `BaseStateBackend`.
- **Custom Brokers**: Design a message broker tailored to your messaging and communication requirements.
- **Custom Runners**: Build a runner that matches your execution environment and task management style.

### Configuring Your Custom Components

Integrating your custom components into your Pynenc application is straightforward. While one way to specify these components is through environment variables, it is important to note that this is just one of several methods available.

To configure using environment variables:

```{code-block} python
    PYNENC__ORCHESTRATOR_CLS="path.to.CustomOrchestrator"
    PYNENC__STATE_BACKEND_CLS="path.to.CustomStateBackend"
    PYNENC__BROKER_CLS="path.to.CustomBroker"
    PYNENC__RUNNER_CLS="path.to.CustomRunner"
```

Replace `path.to.CustomComponent` with the actual import path of your custom component. This method is particularly useful for deploying applications in different environments without modifying the code.

For a comprehensive guide on all available configuration options, including file-based and code-based configurations, please refer to the {doc}`../configuration/index`. This document will provide you with detailed information on tailoring Pynenc to meet your specific requirements.

By offering various configuration methods, Pynenc ensures flexibility and ease of adaptation to a wide range of use cases, environments, and integration requirements.

## Use Case 8: Customizing Data Serialization

Pynenc provides built-in support for common serialization formats like JSON and Pickle through its `JsonSerializer` and `PickleSerializer` classes. However, there might be scenarios where these standard serializers are not suitable for your specific needs, particularly when working with complex objects or requiring a different serialization strategy.

### Creating Custom Serializers

You can create a custom serializer to handle any specific requirements of your tasks. This could be necessary when dealing with complex data types that are not natively supported by JSON or Pickle, or if you need to integrate with external systems that use a different data format.

To create a custom serializer, you need to subclass the `BaseSerializer` and implement the required serialization and deserialization methods. Here's a simplified example:

```{code-block} python
    from pynenc.serializers import BaseSerializer

    class CustomSerializer(BaseSerializer):
        def serialize(self, obj):
            # Implement custom serialization logic
            return serialized_obj

        def deserialize(self, serialized_obj):
            # Implement custom deserialization logic
            return obj
```

### Configuring Your Custom Serializer

Once your custom serializer is implemented, you can configure Pynenc to use it just like any other component:

```{code-block} python
    PYNENC__SERIALIZER_CLS="path.to.CustomSerializer"
```

This is just one way to set the configuration. Pynenc allows various methods to configure your application, including environment variables, config files, or directly in code. For more details on configuration options, refer to the {doc}`../configuration/index`.

## Use Case 9: Argument Caching

Discover Pynenc's argument caching system, designed to optimize task execution by efficiently handling large serialized arguments. This feature is particularly valuable when working with substantial data objects that are frequently passed between distributed tasks.

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
[tool.pynenc.arg_cache]
min_size_to_cache = 1024  # Cache arguments larger than 1KB
local_cache_size = 1000   # Keep 1000 most recent entries
```

This use case demonstrates how Pynenc's argument caching can significantly improve performance in distributed systems by reducing network traffic and serialization overhead.

For a detailed guide and examples, see {doc}`./use_case_009_argument_caching`.

## Use Case 10: Trigger System

Explore Pynenc's powerful trigger system, which enables declarative task scheduling and event-driven workflows. This feature allows you to automatically execute tasks in response to various conditions such as scheduled times, task status changes, results, exceptions, or custom events.

```python
from pynenc import Pynenc
from pynenc.trigger.trigger_builder import TriggerBuilder

app = Pynenc()

@app.task
def source_task(x: int) -> str:
    return f"Processed {x}"

# Define a task that runs when source_task completes successfully
@app.task(triggers=TriggerBuilder().on_status(source_task, statuses=["SUCCESS"]))
def notification_task() -> str:
    return "Source task completed successfully"
```

The trigger system provides a comprehensive framework for automating workflows with features including:

- Diverse trigger conditions (cron expressions, task statuses, results, exceptions, events)
- Flexible argument handling with providers that can generate task arguments dynamically
- Conditional execution with filters based on arguments, results, or payload content
- Composite conditions using AND/OR logic for complex triggering rules

This use case demonstrates how to create self-managing workflows that respond to system events and task outcomes, reducing the need for manual orchestration.

For a detailed guide and examples, see {doc}`./use_case_010_trigger_system`.

## Use Case 11: Workflow System

Discover Pynenc's advanced workflow system for building sophisticated task orchestration with deterministic execution, state management, and automatic replay capabilities. The workflow system enables complex multi-step processes that can recover from failures and maintain consistency across distributed environments.

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def process_order_workflow(order_id: str) -> dict[str, Any]:
    """
    Order processing workflow with deterministic operations.

    All random operations and timestamps are deterministic and will
    replay identically during failure recovery.
    """
    # Generate deterministic tracking number
    tracking_number = f"TRK-{order_id}-{int(process_order_workflow.wf.random() * 100000):05d}"

    # Execute payment processing within workflow context
    payment_result = process_order_workflow.wf.execute_task(
        process_payment, order_id
    )

    # Store workflow state for persistence
    process_order_workflow.wf.set_data("tracking_number", tracking_number)
    process_order_workflow.wf.set_data("payment_id", payment_result.result["payment_id"])

    return {
        "order_id": order_id,
        "tracking_number": tracking_number,
        "payment_status": payment_result.result["status"],
        "workflow_id": process_order_workflow.workflow.workflow_id
    }

@app.task(force_new_workflow=True)
def independent_audit_workflow(order_id: str) -> dict[str, Any]:
    """
    Independent audit workflow with force_new_workflow=True.

    This always creates a new workflow context regardless of calling context.
    """
    audit_id = independent_audit_workflow.wf.uuid()
    return {
        "audit_id": audit_id,
        "order_id": order_id,
        "workflow_id": independent_audit_workflow.workflow.workflow_id
    }
```

The workflow system provides essential features for enterprise-grade task orchestration:

- **Deterministic Execution**: All non-deterministic operations (random, UUID, timestamps) are made deterministic for perfect replay
- **Workflow Identity**: Unique workflow contexts with parent-child relationships and inheritance
- **State Persistence**: Automatic state management with key-value storage for workflow data
- **Task Integration**: Seamless integration with existing Pynenc tasks and execution infrastructure
- **Workflow Boundaries**: Use `force_new_workflow=True` decorator to create independent workflow contexts
- **Failure Recovery**: Workflows can resume from exact points of failure with identical replay behavior

This use case demonstrates how to build robust, stateful workflows that can handle complex business logic while providing reliability guarantees and failure recovery capabilities.

For a detailed guide and examples, see {doc}`./use_case_011_workflow_system`.
