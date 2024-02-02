# Usage Guide

This Usage Guide is designed to provide you with detailed instructions and practical examples to harness the full potential of Pynenc in various scenarios. Whether you're a beginner or an advanced user, this guide aims to help you navigate through the features and functionalities of Pynenc with ease.

```{toctree}
:hidden:
:maxdepth: 2
:caption: Detailed Use Cases

./use_case_001_basic_local_threaded
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

Explore how to set up a distributed Pynenc system using Redis for message brokering and a process-based runner for task execution. This guide will include installation, configuration, and examples of distributed task processing (simple call and parallelization).

For a detailed guide and example, see {doc}`./use_case_002_basic_redis`.

## Use Case 3: Concurrency Control

Pynenc implements concurrency control to avoid concurrent execution and limit routing of similar tasks.

For a detailed guide and example, see {doc}`./use_case_003_concurrency_control`.

## Use Case 4: Running Locally with Synchronous Mode

Discover how to run Pynenc in synchronous mode for local development. This mode runs tasks sequentially, simplifying debugging and initial development.

## Use Case 5: Unit Testing with Synchronous Mode

Find out how to write unit tests for your Pynenc tasks using synchronous mode. This mode allows for straightforward testing by running tasks sequentially and simplifying assertions.

## Use Case 6: Unit Testing with Mem Mode

This section will guide you on how to perform unit testing in `Mem` mode, where all components (broker, orchestrator, etc.) use in-memory implementations. It's a fast way to test without external dependencies.

## Use Case 7: Extending Pynenc for Your Needs

Pynenc's modular design is aimed at providing a flexible framework that can be easily extended to meet your specific requirements. Whether you have a unique use case or need to integrate Pynenc with other systems, this section will guide you through customizing and extending its capabilities.

## Creating Custom Components

Pynenc is built with extensibility in mind, allowing you to create custom components to suit your specific needs. This can be done by subclassing the base classes provided by Pynenc:

- **Custom Orchestrators**: Create a subclass of `BaseOrchestrator` or `RedisOrchestrator` to implement custom orchestration logic.
- **Custom State Backends**: Develop a state backend that aligns with your data storage and retrieval needs by subclassing the `BaseStateBackend`.
- **Custom Brokers**: Design a message broker tailored to your messaging and communication requirements.
- **Custom Runners**: Build a runner that matches your execution environment and task management style.

## Configuring Your Custom Components

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

## Creating Custom Serializers

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

## Configuring Your Custom Serializer

Once your custom serializer is implemented, you can configure Pynenc to use it just like any other component:

```{code-block} python
    PYNENC__SERIALIZER_CLS="path.to.CustomSerializer"
```

This is just one way to set the configuration. Pynenc allows various methods to configure your application, including environment variables, config files, or directly in code. For more details on configuration options, refer to the {doc}`../configuration/index`.
