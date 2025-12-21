# Overview

## Pynenc: A Comprehensive Solution for Distributed Task Orchestration

Pynenc is designed as a versatile and powerful task management system, optimized for orchestrating complex tasks in distributed environments. This overview provides insights into Pynenc's key features, architectural design, and the future direction of its development.

## Core Features and Design Philosophy

Pynenc integrates a range of functionalities and features to streamline distributed task management:

- **Configurable Concurrency Management**: Pynenc offers concurrency control mechanisms across different levels of task execution, enhancing the efficiency and reliability of distributed systems.

- **Task Dependency Handling**: With mechanisms for task prioritization and automatic pausing, Pynenc adeptly manages dependencies to prevent bottlenecks and deadlocks.

- **Cycle Detection**: Pynenc identifies and resolve cyclical dependencies, ensuring robust task execution flows.

- **Modularity and Extensibility**: Pynenc's architecture is built for modularity, supporting a variety of components and allowing for seamless adaptation to diverse operational requirements.

## Invocation Status System

Pynenc uses a declarative, type-safe state machine to manage invocation lifecycles. Each invocation progresses through well-defined states with ownership tracking.

### All Invocation Statuses

| Status                         | Description                                                                |
| ------------------------------ | -------------------------------------------------------------------------- |
| `REGISTERED`                   | Task call has been routed and registered, available for runners to pick up |
| `PENDING`                      | Task was picked by a runner but not yet executed (owned by runner)         |
| `RUNNING`                      | Task is currently executing (owned by runner)                              |
| `PAUSED`                       | Task execution is paused (owned by runner)                                 |
| `RESUMED`                      | Task execution has been resumed after pause (owned by runner)              |
| `KILLED`                       | Task execution was terminated                                              |
| `RETRY`                        | Task finished with a retriable exception, available for re-execution       |
| `SUCCESS`                      | Task completed successfully (final)                                        |
| `FAILED`                       | Task finished with an exception (final)                                    |
| `REROUTED`                     | Task has been re-routed for execution                                      |
| `CONCURRENCY_CONTROLLED`       | Task blocked by concurrency control, waiting for reroute                   |
| `CONCURRENCY_CONTROLLED_FINAL` | Task permanently blocked by concurrency control (final)                    |
| `PENDING_RECOVERY`             | Recovering a PENDING task that exceeded timeout                            |
| `RUNNING_RECOVERY`             | Recovering a RUNNING task whose runner became inactive                     |

### Status Categories

**Available for Run** - Can be picked up by runners:

- `REGISTERED`, `REROUTED`, `RETRY`

**Owned by Runner** - Require ownership validation for transitions:

- `PENDING`, `RUNNING`, `PAUSED`, `RESUMED`

**Recovery Statuses** - Override ownership to recover stuck invocations:

- `PENDING_RECOVERY` - For tasks stuck in PENDING beyond `max_pending_seconds`
- `RUNNING_RECOVERY` - For tasks owned by runners that became inactive

**Final Statuses** - Terminate the invocation lifecycle:

- `SUCCESS`, `FAILED`, `CONCURRENCY_CONTROLLED_FINAL`

### Ownership Model

The status system implements an ownership model to ensure safe concurrent execution:

- **Acquires Ownership**: `PENDING` claims runner ownership when a task is picked up
- **Requires Ownership**: Only the owning runner can transition from `PENDING`, `RUNNING`, `PAUSED`, `RESUMED`
- **Releases Ownership**: Final statuses and recovery statuses release ownership
- **Overrides Ownership**: Recovery statuses bypass ownership validation for stuck invocations

This model ensures that tasks are not accidentally processed by multiple runners while enabling automatic recovery when runners fail.

## Current Implementations and Future Enhancements

- **Plugin Architecture**: Pynenc now uses a modular plugin system for backend implementations:

  - **Redis Plugin** (`pynenc-redis`): Production-ready distributed task management
  - **MongoDB Plugin** (`pynenc-mongodb`): Document-based storage with full feature support
  - **RabbitMQ Plugin** (`pynenc-rabbitmq`): Message queue-based broker for high-throughput scenarios
  - **Memory Backend**: Built-in development/testing mode for local execution
  - **SQLite Backend**: Built-in for single-host testing with process-compatible runners

- **Extensible Design**: The plugin system allows easy integration of additional databases, message queues, and services, enabling seamless adaptation to diverse operational requirements.

- **Planned Extensions**: Future development aims to incorporate additional backend plugins and technologies. This expansion will enhance Pynenc's adaptability and functionality in varied distributed environments.

## Performance Focus and Scalability

Pynenc emphasizes performance and scalability, essential traits for effective distributed task management:

- **High-Performance Execution**: Future iterations will explore the integration of runners written in high-performance languages, boosting the speed and efficiency of task processing.

- **Asynchronous Task Processing**: Pynenc is poised to adopt modern asynchronous programming models, enhancing its effectiveness in IO-bound operations and large-scale applications.

## Community Involvement and Open Development

Pynenc's growth is envisioned to be community-driven, encouraging contributions that enrich its capabilities:

- **Plugin Development**: The community can develop plugins for various backends, message brokers, and storage systems.
- **Integration of Message Brokers**: Plans include exploring integrations with various message brokers through the plugin system.
- **Community Contributions**: We welcome and encourage contributions from the community, ranging from new backend plugins to advanced runner implementations.

## Conclusion

Pynenc is more than a task management tool; it is a framework designed for innovation in distributed environments. Its development is guided by principles of clarity, simplicity, and versatility, ensuring each component's interoperability and the system's overall performance.

We invite the community to join us in advancing distributed task orchestration with Pynenc.
