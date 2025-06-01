# Goals for Pynenc Workflow Design

Primary goal is to enhance Pynenc, a distributed task orchestration framework, with a **workflow system** that supports both simple task grouping and complex, durable orchestration, while maintaining the framework’s simplicity, flexibility, and task-centric philosophy. This implementation do not use a dedicated workflow abstraction with restrictive methods, but integrates workflow capabilities directly into Pynenc’s existing task system. This allows users to build workflows explicitly using tasks, leveraging Pynenc’s features like triggers, retries, and caching, without introducing a heavy abstraction layer.

## What You’re Trying to Achieve

Create a **flexible, lightweight, and powerful workflow system** within Pynenc that:

- **Extends the task system**: Extension of Pynenc, using tasks as the core unit of work.
- **Supports durability**: Workflows should run for extended periods (seconds to years), resuming from the last known state after failures, with robust retry mechanisms.
- **Maintains simplicity**: Keep Pynenc’s task-centric, explicit approach, avoiding the restrictive abstractions and hidden workflow orchestration.
- **Offers flexibility**: Users can build simple task sequences or complex, event-driven workflows using the same primitives, with workflow-specific helpers for advanced features.
- **Ensures backward compatibility**: Existing Pynenc tasks should work unchanged, with workflows as an optional, non-disruptive feature.
- **Provides observability**: Enhanced monitoring tools should visualize workflow execution, task relationships, and failure details.
- **Other capabilities**: Adds other functionalities like durability, fault tolerance, state management in a way that fits Pynenc’s design.

### Specific Requirements

Based on your inputs, here are the detailed requirements for the workflow system:

1. **Task-Centric Workflows**:

   - Workflows are initiated by tasks, each associated with a `workflow_id` (defaulting to the task’s `task_id` or a user-defined alias).
   - Any task executed outside a workflow context automatically creates a new workflow (task-based workflow).
   - Tasks inherit the parent workflow’s context when called within a workflow.

2. **Durability and State Management**:

   - Workflows must persist state in the `state_backend` to resume from the last known state after failures.
   - Support for storing workflow events or payloads to track progress (e.g., via `task.wf.save_state(payload)`).
   - Result caching for tasks and subworkflows to avoid re-execution of completed steps.
   - Helpers to manage non-deterministic functions (e.g., `random`, `datetime`) by storing their results for consistent replay.

3. **Retries and Fault Tolerance**:

   - Workflow tasks should retry on failure, potentially with infinite retries (configurable via `workflow_max_retries`).
   - Support for user-defined retry policies (e.g., exceptions to retry, backoff strategies).
   - Automatic recovery from process or network failures using stored state.

4. **Workflow Interaction via Task Methods**:

   - Tasks access workflow functionality through a `task.wf` interface (e.g., `task_name.wf.some_workflow_related_method(...)`).
   - Methods for saving state, halting workflows, resuming execution, and querying subworkflow results.
   - Workflow-specific exceptions (e.g., `WorkflowHaltException`) to control execution flow.

5. **Event-Driven Execution**:

   - Leverage Pynenc’s trigger system to support event-based workflow continuations (e.g., resuming a workflow when a task completes).
   - Provide helpers to simplify trigger setup for common patterns (e.g., `wait_for_task`).

6. **Monitoring and Observability**:

   - Workflow-specific views in Pynenc’s monitoring tools, showing task trees, execution status, and failure details.
   - Support for visualizing task relationships and re-run capabilities.
   - Hierarchical representation of workflows and subworkflows.

7. **Idempotency and Error Handling**:

   - Ensure tasks are idempotent by default, with options to mark tasks as non-idempotent or invalidate caches.
   - Support for partial failure recovery (e.g., rollback or compensation logic).
   - Helpers to manage retry behavior and reuse previous results for identical subworkflows.

8. **Backward Compatibility and Configuration**:

   - Workflows are optional and disabled by default to avoid disrupting existing task-based applications.
   - Enable workflows via a simple `PynencBuilder` flag (e.g., `enable_workflows=True`).
   - Configurable default workflow parameters (e.g., retry policies) via `PynencBuilder` or task options.
   - Task-specific workflow options (e.g., starting a new workflow, custom `workflow_id`).

9. **Scalability and Performance**:

   - Minimize overhead for task-based workflows, ensuring they’re lightweight for simple use cases.
   - Optimize state persistence and resumption to avoid performance bottlenecks in long-running workflows.
   - Support concurrent workflow execution with configurable limits (e.g., preventing multiple instances of the same workflow).

10. **Other Features**:
    Incorporate key functionalities within Pynenc’s task system, including:
    - **Durability**: Long-running workflows with state resumption.
    - **Fault Tolerance**: Recovery from failures.
    - **Statefulness**: Internal state management without external databases.
    - **Long-Running Operations**: Support for workflows spanning months or years.
    - **Timers and Retries**: Configurable scheduling and retry policies.
    - **Scalability**: Handle large numbers of concurrent workflows.
    - **Deterministic Execution**: Ensure replayability by managing non-deterministic operations.
    - **Activities**: Support for external, non-deterministic tasks.
    - **Signals**: External inputs to modify workflow behavior.
    - **Queries**: Retrieve workflow state without affecting execution.
    - **Child Workflows**: Modular, hierarchical workflows.
    - **Versioning**: Update workflow logic for running instances.
    - **Monitoring and Observability**: Track and debug workflows.
    - **Error Handling**: Robust retry and recovery mechanisms.
    - **State Persistence**: Durable state storage.
    - **Dynamic Workflows**: Runtime logic modifications.
    - **Task Queues**: Load balancing and prioritization.
    - **Security**: Encryption and secure communication.
    - **Idempotency**: Safe retries.
    - **Execution Chains**: Support for long-running sequences.

---

## Trying to Avoid

- **Heavy Abstractions**: Explicit, task-based approach that doesn’t hide orchestration logic.
- **Complexity for Users**: Avoid forcing users to learn a new paradigm; workflows should feel like a natural extension of tasks.
- **Dependency on External Systems**: Pynenc should remain self-contained, using its `state_backend` for persistence.
- **Disruption to Existing Code**: Ensure workflows don’t break existing task-based applications.

---

## Key Challenges

- **Balancing Simplicity and Power**: Providing advanced workflow features (e.g., durability, event-driven execution) while keeping tasks intuitive.
- **User Responsibility**: Ensuring users can easily manage state, retries, and idempotency without excessive boilerplate.
- **Performance**: Minimizing overhead for simple workflows while supporting complex, long-running ones.
- **Debugging and Observability**: Making workflows easy to monitor and debug, despite their distributed nature.

---

## Summary

Goals are clear: design workflow system that’s powerful and deeply integrated with Pynenc’s task system, preserving its simplicity and flexibility. The requirements emphasize durability, user control, and observability, with a focus on leveraging existing features like triggers and caching. By avoiding a separate `Workflow` class, the design is staying true to Pynenc’s philosophy, but it need to ensure that helper methods (e.g., `task.wf`) are intuitive and that documentation guides users effectively.
