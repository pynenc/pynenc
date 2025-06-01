# Workflow System

## Overview

The Pynenc Workflow System enables sophisticated task orchestration with deterministic execution, state management, and automatic replay capabilities. It provides a foundation for building complex, multi-step workflows that can recover from failures and maintain consistency across distributed environments.

## Philosophy and Design

### Core Principles

1. **Deterministic Execution**: All non-deterministic operations (random numbers, timestamps, UUIDs) are made deterministic through seeded generation and state storage, enabling perfect replay of workflow execution.

2. **Workflow Identity**: Each workflow has a unique identity that tracks its lineage and relationships, allowing for hierarchical workflow structures and context inheritance.

3. **State Persistence**: Workflow state is automatically persisted, enabling recovery from failures and resumption from exact points of interruption.

4. **Task Integration**: Workflows seamlessly integrate with Pynenc's existing task system, allowing any task to participate in workflow execution.

### Design Concepts

- **Workflow Context**: Tasks executed within a workflow automatically inherit the workflow context, enabling access to workflow-specific operations and state.

- **Parent-Child Relationships**: Workflows can spawn child workflows, creating hierarchical structures while maintaining clear boundaries and inheritance rules.

- **Deterministic Operations**: Built-in support for deterministic random numbers, timestamps, UUIDs, and task execution that replay identically.

- **Automatic State Management**: The system automatically manages workflow state without requiring explicit state handling in user code.

## Scenario

Consider an e-commerce order processing system that needs to:

1. Process payment with retry logic
2. Generate unique tracking numbers
3. Schedule delivery notifications
4. Handle complex business logic with multiple decision points
5. Maintain audit trails and enable recovery from failures

The workflow system ensures that even if the system crashes mid-processing, the workflow can resume from exactly where it left off, with all random numbers, timestamps, and business decisions replaying identically.

## Implementation

### Basic Workflow Usage

```python
from pynenc import Pynenc
from datetime import timedelta
from typing import Any

app = Pynenc()

@app.task(force_new_workflow=True)
def process_order_workflow(order_id: str) -> dict[str, Any]:
    """
    Main order processing workflow with explicit workflow boundary.

    Using force_new_workflow=True ensures this task always creates a new workflow
    context, providing access to deterministic operations and state management.
    """
    # All random operations are deterministic and will replay identically
    tracking_number = generate_tracking_number(order_id)

    # Process payment with deterministic retry logic
    payment_result = process_payment_workflow.wf.execute_task(
        process_payment, order_id
    )

    # Schedule notifications using deterministic timestamps
    notification_time = process_order_workflow.wf.utc_now() + timedelta(hours=1)

    # Store workflow data for later retrieval
    process_order_workflow.wf.set_data("tracking_number", tracking_number)
    process_order_workflow.wf.set_data("notification_time", notification_time.isoformat())

    return {
        "order_id": order_id,
        "tracking_number": tracking_number,
        "payment_status": payment_result.result,
        "notification_scheduled": notification_time.isoformat()
    }

def generate_tracking_number(order_id: str) -> str:
    """Generate a deterministic tracking number."""
    # This random number will be the same on replay
    random_suffix = int(process_order_workflow.wf.random() * 100000)
    return f"TRK-{order_id}-{random_suffix:05d}"

@app.task
def process_payment(order_id: str) -> dict[str, Any]:
    """Process payment for an order."""
    # Simulate payment processing
    payment_id = process_payment.wf.uuid()
    processing_time = process_payment.wf.utc_now()

    return {
        "payment_id": payment_id,
        "processed_at": processing_time.isoformat(),
        "status": "completed"
    }
```

### Creating New Workflow Boundaries

```python
@app.task(force_new_workflow=True)
def independent_workflow(data: dict[str, Any]) -> dict[str, Any]:
    """
    This task always creates a new workflow, regardless of calling context.

    Use force_new_workflow=True when you want to create a clear workflow boundary
    and ensure that this task starts its own workflow context.
    """
    workflow_id = independent_workflow.workflow.workflow_id

    # This workflow has its own deterministic context
    unique_value = independent_workflow.wf.random()
    timestamp = independent_workflow.wf.utc_now()

    return {
        "workflow_id": workflow_id,
        "unique_value": unique_value,
        "timestamp": timestamp.isoformat(),
        "data": data
    }

@app.task
def call_independent_workflow(input_data: dict[str, Any]) -> dict[str, Any]:
    """Demonstrate calling a task that creates its own workflow."""

    # This task runs in its own workflow context
    parent_workflow_id = call_independent_workflow.workflow.workflow_id

    # Execute a task that forces a new workflow
    child_result = call_independent_workflow.wf.execute_task(
        independent_workflow, input_data
    )

    # The child workflow has a different workflow ID
    child_workflow_id = child_result.workflow.workflow_id

    return {
        "parent_workflow_id": parent_workflow_id,
        "child_workflow_id": child_workflow_id,
        "child_result": child_result.result,
        "workflows_are_different": parent_workflow_id != child_workflow_id
    }
```

### Deterministic Operations

The workflow system provides several deterministic operations that ensure reproducible execution:

```python
@app.task
def deterministic_operations_example() -> dict[str, Any]:
    """Demonstrate deterministic operations in workflows."""

    # Deterministic random number (0.0 to 1.0)
    random_value = deterministic_operations_example.wf.random()

    # Deterministic UUID generation
    unique_id = deterministic_operations_example.wf.uuid()

    # Deterministic timestamp progression
    start_time = deterministic_operations_example.wf.utc_now()

    # Each call advances time deterministically
    later_time = deterministic_operations_example.wf.utc_now()

    # Custom deterministic operation
    def generate_sequence_number() -> int:
        return random.randint(1000, 9999)

    sequence = deterministic_operations_example.wf.deterministic._deterministic_operation(
        "sequence", generate_sequence_number
    )

    return {
        "random_value": random_value,
        "unique_id": unique_id,
        "start_time": start_time.isoformat(),
        "later_time": later_time.isoformat(),
        "sequence_number": sequence
    }
```

### Workflow State Management

```python
@app.task
def stateful_workflow(user_id: str) -> dict[str, Any]:
    """Demonstrate workflow state management."""

    # Store workflow data that persists across executions
    stateful_workflow.wf.set_data("user_id", user_id)
    stateful_workflow.wf.set_data("started_at", stateful_workflow.wf.utc_now().isoformat())

    # Perform some operations
    processing_steps = []

    for step in range(3):
        step_id = stateful_workflow.wf.uuid()
        step_time = stateful_workflow.wf.utc_now()

        processing_steps.append({
            "step": step + 1,
            "step_id": step_id,
            "timestamp": step_time.isoformat()
        })

        # Store intermediate results
        stateful_workflow.wf.set_data(f"step_{step + 1}_result", step_id)

    # Retrieve stored data
    user_id_retrieved = stateful_workflow.wf.get_data("user_id")
    started_at = stateful_workflow.wf.get_data("started_at")

    return {
        "user_id": user_id_retrieved,
        "started_at": started_at,
        "processing_steps": processing_steps,
        "completed_at": stateful_workflow.wf.utc_now().isoformat()
    }
```

### Task Execution Within Workflows

```python
@app.task
def parent_workflow(data: dict[str, Any]) -> dict[str, Any]:
    """Parent workflow that executes child tasks."""

    # Execute tasks deterministically within the workflow
    validation_result = parent_workflow.wf.execute_task(
        validate_data, data
    )

    if validation_result.result["valid"]:
        # Execute processing task only if validation succeeds
        processing_result = parent_workflow.wf.execute_task(
            process_data, data, validation_result.result["normalized_data"]
        )

        # Execute notification task
        notification_result = parent_workflow.wf.execute_task(
            send_notification, processing_result.result
        )

        return {
            "status": "completed",
            "validation": validation_result.result,
            "processing": processing_result.result,
            "notification": notification_result.result
        }
    else:
        return {
            "status": "failed",
            "validation": validation_result.result,
            "error": "Data validation failed"
        }

@app.task
def validate_data(data: dict[str, Any]) -> dict[str, Any]:
    """Validate input data."""
    # Validation logic here
    normalized_data = {k.lower(): v for k, v in data.items()}

    return {
        "valid": len(normalized_data) > 0,
        "normalized_data": normalized_data,
        "validated_at": validate_data.wf.utc_now().isoformat()
    }

@app.task
def process_data(original_data: dict[str, Any], normalized_data: dict[str, Any]) -> dict[str, Any]:
    """Process the validated data."""
    processing_id = process_data.wf.uuid()

    return {
        "processing_id": processing_id,
        "processed_records": len(normalized_data),
        "processed_at": process_data.wf.utc_now().isoformat()
    }

@app.task
def send_notification(processing_result: dict[str, Any]) -> dict[str, Any]:
    """Send processing completion notification."""
    notification_id = send_notification.wf.uuid()

    return {
        "notification_id": notification_id,
        "sent_at": send_notification.wf.utc_now().isoformat(),
        "processing_id": processing_result["processing_id"]
    }
```

### Complex Workflow with Error Handling

```python
@app.task
def robust_processing_workflow(order_data: dict[str, Any]) -> dict[str, Any]:
    """Complex workflow with built-in error handling and retry logic."""

    workflow_start = robust_processing_workflow.wf.utc_now()
    order_id = order_data.get("order_id", robust_processing_workflow.wf.uuid())

    # Store initial workflow state
    robust_processing_workflow.wf.set_data("order_id", order_id)
    robust_processing_workflow.wf.set_data("started_at", workflow_start.isoformat())
    robust_processing_workflow.wf.set_data("status", "processing")

    try:
        # Step 1: Validate order
        validation_result = robust_processing_workflow.wf.execute_task(
            validate_order, order_data
        )

        if not validation_result.result["valid"]:
            robust_processing_workflow.wf.set_data("status", "validation_failed")
            return {
                "order_id": order_id,
                "status": "failed",
                "step": "validation",
                "error": validation_result.result["errors"]
            }

        # Step 2: Process payment
        payment_result = robust_processing_workflow.wf.execute_task(
            process_payment_with_retry, order_data
        )

        robust_processing_workflow.wf.set_data("payment_id", payment_result.result["payment_id"])

        # Step 3: Reserve inventory
        inventory_result = robust_processing_workflow.wf.execute_task(
            reserve_inventory, order_data
        )

        robust_processing_workflow.wf.set_data("reservation_id", inventory_result.result["reservation_id"])

        # Step 4: Generate shipping
        shipping_result = robust_processing_workflow.wf.execute_task(
            generate_shipping_label, order_id, inventory_result.result
        )

        # Mark workflow as completed
        robust_processing_workflow.wf.set_data("status", "completed")
        completion_time = robust_processing_workflow.wf.utc_now()

        return {
            "order_id": order_id,
            "status": "completed",
            "payment_id": payment_result.result["payment_id"],
            "reservation_id": inventory_result.result["reservation_id"],
            "tracking_number": shipping_result.result["tracking_number"],
            "started_at": workflow_start.isoformat(),
            "completed_at": completion_time.isoformat(),
            "processing_time_seconds": (completion_time - workflow_start).total_seconds()
        }

    except Exception as e:
        # Handle workflow errors
        robust_processing_workflow.wf.set_data("status", "error")
        robust_processing_workflow.wf.set_data("error", str(e))

        # Execute cleanup if needed
        cleanup_result = robust_processing_workflow.wf.execute_task(
            cleanup_failed_order, order_id
        )

        return {
            "order_id": order_id,
            "status": "error",
            "error": str(e),
            "cleanup_result": cleanup_result.result
        }

@app.task
def validate_order(order_data: dict[str, Any]) -> dict[str, Any]:
    """Validate order data with deterministic processing."""
    validation_id = validate_order.wf.uuid()
    validated_at = validate_order.wf.utc_now()

    errors = []
    if not order_data.get("customer_id"):
        errors.append("Missing customer_id")
    if not order_data.get("items"):
        errors.append("No items in order")

    return {
        "validation_id": validation_id,
        "valid": len(errors) == 0,
        "errors": errors,
        "validated_at": validated_at.isoformat()
    }

@app.task
def process_payment_with_retry(order_data: dict[str, Any]) -> dict[str, Any]:
    """Process payment with deterministic retry logic."""
    payment_id = process_payment_with_retry.wf.uuid()
    attempt_time = process_payment_with_retry.wf.utc_now()

    # Simulate processing logic with deterministic behavior
    success_chance = process_payment_with_retry.wf.random()

    if success_chance > 0.1:  # 90% success rate
        return {
            "payment_id": payment_id,
            "status": "completed",
            "amount": order_data.get("total", 0),
            "processed_at": attempt_time.isoformat()
        }
    else:
        # In a real implementation, this would trigger retry logic
        return {
            "payment_id": payment_id,
            "status": "failed",
            "error": "Payment processing failed",
            "attempted_at": attempt_time.isoformat()
        }

@app.task
def reserve_inventory(order_data: dict[str, Any]) -> dict[str, Any]:
    """Reserve inventory for order items."""
    reservation_id = reserve_inventory.wf.uuid()
    reserved_at = reserve_inventory.wf.utc_now()

    items = order_data.get("items", [])
    reserved_items = []

    for item in items:
        item_reservation_id = reserve_inventory.wf.uuid()
        reserved_items.append({
            "item_id": item.get("id"),
            "quantity": item.get("quantity"),
            "reservation_id": item_reservation_id
        })

    return {
        "reservation_id": reservation_id,
        "reserved_items": reserved_items,
        "reserved_at": reserved_at.isoformat()
    }

@app.task
def generate_shipping_label(order_id: str, inventory_data: dict[str, Any]) -> dict[str, Any]:
    """Generate shipping label for reserved inventory."""
    tracking_number = f"TRK-{order_id}-{int(generate_shipping_label.wf.random() * 100000):05d}"
    label_id = generate_shipping_label.wf.uuid()
    generated_at = generate_shipping_label.wf.utc_now()

    return {
        "label_id": label_id,
        "tracking_number": tracking_number,
        "order_id": order_id,
        "reservation_id": inventory_data["reservation_id"],
        "generated_at": generated_at.isoformat()
    }

@app.task
def cleanup_failed_order(order_id: str) -> dict[str, Any]:
    """Clean up resources for a failed order."""
    cleanup_id = cleanup_failed_order.wf.uuid()
    cleaned_at = cleanup_failed_order.wf.utc_now()

    return {
        "cleanup_id": cleanup_id,
        "order_id": order_id,
        "actions": ["released_inventory", "voided_payment", "notified_customer"],
        "cleaned_at": cleaned_at.isoformat()
    }
```

## Features

### Deterministic Execution

- **Random Numbers**: `task.wf.random()` generates deterministic random numbers
- **Timestamps**: `task.wf.utc_now()` provides deterministic time progression
- **UUIDs**: `task.wf.uuid()` creates deterministic unique identifiers
- **Task Execution**: `task.wf.execute_task()` ensures deterministic task invocation

### Workflow State Management

- **Data Storage**: `task.wf.set_data(key, value)` and `task.wf.get_data(key)` for persistent key-value storage
- **Workflow Identity**: Each workflow has a unique identity that tracks relationships and lineage
- **Context Inheritance**: Child tasks automatically inherit parent workflow context

### Integration with Task System

- **Seamless Integration**: Any task can participate in workflows without modification
- **Automatic Context**: Tasks executed within workflows automatically get workflow context
- **Flexible Boundaries**: Control when new workflows are created with `force_new_workflow`

### Replay and Recovery

- **Perfect Replay**: Workflows replay identically, including all random operations and timestamps
- **State Persistence**: All workflow state is automatically persisted across executions
- **Failure Recovery**: Workflows can resume from exact points of failure

## Best Practices

1. **Use Deterministic Operations**: Always use workflow-provided operations (`wf.random()`, `wf.utc_now()`, `wf.uuid()`) instead of standard library functions for reproducibility.

2. **Store Important State**: Use `wf.set_data()` to store critical workflow state that should persist across executions.

3. **Design for Idempotency**: Ensure workflow steps can be safely re-executed without side effects.

4. **Handle Errors Gracefully**: Implement proper error handling and cleanup logic within workflows.

5. **Use Workflow Boundaries**: Use `force_new_workflow=True` to create clear boundaries between independent workflow contexts.

6. **Keep Workflows Focused**: Design workflows with clear, single purposes rather than overly complex multi-purpose workflows.

7. **Test Replay Behavior**: Test that your workflows behave correctly when replayed from stored state.

The Pynenc Workflow System provides a robust foundation for building complex, reliable task orchestration with built-in state management, deterministic execution, and automatic replay capabilities.
