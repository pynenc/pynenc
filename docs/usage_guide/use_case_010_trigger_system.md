# Trigger System

## Overview

This guide explores Pynenc's powerful trigger system, which enables declarative task scheduling and event-driven execution. The trigger system allows you to automatically execute tasks in response to various conditions, such as cron schedules, task statuses, results, exceptions, or custom events.

## Prerequisites

For distributed trigger functionality, you'll need a backend plugin:

**Redis Backend**:

```bash
pip install pynenc-redis
```

**MongoDB Backend**:

```bash
pip install pynenc-mongodb
```

The memory-based trigger system is included with the core Pynenc package for development and testing.

## Scenario

The trigger system addresses several common use cases in distributed task orchestration:

- Scheduled task execution using cron expressions
- Automatic task execution when another task completes, fails, or produces specific results
- Recovery or fallback tasks when exceptions occur
- Event-driven workflows where tasks respond to system events
- Complex conditional execution based on task arguments or results

## Implementation

### Basic Trigger Usage

To create triggers, use the `TriggerBuilder` interface provided by the Pynenc application:

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

### Cron-Based Task Scheduling

Schedule tasks to run at specific intervals using cron expressions:

```python
from pynenc import Pynenc
from pynenc.trigger.trigger_builder import TriggerBuilder

app = Pynenc()

# Task that runs every minute
@app.task(triggers=TriggerBuilder().on_cron("* * * * *"))
def scheduled_task() -> str:
    """Run every minute."""
    return f"Executed at {time.time()}"

# Task that runs every 30 minutes
@app.task(triggers=TriggerBuilder().on_cron("*/30 * * * *"))
def half_hour_task() -> str:
    """Run every 30 minutes."""
    return f"Executed at half hour: {time.time()}"
```

### Handling Task Results and Statuses

Trigger tasks based on the status or result of another task:

```python
from pynenc import Pynenc
from pynenc.trigger.trigger_builder import TriggerBuilder
from pynenc.invocation.status import InvocationStatus

app = Pynenc()

@app.task
def division_task(x: int, y: int) -> float:
    """Divide x by y."""
    return x / y

# Trigger on successful completion
@app.task(triggers=TriggerBuilder().on_status(division_task, statuses=["SUCCESS"]))
def success_handler() -> str:
    return "Division completed successfully"

# Trigger on any result (success or failure)
@app.task(triggers=TriggerBuilder().on_any_result(division_task))
def result_handler() -> str:
    return "Division produced a result or error"

# Trigger on specific result value
@app.task(triggers=TriggerBuilder().on_result(division_task, filter_result=2.5))
def specific_result_handler() -> str:
    return "Division produced exactly 2.5"

# Combine conditions with AND logic
@app.task(
    triggers=TriggerBuilder()
    .on_status(division_task, statuses=[InvocationStatus.SUCCESS])
    .on_any_result(division_task)
    .with_logic("and")
)
def combined_and_trigger() -> str:
    return "Both conditions were met"

# Combine conditions with OR logic
@app.task(
    triggers=TriggerBuilder()
    .on_status(division_task, statuses=["SUCCESS"])
    .on_any_result(division_task)
    .with_logic("or")
)
def combined_or_trigger() -> str:
    return "At least one condition was met"
```

### Exception Handling with Triggers

Create recovery or fallback tasks when exceptions occur:

```python
from typing import Any
from pynenc import Pynenc
from pynenc.trigger.trigger_builder import TriggerBuilder
from pynenc.trigger.conditions.exception import ExceptionContext

app = Pynenc()

@app.task
def divide(x: int, y: int) -> float:
    """Divide x by y, may raise exceptions."""
    if x < 0 or y < 0:
        raise ValueError("Negative numbers not allowed")
    return x / y

# Function to extract arguments from exception context
def build_args_from_exception(ctx: ExceptionContext) -> dict[str, Any]:
    """Generate arguments for error reporting task."""
    input_args = ctx.arguments.kwargs
    return {
        "original_args": f"x={input_args.get('x')}, y={input_args.get('y')}",
        "exception_type": ctx.exception_type,
        "exception_message": ctx.exception_message,
    }

# Trigger on ZeroDivisionError exceptions
@app.task(
    triggers=TriggerBuilder()
    .on_exception(divide, exception_types="ZeroDivisionError")
    .with_args_from_exception(build_args_from_exception)
)
def report_div_zero_error(
    original_args: str, exception_type: str, exception_message: str
) -> str:
    """Report division by zero errors."""
    return f"Division with {original_args} failed with {exception_type}: {exception_message}"

# Trigger on ValueError exceptions
@app.task(
    triggers=TriggerBuilder()
    .on_exception(divide, exception_types=["ValueError"])
    .with_args_from_exception(build_args_from_exception)
)
def report_value_error(
    original_args: str, exception_type: str, exception_message: str
) -> str:
    """Report value errors."""
    return f"Division with {original_args} failed with {exception_type}: {exception_message}"

# Trigger on any exception
@app.task(
    triggers=TriggerBuilder()
    .on_exception(divide)  # No exception_types means match any exception
    .with_args_from_exception(build_args_from_exception)
)
def report_any_exception(
    original_args: str, exception_type: str, exception_message: str
) -> str:
    """Report any exception from divide task."""
    return f"Division with {original_args} failed with {exception_type}: {exception_message}"
```

### Dynamic Argument Generation with ArgumentProvider

The `ArgumentProvider` system allows you to dynamically generate arguments for triggered tasks:

```python
from pynenc import Pynenc
from pynenc.trigger.trigger_builder import TriggerBuilder
from pynenc.trigger.conditions.result import ResultContext

app = Pynenc()

@app.task
def process_data(data: dict) -> dict:
    """Process input data and return results."""
    return {"processed": data, "timestamp": time.time()}

# Static argument provider (using fixed values)
@app.task(
    triggers=TriggerBuilder()
    .on_success(process_data)
    .with_args_static({"notification_level": "info", "include_details": True})
)
def notify_with_static_args(notification_level: str, include_details: bool) -> str:
    """Notification with static arguments."""
    return f"Notification sent with level {notification_level}"

# Dynamic argument provider using a function
def generate_notification_args(ctx: ResultContext) -> dict[str, Any]:
    """Generate arguments based on task result."""
    result = ctx.result
    is_urgent = result.get("processed", {}).get("priority") == "high"

    return {
        "notification_level": "urgent" if is_urgent else "info",
        "include_details": is_urgent,
        "source_data": result
    }

@app.task(
    triggers=TriggerBuilder()
    .on_success(process_data)
    .with_args_from_result(generate_notification_args)
)
def notify_with_dynamic_args(
    notification_level: str,
    include_details: bool,
    source_data: dict
) -> str:
    """Notification with dynamically generated arguments."""
    if include_details:
        return f"{notification_level.upper()} notification with data: {source_data}"
    return f"{notification_level.upper()} notification sent"
```

### Conditional Triggering with Argument and Result Filters

Use filters to conditionally trigger tasks based on arguments or results:

```python
from pynenc import Pynenc
from pynenc.trigger.trigger_builder import TriggerBuilder

app = Pynenc()

@app.task
def process_order(order_id: str, amount: float, priority: str = "normal") -> dict:
    """Process an order with the given details."""
    return {"order_id": order_id, "status": "completed", "amount": amount}

# Filter based on static argument values
@app.task(
    triggers=TriggerBuilder()
    .on_status(process_order, call_arguments={"priority": "high"})  # Only trigger for high priority orders
)
def notify_high_priority() -> str:
    """Notification specifically for high priority orders."""
    return "High priority order processed"

# Filter using a function for complex conditions
def is_large_order(arguments: dict) -> bool:
    """Check if this is a large order based on amount."""
    return arguments.get("amount", 0) > 1000.0

@app.task(
    triggers=TriggerBuilder()
    .on_status(process_order, call_arguments=is_large_order)
)
def notify_large_order() -> str:
    """Notification for large orders."""
    return "Large order processed"

# Result filter with static values
@app.task(
    triggers=TriggerBuilder()
    .on_result(process_order, filter_result={"status": "completed"})  # Only for completed orders
)
def log_completed_order() -> str:
    """Log when an order is completed."""
    return "Order completed successfully"

# Result filter with a function
def needs_approval(result: dict) -> bool:
    """Check if order needs approval based on result."""
    return result.get("amount", 0) > 5000.0

@app.task(
    triggers=TriggerBuilder()
    .on_result(process_order, filter_result=needs_approval)
)
def request_manager_approval() -> str:
    """Request approval for large transactions."""
    return "Manager approval requested for large order"
```

### Event-Based Triggers

React to system events with event-based triggers:

```python
from typing import Any
from pynenc import Pynenc
from pynenc.trigger.trigger_builder import TriggerBuilder
from pynenc.trigger.conditions.event import EventContext

app = Pynenc()

# Trigger task when a specific event occurs
@app.task(
    triggers=TriggerBuilder().on_event("user.login")
)
def log_user_login() -> str:
    """Log when a user login event occurs."""
    return "User login event logged"

# With payload filtering
@app.task(
    triggers=TriggerBuilder()
    .on_event("user.login", payload_filter={"role": "admin"})  # Only for admin logins
)
def notify_admin_login() -> str:
    """Special notification for admin logins."""
    return "Admin user logged in"

# Using the event payload to generate task arguments
def extract_user_data(ctx: EventContext) -> dict[str, Any]:
    """Extract user data from event payload."""
    return {
        "user_id": ctx.payload.get("user_id"),
        "timestamp": ctx.payload.get("timestamp"),
        "source_ip": ctx.payload.get("ip_address")
    }

@app.task(
    triggers=TriggerBuilder()
    .on_event("user.login")
    .with_args_from_event(extract_user_data)
)
def record_login_attempt(user_id: str, timestamp: float, source_ip: str) -> str:
    """Record details of login attempt."""
    return f"Login recorded for user {user_id} from {source_ip}"

# Emit an event to trigger the tasks above
def emit_login_event(user_id: str, ip_address: str, role: str = "user") -> str:
    """Emit a login event."""
    payload = {
        "user_id": user_id,
        "timestamp": time.time(),
        "ip_address": ip_address,
        "role": role
    }
    return app.trigger.emit_event("user.login", payload)
```

### Programmatic Trigger Creation

Create triggers programmatically using the TriggerBuilder:

```python
from typing import Any
from pynenc import Pynenc
from pynenc.trigger.trigger_builder import TriggerBuilder
from pynenc.trigger.conditions.result import ResultContext

app = Pynenc()

@app.task
def data_processor(data: dict) -> dict:
    """Process input data."""
    return {"processed": True, "input": data}

@app.task
def notification_task(result: dict, level: str = "info") -> str:
    """Send a notification with the given level."""
    return f"{level.upper()}: Data processed with result {result}"

# Define a function for result filtering (must be at module level)
def is_processed_successfully(result: dict) -> bool:
    """Check if the result indicates successful processing."""
    return result.get("processed", False) is True

# Define a function for argument generation (must be at module level)
def generate_notification_args(ctx: ResultContext) -> dict[str, Any]:
    """Generate arguments for notification task."""
    return {
        "result": ctx.result,
        "level": "success"
    }

# Create a trigger that connects the tasks
trigger = (
    app.trigger
    .on_success(data_processor)
    .on_result(data_processor, filter_result=is_processed_successfully)
    .with_args_from_result(generate_notification_args)
)

# The trigger is now active and will run notification_task when data_processor
# completes successfully and its result matches the filter
```

## Features

### Diverse Trigger Conditions

- **Cron expressions** for time-based scheduling
- **Status changes** for reactive workflows
- **Result-based** triggers for data-driven processing
- **Exception handling** for error recovery
- **Event-based** triggers for system integration
- **Composite conditions** using AND/OR logic

### Flexible Argument Handling

- **ArgumentProvider**: Generate arguments dynamically from context
- **ArgumentFilter**: Conditionally trigger based on task arguments
- **ResultFilter**: Filter execution based on task results
- **Static arguments**: Provide fixed values for triggered tasks

### Multiple Backends

- **Memory-based**: For testing and development (built-in)
- **Redis-based**: For production-grade distributed systems (requires `pynenc-redis` plugin)
- **MongoDB-based**: For document-based distributed systems (requires `pynenc-mongodb` plugin)

### Comprehensive Context Access

Each trigger condition provides rich context information:

- Original task arguments and results
- Exception details for error handling
- Event payloads for event-based triggers
- Execution timestamps and task IDs

## Best Practices

1. **Keep triggers focused**: Each trigger should have a clear, specific purpose.
2. **Use appropriate filters**: Filters reduce unnecessary task executions.
3. **Consider idempotence**: Triggered tasks should handle potential duplicate executions gracefully.
4. **Monitor trigger performance**: Complex trigger conditions can impact system performance.
5. **Use consistent naming**: Develop a naming convention for triggered tasks to improve maintainability.
6. **Avoid lambdas in filters or providers**: Always use module-level named functions for argument filters and providers.
