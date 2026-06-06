# Trigger System

## Overview

This guide explains Pynenc's trigger system: a declarative way to run a task
when a condition is met. Triggers cover cron schedules, user-defined events,
task status changes, result filters, exception filters, and composite AND/OR
conditions.

The key design point is that the rule lives on the task that reacts. The
caller of the upstream task does not have to remember `.link(...)`, build a
chain, or add callback code every time it enqueues work.

For a complete runnable example, see the external
[`trigger_demo` sample](https://github.com/pynenc/samples/tree/main/trigger_demo).
It runs locally with SQLite and covers cron, events, status-driven pipelines,
exception compensation, and composite status/result conditions.

## Prerequisites

**Trigger Backend Configuration**:

You must select one of the available trigger backends:

**For Development and Testing**:

| Backend               | Configure with                  | Notes                                                                                                          |
| --------------------- | ------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Memory-based triggers | `trigger_cls = "MemTrigger"`    | Built-in, single-process only, data is lost on restart. Use with `ThreadRunner` for thread-safe memory access. |
| SQLite-based triggers | `trigger_cls = "SQLiteTrigger"` | Built-in, single-host, persistent storage. Works with any runner sharing the same database file.               |

**For Distributed Production Systems**:

Distributed trigger backends require their respective plugins:

- **Redis Backend**: `pip install pynenc-redis`

  - Configure with: `trigger_cls = "RedisTrigger"`
  - Requires Redis server

- **MongoDB Backend**: `pip install pynenc-mongodb`

  - Configure with: `trigger_cls = "MongoTrigger"`
  - Requires MongoDB server

- **RabbitMQ Backend**: `pip install pynenc-rabbitmq`
  - Configure with: `trigger_cls = "RabbitMQTrigger"`
  - Requires RabbitMQ server

### Configuration Examples

**Using pyproject.toml**:

```toml
[tool.pynenc]
app_id = "my_app"
trigger_cls = "MemTrigger"  # or "SQLiteTrigger", "RedisTrigger", etc.
trigger_task_modules = ["tasks"]
# ...other configuration...
```

:::{important}
Pynenc loads ordinary task modules lazily when a runner receives the first
invocation for a task in that module. That is too late for trigger-backed
tasks: cron, event, status, result, and exception conditions must already be
registered for the app-level atomic service to evaluate them and create their
first invocation.

Add every module that declares `@app.task(triggers=...)` to
`trigger_task_modules`. Runners import those modules at startup; Pynenc does
not eagerly import the rest of your task modules.
:::

For a single-host app using the built-in SQLite components:

```toml
[tool.pynenc]
app_id = "trigger_demo"
orchestrator_cls = "SQLiteOrchestrator"
broker_cls = "SQLiteBroker"
state_backend_cls = "SQLiteStateBackend"
trigger_cls = "SQLiteTrigger"
runner_cls = "ThreadRunner"
trigger_task_modules = ["tasks"]

[tool.pynenc.sqlite]
sqlite_db_path = "trigger_demo.db"
```

**Using PynencBuilder**:

```python
from pynenc.builder import PynencBuilder

# With memory-based triggers (development)
app = (
    PynencBuilder()
    .thread_runner()
    .trigger_memory()  # Enables memory-based triggers
    .trigger_task_modules(["tasks"])
    .build()
)

# With Redis-based triggers (production, requires pynenc-redis)
app = (
    PynencBuilder()
    .redis(url="redis://localhost:6379")
    .process_runner()
    .trigger_task_modules(["tasks"])
    .build()
)
```

**Using environment variables**:

```bash
PYNENC__TRIGGER_CLS=MemTrigger python your_app.py
```

## Scenario

The trigger system addresses several common use cases in distributed task orchestration:

- Scheduled task execution using cron expressions
- Automatic task execution when another task completes, fails, or produces specific results
- Recovery or fallback tasks when exceptions occur
- Event-driven workflows where tasks respond to system events
- Complex conditional execution based on task arguments or results

The `trigger_demo` sample uses a small media-ingestion pipeline to show these
patterns together:

| Task                 | Trigger pattern                                                   | What it demonstrates                                               |
| -------------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------ |
| `ingest_feed`        | `on_cron(...)` and `on_event("feed_updated")`                     | Scheduled refresh and external events can both start the same task |
| `enrich_article`     | `on_event("article_ingested")`                                    | Producer/consumer decoupling through event payloads                |
| `notify_subscribers` | `on_status(enrich_article, call_arguments={...})`                 | A downstream task can react only to matching upstream arguments    |
| `alert_editorial`    | `on_exception(enrich_article, exception_types="EnrichmentError")` | The reactive leg of a Saga-style compensation path                 |
| `generate_digest`    | `on_status(...)` AND `on_result(..., filter_result=...)`          | Composite conditions can combine different condition types         |

Run it locally with:

```bash
git clone https://github.com/pynenc/samples
cd samples/trigger_demo
uv sync
uv run python sample.py
```

## Implementation

### Basic Trigger Usage

To create triggers, use the `TriggerBuilder` interface provided by the Pynenc application. **Note**: Triggers will only execute if you've configured a trigger backend (see Prerequisites above).

```python
from pynenc import Pynenc
from pynenc.invocation.status import InvocationStatus
from pynenc.trigger.trigger_builder import TriggerBuilder

# Ensure triggers are enabled by configuring a trigger backend
app = Pynenc()  # Make sure trigger_cls is configured in pyproject.toml

@app.task
def source_task(x: int) -> str:
    return f"Processed {x}"

# Define a task that runs when source_task completes successfully
# This will only work if triggers are enabled
@app.task(
    triggers=TriggerBuilder().on_status(
        source_task, statuses=[InvocationStatus.SUCCESS]
    )
)
def notification_task() -> str:
    return "Source task completed successfully"
```

When the runner starts, it imports the configured `trigger_task_modules` before
the atomic service begins evaluating conditions. Without that declaration,
trigger-backed tasks in a lazily loaded module are unknown to the runner and
cannot create their first invocation.

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
import time
from typing import Any

from pynenc import Pynenc
from pynenc.invocation.status import InvocationStatus
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
    .on_status(process_data, statuses=[InvocationStatus.SUCCESS])
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
    .on_any_result(process_data)
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
import time
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

### Composite Conditions on a Reacting Task

Combine multiple conditions on the task that should react. This example runs
`notification_task` only when `data_processor` succeeded and its result matches
the result filter.

```python
from typing import Any

from pynenc import Pynenc
from pynenc.invocation.status import InvocationStatus
from pynenc.trigger.trigger_builder import TriggerBuilder
from pynenc.trigger.conditions.result import ResultContext

app = Pynenc()

@app.task
def data_processor(data: dict) -> dict:
    """Process input data."""
    return {"processed": True, "input": data}

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

# The trigger is declared on the task that reacts.
@app.task(
    triggers=TriggerBuilder()
    .on_status(data_processor, statuses=[InvocationStatus.SUCCESS])
    .on_result(data_processor, filter_result=is_processed_successfully)
    .with_args_from_result(generate_notification_args)
    .with_logic("and")
)
def notification_task(result: dict, level: str = "info") -> str:
    """Send a notification with the given level."""
    return f"{level.upper()}: Data processed with result {result}"
```

### Inspecting Emitted Events in Pynmon

Every call to `app.trigger.emit_event(event_code, payload)` is stored on the
trigger backend as an `EventRecord`, even when no trigger condition matches. The
record captures the event code, payload, timestamp, emitter task and invocation
when the event was emitted inside a task, and valid and matched condition IDs.
The trigger backend also indexes the trigger runs and generated invocations that
reference the event.

Open the Pynmon `/events` view to inspect the event stream:

- **Filter by event shape** with `event_code`, time range, matched outcome, and triggered outcome. This separates events that were only recorded, events that matched conditions, and events that actually launched work.
- **Open an event detail** page to see the payload, emitter, valid and matched conditions, trigger runs, and generated invocations.
- **Open the emitter or generated invocation** to move back into the normal invocation detail page. There you can see the trigger run that created the invocation, events emitted by that invocation, and downstream trigger runs caused by those events.
- **Open a trigger run** to see the per-condition participants that satisfied the trigger. For composite triggers, this explains which event, status, result, exception, or cron context satisfied each part of the rule.
- **Show the event in the timeline** to see the event marker, emitter, source invocations, trigger run, and generated invocation in the same visual window when those records exist.
- **Use the Log Explorer** when the starting point is a log line. Entity tokens such as `event:<id>` and `trigger-run:<id>` resolve to the same event and trigger-run pages, and the mini-timeline expands to include referenced emitters.

The main timeline shows triggered event markers by default for the selected time
window. Events that matched conditions but did not create invocations can still
be focused from their event detail page. Events that matched nothing remain in
the event list for auditing without adding noise to the default timeline view.

Auto-purge keeps the storage bounded — see the
[`event_retention_days`, `event_max_records`, `trigger_run_max_records`, and
`event_auto_purge_enabled`](../configuration/index.md)
fields on `ConfigTrigger`.

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

**Built-in Backends** (no plugin required):

- **Memory-based** (`MemTrigger`): Single-process, data lost on restart, thread-safe with ThreadRunner only
- **SQLite-based** (`SQLiteTrigger`): Single-host, persistent storage, works with any runner sharing the database

**Plugin-Based Distributed Backends**:

- **Redis-based** (`RedisTrigger`): Requires `pynenc-redis` plugin
- **MongoDB-based** (`MongoTrigger`): Requires `pynenc-mongodb` plugin
- **RabbitMQ-based** (`RabbitMQTrigger`): Requires `pynenc-rabbitmq` plugin

### Comprehensive Context Access

Each trigger condition provides rich context information:

- Original task arguments and results
- Exception details for error handling
- Event payloads for event-based triggers
- Execution timestamps and task IDs

## Best Practices

1. **Choose appropriate backend**: Use memory/SQLite for development, distributed backends for production
2. **Keep triggers focused**: Each trigger should have a clear, specific purpose
3. **Use appropriate filters**: Filters reduce unnecessary task executions
4. **Consider idempotence**: Triggered tasks should handle potential duplicate executions gracefully
5. **Monitor trigger performance**: Complex trigger conditions can impact system performance
6. **Use consistent naming**: Develop a naming convention for triggered tasks to improve maintainability
7. **Avoid lambdas in filters or providers**: Always use module-level named functions for argument filters and providers
8. **Import trigger task modules at runner startup**: Configure `trigger_task_modules` so the runner imports the modules that declare trigger-backed tasks
9. **Do not mix incompatible argument contexts in one trigger**: If a task can run from cron and from an event, use two trigger builders in a list so each one has its own argument provider
10. **Use workflows for deterministic transactions**: Triggers route reactions; Pynenc workflows are the abstraction for multi-step processes that must resume from a failed step

## Troubleshooting

**Triggers not executing?**

- Ensure the trigger backend service (Redis, MongoDB, RabbitMQ) is running for distributed backends
- Check that the required plugin is installed for distributed backends
- Verify the runner is compatible with your trigger backend (e.g., MemTrigger requires ThreadRunner)
- Verify `trigger_task_modules` includes the modules where trigger-backed tasks are defined
- Verify custom filters and argument providers are module-level named functions, not lambdas or nested functions
