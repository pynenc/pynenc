# Invocation Status System

The invocation status system is a core component of Pynenc that manages the lifecycle of task invocations through a declarative, type-safe state machine. This system provides ownership tracking, automatic recovery, and ensures reliable task execution in distributed environments.

## Overview

Every task invocation in Pynenc progresses through a series of states from registration to completion. The status system enforces valid state transitions, tracks which runner owns each invocation, and enables automatic recovery when runners become unresponsive.

## All Invocation Statuses

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

## Status Categories

### Available for Run

These statuses indicate that an invocation is ready to be picked up by a runner:

| Status       | Description                                                     |
| ------------ | --------------------------------------------------------------- |
| `REGISTERED` | Task call has been routed and registered in the system          |
| `REROUTED`   | Task call has been re-routed after being released               |
| `RETRY`      | Task finished with a retriable exception and is ready for retry |

### Owned by Runner

These statuses require ownership validation - only the owning runner can modify them:

| Status    | Description                                         |
| --------- | --------------------------------------------------- |
| `PENDING` | Task was picked by a runner but not yet executing   |
| `RUNNING` | Task is currently being executed                    |
| `PAUSED`  | Task execution is paused (waiting for dependencies) |
| `RESUMED` | Task execution has been resumed after pausing       |

### Recovery Statuses

These statuses override ownership validation for recovering stuck invocations:

| Status             | Description                                          |
| ------------------ | ---------------------------------------------------- |
| `PENDING_RECOVERY` | Invocation exceeded PENDING timeout, being recovered |
| `RUNNING_RECOVERY` | Invocation owned by inactive runner, being recovered |

### Concurrency Control

These statuses handle concurrency control scenarios:

| Status                         | Description                                           |
| ------------------------------ | ----------------------------------------------------- |
| `CONCURRENCY_CONTROLLED`       | Task blocked by concurrency control, will be rerouted |
| `CONCURRENCY_CONTROLLED_FINAL` | Task blocked and will not be rerouted (final)         |

### Final Statuses

These statuses terminate the invocation lifecycle:

| Status                         | Description                                     |
| ------------------------------ | ----------------------------------------------- |
| `SUCCESS`                      | Task completed without errors                   |
| `FAILED`                       | Task completed with an exception                |
| `CONCURRENCY_CONTROLLED_FINAL` | Task permanently blocked by concurrency control |

## Ownership Model

The status system tracks which runner owns each invocation:

- **Ownership Acquisition**: When a runner picks up an invocation (REGISTERED → PENDING), it acquires ownership.
- **Ownership Validation**: Transitions from owned statuses (PENDING, RUNNING, etc.) require the requesting runner to be the owner.
- **Ownership Release**: Final statuses and rerouted statuses release ownership, making the invocation available for other runners.
- **Ownership Override**: Recovery statuses (PENDING_RECOVERY, RUNNING_RECOVERY) bypass ownership validation to recover stuck invocations.

## Runner Heartbeat and Recovery

Pynenc includes automatic recovery mechanisms for handling stuck invocations:

### PENDING Recovery

When an invocation remains in PENDING status longer than `max_pending_seconds`, the recovery service:

1. Detects the timeout
2. Transitions to PENDING_RECOVERY (overrides ownership)
3. Reroutes the invocation for pickup by another runner

### RUNNING Recovery

When a runner becomes inactive (stops sending heartbeats), the recovery service:

1. Detects the inactive runner via heartbeat monitoring
2. Finds all RUNNING invocations owned by that runner
3. Transitions to RUNNING_RECOVERY (overrides ownership)
4. Reroutes the invocations for pickup by active runners

## Configuration

Control recovery behavior through configuration:

```python
from pynenc.builder import PynencBuilder

app = (
    PynencBuilder()
    .app_id("my_app")
    .max_pending_seconds(300)  # 5 minutes before PENDING recovery
    .build()
)
```

## Concurrency Control Integration

The status system integrates with concurrency control to prevent task saturation:

```python
from pynenc import Pynenc, ConcurrencyControlType

app = Pynenc()

@app.task(
    running_concurrency=ConcurrencyControlType.TASK,
    reroute_on_concurrency_control=False  # Use CONCURRENCY_CONTROLLED_FINAL
)
def exclusive_task() -> str:
    return "Only one instance at a time"
```

When `reroute_on_concurrency_control=False`, blocked invocations receive CONCURRENCY_CONTROLLED_FINAL status instead of being rerouted, preventing unbounded task accumulation.

## Monitoring Status

Use Pynmon to visualize invocation status transitions:

1. Start the monitor: `pynenc --app your_app monitor`
2. Navigate to the Timeline view to see status changes over time
3. Click on individual invocations to see their full status history

The timeline visualization shows:

- Status transitions with timestamps
- Runner ownership changes
- Recovery events
- Final status outcomes
