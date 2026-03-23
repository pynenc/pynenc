# Core Services & Recovery

Pynenc includes built-in **core services** that keep the distributed system self-healing.
Two recovery tasks detect and re-queue stuck invocations automatically, while the
**atomic service** framework ensures exactly one runner processes these services at any
given time — even in a multi-runner deployment.

---

## Architecture Overview

```
Runner main loop (every iteration)
├── Report child heartbeats
├── Check atomic services          ← time-slot gating
│   └── trigger_loop_iteration()   ← cron evaluation + trigger processing
│       ├── Evaluate cron conditions (including recovery task crons)
│       └── Route triggered tasks as normal invocations
└── Process task invocations
```

Core services are **regular Pynenc tasks** registered with cron triggers. They flow
through the same trigger evaluation pipeline as user-defined triggered tasks and are
subject to the same concurrency controls.

---

## Recovery Tasks

### Pending Invocation Recovery

An invocation may get stuck in `PENDING` if the broker accepted the message but the
runner that was supposed to pick it up crashed or became overloaded.

| Setting                            | Default       | Description                             |
| ---------------------------------- | ------------- | --------------------------------------- |
| `recover_pending_invocations_cron` | `*/5 * * * *` | How often the recovery check runs       |
| `max_pending_seconds`              | `5.0`         | Maximum time in PENDING before recovery |

**What happens:**

1. The task queries for invocations that have been `PENDING` longer than `max_pending_seconds`.
2. Each is transitioned to `PENDING_RECOVERY`.
3. The orchestrator re-routes them back through the broker.

```
PENDING ──(timeout)──→ PENDING_RECOVERY → REROUTED → PENDING → RUNNING → ...
```

### Running Invocation Recovery

If a runner process crashes (OOM, segfault, `kill -9`), its invocations remain in
`RUNNING` forever. The heartbeat mechanism detects dead runners, and this task
recovers their work.

| Setting                                | Default        | Description                                          |
| -------------------------------------- | -------------- | ---------------------------------------------------- |
| `recover_running_invocations_cron`     | `*/15 * * * *` | How often the recovery check runs                    |
| `runner_considered_dead_after_minutes` | `10.0`         | Heartbeat timeout before a runner is considered dead |

**What happens:**

1. The task queries for invocations in `RUNNING` whose owning runner hasn't sent a heartbeat within the timeout.
2. Each is transitioned to `RUNNING_RECOVERY`.
3. The orchestrator re-routes them back through the broker.

```
RUNNING ──(dead runner)──→ RUNNING_RECOVERY → REROUTED → PENDING → RUNNING → ...
```

Both recovery tasks use `ConcurrencyControlType.TASK`, ensuring only one instance of
each recovery task executes at a time across the entire system.

---

## Atomic Service: Single-Runner Coordination

### Problem

With N runners, global services (trigger evaluation, recovery) must run **once per cycle**,
not N times. Concurrent execution causes race conditions: duplicate task firings,
conflicting recovery operations.

### Solution: Time-Slot Distribution

The atomic service module divides time into repeating cycles and assigns each runner
an exclusive execution window:

1. **Runner ordering** — active runners are sorted by creation time (oldest first) for
   a stable, deterministic ordering.
2. **Slot calculation** — the cycle interval is divided equally among runners, with a
   safety margin subtracted from each slot.
3. **Modulo clock** — the current timestamp is mapped into the cycle via
   `time % interval`. Each runner checks whether it falls within its window.
4. **Single-runner fast path** — when only one runner exists, it always runs services.

#### Example: 3 Runners, 5-Minute Cycle

```
Slot size: 300s / 3 = 100s each
Spread margin: 60s

Runner 0: [  0s,  40s)
Runner 1: [100s, 140s)
Runner 2: [200s, 240s)
```

### Configuration

| Setting                                 | Default | Description                                   |
| --------------------------------------- | ------- | --------------------------------------------- |
| `atomic_service_interval_minutes`       | `5.0`   | Total cycle length shared across all runners  |
| `atomic_service_spread_margin_minutes`  | `1.0`   | Safety gap subtracted from each slot          |
| `atomic_service_check_interval_minutes` | `0.5`   | How often each runner polls to check its slot |

### Heartbeat and Liveness

Runners report heartbeats to the orchestrator every loop iteration:

- **Parent runners** report their own heartbeat with `can_run_atomic_service=True` and
  report child worker heartbeats separately.
- **Worker threads/processes** have their heartbeats reported by the parent but are not
  eligible for atomic service scheduling.
- A runner is considered **dead** if its last heartbeat is older than
  `runner_considered_dead_after_minutes` (default 10 min).
- Only runners with recent heartbeats and `can_run_atomic_service=True` participate in
  time-slot distribution.

### Execution Time Monitoring

The system validates that each atomic service execution fits within its allocated slot:

- **>80% usage**: Info-level log suggesting the slot is getting tight.
- **Overrun**: Warning-level log indicating atomicity guarantees may be broken.

Tune `atomic_service_interval_minutes` based on these diagnostics.

---

## Relationship with the Trigger System

The atomic service and trigger system are tightly coupled:

1. **Trigger evaluation runs inside the atomic service window** — when a runner enters
   its time slot, it calls `trigger.trigger_loop_iteration()`.

2. **Core tasks are cron-triggered tasks** — both recovery tasks are registered with cron
   expressions via the same `TriggerBuilder` API that user tasks use.

3. **Double-execution prevention** — three layers protect against duplicate firings:

   - **Atomic service time slots** — only one runner evaluates triggers per cycle.
   - **Optimistic locking on cron storage** — `store_last_cron_execution()` uses
     compare-and-swap to prevent the same cron from firing twice.
   - **Trigger run claims** — `claim_trigger_run()` atomically claims execution rights
     for each trigger context.

4. **Event and status triggers also process here** — besides cron conditions, the loop
   processes valid conditions from task status changes, results, exceptions, and custom
   events that were recorded since the last iteration.

```{seealso}
- {doc}`../usage_guide/use_case_010_trigger_system` — guide to creating triggers
- {doc}`../configuration/index` — full configuration reference
- {doc}`runners` — runner types and their execution models
```

---

## Registering Core Tasks

Core tasks are defined in `pynenc/core_tasks.py` using a `CoreTaskRegistry` decorator.
When a runner starts, the registration flow is:

```
Runner.on_start()
  └── init_trigger_tasks_modules()
        ├── Import modules listed in conf.trigger_task_modules
        ├── app.register_core_tasks()
        │     └── Resolve config_cron → cron expression from config
        │         Create Task with triggers=[on_cron(cron_value)]
        └── app.register_deferred_triggers()
              └── Register trigger definitions with the trigger backend
```

This design means core tasks require no special execution path — they are registered,
triggered, routed, and executed through the exact same infrastructure as any
user-defined task.
