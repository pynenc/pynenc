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
│   └── trigger_loop_iteration     ← cron evaluation + trigger processing
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

With N runners, global services (trigger evaluation, recovery) must not run
concurrently. Concurrent execution causes race conditions: duplicate task firings,
conflicting recovery operations.

### Solution: Claim-Based Time Slots

The scheduler is now centered on one pure decision function:
`decide_atomic_service_claim(...)`.

Each runner follows this flow when it checks atomic services:

1. **Refresh membership** — the runner heartbeats itself with
   `can_run_atomic_service=True`, then loads active runners in deterministic order
   (creation time, then runner id).
2. **Reap stale in-progress runs** — before claiming, the orchestrator marks
   `RUNNING` atomic-service executions as abandoned when their owner is no longer
   active.
3. **Compute one claim** — `decide_atomic_service_claim` returns a single
   `AtomicServiceClaim` with either:
   - `atomic_service_run` (this runner may start), or
   - a skip reason (`NOT_ASSIGNED_SLOT`, `SCHEDULED_RUNNER_IN_GRACE`,
     `SLOT_WINDOW_INVALID`, `LATE_START`, `NO_STABLE_RUNNERS`).
4. **Prevent overlap** — even if the slot is assigned, the orchestrator checks for
   another live `RUNNING` execution and skips if one exists.
5. **Start then re-check margin** — once start is recorded, the orchestrator validates
   that enough slot time remains. If too little time is left, it immediately abandons
   the run with `no_margin` and does no trigger work.
6. **Run and finalize** — the runner calls `trigger.trigger_loop_iteration(atomic_service_run)`.
   The execution is finalized as `COMPLETED` or `ABANDONED`.

#### Example: 3 Runners, 5-Minute Cycle

```
Slot size: 300s / 3 = 100s each
Spread margin: 60s

Runner 0: [  0s,  40s)
Runner 1: [100s, 140s)
Runner 2: [200s, 240s)
```

### Configuration

| Setting                                           | Default | Description                                                            |
| ------------------------------------------------- | ------- | ---------------------------------------------------------------------- |
| `atomic_service_interval_minutes`                 | `5.0`   | Total cycle length shared across all eligible runners                  |
| `atomic_service_spread_margin_minutes`            | `1.0`   | Safety gap subtracted from each per-runner slot (except single-runner) |
| `atomic_service_check_interval_minutes`           | `0.5`   | How often each runner evaluates an atomic-service claim                |
| `atomic_service_max_start_slot_fraction`          | `0.5`   | Abort starts that consume too much of the assigned slot before running |
| `atomic_service_membership_stabilization_minutes` | `0.0`   | Keep new runners in membership while temporarily non-runnable          |
| `atomic_service_min_run_margin_seconds`           | `0.05`  | Minimum remaining slot time required after claiming                    |
| `atomic_service_execution_retention_minutes`      | `60.0`  | Age retention window for execution audit records                       |
| `atomic_service_execution_max_records`            | `1000`  | Capacity cap for execution audit records                               |

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

### Timing Safety Rules

The scheduler enforces two independent timing checks:

- **Late-start guard (`atomic_service_max_start_slot_fraction`)**:
  if claim/start work begins too late within the slot, the attempt is skipped.
- **Minimum-margin guard (`atomic_service_min_run_margin_seconds`)**:
  if the remaining slot time after claiming is below the configured minimum, the run is
  immediately marked `ABANDONED:no_margin`.

In addition, `atomic_service_membership_stabilization_minutes` can reserve slots for
newly seen runners while they are still in a grace period.

---

## Relationship with the Trigger System

The atomic service and trigger system are tightly coupled:

1. **Trigger evaluation runs inside an atomic-service claim** — when a runner is
   cleared to start, the orchestrator returns an `AtomicServiceRun`, and the runner
   calls `trigger.trigger_loop_iteration(atomic_service_run)`.

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
