# Concurrency Control

## Overview

This guide provides a detailed look at the `concurrency_control` sample, showcasing concurrency control mechanisms within Pynenc for task processing. It demonstrates how tasks can be managed and executed according to specific concurrency requirements.

The full source code is available on GitHub: [concurrency_control](https://github.com/pynenc/samples/tree/main/concurrency_control).

## Scenario

This example explores various concurrency control methods, including disabling concurrency for task registration and execution, and enforcing task-level concurrency control during registration and runtime. It illustrates configuring Pynenc for handling concurrency effectively.

## Setup

### Requirements

- Python 3.11 or higher.
- Installed Pynenc library.

### Project Files

- `tasks.py`: Defines tasks with different concurrency control settings.
- `sample.py`: Demonstrates how concurrency control settings impact task execution.

## Demonstration

### Defining Tasks with Concurrency Control

In `tasks.py`, tasks are defined with concurrency control settings. You can specify concurrency directly per task or globally using `PynencBuilder`.

#### Option 1: Direct Initialization (Task-Specific Controls)

```python
from pynenc import Pynenc, ConcurrencyControlType
from typing import NamedTuple
import time

app = Pynenc()

@app.task(registration_concurrency=ConcurrencyControlType.DISABLED)
def get_own_invocation_id() -> str:
    return get_own_invocation_id.invocation.invocation_id

@app.task(registration_concurrency=ConcurrencyControlType.TASK)
def get_own_invocation_id_registration_concurrency() -> str:
    return get_own_invocation_id_registration_concurrency.invocation.invocation_id

class SleepResult(NamedTuple):
    start: float
    end: float

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

#### Option 2: Using PynencBuilder (Default Concurrency Controls)

Alternatively, use `PynencBuilder` to configure default concurrency controls for all tasks, with the option to override them individually. This builder configuration can coexist with other configuration methods such as `pyproject.toml`, environment variables, YAML, and JSON.

```python
from pynenc import Pynenc, ConcurrencyControlType
from pynenc.builder import PynencBuilder
from typing import NamedTuple
import time

app = (
    PynencBuilder()
    .concurrency_control(
        running_concurrency=ConcurrencyControlType.DISABLED,  # Default running concurrency
        registration_concurrency=ConcurrencyControlType.DISABLED  # Default registration concurrency
    )
    .build()
)

@app.task  # Inherits DISABLED concurrency for both running and registration
def get_own_invocation_id() -> str:
    return get_own_invocation_id.invocation.invocation_id

@app.task(registration_concurrency=ConcurrencyControlType.TASK)  # Overrides default registration concurrency
def get_own_invocation_id_registration_concurrency() -> str:
    return get_own_invocation_id_registration_concurrency.invocation.invocation_id

class SleepResult(NamedTuple):
    start: float
    end: float

@app.task  # Inherits DISABLED running concurrency
def sleep_without_running_concurrency(seconds: float) -> SleepResult:
    start = time.time()
    time.sleep(seconds)
    return SleepResult(start=start, end=time.time())

@app.task(running_concurrency=ConcurrencyControlType.TASK)  # Overrides default running concurrency
def sleep_with_running_concurrency(seconds: float) -> SleepResult:
    start = time.time()
    time.sleep(seconds)
    return SleepResult(start=start, end=time.time())
```

- `get_own_invocation_id` uses default (`DISABLED`) concurrency.
- `get_own_invocation_id_registration_concurrency` explicitly overrides default registration concurrency to `TASK`.
- `sleep_without_running_concurrency` and `sleep_with_running_concurrency` highlight inherited versus overridden concurrency controls for running tasks.

Using `PynencBuilder.concurrency_control()`, you define global defaults easily, applying consistency across tasks while retaining flexibility.

### Executing Tasks with Concurrency Controls

The `sample.py` script demonstrates how concurrency settings influence task execution:

#### Running Without Concurrency Control

Illustrates execution without enforced concurrency, creating separate invocation IDs per call.

```python
def run_without_concurrency_control() -> None:
    invocations = [tasks.get_own_invocation_id() for _ in range(10)]
    logger.info(f"Invocation ids: " + ", ".join(i.invocation_id for i in invocations))
```

#### Running with Registration Concurrency Control

Demonstrates that registration concurrency control (`TASK`) routes multiple calls to a single invocation.

```python
def run_with_registration_concurrency_control() -> None:
    invocations = [tasks.get_own_invocation_id_registration_concurrency() for _ in range(3)]
    unique_invocation_ids = set(i.invocation_id for i in invocations)
    logger.info(f"Unique invocation_id: {unique_invocation_ids}")
```

#### Running with Execution (Running) Concurrency Control

Demonstrates the difference between parallel and sequential execution based on running concurrency settings.

```python
def run_with_running_concurrency_control() -> None:
    # Without concurrency control: parallel execution
    no_control_invocations = [
        tasks.sleep_without_running_concurrency(0.1) for _ in range(10)
    ]
    no_control_results = [i.result for i in no_control_invocations]
    if not any_run_in_parallel(no_control_results):
        raise ValueError(f"Expected parallel execution, got {no_control_results}")

    # With concurrency control: sequential execution
    controlled_invocations = [
        tasks.sleep_with_running_concurrency(0.1) for _ in range(10)
    ]
    controlled_results = [i.result for i in controlled_invocations]
    if any_run_in_parallel(controlled_results):
        raise ValueError(f"Expected sequential execution, got {controlled_results}")
```

Each demonstration section aims to clearly illustrate how different concurrency configurations affect task execution within Pynenc.

## Per-key Concurrency with `KEYS` and `key_arguments`

The `TASK`-level controls above apply globally — they treat _every_ invocation
of a task as competing for the same slot. In real systems you usually want
something narrower: serialise only the calls that share a key, and let
everything else run in parallel.

`ConcurrencyControlType.KEYS` combined with `key_arguments=("...",)` does
exactly that. The orchestrator looks only at the named arguments when
deciding whether two invocations conflict.

```python
from pynenc import Pynenc
from pynenc.conf.config_task import ConcurrencyControlType

app = Pynenc()

@app.task(
    running_concurrency=ConcurrencyControlType.KEYS,
    key_arguments=("account_id",),
    reroute_on_concurrency_control=True,
)
def call_external_api(account_id: str, payload: dict) -> str:
    ...
```

With this configuration:

- At most **one running invocation per `account_id`** at any time.
- Different `account_id` values run **in parallel** across all your workers.
- Blocked invocations are re-queued (`reroute_on_concurrency_control=True`)
  rather than discarded.

This is the right primitive for third-party APIs that limit concurrency per
account, per tenant, or per resource — without needing an external lock
service or a per-tenant worker pool.

`registration_concurrency=KEYS` works the same way but at enqueue time:
duplicate invocations for the same key are deduped before they ever reach a
worker. Useful when something keeps re-triggering the same logical job and
you only need it done once per key.

The full runnable example, with a FastAPI server that detects collisions, is
in the [`concurrency_demo`](https://github.com/pynenc/samples/tree/main/concurrency_demo)
sample.

```{note}
Today's primitive enforces *exactly one* in-flight invocation per key.
Multi-slot per key (e.g. "up to 5 in flight per account") and time-window
rate limits (e.g. "100 per minute per account") are on the roadmap.
```

## Conclusion

The `concurrency_control` sample introduces concurrency management within Pynenc clearly and practically. By using task-specific settings or global defaults via `PynencBuilder`, developers gain powerful and flexible options for controlling concurrent task execution.
