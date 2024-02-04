# Concurrency Control Demonstration

## Overview

This guide provides a detailed look at the `concurrency_control` sample, showcasing the use of concurrency control mechanisms within Pynenc for task processing. This example demonstrates how to manage task execution using different concurrency controls, ensuring tasks are executed according to specific concurrency requirements.

The full source code for this demonstration is available on GitHub: [concurrency_control](https://github.com/pynenc/samples/tree/main/concurrency_control).

## Scenario

The scenario covers various aspects of concurrency control, including disabling concurrency for task registration and execution, and enforcing task-level concurrency control for registration and running events. It's designed to help understand how Pynenc can be configured to handle concurrency in task processing.

## Setup

### Requirements

- Python 3.11 or higher.
- Pynenc library installed.

### Project Files

- `tasks.py`: Defines tasks with different concurrency control settings.
- `sample.py`: Script to demonstrate the effects of concurrency control on task execution.

## Demonstration

### Defining Tasks with Concurrency Control

In `tasks.py`, we define several tasks with different concurrency control settings:

- `get_own_invocation_id` shows a task without concurrency control, allowing multiple invocations.
- `get_own_invocation_id_registration_concurrency` uses `TASK` level registration concurrency, limiting to a single task registration.
- `sleep_without_running_concurrency` and `sleep_with_running_concurrency` illustrate the effects of disabling and enabling running concurrency control, respectively.

```python
from pynenc import Pynenc, ConcurrencyControlType

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

These tasks are designed to illustrate how registration concurrency control affects task execution and invocation ID generation.

### Executing Tasks with Different Concurrency Controls

The `sample.py` script demonstrates the execution and effects of concurrency control settings on tasks:

#### `run_without_concurrency_control()`

Shows the behavior of task execution without any concurrency control, where each call to the task generates a new invocation ID.

```python
def run_without_concurrency_control() -> None:
    invocations = [tasks.get_own_invocation_id() for _ in range(10)]
    logger.info(f"Invocation ids: " + ", ".join(i.invocation_id for i in invocations))
```

#### `run_with_registration_concurrency_control()`

Demonstrates task-level registration concurrency control, where multiple task calls result in a single routed invocation, illustrating the `TASK` level concurrency control for task registration.

```python
def run_with_registration_concurrency_control() -> None:
    invocations = [tasks.get_own_invocation_id_registration_concurrency() for _ in range(3)]
    logger.info(f"Unique invocation_id: {set(invocation_ids)}")
```

#### `run_with_running_concurrency_control()`

Explores the effects of running concurrency control on task execution, contrasting tasks executed with and without running concurrency control to highlight the differences in parallel versus sequential execution.

```python
def run_with_running_concurrency_control() -> None:
    ...
    # check that without control runs in parallel
    no_control_invocations = [
        tasks.sleep_without_running_concurrency(0.1) for _ in range(10)
    ]
    no_control_results = [i.result for i in no_control_invocations]
    if not any_run_in_parallel(no_control_results):
        raise ValueError(f"Expected parallel execution, got {no_control_results}")

    # check that with control does not run in parallel
    controlled_invocations = [
        tasks.sleep_with_running_concurrency(0.1) for _ in range(10)
    ]
    controlled_results = [i.result for i in controlled_invocations]
    if any_run_in_parallel(controlled_results):
        raise ValueError(f"Expected sequential execution, got {controlled_results}")
    ...
```

Each section of this guide aims to provide a clear understanding of how concurrency control can be implemented and managed within Pynenc, offering insights into practical applications of these features.

## Conclusion

The `concurrency_control` sample serves as an introduction to using concurrency control features in Pynenc. By exploring different settings and their impact on task execution, developers can gain insights into how to effectively manage task concurrency in their applications.
