# Basic Local Threaded Demonstration

## Overview

This guide provides a simple demonstration of using Pynenc in a non-distributed, local-threaded environment. The full source code for this demonstration is available on GitHub: [basic_local_threaded_example](https://github.com/pynenc/samples/tree/main/basic_local_threaded_example).

```{note}
This demonstration is intended for educational and testing purposes and is not suitable for a production environment.
```

## Scenario

This scenario illustrates the simplest use case of Pynenc, where tasks are defined and executed within the same Python process using threading. It demonstrates how tasks can be managed and processed using the Pynenc library in a controlled environment.

## Setup

### Requirements

- Python 3.11 or higher.
- Pynenc library installed.

### Project Files

- `tasks.py`: Contains the definition of a simple `add` task.
- `sample.py`: Demonstrates executing the `add` task in various modes.

## Demonstration

### Defining a Task

In `tasks.py`, we define a basic task using the Pynenc decorator:

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    add.logger.info(f"{add.task_id=} Adding {x} + {y}")
    return x + y
```

This `add` function is a simple task that logs its operation and returns the sum of two integers.

## Executing Tasks

In `sample.py`, we demonstrate different ways to execute the add task:

### Running on a Separate Thread

```python
import threading
import tasks

def run_add_on_thread() -> None:
    def run_in_thread() -> None:
        tasks.app.conf.runner_cls = "ThreadRunner"
        tasks.app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    invocation = tasks.add(1, 2)
    thread.join()
```

This function runs the add task on a separate thread, illustrating basic parallel processing within a single process.

### Synchronous Execution

```python
def run_sync() -> None:
    tasks.app.conf.dev_mode_force_sync_tasks = True
    invocation = tasks.add(1, 2)

```

Here, the task is forced to run synchronously on the main thread.

### Handling Timeouts

```python
def run_without_worker_add() -> None:
    """Run add task on the main thread"""
    results = []

    def run_task_with_timeout() -> None:
        logger.info("Running task with timeout")
        invocation = tasks.add(1, 2)
        results.append(invocation.result)
        logger.info(f"Result: {invocation.result}")

    thread = threading.Thread(target=run_task_with_timeout, daemon=True)
    thread.start()
    thread.join(timeout=2)
    if results != []:
        raise ValueError(f"Expected [], got {results}")
    logger.info(f"Task timeout, there was no worker to run the task")
```

This function demonstrates how to handle scenarios where the task execution might hang indefinitely due to the lack of an external worker.

## Conclusion

This basic demonstration provides an introductory understanding of how Pynenc can be used for task processing in a local, threaded environment. It highlights the versatility of Pynenc in handling tasks and serves as a foundational guide for more complex implementations.
