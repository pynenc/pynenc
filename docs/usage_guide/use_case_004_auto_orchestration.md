# Automatic Orchestration

## Overview

This guide explores the `auto_orchestration` sample, highlighting Pynenc's automatic orchestration capabilities in managing task dependencies. Through the use of a Fibonacci function, this example demonstrates how Pynenc intelligently schedules task execution based on the dependencies among tasks, ensuring that tasks are executed in the correct order and that dependent tasks wait for their prerequisites to complete.

The full source code for this demonstration is available on GitHub: [auto_orchestration](https://github.com/pynenc/samples/tree/main/auto_orchestration).

## Scenario

The focus of this scenario is to illustrate Pynenc's ability to handle complex task dependencies automatically. It uses a recursive implementation of the Fibonacci sequence as a practical example of how tasks that depend on the results of other tasks are orchestrated.

## Setup

### Requirements

- Python 3.11 or higher.
- Pynenc library installed.

### Project Files

- `tasks.py`: Contains the definition of the Fibonacci task, showcasing recursive calls within the Pynenc framework.
- `sample.py`: Demonstrates automatic task orchestration by executing the Fibonacci calculation in a separate thread.

## Demonstration

### Defining a Recursive Task

In `tasks.py`, the Fibonacci task is defined to call itself recursively for values of `n`. This setup mimics a common pattern in software development where a task's execution depends on the results of its previous instances.

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def fibonacci(n: int) -> int:
    fibonacci.logger.info(f"Calculating fibonacci({n})")
    if n <= 1:
        result = n
    else:
        result = fibonacci(n - 1).result + fibonacci(n - 2).result
    fibonacci.logger.info(f"Result of fibonacci({n}) is {result}")
    return result
```

### Orchestrating Dependent Tasks

The `sample.py` script demonstrates how Pynenc orchestrates the execution of the Fibonacci task, showcasing the library's ability to manage dependencies automatically.

#### `run_fibonnaci_in_a_thread()`

This function triggers the Fibonacci task within a separate thread, highlighting how Pynenc pauses and resumes tasks as needed to respect their dependencies.

```python
import logging
import threading
import tasks

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_fibonnaci_in_a_thread() -> None:
    def run_in_thread() -> None:
        tasks.app.conf.runner_cls = "ThreadRunner"
        tasks.app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    invocation = tasks.fibonacci(3)
    logger.info(f"Result: {invocation.result}")

    tasks.app.runner.stop_runner_loop()
    thread.join()
```

## Conclusion

The `auto_orchestration` sample provides a clear example of how Pynenc's orchestration system simplifies the management of complex task dependencies. Through the Fibonacci sequence, it showcases the library's capacity to automatically pause and resume tasks based on their dependencies, ensuring that tasks are executed in an orderly and efficient manner.
