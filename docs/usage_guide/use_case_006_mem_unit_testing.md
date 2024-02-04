# In-Memory Unit Testing

## Overview

This guide introduces the `mem_unit_test` sample, demonstrating the effectiveness of unit testing Pynenc tasks using entirely in-memory components. By configuring Pynenc to use in-memory brokers, orchestrators, and state backends, developers can perform rapid and isolated testing of task logic without the complexities of an asynchronous execution environment.

The full source code for this demonstration is available on GitHub: [mem_unit_test](https://github.com/pynenc/samples/tree/main/mem_unit_test).

## Scenario

The primary goal of the `mem_unit_test` scenario is to showcase unit testing of Pynenc tasks using in-memory components, ensuring tests are executed quickly and deterministically. This approach is especially suited for environments where speed and test isolation are paramount.

## Setup

### Requirements

- Python 3.11 or higher.
- Pynenc library installed.

### Project Files

- `tasks.py`: Defines a simple `add` task, suitable for demonstrating in-memory unit testing.
- `test_add.py`: Contains unit tests for the `add` task, employing in-memory components for synchronous execution.

## Demonstration

### Defining the Task

The task defined in `tasks.py` serves as a basic example to illustrate task testing:

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    add.logger.info(f"{add.task_id=} Adding {x} + {y}")
    return x + y
```

### Executing Tests with In-Memory Components

The unit tests in `test_add.py` demonstrate configuring Pynenc to use in-memory components, facilitating synchronous task execution within the testing framework:

```python
import os
import threading
import unittest
import tasks

class TestAddTask(unittest.TestCase):
    def setUp(self):
        # Configure Pynenc for in-memory testing
        tasks.app.conf.dev_mode_force_sync_tasks = False
        tasks.app.conf.orchestrator_cls = "MemOrchestrator"
        tasks.app.conf.broker_cls = "MemBroker"
        tasks.app.conf.state_backend_cls = "MemStateBackend"
        tasks.app.conf.runner_cls = "ThreadRunner"

        # Start the runner thread for task execution
        self.thread = threading.Thread(target=tasks.app.runner.run, daemon=True)
        self.thread.start()

    def tearDown(self):
        tasks.app.runner.stop_runner_loop()
        self.thread.join()

    def test_add(self):
        result = tasks.add(1, 2).result
        self.assertEqual(result, 3)
```

#### Environment Variable Configuration Example

```python
class TestAddTaskEnviron(unittest.TestCase):
    def setUp(self) -> None:
        # Patch environment variables for in-memory configuration
        self.patcher = patch.dict(os.environ, {
            "PYNENC__DEV_MODE_FORCE_SYNC_TASKS": "False",
            "PYNENC__RUNNER_CLS": "ThreadRunner",
            "PYNENC__ORCHESTRATOR_CLS": "MemOrchestrator",
            "PYNENC__BROKER_CLS": "MemBroker",
            "PYNENC__STATE_BACKEND_CLS": "MemStateBackend",
        })
        self.patcher.start()
        # Start a separate thread for the Pynenc runner
        self.thread = threading.Thread(target=self.run_in_thread, daemon=True)
        self.thread.start()

    def tearDown(self):
        # Cleanup: stop the runner and patch
        tasks.app.runner.stop_runner_loop()
        self.thread.join()
        self.patcher.stop()

    def test_add(self):
        # Test the add task
        invocation = tasks.add(1, 2)
        self.assertEqual(invocation.result, 3)
```

This setup allows tests to run as if they were part of a distributed Pynenc environment, but without the need for external dependencies or an asynchronous runtime.

## Conclusion

The `mem_unit_test` sample underscores Pynenc's adaptability for unit testing, providing a streamlined approach for verifying task logic. By utilizing in-memory components, developers can execute tasks synchronously within tests, offering a fast and reliable method for ensuring the accuracy and stability of task implementations.
