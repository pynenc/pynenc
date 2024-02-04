# Synchronous Unit Testing

## Overview

This guide dives into the `sync_unit_testing` sample, showcasing Pynenc's support for synchronous unit testing. This feature is crucial for developers looking to test their tasks in isolation without the need for an asynchronous task runner. By leveraging Python's unittest framework and Pynenc's synchronous execution mode, this example demonstrates how to effectively unit test tasks to ensure reliability and correctness.

The full source code for this demonstration is available on GitHub: [sync_unit_testing](https://github.com/pynenc/samples/tree/main/sync_unit_testing).

## Scenario

The `sync_unit_testing` scenario is designed to illustrate how tasks defined with Pynenc can be tested synchronously, simplifying the testing process by running tasks immediately and sequentially during the test execution. This approach is particularly useful for testing the logic of tasks without involving the complexity of asynchronous execution.

## Setup

### Requirements

- Python 3.11 or higher.
- Pynenc library installed.

### Project Files

- `tasks.py`: Contains a simple `add` task definition using Pynenc, suitable for demonstrating unit testing.
- `test_add.py`: Includes unit tests for the `add` task, showcasing two methods for enforcing synchronous execution within tests.

## Demonstration

### Defining a Task

The `add` task defined in `tasks.py` is a basic example of a task that can be used for unit testing:

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    add.logger.info(f"{add.task_id=} Adding {x} + {y}")
    return x + y
```

This task simply adds two numbers and logs the operation, making it ideal for testing the setup and execution of tasks with Pynenc.

### Testing Tasks Synchronously

The unit tests in `test_add.py` demonstrate two approaches to testing tasks synchronously:

#### Direct Configuration

The `TestAddTaskConf` class shows how to directly set the Pynenc configuration to force synchronous task execution:

```python
class TestAddTaskConf(unittest.TestCase):
    def setUp(self) -> None:
        tasks.app.conf.dev_mode_force_sync_tasks = True

    def test_add(self) -> None:
        invocation = tasks.add(1, 2)
        self.assertEqual(invocation.result, 3)
```

#### Environment Variable Patching

The `TestAddTaskEnviron` class uses `unittest.mock.patch.dict` to simulate setting an environment variable that forces synchronous execution:

```python
class TestAddTaskEnviron(unittest.TestCase):
    def setUp(self) -> None:
        self.patcher = patch.dict(os.environ, {"PYNENC__DEV_MODE_FORCE_SYNC_TASKS": "True"})
        self.patcher.start()

    def tearDown(self) -> None:
        self.patcher.stop()

    def test_add(self) -> None:
        invocation = tasks.add(1, 2)
        self.assertEqual(invocation.result, 3)
```

Both methods ensure that tasks are executed synchronously, facilitating isolated unit tests without the need for an external task runner.

## Conclusion

The `sync_unit_testing` sample provides an essential foundation for writing and executing unit tests for tasks managed by Pynenc. By highlighting methods for synchronous task execution within tests, it empowers developers to conduct thorough and reliable testing of their task logic in isolation, ensuring the robustness and correctness of their applications.
