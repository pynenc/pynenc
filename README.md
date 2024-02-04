<p align="center">
  <img src="https://pynenc.org/assets/img/avatar-icon.png" alt="Pynenc" width="300">
</p>
<h1 align="center">Pynenc</h1>
<p align="center">
    <em>A task management system for complex distributed orchestration</em>
</p>
<p align="center">
    <a href="https://pypi.org/project/pynenc" target="_blank">
        <img src="https://img.shields.io/pypi/v/pynenc?color=%2334D058&label=pypi%20package" alt="Package version">
    </a>
    <a href="https://pypi.org/project/pynenc" target="_blank">
        <img src="https://img.shields.io/pypi/pyversions/pynenc.svg?color=%2334D058" alt="Supported Python versions">
    </a>
    <a href="https://github.com/pynenc/pynenc/commits/main">
        <img src="https://img.shields.io/github/last-commit/pynenc/pynenc" alt="GitHub last commit">
    </a>
    <a href="https://github.com/pynenc/pynenc/graphs/contributors">
        <img src="https://img.shields.io/github/contributors/pynenc/pynenc" alt="GitHub contributors">
    </a>
    <a href="https://github.com/pynenc/pynenc/issues">
        <img src="https://img.shields.io/github/issues/pynenc/pynenc" alt="GitHub issues">
    </a>
    <a href="https://github.com/pynenc/pynenc/blob/main/LICENSE">
        <img src="https://img.shields.io/github/license/pynenc/pynenc" alt="GitHub license">
    </a>
    <a href="https://github.com/pynenc/pynenc/stargazers">
        <img src="https://img.shields.io/github/stars/pynenc/pynenc?style=social" alt="GitHub Repo stars">
    </a>
    <a href="https://github.com/pynenc/pynenc/network/members">
        <img src="https://img.shields.io/github/forks/pynenc/pynenc?style=social" alt="GitHub forks">
    </a>
</p>

---

**Documentation**: <a href="https://docs.pynenc.org" target="_blank">https://docs.pynenc.org</a>

**Source Code**: <a href="https://github.com/pynenc/pynenc" target="_blank">https://github.com/pynenc/pynenc</a>

---

Pynenc addresses the complex challenges of task management in distributed environments, offering a robust solution for developers looking to efficiently orchestrate asynchronous tasks across multiple systems. By combining intuitive configuration with advanced features like automatic task prioritization and cycle detection, Pynenc empowers developers to build scalable and reliable distributed applications with ease.

## Key Features

- **Intuitive Orchestration**: Simplifies the setup and management of tasks in distributed systems, focusing on usability and practicality.

- **Configurable Concurrency Management**: Pynenc offers versatile concurrency control mechanisms at various levels. It includes:

  - **Task-Level Concurrency**: Ensures only one instance of a specific task is in a running state at any given time.
  - **Argument-Level Concurrency**: Limits concurrent execution based on the arguments of the task, allowing only one task with a unique set of arguments to be running or pending.
  - **Key Argument-Level Concurrency**: Further refines control by focusing on key arguments, ensuring uniqueness in task execution based on specified key arguments.

  This structured approach to concurrency management in Pynenc allows for precise control over task execution, ensuring efficient handling of tasks without overloading the system and adhering to specified constraints.

- **Automatic Task Prioritization**: Pynenc prioritizes tasks by simply counting the number of dependencies each task has. The task with the most dependencies is selected first.

- **Automatic Task Pausing**: Pynenc pauses tasks that are waiting for other tasks to complete. So those with higher priority (has more dependent task waiting for them) can run instead, instead of blocking a runner and preventing deadlocks.

- **Cycle Detection**: Automatically detects cyclical dependencies among tasks and raises exceptions to prevent endless loops in task execution.

- **Modularity and Extensibility**: Pynenc is built with modularity at its core, supporting various components such as orchestrators, brokers, state backends, runners, and serializers. Currently compatible with Redis and a development/test mode using an in-memory synchronous version, Pynenc is designed to be extensible. Future plans include support for additional databases, queues, and services, enabling easy customization and adaptation to different operational needs and environments.

## Installation

Installing Pynenc is a simple process that can be done using pip. Just run the following command in your terminal:

```bash
pip install pynenc
```

This command will download and install Pynenc along with its dependencies. Once the installation is complete, you can start using Pynenc in your Python projects.

For more detailed instructions and advanced installation options, please refer to the [Pynenc Documentation](https://docs.pynenc.org/).

## Quick Start Example

To get started with Pynenc, here's a simple example that demonstrates the creation of a distributed task for adding two numbers. Follow these steps to quickly set up a basic task and execute it.

1. **Define a Task**: Create a file named `tasks.py` and define a simple addition task:

   ```python
   from pynenc import Pynenc

   app = Pynenc()

   @app.task
   def add(x: int, y: int) -> int:
       add.logger.info(f"{add.task_id=} Adding {x} + {y}")
       return x + y
   ```

2. **Start Your Runner or Run Synchronously:**

   Before executing the task, decide if you want to run it asynchronously with a runner or synchronously for testing or development purposes.

   - **Asynchronously:**
     Start a runner in a separate terminal or script:

     ```bash
     pynenc --app=tasks.app runner start
     ```

     Check for the [basic_redis_example](https://github.com/pynenc/samples/tree/main/basic_redis_example)

   - **Synchronously:**
     For test or local demonstration, to try synchronous execution, you can set the environment variable `PYNENC__DEV_MODE_FORCE_SYNC_TASKS=True` to force tasks to run in the same thread.

3. **Execute the Task:**

   ```python
   result = add(1, 2).result
   print(result)  # This will output the result of 1 + 2
   ```

For a complete guide on how to set up and run pynenc, visit our [samples library](https://github.com/pynenc/samples).

## Requirements

To use Pynenc in a distributed system, the current primary requirement is:

- **Redis**: As of now, Pynenc requires a Redis server to handle distributed task management. Ensure that you have Redis installed and running in your environment.

Future Updates:

- Pynenc is being developed to support additional databases and message queues. This will expand its compatibility and usability in various distributed systems.

## Contact or Support

If you need help with Pynenc or want to discuss any aspects of its usage, feel free to reach out through the following channels:

- **[GitHub Issues](https://github.com/pynenc/pynenc/issues)**: For bug reports, feature requests, or other technical queries, please use our GitHub Issues page. You can create a new issue or contribute to existing discussions.

- **[GitHub Discussions](https://github.com/pynenc/pynenc/discussions)**: For more general questions, ideas exchange, or discussions about Pynenc, consider using GitHub Discussions on our repository. It's a great place to connect with other users and the development team.

Remember, your feedback and contributions are essential in helping Pynenc grow and improve!

## License

Pynenc is released under the [MIT License](https://github.com/pynenc/pynenc/blob/main/LICENSE).
