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

## Requirements

- **Python 3.8+**
- **Redis**: For distributed task management.

## Key Features

- **Fast and Efficient**: Enables quick setup and management of distributed tasks with minimal overhead.
- **Advanced Concurrency Control**: Offers comprehensive mechanisms for controlling task execution concurrency, including task-level, argument-level, and key argument-level concurrency.
- **Automatic Task Prioritization and Pausing**: Intelligent task handling that maximizes resource efficiency and prevents deadlocks.
- **Cycle Detection**: Prevents endless loops in task execution with automated cycle detection.
- **Extensible**: Designed for modularity, allowing for easy integration with various components and future expansion.

## Installation

```bash
pip install pynenc
```

For detailed installation instructions, see the [Pynenc Documentation](https://docs.pynenc.org/).

## Quick Start

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    return x + y
```

For more examples and guides, visit our [samples library](https://github.com/pynenc/samples).

## Contact or Support

If you need help with Pynenc or want to discuss any aspects of its usage, feel free to reach out through the following channels:

- **[GitHub Issues](https://github.com/pynenc/pynenc/issues)**: For bug reports, feature requests, or other technical queries, please use our GitHub Issues page. You can create a new issue or contribute to existing discussions.

- **[GitHub Discussions](https://github.com/pynenc/pynenc/discussions)**: For more general questions, ideas exchange, or discussions about Pynenc, consider using GitHub Discussions on our repository. It's a great place to connect with other users and the development team.

Remember, your feedback and contributions are essential in helping Pynenc grow and improve!

## License

Pynenc is released under the [MIT License](https://github.com/pynenc/pynenc/blob/main/LICENSE).
