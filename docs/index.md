# Welcome to Pynenc's Documentation!

**Pynenc: A task management system for complex distributed orchestration.**

## Introduction

Pynenc is a task management tool designed for orchestration in distributed Python environments. It simplifies the orchestration of tasks with an emphasis on user-friendly configuration and efficient execution.

```{toctree}
:hidden:
:maxdepth: 2
:caption: Table of Contents

overview
getting_started/index
usage_guide/index
configuration/index
cli/index
apidocs/index.rst
contributing/index
faq
changelog
license
```

## Key Features

- Intuitive Orchestration
- Configurable Concurrency Management
- Workflow System for Complex Task Orchestration with Deterministic Execution
- Trigger System for Event-Driven and Scheduled Tasks
- Automatic Task Prioritization
- Automatic Task Pausing
- Cycle Detection
- Modularity and Extensibility
- Flexible Configuration Builder

For more details on these features, refer to the {doc}`usage_guide/index`.

## Installation

Pynenc can be easily installed using pip:

```bash
pip install pynenc
```

Refer to the {doc}`getting_started/index` section for more detailed installation instructions.

## Quick Start

Define your first task:

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    return x + y
```

And get the result (requires a distributed runner, redis or dev mode):

```python
result = add(1, 2).result
```

Get started quickly with a basic example in the {doc}`getting_started/index` section.

## Requirements

Pynenc currently requires Redis for distributed task management. Future updates will expand its compatibility with other databases and message queues.

## Contact or Support

Need help or want to discuss Pynenc? Check out our [GitHub Issues](https://github.com/pynenc/pynenc/issues) and [GitHub Discussions](https://github.com/pynenc/pynenc/discussions).

## License

Pynenc is released under the MIT License. For more information, see {doc}`license`.
