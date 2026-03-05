# Pynenc Documentation

**A task management system for complex distributed orchestration in Python.**

Pynenc manages the routing, scheduling, and execution of tasks across distributed Python processes. It provides automatic orchestration, concurrency control, workflow management, and a plugin-based architecture for swapping backends without changing application code.

## Installation

```bash
pip install pynenc
```

### Backend Plugins

| Plugin   | Install                       | Backend                |
| -------- | ----------------------------- | ---------------------- |
| Redis    | `pip install pynenc-redis`    | Redis backend plugin   |
| MongoDB  | `pip install pynenc-mongodb`  | Document-store backend |
| RabbitMQ | `pip install pynenc-rabbitmq` | Message-broker backend |

### Monitoring

```bash
pip install pynenc[monitor]
```

## Quick Start

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    return x + y

# Route and retrieve the result
result = add(1, 2).result
```

For a complete walkthrough, see the {doc}`getting_started/index` tutorial.

## What's New in v0.1.0

- **Plugin Architecture**: Modular backend system with Redis, MongoDB, and RabbitMQ as separate plugins
- **Invocation State Machine**: Declarative, type-safe status management with ownership tracking
- **Runner Recovery**: Automatic detection and recovery of stuck invocations from inactive runners
- **Enhanced Monitoring**: SVG-based timeline visualization, runner monitoring, workflow tracking
- **Fluent Builder API**: Extensible `PynencBuilder` with plugin-provided methods

See the {doc}`changelog` for the complete list of changes.

```{toctree}
:hidden:
:maxdepth: 2
:caption: Learn

getting_started/index
usage_guide/index
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Understand

overview
faq
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Reference

configuration/index
cli/index
monitoring/index
reference/builder
reference/runners
reference/serializers
apidocs/index.rst
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Project

contributing/index
changelog
license
```
