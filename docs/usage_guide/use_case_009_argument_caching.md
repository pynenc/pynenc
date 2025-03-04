# Argument Caching

## Overview

This guide demonstrates Pynenc's argument caching system, which optimizes task execution by caching large serialized arguments. This feature is particularly useful when working with large objects that are passed frequently between tasks.

## Scenario

The argument caching scenario shows how to:

- Configure argument caching thresholds
- Control caching behavior per task
- Use different cache backends (Redis/Memory)

## Implementation

### Basic Usage

```python
from pynenc import Pynenc
import numpy as np

app = Pynenc()

@app.task
def process_array(data: np.ndarray) -> float:
    """Process a large numpy array."""
    return float(data.mean())

# Large arrays will be automatically cached
large_array = np.random.rand(1000000)
result = process_array(large_array)
```

### Configuration

Configure caching behavior in `pyproject.toml`:

```toml
[tool.pynenc]
arg_cache_cls = "RedisArgCache"  # or "MemArgCache"

[tool.pynenc.arg_cache]
min_size_to_cache = 1024  # Cache arguments larger than 1KB
local_cache_size = 1000   # Keep 1000 most recent entries in local cache
```

### Controlling Cache Behavior

Disable caching for specific arguments:

```python
@app.task
def process_data(data: bytes, *, disable_cache_args: list[str] = None) -> str:
    """Process data with optional cache control."""
    return len(data)

# Prevent caching of the 'data' argument
result = process_data(large_bytes, disable_cache_args=["data"])
```

## Features

- **Automatic Caching**: Large arguments are automatically cached based on size threshold
- **Multiple Backends**: Support for Redis (distributed) and Memory (local) caching
- **Cache Sharing**: Shared cache across processes using runner-level storage
- **Smart Detection**: Prevents redundant serialization of identical objects
- **LRU Cache**: Maintains most recently used items within configured limits

For more details on configuration options, refer to the {doc}`../configuration/index`.
