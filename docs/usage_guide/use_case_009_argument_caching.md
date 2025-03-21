# Argument Caching

## Overview

This guide demonstrates Pynenc's argument caching system, which optimizes task execution by caching large serialized arguments. This feature is particularly useful when working with large objects frequently passed between tasks.

## Scenario

The argument caching scenario illustrates how to:

- Configure argument caching thresholds
- Control caching behavior per task
- Utilize different cache backends (Redis/Memory)

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
large_array = np.random.rand(1_000_000)
result = process_array(large_array)
```

### Configuration

Configure caching behavior using either `pyproject.toml` or `PynencBuilder`:

#### Using pyproject.toml

Set up caching in your `pyproject.toml` file:

```toml
[tool.pynenc]
arg_cache_cls = "RedisArgCache"  # or "MemArgCache"

[tool.pynenc.arg_cache]
min_size_to_cache = 1024  # Cache arguments larger than 1KB
local_cache_size = 1000   # Keep 1000 most recent entries in local cache
```

#### Using PynencBuilder

Alternatively, configure argument caching programmatically using `PynencBuilder` in your application code (e.g., in `tasks.py`):

```python
from pynenc import Pynenc
from pynenc.builder import PynencBuilder
import numpy as np

app = (
    PynencBuilder()
    .redis(url="redis://localhost:6379")   # Required for Redis caching
    .arg_cache(
        mode="redis",                      # Sets RedisArgCache; options: "redis", "memory", "disabled"
        min_size_to_cache=1024,            # Cache arguments larger than 1KB
        local_cache_size=1000              # Keep 1000 most recent entries in local cache
    )
    .build()
)

@app.task
def process_array(data: np.ndarray) -> float:
    """Process a large numpy array."""
    return float(data.mean())

# Large arrays will be automatically cached
large_array = np.random.rand(1_000_000)
result = process_array(large_array)
```

The `.arg_cache()` method allows selecting one of the following modes:

- `"redis"`: (Default; requires `.redis()` configuration)
- `"memory"`: For local testing or development purposes
- `"disabled"`: Completely disables argument caching

Additionally, `.arg_cache()` directly accepts parameters like `min_size_to_cache` and `local_cache_size`, simplifying configuration compared to using `.custom_config()`.

### Controlling Cache Behavior

Disable caching for specific arguments:

```python
@app.task
def process_data(data: bytes, *, disable_cache_args: list[str] = None) -> int:
    """Process data with optional cache control."""
    return len(data)

# Prevent caching of the 'data' argument
result = process_data(large_bytes, disable_cache_args=["data"])
```

## Features

- **Automatic Caching**: Large arguments automatically cached based on the defined size threshold.
- **Multiple Backends**: Supports Redis (distributed) and memory-based (local) caching.
- **Cache Sharing**: Shared cache across processes using runner-level storage.
- **Smart Detection**: Avoids redundant serialization of identical objects.
- **LRU Cache**: Maintains recently used items within configured limits.

For more details on configuration options, refer to the [Configuration documentation](../configuration/index).
