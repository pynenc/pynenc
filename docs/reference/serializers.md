# Serializers

Reference for Pynenc serializer implementations, their type support, and tradeoffs.

## Overview

Serializers convert task arguments and return values to strings for storage and transport. All serializers implement two static methods:

```python
class BaseSerializer(ABC):
    @staticmethod
    @abstractmethod
    def serialize(obj: Any) -> str: ...

    @staticmethod
    @abstractmethod
    def deserialize(obj: str) -> Any: ...
```

Select a serializer via configuration:

```toml
[tool.pynenc]
serializer_cls = "JsonPickleSerializer"
```

Or the builder:

```python
PynencBuilder().serializer_json_pickle().build()
```

## JsonSerializer

Pure JSON serialization with a custom encoder/decoder. Handles a defined set of types safely.

### Supported Types

| Type                                                                | Serialization                                   |
| ------------------------------------------------------------------- | ----------------------------------------------- |
| JSON-native (`str`, `int`, `float`, `bool`, `None`, `list`, `dict`) | Standard `json.dumps`                           |
| `Enum` (including `IntEnum`, `StrEnum`)                             | `{module, qualname, value}` wrapper             |
| Built-in exceptions (`ValueError`, `TypeError`, etc.)               | `{type, args, message}` wrapper                 |
| Custom exceptions                                                   | `{module, qualname, args, message}` wrapper     |
| `JsonSerializable` implementors                                     | `{module, qualname, data}` wrapper via protocol |

Types not in this list raise `TypeError` during serialization.

### `JsonSerializable` Protocol

Any class implementing this protocol gets automatic round-trip serialization with `JsonSerializer`:

```python
class MyData:
    def __init__(self, value: int):
        self.value = value

    def to_json(self) -> dict:
        return {"value": self.value}

    @classmethod
    def from_json(cls, data: dict) -> "MyData":
        return cls(**data)
```

See {doc}`../usage_guide/use_case_007_json_serializable` for a complete example.

### Tradeoffs

- **Safe**: No arbitrary code execution on deserialization
- **Human-readable**: Output is standard JSON
- **Limited type support**: Custom classes must implement `JsonSerializable`

## JsonPickleSerializer

Uses the `jsonpickle` library to serialize arbitrary Python objects as JSON.

### Supported Types

Handles virtually all Python types including dataclasses, NamedTuples, custom objects, and nested structures — without requiring any protocol implementation.

### Tradeoffs

- **Broad type support**: Works with nearly any Python type out of the box
- **Human-readable**: Output is JSON text
- **Unsafe**: `jsonpickle` can execute arbitrary code during deserialization — use only with trusted data

```{warning}
`jsonpickle` deserialization can execute arbitrary code. Only use `JsonPickleSerializer` when all data in the broker and state backend is from trusted sources.
```

## PickleSerializer

Uses Python's `pickle` module with base64 encoding for string output.

### Supported Types

Handles all picklable Python objects.

### Tradeoffs

- **Broadest type support**: Anything `pickle` can handle
- **Not human-readable**: Output is base64-encoded binary
- **Unsafe**: Same arbitrary code execution risks as `pickle`

## Comparison

| Serializer               | Format | Human-Readable | Type Support                                        | Security | Protocol Required      |
| ------------------------ | ------ | -------------- | --------------------------------------------------- | -------- | ---------------------- |
| **JsonSerializer**       | JSON   | Yes            | JSON-native + Enum + Exception + `JsonSerializable` | Safe     | Yes (for custom types) |
| **JsonPickleSerializer** | JSON   | Yes            | Nearly all Python types                             | Unsafe   | No                     |
| **PickleSerializer**     | Base64 | No             | All picklable types                                 | Unsafe   | No                     |

## Choosing a Serializer

- **`JsonSerializer`**: Best for environments where you control all task argument types and need safe deserialization. Requires implementing `JsonSerializable` for custom classes.
- **`JsonPickleSerializer`** (default): Best for rapid development and internal systems where all data is trusted. No protocol implementation needed.
- **`PickleSerializer`**: Use when you need maximum compatibility and don't need human-readable output.

## Reserved Keys

`JsonSerializer` uses reserved keys in the serialized JSON to identify special types:

| Key                             | Constant                         | Purpose                      |
| ------------------------------- | -------------------------------- | ---------------------------- |
| `__pynenc__std_py_exc__`        | `ReservedKeys.ERROR`             | Built-in Python exceptions   |
| `__pynenc__client_exception__`  | `ReservedKeys.CLIENT_EXCEPTION`  | User-defined exceptions      |
| `__pynenc__json_serializable__` | `ReservedKeys.JSON_SERIALIZABLE` | `JsonSerializable` objects   |
| `__pynenc__enum__`              | `ReservedKeys.ENUM`              | Enum values                  |
| `__pynenc__client_data__`       | `ReservedKeys.CLIENT_DATA`       | Client data store references |

These are defined in `pynenc.serializer.constants.ReservedKeys`.

See {doc}`../configuration/index` for serializer configuration options.
See {doc}`builder` for programmatic serializer selection with the builder API.
