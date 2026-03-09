# Custom JSON Serialization

## Overview

Pynenc's `JsonSerializer` uses `DefaultJSONEncoder` under the hood. By default it
handles Python exceptions and primitive types. If you pass a custom domain object as
a task argument or return value you will get a `TypeError` unless you either:

1. Switch to `JsonPickleSerializer` (no code changes, but larger payloads), or
2. Implement the `JsonSerializable` protocol on your class — two methods,
   `to_json()` and `from_json()`, that give full round-trip support.

Option 2 keeps payloads small and explicit, and requires no dependency on third-party
serialization libraries.

## The `JsonSerializable` Protocol

```python
from pynenc.serializer.json_serializer import JsonSerializable
```

Any class that implements **both** `to_json()` and `from_json()` satisfies the protocol:

| Method                         | Direction     | Signature                                             |
| ------------------------------ | ------------- | ----------------------------------------------------- |
| `to_json(self) -> Any`         | object → JSON | Return a JSON-native value (`dict`, `list`, `str`, …) |
| `from_json(cls, data) -> Self` | JSON → object | Reconstruct from the value `to_json` returned         |

`DefaultJSONEncoder` embeds the class's module and qualified name alongside the
serialized data so that `JsonSerializer.deserialize` can find the class and call
`from_json` automatically.

## Implementation

### 1. Define your domain object

```python
from pynenc.serializer.json_serializer import JsonSerializable


class Money:
    def __init__(self, amount: float, currency: str) -> None:
        self.amount = amount
        self.currency = currency

    def to_json(self) -> dict:
        return {"amount": self.amount, "currency": self.currency}

    @classmethod
    def from_json(cls, data: dict) -> "Money":
        return cls(data["amount"], data["currency"])
```

### 2. Use it in tasks normally

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def calculate_total(unit_price: Money, quantity: int) -> Money:
    return Money(unit_price.amount * quantity, unit_price.currency)
```

Both the argument and the return value are transparently serialized and deserialized —
the runner receives a proper `Money` instance, not a raw `dict`.

### 3. Verify round-trip manually

```python
from pynenc.serializer import JsonSerializer

price = Money(9.99, "EUR")
serialized = JsonSerializer.serialize(price)
restored = JsonSerializer.deserialize(serialized)

assert isinstance(restored, Money)
assert restored.amount == 9.99
assert restored.currency == "EUR"
```

## Checking Protocol Compliance at Runtime

Because `JsonSerializable` is `@runtime_checkable`, you can verify compliance before
calling a task:

```python
from pynenc.serializer.json_serializer import JsonSerializable

assert isinstance(Money(1.0, "USD"), JsonSerializable)  # passes
```

## Comparison of Serializers

| Approach                              | Setup            | Payload size     | Round-trip |
| ------------------------------------- | ---------------- | ---------------- | ---------- |
| `JsonSerializer` + `JsonSerializable` | Add two methods  | Small (explicit) | Automatic  |
| `JsonPickleSerializer`                | None             | Larger           | Automatic  |
| Custom encoder subclass               | Subclass encoder | Any              | Any        |

## When to Use

- You control the class definition and want minimal, readable JSON payloads.
- You are integrating legacy domain objects into Pynenc tasks.
- You want static-analysis-friendly typing via the `Protocol`.

## See Also

- {doc}`use_case_009_client_data_store` — caching large serialized arguments
