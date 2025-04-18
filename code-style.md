# Pynenc Code Guidelines

## Type System

- Type hints are mandatory for all function parameters and return values
- Use built-in types instead of typing imports:
  - Use `list[str]` not `List[str]`
  - Use `dict[str, int]` not `Dict[str, int]`
  - Use `tuple[int, str]` not `Tuple[int, str]`
  - Use `type[MyClass]` not `Type[MyClass]`
  - Use `x: str | None` not `Optional[str]`
- If a Class is only imported for typing it should be imported under TYPE_CHECKING and use with quotes

```python
if TYPE_CHECKING:
    from pynenc.app import Pynenc

def function(app: "Pynenc"):
  """Properly documented docstring..."""
```

## Code Structure

- Avoid deep nesting; extract logic to functions with descriptive names when indentation exceeds 2 levels
- Keep functions focused and concise
- Use walrus operator (`:=`) when it reduces unnecessary variable assignments
- Use structural pattern matching (`match`) for complex conditionals when it improves readability

## Documentation

- Document "why" not "what" in comments
- Class and function docstrings should explain purpose and behavior concisely

## Review Process

- Always examine existing code for improvements
- Refactor for clarity and consistency with project patterns

## Documentation Examples

### Module Documentation

```python
"""
Base classes and interfaces for Pynenc's trigger system.

This module defines the core abstractions for time-based scheduling and event-driven task execution.
The trigger component enables declarative scheduling of tasks through cron expressions, events,
and task dependency chains.

Key components:
- BaseTrigger: Abstract base class for trigger implementations
- TriggerCondition: Base class for different trigger conditions
- EventDefinition: Structured definition of event types
- TriggerDefinition: Configuration linking conditions to tasks
"""
```

### Important Function Documentation

```python
def emit_event(self, event_code: str, payload: dict[str, Any] = None) -> EventInstance:
    """
    Emit an event into the system to potentially trigger tasks.

    Creates and processes an event against registered trigger definitions.
    Any matching triggers will execute their associated tasks using provided
    payload data.

    :param event_code: Identifier for the event category
    :param payload: Data associated with the event
    :return: The created event instance with metadata
    :raises ValueError: If event_code is not registered
    """
```

### Basic Function Documentation

```python
def is_active(self, trigger_id: str) -> bool:
    """
    Check if a trigger is currently active.

    :param trigger_id: ID of the trigger to check
    :return: True if trigger exists and is active
    """
```
