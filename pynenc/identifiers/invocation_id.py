"""Invocation identifier type with validation.

Lightweight wrapper around UUID strings for type safety and validation.
Unlike TaskId/CallId, InvocationId has no internal structure - it's an
opaque identifier generated for each execution instance.
"""

from typing import NewType
import uuid


# Strong typing for invocation IDs
InvocationId = NewType("InvocationId", str)


def generate_invocation_id() -> InvocationId:
    """Generate a new unique invocation ID.

    :return: A new UUID-based invocation identifier
    """
    return InvocationId(str(uuid.uuid4()))
