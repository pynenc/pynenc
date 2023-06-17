from abc import ABC
from typing import Any, Optional


class SingleInvocationPending:
    """Only one execution request for that task will exists in the system"""

    def get_key_arguments(self, arguments: dict[str, Any]) -> Optional[dict[str, Any]]:
        del arguments
        return None


class SingleInvocationPerArgumentsPending(SingleInvocationPending):
    """Only one task request per unique set of arguments can be pending in the system"""

    def get_key_arguments(self, arguments: dict[str, Any]) -> Optional[dict[str, Any]]:
        return arguments


class SingleInvocationPerKeyArgumentsPending(SingleInvocationPending):
    """Only one task request per unique set of key arguments can be pending in the system"""

    def __init__(self, keys: list[str]) -> None:
        self.keys = keys

    def get_key_arguments(self, arguments: dict[str, Any]) -> Optional[dict[str, Any]]:
        return {key: arguments[key] for key in self.keys}
