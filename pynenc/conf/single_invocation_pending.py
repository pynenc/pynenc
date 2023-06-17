from abc import ABC
from typing import Any, Optional


class SingleInvocation:
    """Only one invocation in status REGISTERED for the specific task will exists in the system

    on_diff_args_raise: bool -> Behaviour when found an existing invocation with different arguments
        - False (Default)       Returns a ReusedInvocation with the different arguments
        - True:                 Raises an exception

    """

    # REGISTERED is the initial status, even before pending
    # when the start gets to run or it's managed by the system it will let new calls be registered

    def __init__(self, on_diff_args_raise: bool = False) -> None:
        self.on_diff_args_raise = on_diff_args_raise

    def get_key_arguments(self, arguments: dict[str, Any]) -> Optional[dict[str, Any]]:
        del arguments
        return None


class SingleInvocationPerArguments(SingleInvocation):
    """Only one task request per unique set of arguments can be status:registed in the system"""

    def get_key_arguments(self, arguments: dict[str, Any]) -> Optional[dict[str, Any]]:
        return arguments


class SingleInvocationPerKeyArguments(SingleInvocation):
    """Only one task request per unique set of key arguments can be status:registed in the system

    on_diff_args_raise: bool -> Behaviour when found an existing invocation with different arguments
        - False (Default)       Returns a ReusedInvocation with the different arguments
        - True:                 Raises an exception

    """

    def __init__(self, keys: list[str], on_diff_args_raise: bool = False) -> None:
        self.on_diff_args_raise = on_diff_args_raise
        self.keys = keys

    def get_key_arguments(self, arguments: dict[str, Any]) -> Optional[dict[str, Any]]:
        return {key: arguments[key] for key in self.keys}
