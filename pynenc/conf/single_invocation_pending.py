from typing import Any, Optional

from .base_config_option import BaseConfigOption


class SingleInvocation(BaseConfigOption):
    """Only one invocation in status REGISTERED for the specific task will exists in the system

    on_diff_args_raise: bool -> Behaviour when found an existing invocation with different arguments
        - False (Default)       Returns a ReusedInvocation with the different arguments
        - True:                 Raises an exception

    """

    # REGISTERED is the initial status, even before pending
    # when the start gets to run or it's managed by the system it will let new calls be registered

    def __init__(self, on_diff_args_raise: bool = False) -> None:
        self.on_diff_args_raise = on_diff_args_raise

    def get_key_arguments(
        self, serialized_arguments: dict[str, str]
    ) -> Optional[dict[str, str]]:
        del serialized_arguments
        return None

    def _to_dict(self) -> dict[str, Any]:
        """Returns a dictionary with the options"""
        return self.__dict__

    @classmethod
    def _from_dict(cls, options_dict: dict[str, Any]) -> "SingleInvocation":
        """Returns a new options from a dictionary"""
        return cls(**options_dict)


class SingleInvocationPerArguments(SingleInvocation):
    """Only one task request per unique set of arguments can be status:registed in the system"""

    def get_key_arguments(
        self, serialized_arguments: dict[str, str]
    ) -> Optional[dict[str, str]]:
        return serialized_arguments


class SingleInvocationPerKeyArguments(SingleInvocation):
    """Only one task request per unique set of key arguments can be status:registed in the system

    on_diff_args_raise: bool -> Behaviour when found an existing invocation with different arguments
        - False (Default)       Returns a ReusedInvocation with the different arguments
        - True:                 Raises an exception

    """

    def __init__(self, keys: list[str], on_diff_args_raise: bool = False) -> None:
        self.on_diff_args_raise = on_diff_args_raise
        self.keys = keys

    def get_key_arguments(
        self, serialized_arguments: dict[str, str]
    ) -> Optional[dict[str, str]]:
        return {key: serialized_arguments[key] for key in self.keys}
