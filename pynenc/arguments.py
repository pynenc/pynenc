import hashlib
import inspect
from functools import cached_property
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from pynenc.types import Args, Func


class Arguments:
    """
    Represents a distinctive set of arguments used in a task call.

    This class facilitates the handling of arguments by providing functionalities such as
    generating a unique identifier for a set of arguments, and supporting hashability and equality checks.

    :param Optional[Args] kwargs:
        Keyword arguments to initialize the Arguments object.
        Defaults to an empty dictionary if None is provided.
    """

    def __init__(self, kwargs: Optional["Args"] = None) -> None:
        self.kwargs: "Args" = kwargs or {}

    @classmethod
    def from_call(cls, func: "Func", *args: Any, **kwargs: Any) -> "Arguments":
        """
        Creates an Arguments object from a function call.

        It uses inspect.signature to determine the function's signature and binds the provided arguments to it.

        :param Func func: The function whose arguments are to be processed.
        :param Any args: Positional arguments of the function call.
        :param Any kwargs: Keyword arguments of the function call.
        :return: An instance of Arguments representing the arguments of the call.
        """
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        return cls(bound_args.arguments)

    @cached_property
    def args_id(self) -> str:
        """
        Generates a unique identifier for the set of arguments.

        The identifier is a SHA-256 hash of the sorted argument names and values, ensuring uniqueness.

        :return: A string representing the unique identifier of the arguments.
        """
        if not self.kwargs:
            return "no_args"
        sorted_items = sorted(self.kwargs.items())
        args_str = "".join([f"{k}:{v}" for k, v in sorted_items])
        return hashlib.sha256(args_str.encode()).hexdigest()

    def __hash__(self) -> int:
        return hash(self.args_id)

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Arguments):
            return False
        return self.args_id == __value.args_id

    def __str__(self) -> str:
        return str(self.kwargs)

    def __repr__(self) -> str:
        return f"Arguments({self.kwargs})"
