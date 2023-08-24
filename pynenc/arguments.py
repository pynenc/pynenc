import hashlib
import inspect
from functools import cached_property
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from .types import Args, Func


class Arguments:
    def __init__(self, kwargs: Optional["Args"] = None) -> None:
        self.kwargs: "Args" = kwargs or {}

    @classmethod
    def from_call(cls, func: "Func", *args: Any, **kwargs: Any) -> "Arguments":
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        return cls(bound_args.arguments)

    @cached_property
    def args_id(self) -> str:
        """Generate a unique id for these arguments"""
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
