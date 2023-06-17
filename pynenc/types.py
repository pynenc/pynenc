import typing

if typing.TYPE_CHECKING:
    from typing_extensions import ParamSpec

    Params = ParamSpec("Params")
else:
    Params = typing.TypeVar("Params")

Result = typing.TypeVar("Result")
Func = typing.Callable[Params, Result]
Args = typing.Dict[str, typing.Any]
