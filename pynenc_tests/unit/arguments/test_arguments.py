import pytest

from pynenc.arguments import Arguments


def test_arguments_init() -> None:
    args = Arguments(kwargs={"a": 1, "b": 2})
    assert args.kwargs == {"a": 1, "b": 2}


def test_arguments_from_call() -> None:
    def sample_function(a: int, b: int, c: int = 3) -> None:
        pass

    args = Arguments.from_call(sample_function, 1, b=2)
    expected_kwargs = {"a": 1, "b": 2, "c": 3}
    assert args.kwargs == expected_kwargs


def test_arguments_unhashable() -> None:
    args1 = Arguments(kwargs={"a": 1, "b": 2})
    with pytest.raises(TypeError):
        hash(args1)
    args_empty = Arguments(kwargs={})
    with pytest.raises(TypeError):
        hash(args_empty)


def test_arguments_equality_forbidden() -> None:
    with pytest.raises(NotImplementedError):
        assert Arguments() == Arguments()
