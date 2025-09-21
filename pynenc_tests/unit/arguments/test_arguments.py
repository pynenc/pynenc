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


def test_args_id() -> None:
    args = Arguments(kwargs={"a": 1, "b": 2})
    args_id = args.args_id
    assert isinstance(args_id, str)
    assert len(args_id) == 64  # Length of a SHA256 hash


def test_arguments_hash() -> None:
    args1 = Arguments(kwargs={"a": 1, "b": 2})
    args2 = Arguments(kwargs={"a": 1, "b": 2})
    assert hash(args1) == hash(args2)
    args3 = Arguments(kwargs={"a": 3, "b": 4})
    assert hash(args1) != hash(args3)
    args4 = Arguments(kwargs={})
    assert hash(args4) == hash(args4)


def test_arguments_equality() -> None:
    args1 = Arguments(kwargs={"a": 1, "b": 2})
    args2 = Arguments(kwargs={"a": 1, "b": 2})
    args3 = Arguments(kwargs={"a": 3, "b": 4})
    assert args1 == args2
    assert args1 != args3
    assert args1 != "a string"
