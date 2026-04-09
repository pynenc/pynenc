from pynenc.call import compute_args_id


def test_args_id() -> None:
    args = {"a": "1", "b": "2"}
    args_id = compute_args_id(args)
    assert isinstance(args_id, str)
    assert len(args_id) == 64  # Length of a SHA256 hash


def test_empty_args_returns_no_args() -> None:
    assert compute_args_id({}) == "no_args"


def test_deterministic_ordering() -> None:
    """Same args in different insertion order produce the same hash."""
    h1 = compute_args_id({"x": "42", "y": "hello"})
    h2 = compute_args_id({"y": "hello", "x": "42"})
    assert h1 == h2


def test_different_args_different_hash() -> None:
    h1 = compute_args_id({"x": "42"})
    h2 = compute_args_id({"x": "43"})
    assert h1 != h2


def test_no_delimiter_collision() -> None:
    """Ensure delimiter chars inside values don't cause hash collisions."""
    h1 = compute_args_id({"a": "b;c=d"})
    h2 = compute_args_id({"a": "b", "c": "d"})
    assert h1 != h2


def test_unicode_args() -> None:
    h = compute_args_id({"key": "日本語テスト"})
    assert isinstance(h, str)
    assert len(h) == 64
