from pynenc.call import compute_args_id


def test_args_id() -> None:
    args = {"a": "1", "b": "2"}
    args_id = compute_args_id(args)
    assert isinstance(args_id, str)
    assert len(args_id) == 64  # Length of a SHA256 hash
