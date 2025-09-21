from pynenc.orchestrator.mem_orchestrator import ArgPair


def test_argpair_initialization() -> None:
    key = "test_key"
    value = [1, 2, 3]
    pair = ArgPair(key, value)

    assert pair.key == key
    assert pair.value == value


def test_argpair_hash() -> None:
    pair1 = ArgPair("key", [1, 2, 3])
    pair2 = ArgPair("key", [1, 2, 3])
    pair3 = ArgPair("key", [4, 5, 6])

    assert hash(pair1) == hash(pair2)
    assert hash(pair1) != hash(pair3)


def test_argpair_equality() -> None:
    pair1 = ArgPair("key", [1, 2, 3])
    pair2 = ArgPair("key", [1, 2, 3])
    pair3 = ArgPair("key", [4, 5, 6])
    non_pair = "not_an_argpair"

    assert pair1 == pair2
    assert pair1 != pair3
    assert pair1 != non_pair


def test_argpair_string_representation() -> None:
    pair = ArgPair("key", [1, 2, 3])
    expected_str = "key:[1, 2, 3]"
    expected_repr = f"ArgPair({expected_str})"

    assert str(pair) == expected_str
    assert repr(pair) == expected_repr
