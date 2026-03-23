"""Tests for scanning a module for Pynenc instances."""

import types

import pytest

from pynenc.app import Pynenc
from pynenc.util import import_app


def test_finds_instance() -> None:
    app = Pynenc()
    module = types.ModuleType("test_mod")
    module.app = app  # type: ignore[attr-defined]

    assert import_app._find_pynenc_in_module(module) is app


def test_no_instance_raises() -> None:
    module = types.ModuleType("empty_mod")
    module.x = 42  # type: ignore[attr-defined]

    with pytest.raises(ValueError, match="No Pynenc.*instance found"):
        import_app._find_pynenc_in_module(module)
