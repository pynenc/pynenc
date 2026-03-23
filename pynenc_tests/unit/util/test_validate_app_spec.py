"""Tests for app spec format validation."""

import pytest

from pynenc.util import import_app


def test_valid_dotted_path_passes() -> None:
    import_app._validate_app_spec("tasks.app")  # should not raise


def test_colon_raises_with_help() -> None:
    with pytest.raises(ValueError, match="dot notation"):
        import_app._validate_app_spec("mod:var")
