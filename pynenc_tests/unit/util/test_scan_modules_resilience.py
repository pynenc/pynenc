"""Tests for _find_pynenc_by_id_in_module and create_app_from_info resilience.

Verifies that scanning arbitrary modules for Pynenc instances handles
exceptions from lazy-loading modules (e.g. ``six.moves`` triggering
``ModuleNotFoundError``) without crashing.
"""

import logging
import types
from unittest.mock import patch

import pytest

from pynenc.app import Pynenc
from pynenc.app_info import AppInfo
from pynenc.util import import_app


# ---------------------------------------------------------------------------
# _find_pynenc_by_id_in_module — getattr raises arbitrary exceptions
# ---------------------------------------------------------------------------


def test_getattr_raising_import_error_is_skipped() -> None:
    """ModuleNotFoundError from lazy __getattr__ must not crash the scan."""
    module = types.ModuleType("lazy_mod")

    class _LazyDescriptor:
        def __get__(self, obj: object, objtype: type | None = None) -> None:
            raise ModuleNotFoundError("No module named '_gdbm'")

    module.dbm_gnu = _LazyDescriptor()  # type: ignore[attr-defined]

    result = import_app._find_pynenc_by_id_in_module(module, "lazy_mod", "any_id")
    assert result is None


def test_getattr_raising_runtime_error_is_skipped() -> None:
    """RuntimeError from exotic __getattr__ must not crash the scan."""
    module = types.ModuleType("weird_mod")

    class _Boom:
        def __get__(self, obj: object, objtype: type | None = None) -> None:
            raise RuntimeError("descriptor chaos")

    module.chaos = _Boom()  # type: ignore[attr-defined]

    result = import_app._find_pynenc_by_id_in_module(module, "weird_mod", "any_id")
    assert result is None


def test_finds_matching_app_id() -> None:
    """Normal case: module has a Pynenc instance with matching app_id."""
    app = Pynenc()
    module = types.ModuleType("good_mod")
    module.app = app  # type: ignore[attr-defined]

    result = import_app._find_pynenc_by_id_in_module(module, "good_mod", app.app_id)
    assert result is app


def test_skips_non_matching_app_id() -> None:
    """Returns None when app_id doesn't match."""
    app = Pynenc()
    module = types.ModuleType("good_mod")
    module.app = app  # type: ignore[attr-defined]

    result = import_app._find_pynenc_by_id_in_module(
        module, "good_mod", "nonexistent_id"
    )
    assert result is None


# ---------------------------------------------------------------------------
# create_app_from_info — _scan_loaded_modules failure must not prevent fallback
# ---------------------------------------------------------------------------


def test_create_app_from_info_returns_none_when_scan_raises() -> None:
    """If _scan_loaded_modules raises ImportError, create_app_from_info returns None
    so that Pynenc.from_info's config-based fallback can execute."""
    app_info = AppInfo(
        app_id="test_app",
        module="some.module",
        module_filepath=None,
        app_variable=None,
    )

    with (
        patch.object(import_app, "_import_app_from_module", return_value=None),
        patch.object(
            import_app,
            "_scan_loaded_modules",
            side_effect=ModuleNotFoundError("_gdbm"),
        ),
    ):
        result = import_app.create_app_from_info(app_info)

    assert result is None


def test_create_app_from_info_logs_warning_on_unexpected_error(
    caplog: "pytest.LogCaptureFixture",
) -> None:
    """Unexpected exceptions from _scan_loaded_modules are logged at WARNING
    so they surface for debugging, but don't crash the caller."""
    app_info = AppInfo(
        app_id="test_app",
        module="some.module",
        module_filepath=None,
        app_variable=None,
    )

    with (
        patch.object(import_app, "_import_app_from_module", return_value=None),
        patch.object(
            import_app,
            "_scan_loaded_modules",
            side_effect=RuntimeError("totally unexpected"),
        ),
        caplog.at_level(logging.WARNING, logger="pynenc.util.import_app"),
    ):
        result = import_app.create_app_from_info(app_info)

    assert result is None
    assert "Unexpected error scanning modules" in caplog.text
    assert "test_app" in caplog.text


def test_create_app_from_info_succeeds_via_scan() -> None:
    """Normal path: _scan_loaded_modules finds the app."""
    app = Pynenc()
    app_info = AppInfo(
        app_id=app.app_id,
        module="some.module",
        module_filepath=None,
        app_variable=None,
    )

    with (
        patch.object(import_app, "_import_app_from_module", return_value=None),
        patch.object(import_app, "_scan_loaded_modules", return_value=app),
    ):
        result = import_app.create_app_from_info(app_info)

    assert result is app
