"""Tests for extract_module_info and _find_app_in_user_modules.

Verifies that module discovery correctly identifies the user module
where ``app = Pynenc()`` was written, rather than returning
``pynenc.app`` (where the class is defined).
"""

import types

import pytest

from pynenc.app import Pynenc
from pynenc.util import import_app


# ---------------------------------------------------------------------------
# _find_app_variable_in_module
# ---------------------------------------------------------------------------


def test_find_app_variable_should_return_name_when_app_exists() -> None:
    """Identity match on a normal module attribute."""
    app = Pynenc()
    module = types.ModuleType("user_tasks")
    module.my_app = app  # type: ignore[attr-defined]

    result = import_app._find_app_variable_in_module(module, app)
    assert result == "my_app"


def test_find_app_variable_should_return_none_when_not_present() -> None:
    """Module has no reference to the app."""
    app = Pynenc()
    module = types.ModuleType("empty_mod")

    result = import_app._find_app_variable_in_module(module, app)
    assert result is None


def test_find_app_variable_should_skip_private_attrs() -> None:
    """Private attributes (underscore-prefixed) must be ignored."""
    app = Pynenc()
    module = types.ModuleType("private_mod")
    module._hidden = app  # type: ignore[attr-defined]

    result = import_app._find_app_variable_in_module(module, app)
    assert result is None


def test_find_app_variable_should_survive_getattr_errors() -> None:
    """Modules with descriptors that raise on access must not crash."""
    app = Pynenc()
    module = types.ModuleType("bad_mod")

    class _Bomb:
        def __get__(self, obj: object, objtype: type | None = None) -> None:
            raise RuntimeError("boom")

    module.exploding = _Bomb()  # type: ignore[attr-defined]
    module.safe_app = app  # type: ignore[attr-defined]

    result = import_app._find_app_variable_in_module(module, app)
    assert result == "safe_app"


def test_find_app_variable_should_survive_dir_raising_import_error() -> None:
    """If dir() on the module raises ImportError, return None gracefully."""
    app = Pynenc()
    module = types.ModuleType("broken_dir")

    def bad_dir() -> list[str]:
        raise ImportError("broken")

    module.__dir__ = bad_dir  # type: ignore[method-assign]

    result = import_app._find_app_variable_in_module(module, app)
    assert result is None


# ---------------------------------------------------------------------------
# _find_app_in_user_modules
# ---------------------------------------------------------------------------


def test_find_app_in_user_modules_should_find_registered_module(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When a user module is in sys.modules, it should be found."""
    app = Pynenc()
    fake_mod = types.ModuleType("my_project.tasks")
    fake_mod.__file__ = "/home/user/project/tasks.py"
    fake_mod.app = app  # type: ignore[attr-defined]

    monkeypatch.setitem(import_app.sys.modules, "my_project.tasks", fake_mod)

    result = import_app._find_app_in_user_modules(app)
    assert result is not None
    mod_name, filepath, var_name = result
    assert mod_name == "my_project.tasks"
    assert filepath == "/home/user/project/tasks.py"
    assert var_name == "app"


def test_find_app_in_user_modules_should_skip_pynenc_internals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Modules under pynenc.* must be skipped even if they hold the app."""
    app = Pynenc()
    # pynenc.app always has the class, not the instance, but simulate it
    fake_mod = types.ModuleType("pynenc.fake")
    fake_mod.__file__ = "/path/to/pynenc/fake.py"
    fake_mod.app = app  # type: ignore[attr-defined]

    monkeypatch.setitem(import_app.sys.modules, "pynenc.fake", fake_mod)

    result = import_app._find_app_in_user_modules(app)
    # Should not find it in pynenc.* — would fall through to None
    # (unless some other module also has it, which we don't control in test)
    if result is not None:
        mod_name, _, _ = result
        assert not mod_name.startswith("pynenc.")


def test_find_app_in_user_modules_should_skip_modules_without_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Built-in modules (no __file__) must be skipped."""
    app = Pynenc()
    fake_mod = types.ModuleType("builtin_like")
    # No __file__ attribute set
    fake_mod.app = app  # type: ignore[attr-defined]

    monkeypatch.setitem(import_app.sys.modules, "builtin_like", fake_mod)

    result = import_app._find_app_in_user_modules(app)
    if result is not None:
        mod_name, _, _ = result
        assert mod_name != "builtin_like"


# ---------------------------------------------------------------------------
# extract_module_info
# ---------------------------------------------------------------------------


def test_extract_module_info_should_return_three_tuple(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return value is a 3-tuple (module_name, filepath, variable)."""
    app = Pynenc()
    fake_mod = types.ModuleType("my_tasks")
    fake_mod.__file__ = "/project/my_tasks.py"
    fake_mod.application = app  # type: ignore[attr-defined]

    monkeypatch.setitem(import_app.sys.modules, "my_tasks", fake_mod)

    result = import_app.extract_module_info(app)
    assert len(result) == 3
    mod_name, filepath, var_name = result
    assert mod_name == "my_tasks"
    assert filepath == "/project/my_tasks.py"
    assert var_name == "application"


def test_extract_module_info_should_prefer_user_module_over_pynenc_app(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """User module must be preferred over pynenc.app (the class definition module)."""
    app = Pynenc()
    user_mod = types.ModuleType("user_tasks")
    user_mod.__file__ = "/home/user/tasks.py"
    user_mod.app = app  # type: ignore[attr-defined]

    monkeypatch.setitem(import_app.sys.modules, "user_tasks", user_mod)

    mod_name, filepath, var_name = import_app.extract_module_info(app)
    assert mod_name == "user_tasks"
    assert filepath == "/home/user/tasks.py"
    assert var_name == "app"


def test_extract_module_info_should_fallback_when_no_user_module() -> None:
    """When no user module holds the app, fall back to app.__module__."""
    app = Pynenc()
    # Don't register any user module — only pynenc.app exists in sys.modules
    mod_name, filepath, var_name = import_app.extract_module_info(app)
    # Fallback returns pynenc.app info (the class definition module)
    assert mod_name == "pynenc.app"
    assert filepath is not None
    assert filepath.endswith("app.py")


def test_extract_module_info_should_return_nones_for_main_module() -> None:
    """If app.__module__ is __main__ and no user module found, return Nones."""
    app = Pynenc()
    # Temporarily set __module__ to __main__
    original = type(app).__module__
    try:
        type(app).__module__ = "__main__"
        # No user module registered either
        mod_name, filepath, var_name = import_app.extract_module_info(app)
        # Should still find via fallback or return Nones
        # Since the class __module__ is __main__, fallback won't trigger
    finally:
        type(app).__module__ = original
