"""
Utilities for discovering and loading Pynenc application instances.

The CLI uses ``--app`` to locate the user's ``Pynenc`` instance.  Accepted
formats (see :func:`find_app_instance`):

- **module.attr** -- ``tasks.app`` imports ``tasks`` module, finds the ``Pynenc`` instance.
- **package.module** -- ``mypackage.tasks`` standard Python import.
- **file path** -- ``path/to/tasks.py`` loads the file directly.

Key components:
- find_app_instance: Main entry point for ``--app`` resolution.
- extract_module_info: Extracts module metadata from a live Pynenc instance.
- create_app_from_info: Re-hydrates a Pynenc instance from stored AppInfo.
"""

import importlib
import importlib.util
import logging
import os
import sys
import types

from pynenc.app import Pynenc
from pynenc.app_info import AppInfo

logger = logging.getLogger(__name__)

APP_FORMAT_HELP = (
    "The --app value must be a dotted path to the module containing your "
    "Pynenc() instance.\n"
    "\n"
    "Examples:\n"
    "  pynenc --app tasks.app runner start       # loads tasks.py, finds Pynenc instance\n"
    "  pynenc --app mypackage.tasks runner start  # imports mypackage.tasks\n"
    "  pynenc --app path/to/tasks.py runner start # loads file directly\n"
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _validate_app_spec(app_spec: str) -> None:
    """
    Reject common format mistakes with actionable error messages.

    :param str app_spec: The raw ``--app`` value from the CLI.
    :raises ValueError: If the format is invalid.
    """
    if ":" in app_spec:
        module_part, var_name = app_spec.split(":", 1)
        raise ValueError(
            f"Invalid --app format '{app_spec}'. "
            f"Use dot notation instead: '--app {module_part}.{var_name}'.\n\n"
            + APP_FORMAT_HELP
        )


def _is_file_path(app_spec: str) -> bool:
    """Check whether the spec looks like a filesystem path rather than a module name."""
    return os.sep in app_spec or app_spec.endswith(".py")


def _find_pynenc_in_module(module: types.ModuleType) -> Pynenc:
    """
    Scan a loaded module for a ``Pynenc`` instance.

    :param types.ModuleType module: The module to scan.
    :return: The first ``Pynenc`` instance found.
    :raises ValueError: If no instance is found.
    """
    for name in dir(module):
        obj = getattr(module, name)
        if isinstance(obj, Pynenc):
            return obj

    module_name = getattr(module, "__name__", "unknown")
    raise ValueError(
        f"No Pynenc() instance found in module '{module_name}'.\n"
        f"Make sure the file defines a variable like:  app = Pynenc()\n\n"
        + APP_FORMAT_HELP
    )


def _load_module_from_file(file_path: str) -> types.ModuleType:
    """
    Load a ``.py`` file as a module and register it in ``sys.modules``.

    The file's directory is added to ``sys.path`` so child processes
    (e.g. ``multiprocessing.spawn``) can re-import the module by name.

    :param str file_path: Absolute or relative path to the ``.py`` file.
    :return: The loaded module.
    :raises ValueError: If the file does not exist or the spec cannot be created.
    :raises ModuleNotFoundError: If the file has unresolvable imports.
    """
    file_path = os.path.abspath(file_path)
    if not os.path.isfile(file_path):
        raise ValueError(f"File not found: {file_path}")

    module_name = os.path.splitext(os.path.basename(file_path))[0]
    module_dir = os.path.dirname(file_path)

    if module_dir not in sys.path:
        sys.path.insert(0, module_dir)

    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not create module spec for '{file_path}'.")

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            f"Failed to load '{file_path}': {exc}.\n"
            f"Ensure all imports in the file are installed and available."
        ) from exc

    sys.modules[module_name] = module
    return module


# ---------------------------------------------------------------------------
# Resolution strategies
# ---------------------------------------------------------------------------


def _resolve_file_path(app_spec: str) -> types.ModuleType:
    """
    Resolve an ``--app`` value that is a filesystem path.

    :param str app_spec: A path like ``path/to/tasks.py`` or ``path/to/tasks``.
    :return: The loaded module.
    """
    path = app_spec if app_spec.endswith(".py") else f"{app_spec}.py"
    return _load_module_from_file(path)


def _resolve_dotted_path(app_spec: str) -> types.ModuleType:
    """
    Resolve a dotted ``--app`` value like ``tasks.app`` or ``mypackage.tasks``.

    Strategies (tried in order):

    1. ``importlib.import_module(app_spec)`` — works for installed packages.
    2. Import the parent module (last component treated as attribute name),
       e.g. ``pkg.mod.app`` → import ``pkg.mod``.
    3. Treat the first component as a local ``.py`` file in cwd,
       e.g. ``tasks.app`` → load ``tasks.py``.

    :param str app_spec: The dotted module path.
    :return: The loaded module.
    :raises ValueError: If no strategy succeeds.
    """
    # Strategy 1: full path as module
    try:
        return importlib.import_module(app_spec)
    except ModuleNotFoundError:
        pass

    parts = app_spec.split(".")
    if len(parts) < 2:
        raise ValueError(
            f"Could not import module '{app_spec}'. "
            f"No module with that name is installed and the name has no dot separator.\n\n"
            + APP_FORMAT_HELP
        )

    # Strategy 2: parent module (last component is attribute name)
    parent_module = ".".join(parts[:-1])
    try:
        return importlib.import_module(parent_module)
    except ModuleNotFoundError:
        pass

    # Strategy 3: <file_stem>.py from cwd
    file_stem = parts[0]
    candidate = os.path.join(os.getcwd(), f"{file_stem}.py")
    if os.path.isfile(candidate):
        return _load_module_from_file(candidate)

    raise ValueError(
        f"Could not import '{app_spec}'.\n"
        f"Tried:\n"
        f"  1. importlib.import_module('{app_spec}') -> ModuleNotFoundError\n"
        f"  2. importlib.import_module('{parent_module}') -> ModuleNotFoundError\n"
        f"  3. Loading '{candidate}' -> file not found\n\n" + APP_FORMAT_HELP
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def find_app_instance(app_spec: str | None) -> Pynenc:
    """
    Find and load a ``Pynenc`` application instance.

    Accepted ``--app`` formats:

    - ``tasks.app`` -- loads ``tasks.py`` from the current directory,
      scans for a ``Pynenc()`` instance.
    - ``mypackage.tasks`` -- standard ``importlib.import_module``.
    - ``path/to/tasks.py`` -- loads the file directly.

    :param str | None app_spec: The ``--app`` value from the CLI.
    :return: The ``Pynenc`` application instance.
    :raises ValueError: If the spec is missing, malformed, or has no Pynenc instance.
    """
    if not app_spec:
        raise ValueError(f"No --app value provided.\n\n{APP_FORMAT_HELP}")

    logger.debug("Resolving --app '%s'", app_spec)
    _validate_app_spec(app_spec)

    if _is_file_path(app_spec):
        module = _resolve_file_path(app_spec)
    else:
        module = _resolve_dotted_path(app_spec)

    return _find_pynenc_in_module(module)


def _find_app_in_user_modules(
    app: "Pynenc",
) -> tuple[str, str, str] | None:
    """
    Scan loaded non-pynenc modules for one that holds ``app`` as a top-level variable.

    The ``Pynenc`` class is defined in ``pynenc.app``, so ``app.__module__``
    always returns ``"pynenc.app"`` — not the user module where
    ``app = Pynenc()`` was written.  This helper finds the correct user module
    by identity-checking attributes across all loaded modules.

    :param Pynenc app: The application instance to locate.
    :return: ``(module_name, module_filepath, variable_name)`` or ``None``.
    """
    for mod_name, mod in list(sys.modules.items()):
        if mod_name.startswith("_") or mod_name == "__main__":
            continue
        if mod_name.startswith("pynenc.") or mod_name == "pynenc":
            continue
        if not hasattr(mod, "__file__") or mod.__file__ is None:
            continue
        if variable := _find_app_variable_in_module(mod, app):
            return mod_name, mod.__file__, variable
    return None


def _find_app_variable_in_module(module: types.ModuleType, app: "Pynenc") -> str | None:
    """
    Return the public attribute name in ``module`` whose value is ``app``.

    :param types.ModuleType module: Module to inspect.
    :param Pynenc app: The instance to match by identity.
    :return: Attribute name, or ``None`` if not found.
    """
    try:
        names = dir(module)
    except (ImportError, TypeError):
        return None
    for name in names:
        if name.startswith("_"):
            continue
        try:
            if getattr(module, name) is app:
                return name
        except (AttributeError, TypeError, ImportError):
            continue
        except Exception:
            continue
    return None


def extract_module_info(
    app: "Pynenc",
) -> tuple[str | None, str | None, str | None]:
    """
    Extract module name, filepath, and app variable name from a Pynenc instance.

    Scans loaded user modules to find the one that holds ``app`` as a
    top-level variable.  Falls back to ``app.__module__`` when no user
    module claims it (e.g. during unit tests).

    :param Pynenc app: Pynenc application instance.
    :return: Tuple of (module_name, module_filepath, app_variable_name).
    """
    try:
        if result := _find_app_in_user_modules(app):
            return result

        # Fallback: use app.__module__ (may be "pynenc.app" in tests)
        if app.__module__ != "__main__":
            module = sys.modules.get(app.__module__)
            if module and hasattr(module, "__file__"):
                var_name = _find_app_variable_in_module(module, app)
                return app.__module__, module.__file__, var_name
    except Exception:
        pass

    return None, None, None


def _import_app_from_module(app_info: AppInfo) -> Pynenc | None:
    """
    Try to import the original module and retrieve the app by variable name.

    :param AppInfo app_info: Application metadata with module path and variable.
    :return: The matching ``Pynenc`` instance, or ``None``.
    """
    if app_info.module == "__main__":
        return None
    if not app_info.module_filepath or not app_info.app_variable:
        return None

    try:
        module_dir = os.path.dirname(app_info.module_filepath)
        if module_dir not in sys.path:
            sys.path.insert(0, module_dir)
        module = importlib.import_module(app_info.module)
    except (ImportError, AttributeError) as exc:
        logger.debug("Could not import module %s: %s", app_info.module, exc)
        return None

    app = getattr(module, app_info.app_variable, None)
    if isinstance(app, Pynenc) and app.app_id == app_info.app_id:
        logger.info("Found app %s in module %s", app_info.app_id, app_info.module)
        return app
    return None


def _scan_loaded_modules(app_id: str) -> Pynenc | None:
    """
    Scan already-imported modules for a ``Pynenc`` instance matching ``app_id``.

    :param str app_id: The application ID to match.
    :return: The matching instance, or ``None``.
    """
    for mod_name, mod in list(sys.modules.items()):
        if mod_name.startswith("_"):
            continue
        if app := _find_pynenc_by_id_in_module(mod, mod_name, app_id):
            return app
    return None


def _find_pynenc_by_id_in_module(
    module: types.ModuleType, mod_name: str, app_id: str
) -> Pynenc | None:
    """
    Check a single module for a ``Pynenc`` instance with the given ``app_id``.

    :param types.ModuleType module: The module to inspect.
    :param str mod_name: Module name for logging.
    :param str app_id: The application ID to match.
    :return: The matching instance, or ``None``.
    """
    try:
        attrs = dir(module)
    except (AttributeError, ImportError):
        return None

    for attr_name in attrs:
        if attr_name.startswith("_"):
            continue
        try:
            attr = getattr(module, attr_name)
        except (AttributeError, TypeError, ImportError):
            continue
        except Exception as e:
            logger.debug(
                "Unexpected %s accessing %s.%s — skipping",
                type(e).__name__,
                mod_name,
                attr_name,
                exc_info=True,
            )
            continue
        if isinstance(attr, Pynenc) and attr.app_id == app_id:
            logger.info("Found app %s in module %s", app_id, mod_name)
            return attr
    return None


def create_app_from_info(app_info: AppInfo) -> Pynenc | None:
    """
    Re-hydrate a ``Pynenc`` instance from stored ``AppInfo`` metadata.

    Strategies (tried in order):

    1. Import the original module and retrieve the named variable.
    2. Scan already-imported modules for a matching ``app_id``.

    :param AppInfo app_info: Application metadata.
    :return: The re-hydrated instance, or ``None`` if not found.
    """
    if app := _import_app_from_module(app_info):
        return app
    if app_info.module != "__main__":
        try:
            return _scan_loaded_modules(app_info.app_id)
        except (ImportError, AttributeError, TypeError):
            logger.debug("Module scan failed for %s", app_info.app_id, exc_info=True)
        except Exception:
            logger.warning(
                "Unexpected error scanning modules for %s",
                app_info.app_id,
                exc_info=True,
            )
    return None
