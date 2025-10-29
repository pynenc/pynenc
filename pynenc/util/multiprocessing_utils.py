"""
Multiprocessing utilities and configuration for Pynenc runners.

This module ensures consistent multiprocessing behavior across platforms,
particularly for macOS which requires the 'spawn' start method to avoid
fork-related issues with certain libraries and debuggers.

Key components:
- configure_multiprocessing: Sets up spawn method for cross-platform compatibility
- ensure_safe_multiprocessing: Validates that multiprocessing can be safely used
"""

import multiprocessing
import threading
import warnings


def configure_multiprocessing() -> None:
    """
    Configure multiprocessing to use spawn method on all platforms.

    This prevents issues with:
    - macOS fork safety problems
    - VSCode debugger compatibility
    - Manager() connection issues

    Should be called once at module import, idempotent if called multiple times.
    Designed to work even if client code doesn't use `if __name__ == '__main__':` guard.
    """
    if multiprocessing.get_start_method(allow_none=True) != "spawn":
        try:
            multiprocessing.set_start_method("spawn", force=True)
        except RuntimeError:
            # Already set in a parent process, which is fine
            pass


def ensure_safe_multiprocessing() -> None:
    """
    Validate that multiprocessing can be safely used in the current environment.

    Warns if code is running in a non-main thread, which could indicate
    improper usage of multiprocessing (e.g., missing `if __name__ == '__main__':` guard).

    This should be called from runner initialization to detect unsafe multiprocessing
    conditions early, before Manager() or Process spawning occurs.
    """
    if threading.current_thread() is not threading.main_thread():
        warnings.warn(
            "Multiprocessing runner initialized in a non-main thread. "
            "This may indicate missing `if __name__ == '__main__':` guard in your main module. "
            "Consider wrapping your application startup code in a main guard to avoid "
            "recursive spawning and connection issues.",
            RuntimeWarning,
            stacklevel=3,
        )


# Configure at module import
configure_multiprocessing()
